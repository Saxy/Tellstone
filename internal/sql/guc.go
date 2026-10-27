/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: guc.go
Description: Session GUC state -- the SET/SHOW variables a client configures on
its connection. The state lives on pgConn (ADR-016 decision 6). SET of a known
name is recorded so SHOW can return it; SET of any other name is accepted and
ignored, because this engine has no settings catalog and a client that sets an
unrecognised name must still see the statement succeed. SET LOCAL is scoped to
the current transaction block and reverted at COMMIT/ROLLBACK.
*/
package sql

import "strings"

// gucDefaults are the values SHOW returns for a known variable the session has
// not changed. They mirror the ParameterStatus values sent at connect, plus a
// few names introspection reads. An unknown name SHOWs an empty string, which
// is PostgreSQL's answer for a validly-formatted but unset variable.
var gucDefaults = map[string]string{
	"server_version":              "14.10",
	"server_version_num":          "140010",
	"client_encoding":             "UTF8",
	"standard_conforming_strings": "on",
	"integer_datetimes":           "on",
	"search_path":                 `"$user", public`,
	"application_name":            "",
	"datestyle":                   "ISO, MDY",
	"timezone":                    "UTC",
	"extra_float_digits":          "1",
	"transaction_isolation":       "read committed",
	"transaction_read_only":       "off",
	"is_superuser":                "off",
	"session_authorization":       "default",
	"max_identifier_length":       "63",
	"server_encoding":             "UTF8",
}

// gucName canonicalizes a variable name the way PostgreSQL compares GUCs:
// case-insensitively, so SHOW timezone and SHOW TimeZone are the same variable.
func gucName(name string) string { return strings.ToLower(name) }

// reportableGUCs maps a canonical GUC name onto the parameter name PostgreSQL
// sends in a ParameterStatus when the variable changes. Only these GUCs are
// reported over the wire; a SET of any other name changes only session state.
var reportableGUCs = map[string]string{
	"application_name":            "application_name",
	"client_encoding":             "client_encoding",
	"datestyle":                   "DateStyle",
	"timezone":                    "TimeZone",
	"standard_conforming_strings": "standard_conforming_strings",
	"integer_datetimes":           "integer_datetimes",
	"server_version":              "server_version",
	"server_encoding":             "server_encoding",
	"is_superuser":                "is_superuser",
	"session_authorization":       "session_authorization",
}

// seedStartupGUCs records the GUCs a client sent in its StartupMessage. Only
// names this engine models are stored; the rest (user, database, options) are
// handled elsewhere or carry no session-valued meaning. The keys arrive with
// PostgreSQL's own casing (DateStyle, TimeZone), which gucName folds to the
// canonical lowercase form.
func (c *pgConn) seedStartupGUCs(params map[string]string) {
	for key, value := range params {
		name := gucName(key)
		switch name {
		case "application_name", "client_encoding", "datestyle", "timezone",
			"extra_float_digits", "standard_conforming_strings", "search_path":
			if value != "" {
				c.gucs[name] = value
			}
		}
	}
}

// applySet records a SET on the connection. A known name is stored; an unknown
// name is accepted and ignored, matching how PostgreSQL treats an unrecognised
// custom placeholder. A SET LOCAL is stored in the transaction-local layer,
// which is discarded when the block ends; after the block the session value
// shows through again. A reset restores the default by deleting the override.
func (c *pgConn) applySet(g *gucPlan) {
	if len(g.Multi) > 0 {
		// A multi-variable SET (SET SESSION CHARACTERISTICS ...) assigns each
		// pair exactly as a single-variable SET would.
		for i := range g.Multi {
			c.applySet(&gucPlan{
				Name:     g.Multi[i].Name,
				Value:    g.Multi[i].Value,
				HasValue: true,
				IsLocal:  g.IsLocal,
			})
		}
		return
	}
	if g.IsReset {
		if g.Name == "" { // RESET ALL
			c.gucs = map[string]string{}
			if c.txn != nil {
				c.txn.localGUCs = map[string]string{}
			}
			return
		}
		name := gucName(g.Name)
		delete(c.gucs, name)
		if c.txn != nil {
			delete(c.txn.localGUCs, name)
		}
		return
	}
	// SET LOCAL outside a transaction block is a no-op that PostgreSQL reports
	// with a warning; the warning is emitted by execSet, which has the wire
	// layer. Here the value is simply not stored anywhere.
	if g.IsLocal && c.txn == nil {
		return
	}
	name := gucName(g.Name)
	if _, known := gucDefaults[name]; !known {
		// Unknown name: accepted and ignored. It is not stored, so SHOW of it
		// keeps reporting the empty default rather than a value that would
		// imply the engine understood it.
		return
	}
	if g.IsLocal {
		c.txn.localGUCs[name] = g.Value
		return
	}
	if c.gucs == nil {
		c.gucs = map[string]string{}
	}
	c.gucs[name] = g.Value
}

// showValue returns the value SHOW reports for a variable: the transaction-local
// override, then the session override, then the default, then empty.
func (c *pgConn) showValue(name string) string {
	name = gucName(name)
	if c.txn != nil {
		if v, ok := c.txn.localGUCs[name]; ok {
			return v
		}
	}
	if v, ok := c.gucs[name]; ok {
		return v
	}
	return gucDefaults[name]
}

// execSet applies a SET/RESET. It touches no store and cannot fail: every
// variable name is accept-and-ignore, which is what a startup sequence needs.
// A SET LOCAL outside a transaction block changes nothing and reports the
// PostgreSQL warning (SQLSTATE 25P01) through the outcome's notices.
func (s *Server) execSet(c *pgConn, plan *Plan) (*execOutcome, error) {
	out := &execOutcome{tag: "SET"}
	if plan.GUC.IsLocal && c.txn == nil {
		out.notices = append(out.notices, noticeInfo{
			code: errNoActiveSQLTransaction,
			msg:  "SET LOCAL can only be used in transaction blocks",
		})
	}
	// Capture the reportable variables' values before and after, so a SET that
	// actually changed a reported setting sends a ParameterStatus while one
	// that set the value it already had does not (matching PostgreSQL, which
	// only reports an actual change).
	before := c.reportableSnapshot()
	c.applySet(plan.GUC)
	out.paramStatus = c.changedReportable(before)
	return out, nil
}

// reportableSnapshot records the current value of every GUC PostgreSQL reports
// over the wire, so a SET can tell which of them changed.
func (c *pgConn) reportableSnapshot() map[string]string {
	snap := make(map[string]string, len(reportableGUCs))
	for name := range reportableGUCs {
		snap[name] = c.showValue(name)
	}
	return snap
}

// changedReportable returns the ParameterStatus pairs for every reportable GUC
// whose value differs from the snapshot, in a stable order.
func (c *pgConn) changedReportable(before map[string]string) [][2]string {
	var out [][2]string
	for name, pname := range reportableGUCs {
		if now := c.showValue(name); now != before[name] {
			out = append(out, [2]string{pname, now})
		}
	}
	sortPairs(out)
	return out
}

// sortPairs orders ParameterStatus pairs by parameter name so a multi-variable
// change is emitted deterministically.
func sortPairs(p [][2]string) {
	for i := 1; i < len(p); i++ {
		for j := i; j > 0 && p[j][0] < p[j-1][0]; j-- {
			p[j], p[j-1] = p[j-1], p[j]
		}
	}
}

// execShow renders SHOW <name> and SHOW ALL as a one-column result set. The
// result column is named after the variable for SHOW <name>, matching
// PostgreSQL, and the rows are ordered by name for SHOW ALL.
func (s *Server) execShow(c *pgConn, plan *Plan) (*execOutcome, error) {
	g := plan.GUC
	if g.Name == "" { // SHOW ALL
		names := make([]string, 0, len(gucDefaults))
		for name := range gucDefaults {
			names = append(names, name)
		}
		sortStrings(names)
		rows := make([][][]byte, 0, len(names))
		for _, name := range names {
			rows = append(rows, [][]byte{[]byte(name), []byte(c.showValue(name)), []byte("")})
		}
		return &execOutcome{tag: "SHOW", rows: rows, selectRows: true}, nil
	}
	val := c.showValue(g.Name)
	return &execOutcome{tag: "SHOW", rows: [][][]byte{{[]byte(val)}}, selectRows: true}, nil
}

// sortStrings is a tiny insertion sort so guc.go needs no import beyond strings
// for a list that is never more than a couple of dozen entries.
func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}
