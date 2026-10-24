/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: stagea_test.go
Description: End-to-end Stage A tests (ADR-016): session GUCs, the FROM-less
SELECT a client's startup probe uses, multi-statement simple queries, the empty
query response, and the StartupMessage parameter capture. Each test drives the
whole round trip over the socket, because the behaviours are only observable at
the wire seam between translation, execution and the connection.
*/
package sql

import (
	"testing"
)

// showRow runs a SHOW, checks the tag is SHOW, and returns the single row's
// cells. SHOW reports the command tag SHOW, not SELECT n, so it cannot use the
// selectRow helper.
func showRow(t *testing.T, cl *tclient, q string) [][]byte {
	t.Helper()
	frames := cl.query(q)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("%s: unexpected error %s %s", q, code, msg)
	}
	if tag := findTag(frames); tag != "SHOW" {
		t.Fatalf("%s: tag = %q, want SHOW", q, tag)
	}
	for _, f := range frames {
		if f.typ != msgDataRow {
			continue
		}
		row, err := parseDataRow(f.val)
		if err != nil {
			t.Fatalf("%s: parseDataRow: %v", q, err)
		}
		return row
	}
	t.Fatalf("%s: no DataRow", q)
	return nil
}

// TestSelectExprConstant pins the simplest startup probe: SELECT 1 returns one
// row, one column, the text "1", and the tag carries the row count.
func TestSelectExprConstant(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	row := selectRow(t, cl, `SELECT 1`)
	if len(row) != 1 || string(row[0]) != "1" {
		t.Fatalf("SELECT 1 = %q, want [1]", row)
	}

	// A string literal keeps its text and needs no table.
	srow := selectRow(t, cl, `SELECT 'hello'`)
	if len(srow) != 1 || string(srow[0]) != "hello" {
		t.Fatalf("SELECT 'hello' = %q, want [hello]", srow)
	}

	// Arithmetic over literals is evaluated at translation.
	arow := selectRow(t, cl, `SELECT 2 + 3`)
	if len(arow) != 1 || string(arow[0]) != "5" {
		t.Fatalf("SELECT 2 + 3 = %q, want [5]", arow)
	}
}

// TestSelectExprFunctions checks version() and current_database() answer from
// session state without a catalog, and that a probe can project several columns
// in one statement.
func TestSelectExprFunctions(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	vrow := selectRow(t, cl, `SELECT version()`)
	if len(vrow) != 1 || len(vrow[0]) == 0 {
		t.Fatalf("SELECT version() = %q, want a non-empty version string", vrow)
	}

	drow := selectRow(t, cl, `SELECT current_database()`)
	if len(drow) != 1 || string(drow[0]) != DefaultDB {
		t.Fatalf("SELECT current_database() = %q, want [%s]", drow, DefaultDB)
	}

	// Two expressions in one target list produce two columns in one row.
	multi := selectRow(t, cl, `SELECT 1, 'x'`)
	if len(multi) != 2 || string(multi[0]) != "1" || string(multi[1]) != "x" {
		t.Fatalf("SELECT 1, 'x' = %q, want [1 x]", multi)
	}
}

// TestSelectExprCurrentTimestamp checks the value-function form answers with a
// timestamp and does not need a FROM.
func TestSelectExprCurrentTimestamp(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	row := selectRow(t, cl, `SELECT current_timestamp`)
	if len(row) != 1 || len(row[0]) == 0 {
		t.Fatalf("SELECT current_timestamp = %q, want a timestamp", row)
	}
}

// TestSetShowRoundTrip drives SET then SHOW for a known variable, a RESET back
// to the default, and the accept-and-ignore rule for an unknown name.
func TestSetShowRoundTrip(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	// The default before any SET.
	def := showRow(t, cl, `SHOW application_name`)
	if len(def) != 1 || string(def[0]) != "" {
		t.Fatalf("SHOW application_name before SET = %q, want empty", def)
	}

	queryOK(t, cl, `SET application_name = 'tellstone-test'`, "SET")

	got := showRow(t, cl, `SHOW application_name`)
	if len(got) != 1 || string(got[0]) != "tellstone-test" {
		t.Fatalf("SHOW application_name = %q, want [tellstone-test]", got)
	}

	// A known variable with a default the client has not changed.
	tz := showRow(t, cl, `SHOW timezone`)
	if len(tz) != 1 || string(tz[0]) != "UTC" {
		t.Fatalf("SHOW timezone = %q, want [UTC]", tz)
	}

	// RESET restores the default.
	queryOK(t, cl, `RESET application_name`, "SET")
	afterReset := showRow(t, cl, `SHOW application_name`)
	if len(afterReset) != 1 || string(afterReset[0]) != "" {
		t.Fatalf("SHOW application_name after RESET = %q, want empty", afterReset)
	}

	// An unknown name is accepted and ignored: the SET succeeds, and SHOW of it
	// keeps reporting empty rather than the value that was set.
	queryOK(t, cl, `SET some_unknown_guc = 'x'`, "SET")
	unknown := showRow(t, cl, `SHOW some_unknown_guc`)
	if len(unknown) != 1 || string(unknown[0]) != "" {
		t.Fatalf("SHOW some_unknown_guc = %q, want empty (accepted and ignored)", unknown)
	}
}

// TestSetLocalScopedToTransaction checks SET LOCAL is discarded at block end,
// so the session value shows through again, and that it warns outside a block.
func TestSetLocalScopedToTransaction(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `SET application_name = 'session'`, "SET")

	queryOK(t, cl, `BEGIN`, "BEGIN")
	queryOK(t, cl, `SET LOCAL application_name = 'local'`, "SET")

	// Inside the block the LOCAL value wins.
	inTxn := showRow(t, cl, `SHOW application_name`)
	if len(inTxn) != 1 || string(inTxn[0]) != "local" {
		t.Fatalf("SHOW inside block = %q, want [local]", inTxn)
	}

	queryOK(t, cl, `COMMIT`, "COMMIT")

	// After the block the session value is visible again.
	afterTxn := showRow(t, cl, `SHOW application_name`)
	if len(afterTxn) != 1 || string(afterTxn[0]) != "session" {
		t.Fatalf("SHOW after COMMIT = %q, want [session]", afterTxn)
	}

	// SET LOCAL outside a block changes nothing and warns.
	frames := cl.query(`SET LOCAL application_name = 'nope'`)
	if findFrame(frames, msgNotice) == nil {
		t.Fatal("SET LOCAL outside a block: expected a WARNING notice")
	}
	afterWarn := showRow(t, cl, `SHOW application_name`)
	if len(afterWarn) != 1 || string(afterWarn[0]) != "session" {
		t.Fatalf("SHOW after warned SET LOCAL = %q, want [session] (unchanged)", afterWarn)
	}
}

// TestSetEmitsParameterStatus checks a SET of a reportable variable sends a
// ParameterStatus before its CommandComplete, and a no-op SET does not.
func TestSetEmitsParameterStatus(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	frames := cl.query(`SET application_name = 'reported'`)
	ps := findFrame(frames, msgParameterStatus)
	if ps == nil {
		t.Fatal("SET application_name: expected a ParameterStatus")
	}
	name, rest, err := consumeCString(ps.val)
	if err != nil || name != "application_name" {
		t.Fatalf("ParameterStatus name = %q err = %v, want application_name", name, err)
	}
	val, _, _ := consumeCString(rest)
	if val != "reported" {
		t.Fatalf("ParameterStatus value = %q, want reported", val)
	}

	// Setting the same value again changes nothing, so no ParameterStatus.
	again := cl.query(`SET application_name = 'reported'`)
	if findFrame(again, msgParameterStatus) != nil {
		t.Fatal("no-op SET unexpectedly emitted a ParameterStatus")
	}

	// A non-reportable variable never emits one.
	other := cl.query(`SET extra_float_digits = 5`)
	if findFrame(other, msgParameterStatus) != nil {
		t.Fatal("SET extra_float_digits unexpectedly emitted a ParameterStatus")
	}
}

// TestShowAll checks SHOW ALL returns the three PostgreSQL columns and at least
// the known variables.
func TestShowAll(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	frames := cl.query(`SHOW ALL`)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("SHOW ALL: unexpected error %s %s", code, msg)
	}
	var rows int
	for _, f := range frames {
		if f.typ == msgDataRow {
			rows++
		}
	}
	if rows == 0 {
		t.Fatal("SHOW ALL returned no rows")
	}
}

// TestMultiStatementSimpleQuery checks several statements in one Q message each
// get their own CommandComplete, in order, through a single ReadyForQuery.
func TestMultiStatementSimpleQuery(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	frames := cl.query(`SELECT 1; SELECT 2; SELECT 3`)
	if code, msg, ok := findError(frames); ok {
		t.Fatalf("multi-statement: unexpected error %s %s", code, msg)
	}
	var tags []string
	for _, f := range frames {
		if f.typ == msgCommandComplete {
			s, _, _ := consumeCString(f.val)
			tags = append(tags, s)
		}
	}
	if len(tags) != 3 || tags[0] != "SELECT 1" || tags[1] != "SELECT 1" || tags[2] != "SELECT 1" {
		t.Fatalf("multi-statement tags = %q, want three SELECT 1", tags)
	}
}

// TestEmptyQueryResponse checks a blank and a semicolon-only query are answered
// with EmptyQueryResponse and no CommandComplete.
func TestEmptyQueryResponse(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	for _, q := range []string{"", "   ", ";"} {
		frames := cl.query(q)
		if findFrame(frames, msgEmptyQuery) == nil {
			t.Fatalf("query %q: expected EmptyQueryResponse, got none", q)
		}
		if findFrame(frames, msgCommandComplete) != nil {
			t.Fatalf("query %q: EmptyQueryResponse must not be followed by CommandComplete", q)
		}
	}
}

// TestStartupParamSeedsSession checks a parameter sent in the StartupMessage is
// visible to SHOW before any SET.
func TestStartupParamSeedsSession(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())

	cl.sendStartup(startupBody(protocolVersionNumber,
		"user", "default", "database", "test",
		"application_name", "from-startup",
		"client_encoding", "UTF8"))
	cl.expectLine(msgAuthentication)
	for i := 0; i < 4; i++ {
		cl.expectLine(msgParameterStatus)
	}
	cl.expectLine(msgBackendKeyData)
	cl.expectLine(msgReadyForQuery)

	got := showRow(t, cl, `SHOW application_name`)
	if len(got) != 1 || string(got[0]) != "from-startup" {
		t.Fatalf("SHOW application_name from startup = %q, want [from-startup]", got)
	}
}

// TestTranslateStageAUnits exercises translation of the new statement classes
// directly, so a shape that must be refused is pinned without a socket.
func TestTranslateStageAUnits(t *testing.T) {
	// SET forms.
	for _, q := range []string{
		`SET application_name = 'x'`,
		`SET SESSION application_name = 'x'`,
		`SET LOCAL application_name = 'x'`,
		`SET TIME ZONE 'UTC'`,
		`RESET application_name`,
		`RESET ALL`,
		`SET extra_float_digits = 3`,
		`SET search_path TO public`,
	} {
		p, err := Translate(q)
		if err != nil {
			t.Fatalf("Translate(%q): %v", q, err)
		}
		if p.Kind != StmtSet {
			t.Fatalf("Translate(%q): kind = %v, want StmtSet", q, p.Kind)
		}
	}

	// SET LOCAL marks the plan local.
	loc, err := Translate(`SET LOCAL application_name = 'x'`)
	if err != nil || loc.GUC == nil || !loc.GUC.IsLocal {
		t.Fatalf("SET LOCAL: plan = %+v err = %v, want IsLocal", loc, err)
	}
	// RESET ALL carries no name.
	ra, err := Translate(`RESET ALL`)
	if err != nil || ra.GUC == nil || !ra.GUC.IsReset || ra.GUC.Name != "" {
		t.Fatalf("RESET ALL: plan = %+v err = %v", ra, err)
	}

	// SHOW forms.
	sh, err := Translate(`SHOW application_name`)
	if err != nil || sh.Kind != StmtShow || len(sh.Cols) != 1 {
		t.Fatalf("SHOW application_name: plan = %+v err = %v", sh, err)
	}
	shAll, err := Translate(`SHOW ALL`)
	if err != nil || len(shAll.Cols) != 3 {
		t.Fatalf("SHOW ALL: plan = %+v err = %v", shAll, err)
	}

	// FROM-less SELECT forms.
	for _, q := range []string{
		`SELECT 1`,
		`SELECT 'x'`,
		`SELECT 1.5`,
		`SELECT true`,
		`SELECT NULL`,
		`SELECT version()`,
		`SELECT current_database()`,
		`SELECT current_timestamp`,
		`SELECT 2 + 3`,
		`SELECT 10 / 4`,
		`SELECT 1, 'two', version()`,
	} {
		p, err := Translate(q)
		if err != nil {
			t.Fatalf("Translate(%q): %v", q, err)
		}
		if p.Kind != StmtSelectExpr {
			t.Fatalf("Translate(%q): kind = %v, want StmtSelectExpr", q, p.Kind)
		}
	}

	// FROM-less SELECT with a relation-requiring clause is refused.
	for _, q := range []string{
		`SELECT 1 WHERE 1 = 1`,
		`SELECT 1 ORDER BY 1`,
		`SELECT count(*)`,
		`SELECT nosuchfunc()`,
	} {
		if _, err := Translate(q); err == nil {
			t.Fatalf("Translate(%q) succeeded, want error", q)
		}
	}

	// Multi-statement parsing and the empty case.
	plans, err := TranslateAll(`SELECT 1; SELECT 2`)
	if err != nil || len(plans) != 2 {
		t.Fatalf("TranslateAll two statements: plans = %d err = %v, want 2", len(plans), err)
	}
	if empty, err := TranslateAll(`  ;  `); err != nil || len(empty) != 0 {
		t.Fatalf("TranslateAll semicolon-only: plans = %d err = %v, want 0", len(empty), err)
	}
}

// TestSetLocalRevertedOnRollback checks a LOCAL value is discarded on ROLLBACK
// as it is on COMMIT.
func TestSetLocalRevertedOnRollback(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")

	queryOK(t, cl, `SET application_name = 'base'`, "SET")
	queryOK(t, cl, `BEGIN`, "BEGIN")
	queryOK(t, cl, `SET LOCAL application_name = 'temp'`, "SET")
	queryOK(t, cl, `ROLLBACK`, "ROLLBACK")

	got := showRow(t, cl, `SHOW application_name`)
	if len(got) != 1 || string(got[0]) != "base" {
		t.Fatalf("SHOW after ROLLBACK = %q, want [base]", got)
	}
}
