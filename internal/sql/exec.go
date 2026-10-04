/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: exec.go
Description: Runs a translated plan against the shared store and emits the
CommandComplete tag a data statement reports. This is where RBAC is enforced
per statement, where conditional writes keep INSERT and UPDATE atomic against
concurrent clients, and where transaction blocks, NULL handling and the audit
trail are decided.
*/
package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/keyspace"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

// execOutcome is the materialized result of one statement execution.
type execOutcome struct {
	tag        string
	row        [][]byte // SELECT: column cells (wire-encoded, nil = NULL); nil = empty result
	selectRows bool
}

// isWrite reports whether a statement mutates data. Transaction statements are
// excluded: they carry no key and touch no data. DDL is excluded too, and
// handled separately: it writes a catalog key rather than row data, so it is
// governed by the catalog's own rules rather than by the row transaction
// restriction.
func (k StmtKind) isWrite() bool {
	switch k {
	case StmtInsert, StmtUpdate, StmtDelete:
		return true
	default:
		return false
	}
}

// isDDL reports whether a statement changes the schema.
func (k StmtKind) isDDL() bool {
	return k == StmtCreateTable || k == StmtDropTable
}

// command maps a statement onto the RBAC command it needs, plus the lowercase
// token the binary frontend records in ACL LOG and the audit trail.
func (k StmtKind) command() (uint16, string) {
	switch k {
	case StmtInsert, StmtUpdate:
		return rbac.CmdSet, "set"
	case StmtDelete:
		return rbac.CmdDel, "del"
	default:
		return rbac.CmdGet, "get"
	}
}

// authKey is the key an RBAC prefix rule is matched against.
//
// A prefix grant names physical keyspace, so the decision has to be made against
// the namespace the statement will touch. The plan's key cannot be used for that:
// on a catalog table it holds nothing but a row id, so comparing a bare "1"
// against a grant of "tellstone/users/" never matches and every table-scoped
// grant would deny the very table it names. It is also absent on INSERT, whose
// row id arrives among the column values.
//
// The table's own key prefix is used instead. It needs no schema and no bound
// row, so it is available before the catalog is read -- which is what keeps the
// authorization ahead of resolvePlan, where an unauthorized session learns
// nothing about which tables exist. It covers every row of the table, so a grant
// cannot be satisfied by naming one row id, and a prefix wide enough for one
// table cannot reach into another.
func authKey(plan *Plan, params []paramVal) ([]byte, error) {
	if plan.Table == tableName {
		// The implicit table stores its keys verbatim under their own names, so
		// the plan's key is already the physical key and the prefix rules were
		// written against it. A Describe arrives before any Bind, so a
		// parameterized statement has no value to resolve here; the prefix check
		// is deferred to execution, where it has one. Only the implicit table
		// reaches this branch -- a catalog table is decided by its prefix below
		// and needs neither the schema nor the bound row.
		if plan.Key.Param > 0 && len(params) == 0 {
			return []byte(tableName + "/"), nil
		}
		return plan.Key.resolve(params)
	}
	return []byte(TablePrefix(DefaultDB, plan.Table)), nil
}

// authorize applies the session's command bit and key prefixes to a plan and
// records the attempt. It is a no-op for a session with no authentication.
//
// It is deliberately separate from execute because the extended protocol needs
// it earlier: Describe of a prepared statement has to report parameter and result
// types, which means reading the table's schema, and that must not happen for a
// statement the session is not allowed to run.
func (s *Server) authorize(c *pgConn, plan *Plan, params []paramVal) error {
	if c.auth == nil || c.auth.session == nil {
		return nil
	}
	key, err := authKey(plan, params)
	if err != nil {
		return err
	}
	cmd, name := plan.Kind.command()
	if !c.auth.session.IsAllowed(cmd, key) {
		if s.policy != nil {
			s.policy.LogDenied(c.user, c.remoteAddr, name, string(key))
		}
		s.auditDenied(c, name, key)
		return &pgError{code: errInsufficientPrivilege, msg: "permission denied for table " + plan.Table}
	}
	c.auth.session.CountCommand()
	s.auditCommand(c, name, key)
	return nil
}

// execute runs a translated plan against the shared store with the bound
// parameters. RBAC enforcement mirrors the binary frontend: the session's
// command bit and key prefixes are consulted on every data statement.
func (s *Server) execute(c *pgConn, plan *Plan, params []paramVal) (*execOutcome, error) {
	switch plan.Kind {
	case StmtBegin:
		c.inTxn = true
		return &execOutcome{tag: "BEGIN"}, nil
	case StmtCommit:
		c.inTxn = false
		return &execOutcome{tag: "COMMIT"}, nil
	case StmtRollback:
		c.inTxn = false
		return &execOutcome{tag: "ROLLBACK"}, nil
	}

	// DDL is dispatched before the RBAC and transaction machinery below, both
	// of which are row-oriented: the RBAC check needs a key predicate, and the
	// transaction restriction exists because a row write would already be
	// durable when ROLLBACK ran. A catalog write has neither shape.
	if plan.Kind.isDDL() {
		return s.execDDL(c, plan)
	}

	// The store exposes single-key operations with no enclosing transaction,
	// so a write issued inside a transaction block would already be durable by
	// the time ROLLBACK ran. Refusing the write is the only way to keep the
	// block's promise: a ROLLBACK then genuinely has nothing to undo.
	if c.inTxn && plan.Kind.isWrite() {
		return nil, &pgError{
			code: errFeatureNotSupported,
			msg:  "data statements are not supported inside a transaction block",
		}
	}

	if err := s.authorize(c, plan, params); err != nil {
		return nil, err
	}

	// The table is resolved here, after the RBAC decision above, so a session
	// without permission learns nothing about which tables exist.
	if err := s.resolvePlan(plan); err != nil {
		return nil, err
	}

	switch plan.Kind {
	case StmtExplain:
		return s.execExplain(plan)
	case StmtAnalyze:
		return s.execAnalyze(plan)
	case StmtSelect:
		return s.execSelect(c, plan, params)
	case StmtInsert:
		return s.execInsert(plan, params)
	case StmtUpdate:
		return s.execUpdate(plan, params)
	case StmtDelete:
		return s.execDelete(plan, params)
	default:
		return nil, &pgError{code: errFeatureNotSupported, msg: "unhandled statement kind"}
	}
}

// auditCommand records one authorized statement with the same field set and
// event the binary frontend uses, so both protocols land in one audit trail.
func (s *Server) auditCommand(c *pgConn, command string, key []byte) {
	if s.audit == nil {
		return
	}
	s.audit.Record(audit.EventCommand, "command dispatched",
		log.String("command", command),
		log.String("key", string(key)),
		log.String("user", c.user),
		log.String("remote_addr", c.remoteAddr),
		log.String("protocol", "sql"),
	)
}

// auditDenied records one RBAC rejection, mirroring Ctx.deny in the command
// layer so a denial is visible in the audit trail whichever protocol issued it.
func (s *Server) auditDenied(c *pgConn, command string, key []byte) {
	if s.audit == nil {
		return
	}
	s.audit.Record(audit.EventACLDeny, "command denied by rbac policy",
		log.String("user", c.user),
		log.String("command", command),
		log.String("key", string(key)),
		log.String("remote_addr", c.remoteAddr),
		log.String("protocol", "sql"),
	)
}

func (s *Server) execSelect(c *pgConn, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execSelectRow(plan, params)
	}
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	val, found, serr := s.store.GetErr(string(key))
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if !found {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	row := make([][]byte, len(plan.Cols))
	for i, col := range plan.Cols {
		switch col {
		case colKey:
			row[i] = key
		case colValue:
			row[i] = encodeByteaText(val)
		}
	}
	return &execOutcome{tag: "SELECT 1", row: row, selectRows: true}, nil
}

func (s *Server) execInsert(plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execInsertRow(plan, params)
	}
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	val, err := plan.Val.resolveValue(params)
	if err != nil {
		return nil, err
	}
	switch plan.Conflict {
	case ConflictDoUpdate:
		// ON CONFLICT DO UPDATE SET value = excluded.value replaces the whole
		// proposed row, so an unconditional write is exactly the semantics:
		// it creates the row when absent and overwrites it when present.
		if serr := s.store.Set(string(key), val, 0); serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	case ConflictDoNothing:
		// The existing row must survive, so the write may only create. A
		// conflicting key is a successful no-op, reported as zero rows.
		applied, serr := s.store.SetIfAbsent(string(key), val, 0)
		if serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		if !applied {
			return &execOutcome{tag: "INSERT 0 0"}, nil
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	default:
		// Real primary-key semantics: a plain INSERT creates the row and must
		// not overwrite one that already exists. The create-if-absent
		// precondition and the write are one atomic store operation, so two
		// concurrent inserts of the same key cannot both report success.
		applied, serr := s.store.SetIfAbsent(string(key), val, 0)
		if serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		if !applied {
			return nil, &pgError{code: errDuplicateKey, msg: `duplicate key value violates unique constraint "tellstone"`}
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	}
}

func (s *Server) execUpdate(plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execUpdateRow(plan, params)
	}
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	val, err := plan.Val.resolveValue(params)
	if err != nil {
		return nil, err
	}
	// UPDATE may only modify a row that exists. Folding the existence check into
	// the write keeps the affected-row count honest: a row deleted
	// concurrently reports zero rows instead of being silently resurrected.
	applied, serr := s.store.SetIfPresent(string(key), val, 0)
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if !applied {
		return &execOutcome{tag: "UPDATE 0"}, nil
	}
	return &execOutcome{tag: "UPDATE 1"}, nil
}

func (s *Server) execDelete(plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execDeleteRow(plan, params)
	}
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	ok, serr := s.store.Delete(string(key))
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if ok {
		return &execOutcome{tag: "DELETE 1"}, nil
	}
	return &execOutcome{tag: "DELETE 0"}, nil
}

// resolve binds a parameter or returns the literal. key-column references
// reject nulls the way a NOT NULL primary key does.
func (v ValRef) resolve(params []paramVal) ([]byte, error) {
	if v.Param > 0 {
		i := v.Param - 1
		if i >= len(params) {
			return nil, &pgError{
				code: errProtocolViolation,
				msg:  fmt.Sprintf("bind message supplies %d parameters but prepared statement requires %d", len(params), v.Param),
			}
		}
		if params[i].null {
			return nil, &pgError{code: errNotNullViolation, msg: fmt.Sprintf("null value in column %q violates not-null constraint", colKey)}
		}
		return params[i].val, nil
	}
	return v.Literal, nil
}

// resolveValue binds the value column, honoring the bytea text encoding of
// extended-protocol text-format parameters and of SQL bytea literals shaped
// "\x...". Binary-format parameters pass through untouched.
func (v ValRef) resolveValue(params []paramVal) ([]byte, error) {
	if v.Param > 0 {
		i := v.Param - 1
		if i >= len(params) {
			return nil, &pgError{
				code: errProtocolViolation,
				msg:  fmt.Sprintf("bind message supplies %d parameters but prepared statement requires %d", len(params), v.Param),
			}
		}
		if params[i].null {
			// The store has no way to represent a NULL distinct from an empty
			// payload: presence is the only signal Get returns. Storing NULL as
			// an empty value would make the row read back as '' instead of NULL,
			// so the column is declared NOT NULL and a NULL is rejected here.
			return nil, &pgError{code: errNotNullViolation, msg: fmt.Sprintf("null value in column %q violates not-null constraint", colValue)}
		}
		if params[i].format == resultFormatText {
			return decodeByteaText(params[i].val)
		}
		return params[i].val, nil
	}
	// SQL literal: decode bytea hex syntax when present, otherwise this is a
	// plain text payload kept verbatim.
	return decodeByteaText(v.Literal)
}

// catalog returns the catalog for the single database this phase serves.
func (s *Server) catalog() *Catalog { return NewCatalog(s.store, DefaultDB) }

// execDDL runs CREATE TABLE and DROP TABLE against the catalog.
//
// DDL is authorized separately from data statements, against the catalog key it
// will write rather than a row key. The key is a child of the database prefix
// the session would already need in order to write any table, so this does not
// widen what a role can do; it stops a session from creating tables by writing
// catalog keys it was never granted.
func (s *Server) execDDL(c *pgConn, plan *Plan) (*execOutcome, error) {
	key := MetaTableKey(DefaultDB, plan.Table)
	if c.auth != nil && c.auth.session != nil {
		if !c.auth.session.IsAllowed(rbac.CmdSet, []byte(key)) {
			s.auditDenied(c, "ddl", []byte(key))
			return nil, &pgError{code: errInsufficientPrivilege, msg: "permission denied to change schema"}
		}
		c.auth.session.CountCommand()
		s.auditCommand(c, "ddl", []byte(key))
	}
	cat := s.catalog()
	switch plan.Kind {
	case StmtCreateTable:
		var created bool
		var err error
		if plan.IfNotExists {
			created, err = cat.CreateIfNotExists(plan.Schema)
			if err != nil {
				return nil, s.schemaError(err)
			}
			if !created {
				// The name is taken and the clause waives the error. Reporting
				// "CREATE TABLE" rather than "CREATE TABLE 0" is what libpq
				// clients expect from a notice-severity result.
				return &execOutcome{tag: "CREATE TABLE"}, nil
			}
			return &execOutcome{tag: "CREATE TABLE"}, nil
		}
		if err := cat.Create(plan.Schema); err != nil {
			return nil, s.schemaError(err)
		}
		return &execOutcome{tag: "CREATE TABLE"}, nil
	case StmtDropTable:
		// The table's statistics are forgotten only once the drop has actually
		// happened. Forgetting first would lose the count of a DROP that then
		// failed -- a populated table, say -- leaving the table in place with no
		// statistics and no way to tell why.
		dropped := false
		if plan.IfExists {
			if _, err := cat.DropIfExists(plan.Table); err != nil {
				return nil, s.schemaError(err)
			}
			dropped = true
		} else {
			if err := cat.Drop(plan.Table); err != nil {
				return nil, s.schemaError(err)
			}
			dropped = true
		}
		if dropped {
			s.stats.Forget(DefaultDB, plan.Table)
		}
		return &execOutcome{tag: "DROP TABLE"}, nil
	default:
		return nil, &pgError{code: errFeatureNotSupported, msg: "unhandled statement kind"}
	}
}

// schemaError maps a catalog failure onto the SQLSTATE a client expects, so a
// duplicate table is a duplicate-object error rather than a generic failure.
func (s *Server) schemaError(err error) error {
	switch {
	case errors.Is(err, ErrTableExists):
		// The name is the subject, not the wrapping error, so the message names
		// the table rather than repeating the catalog's own wording.
		return &pgError{code: errDuplicateTable, msg: "relation already exists: " + err.Error()}
	case errors.Is(err, ErrNoSuchTable):
		return &pgError{code: errUndefinedTable, msg: err.Error()}
	case errors.Is(err, ErrTableNotEmpty):
		// Not a fault and not a conflict: this phase has no way to remove a
		// table's rows as part of dropping it, so it declines rather than
		// leaving keys nothing describes. Deleting the rows first works.
		return &pgError{code: errFeatureNotSupported, msg: err.Error()}
	case errors.Is(err, ErrEmptinessUnverifiable):
		// A cluster that could not answer is a fault, and is reported as one
		// rather than as the feature limitation above, because retrying is the
		// correct client response and deleting rows would not help.
		return &pgError{code: errInternal, msg: err.Error()}
	case errors.Is(err, ErrSchemaCorrupt):
		return &pgError{code: errInternal, msg: err.Error()}
	default:
		return &pgError{code: errIoError, msg: err.Error()}
	}
}

// The row-oriented execution path. These handle a statement whose table has a
// catalog definition, where a row is a key range rather than a single key. The
// implicit tellstone table keeps the single-key path above, unchanged: it
// predates the catalog, its rows are one key, and its RBAC and audit behaviour
// is already covered by tests that must not shift.

// execSelectRow reconstructs a row by range scan and projects it.
func (s *Server) execSelectRow(plan *Plan, params []paramVal) (*execOutcome, error) {
	if err := refuseMultiRow(plan.Phys); err != nil {
		return nil, err
	}
	rowID, err := plan.rowID(params)
	if err != nil {
		return nil, err
	}
	sch := plan.Schema
	cells, found, err := s.readRow(sch, rowID)
	if err != nil {
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	if !found {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	row := make([][]byte, len(plan.Cols))
	for i, name := range plan.Cols {
		idx, ok := sch.columnIndex(name)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", name)}
		}
		cell, err := wireCell(sch.Columns[idx].Type, cells[idx].value)
		if err != nil {
			// The bytes are in the store but do not decode as the column's
			// type, which means the row was written by something other than this
			// write path. Reporting it beats returning a wrong value.
			return nil, &pgError{code: errDataCorrupt, msg: fmt.Sprintf("column %q: %v", name, err)}
		}
		row[i] = cell
	}
	return &execOutcome{tag: "SELECT 1", row: row, selectRows: true}, nil
}

// execInsertRow writes a multi-column row, failing on a duplicate row id.
func (s *Server) execInsertRow(plan *Plan, params []paramVal) (*execOutcome, error) {
	sch := plan.Schema
	rowID, err := plan.rowID(params)
	if err != nil {
		return nil, err
	}
	cells, err := plan.buildCells(params, true)
	if err != nil {
		return nil, err
	}
	// The primary key column takes the row id, whatever the statement listed.
	// Accepting a second, conflicting value for it would let one statement
	// claim two identities.
	//
	// The row id is a string, so it cannot be handed to EncodeValue directly:
	// that would fail for every type but text. buildCells has already encoded
	// the column correctly for every other type, so the right source here is
	// that encoding, re-read through the type rather than assumed.
	pkType := sch.Columns[sch.PrimaryKey].Type
	var pkCell []byte
	switch pkType {
	case TypeInt, TypeBigInt:
		// The row id is hex, not decimal text, so it has to be read back with
		// the row id's own decoder. Parsing it as a decimal number instead
		// would succeed and produce a different integer -- "8000000000000001"
		// is a valid decimal and is the row id of 1 -- so the row would be
		// filed under an id the client never asked for.
		n, err := keyspace.DecodeIntRowID(rowID)
		if err != nil {
			return nil, err
		}
		pkCell, err = EncodeValue(pkType, n)
		if err != nil {
			return nil, err
		}
	default:
		var err error
		if pkCell, err = encodeTextAs(pkType, []byte(rowID)); err != nil {
			return nil, err
		}
	}
	cells[sch.PrimaryKey] = rowValue{value: pkCell, set: true}
	if err := s.insertRow(sch, rowID, cells); err != nil {
		if errors.Is(err, ErrDuplicateRow) {
			return nil, &pgError{
				code: errDuplicateKey,
				msg:  fmt.Sprintf("duplicate key value violates unique constraint %q", sch.PrimaryKeyName()),
			}
		}
		if errors.Is(err, ErrNotNull) {
			// A column the schema declares NOT NULL was left absent. That is a
			// constraint violation the client can fix, so it must not be
			// reported as a store failure.
			return nil, &pgError{code: errNotNullViolation, msg: err.Error()}
		}
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	return &execOutcome{tag: "INSERT 0 1"}, nil
}

// execUpdateRow rewrites the listed columns of an existing row, reporting zero
// affected rows when it is not there.
func (s *Server) execUpdateRow(plan *Plan, params []paramVal) (*execOutcome, error) {
	sch := plan.Schema
	if err := refuseMultiRow(plan.Phys); err != nil {
		return nil, err
	}
	rowID, err := plan.rowID(params)
	if err != nil {
		return nil, err
	}
	ok, err := s.rowExists(sch, rowID)
	if err != nil {
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	if !ok {
		return &execOutcome{tag: "UPDATE 0"}, nil
	}
	cells, err := plan.buildCells(params, false)
	if err != nil {
		return nil, err
	}
	if err := s.updateRow(sch, rowID, cells); err != nil {
		if errors.Is(err, ErrNotNull) {
			return nil, &pgError{code: errNotNullViolation, msg: err.Error()}
		}
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	return &execOutcome{tag: "UPDATE 1"}, nil
}

// execDeleteRow removes a row and every one of its column keys.
func (s *Server) execDeleteRow(plan *Plan, params []paramVal) (*execOutcome, error) {
	if err := refuseMultiRow(plan.Phys); err != nil {
		return nil, err
	}
	rowID, err := plan.rowID(params)
	if err != nil {
		return nil, err
	}
	ok, serr := s.deleteRow(plan.Schema, rowID)
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if ok {
		return &execOutcome{tag: "DELETE 1"}, nil
	}
	return &execOutcome{tag: "DELETE 0"}, nil
}

// rowID resolves the statement's primary key value into the row id used in keys.
//
// On a catalog table the primary key has a declared type, so the text is parsed
// as that type and rendered back to its canonical form. That round trip is what
// makes one value have one identity: `1`, `01` and `+1` name the same row, as
// they do in PostgreSQL, instead of becoming three rows that a range scan would
// then have to merge. It also rejects a value the column's type cannot hold at
// the point it is named, rather than at read time.
//
// The implicit table's key is already text, so it passes through unchanged.
func (p *Plan) rowID(params []paramVal) (string, error) {
	key, err := p.Key.resolve(params)
	if err != nil {
		return "", err
	}
	if len(key) == 0 {
		return "", &pgError{code: errNotNullViolation, msg: "primary key must not be empty"}
	}
	if p.Schema == nil {
		return string(key), nil
	}
	pk := p.Schema.Columns[p.Schema.PrimaryKey]
	if pk.Type == TypeVarchar {
		// Text is already canonical. Parsing it would only risk a rejection of
		// a value the column can hold.
		return string(key), nil
	}
	// An integer row id becomes 16 hex digits of the sign-flipped value rather
	// than a decimal string. The reason is not aesthetics: a range scan needs
	// the row id to sort in value order, and a variable-width decimal string
	// does not ("10" sorts before "9"), while fixed-width hex does. Hex also
	// contains neither '%' nor '/', which the unconditional row-id escape would
	// otherwise rewrite, and the escape is not order-preserving. See
	// keyspace.EncodeIntRowID.
	if pk.Type == TypeInt || pk.Type == TypeBigInt {
		n, err := strconv.ParseInt(strings.TrimSpace(string(key)), 10, 64)
		if err != nil {
			return "", &pgError{code: errSyntax, msg: fmt.Sprintf("invalid input syntax for type %s: %q", pk.Type, truncateForError(key))}
		}
		return keyspace.EncodeIntRowID(n), nil
	}
	enc, err := encodeTextAs(pk.Type, key)
	if err != nil {
		return "", err
	}
	canon, err := wireCell(pk.Type, enc)
	if err != nil {
		return "", &pgError{code: errDataCorrupt, msg: err.Error()}
	}
	return string(canon), nil
}

// buildCells turns the statement's column list into a cell set in schema order.
//
// The two callers differ in how they treat a column the statement did not name.
// An insert must decide every column: an unnamed one takes its schema default,
// or stays absent if it has none. An update must leave an unnamed column alone,
// which is why the cells are values with an explicit set flag rather than plain
// byte slices -- nil already means "set to NULL" and cannot mean both.
func (p *Plan) buildCells(params []paramVal, insert bool) (rowCells, error) {
	sch := p.Schema
	if len(p.Columns) != len(p.Values) {
		return nil, &pgError{
			code: errProtocolViolation,
			msg:  fmt.Sprintf("statement names %d columns but supplies %d values", len(p.Columns), len(p.Values)),
		}
	}
	cells := make(rowCells, len(sch.Columns))
	named := make(map[string]ValRef, len(p.Columns))
	for i, name := range p.Columns {
		if _, ok := sch.columnIndex(name); !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", name)}
		}
		named[name] = p.Values[i]
	}
	for i := range sch.Columns {
		col := &sch.Columns[i]
		ref, given := named[col.Name]
		if !given {
			if insert {
				// The primary key is filled by the caller from the row id, and
				// must not be defaulted: a defaulted key is rejected at DDL
				// time, so reaching here means the schema is inconsistent.
				if col.Default != nil {
					cells[i] = rowValue{value: col.Default, set: true}
				}
			}
			continue
		}
		value, isNull, err := resolveColumnRef(col, ref, params)
		if err != nil {
			return nil, err
		}
		if isNull {
			// An explicit NULL on an insert falls back to the column's default
			// when the column is NOT NULL, matching how a defaulted column
			// behaves when the statement simply omits it.
			if insert && col.Default != nil && !col.Nullable {
				cells[i] = rowValue{value: col.Default, set: true}
				continue
			}
			cells[i] = rowValue{value: nil, set: true}
			continue
		}
		cells[i] = rowValue{value: value, set: true}
	}
	return cells, nil
}

// resolveColumnRef binds one column's value, returning the encoded value and
// whether the value was NULL.
func resolveColumnRef(col *Column, ref ValRef, params []paramVal) ([]byte, bool, error) {
	// A NULL has to be recognized before anything reads its value. It carries
	// no bytes at all, so falling through to the literal path would encode an
	// empty value for it -- which for a text or bytea column is a real value,
	// and would make "set to NULL" indistinguishable from "set to empty".
	if ref.Null {
		return nil, true, nil
	}
	if ref.Param > 0 {
		i := ref.Param - 1
		if i >= len(params) {
			return nil, false, &pgError{
				code: errProtocolViolation,
				msg:  fmt.Sprintf("bind message supplies %d parameters but statement references $%d", len(params), ref.Param),
			}
		}
		if params[i].null {
			if !col.Nullable {
				return nil, false, &pgError{
					code: errNotNullViolation,
					msg:  fmt.Sprintf("null value in column %q violates not-null constraint", col.Name),
				}
			}
			return nil, true, nil
		}
		text := params[i].val
		if params[i].format == resultFormatBinary {
			// A binary-format parameter is already the column's own encoding,
			// since the client was told the column's OID. It is validated by
			// decoding it, which validateRowCells does.
			return text, false, nil
		}
		enc, err := encodeTextAs(col.Type, text)
		if err != nil {
			return nil, false, err
		}
		return enc, false, nil
	}
	// A SQL literal. An empty literal for a textual column is an empty string,
	// not NULL, because NULL is written as the keyword and arrives as a
	// distinct parse node.
	enc, err := encodeTextAs(col.Type, ref.Literal)
	if err != nil {
		return nil, false, err
	}
	return enc, false, nil
}

// resolvePlan attaches a catalog table's schema to a DML plan, and expands a
// "*" projection against it.
//
// This is where a DML statement's table is settled. Translation cannot do it:
// the plan is built before the statement runs, and the table may have been
// created by another client in between. Reporting a missing table from a
// hardcoded name check -- as an earlier version did -- told clients that a table
// the catalog had just accepted was absent, which is worse than a late answer
// because it is confidently wrong.
//
// It is idempotent: Describe and Execute both need the schema, and re-reading
// the catalog for the same plan would make a single statement's answer depend on
// when it was asked.
func (s *Server) resolvePlan(plan *Plan) error {
	switch plan.Kind {
	case StmtSelect, StmtInsert, StmtUpdate, StmtDelete:
	default:
		// Transaction statements and DDL carry no data table to resolve.
		return nil
	}
	// The implicit table keeps the Phase 8 single-key path and has no catalog
	// entry, so it never resolves a schema.
	if plan.Table == tableName {
		return nil
	}
	if plan.Schema == nil {
		sch, err := s.catalog().Get(plan.Table)
		if err != nil {
			if errors.Is(err, ErrNoSuchTable) {
				return &pgError{code: errUndefinedTable, msg: fmt.Sprintf("relation %q does not exist", plan.Table)}
			}
			return s.schemaError(err)
		}
		plan.Schema = sch
	}
	switch plan.Kind {
	case StmtSelect, StmtUpdate, StmtDelete:
		if err := s.planAccess(plan); err != nil {
			return err
		}
	}
	// A "*" projection is expanded into the schema's columns, in declaration
	// order, which is also the order the cells will arrive in.
	if len(plan.Cols) == 1 && plan.Cols[0] == "*" {
		plan.Cols = plan.Schema.RowIDColumns()
	}
	// An INSERT with no column list lines its values up with the schema's
	// columns, which is only knowable now.
	if plan.Kind == StmtInsert && len(plan.Columns) == 0 {
		plan.Columns = plan.Schema.RowIDColumns()
	}
	if plan.Kind == StmtInsert {
		// An INSERT carries its primary key in the value list, at whatever
		// position that column happens to occupy. The row id is derived from
		// it, so it is bound here to the one field the row helpers read.
		pk := plan.Schema.PrimaryKeyName()
		idx := -1
		for i, name := range plan.Columns {
			if name == pk {
				idx = i
				break
			}
		}
		if idx < 0 {
			return &pgError{code: errNotNullViolation, msg: fmt.Sprintf("INSERT must supply the primary key %q", pk)}
		}
		if plan.Values[idx].Null {
			return &pgError{code: errNotNullViolation, msg: fmt.Sprintf("null value in column %q violates not-null constraint", pk)}
		}
		plan.Key = plan.Values[idx]
	}
	return nil
}

// planAccess chooses the access method for a data statement and records it on
// the plan.
//
// It is the seam between translation and execution. Translation is a pure
// function over a parse tree and cannot see the schema; execution can, and the
// planner needs it. Keeping the decision here means the plan a client reads
// under EXPLAIN is produced by the same call that would decide execution, so
// the two cannot disagree about which method was chosen.
func (s *Server) planAccess(plan *Plan) error {
	if plan.Schema == nil {
		return nil
	}
	// The point-lookup path is already carried on the plan as WhereCol/Key and
	// is what this phase executes. It is planned too, so EXPLAIN reports it and
	// the refusal below has something to compare against.
	filter, err := plan.predicate()
	if err != nil {
		return err
	}
	o := &optimizer{sch: plan.Schema, stats: s.statsFor(plan.Schema)}
	phys, err := o.plan(filter)
	if err != nil {
		return err
	}
	plan.Phys = phys
	return nil
}

// predicate returns the statement's WHERE clause as an expression tree.
func (p *Plan) predicate() (Expr, error) {
	if p.Where == nil {
		return nil, nil
	}
	return translateExpr(p.Where)
}

// executable reports whether this phase can run a plan.
//
// Only a point lookup can. The wire protocol here returns a single DataRow, so a
// plan that can match many rows has nowhere to put them: truncating at one row
// would be a silent wrong answer, and returning only the first would be the
// same answer dressed as a complete one. Phase 11 adds the streaming pipeline
// that a range or a scan can be delivered through.
func (p *physicalPlan) executable() bool {
	return p != nil && p.Kind == PlanPointLookup && !p.EmptyRange
}

// execExplain renders the wrapped statement's plan without running it.
func (s *Server) execExplain(plan *Plan) (*execOutcome, error) {
	if err := s.resolvePlan(plan.Inner); err != nil {
		return nil, err
	}
	lines := Explain(plan.Inner.Phys, s.statsFor(plan.Inner.Schema), time.Now())
	text := strings.Join(textLines(lines), "\n")
	return &execOutcome{tag: "EXPLAIN", row: [][]byte{[]byte(text)}, selectRows: true}, nil
}

// textLines renders already-encoded cells as one string per line.
func textLines(cells [][]byte) []string {
	out := make([]string, len(cells))
	for i, c := range cells {
		out[i] = string(c)
	}
	return out
}

// execAnalyze collects local row counts for the optimizer.
func (s *Server) execAnalyze(plan *Plan) (*execOutcome, error) {
	if len(plan.AnalyzeTables) == 0 {
		stats, err := s.analyzeAll()
		if err != nil {
			return nil, err
		}
		total := uint64(0)
		for _, st := range stats {
			total += st.Rows
		}
		return &execOutcome{tag: fmt.Sprintf("ANALYZE %d", total)}, nil
	}
	for _, name := range plan.AnalyzeTables {
		sch, err := s.catalog().Get(name)
		if err != nil {
			if errors.Is(err, ErrNoSuchTable) {
				return nil, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("relation %q does not exist", name)}
			}
			return nil, s.schemaError(err)
		}
		if _, err := s.analyzeTable(sch); err != nil {
			return nil, err
		}
	}
	return &execOutcome{tag: fmt.Sprintf("ANALYZE %d", len(plan.AnalyzeTables))}, nil
}

// refuseMultiRow rejects a plan this phase cannot deliver.
//
// The message names the phase it is waiting for, because "not supported" with
// no reason invites a client to conclude the query is wrong rather than the
// engine being incomplete. It names the plan it was refused for too: a client
// that sees `Range Scan` knows to retry with an equality, and one that sees
// `Seq Scan` knows to wait.
func refuseMultiRow(p *physicalPlan) error {
	if p == nil || p.executable() {
		return nil
	}
	if p.EmptyRange {
		// An empty range has no rows to return, which is a complete answer and
		// not a truncation. Phase 11 will produce it as a zero-row result; here
		// it is still a single-row protocol, so the honest answer is refusal
		// rather than a fabricated empty set that a client could not tell from
		// a query that matched nothing.
		return &pgError{
			code: errFeatureNotSupported,
			msg:  fmt.Sprintf("%s matches no rows; returning a multi-row result is deferred to Phase 11", p.Kind),
		}
	}
	return &pgError{
		code: errFeatureNotSupported,
		msg:  fmt.Sprintf("plan %s cannot be executed: multi-row execution pipeline deferred to Phase 11", p.Kind),
	}
}
