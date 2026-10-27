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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/keyspace"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

// execOutcome is the materialized result of one statement execution.
//
// rows carries every row a SELECT returns, one []byte of wire-encoded column
// cells per row (nil cell = NULL). It is the buffer the wire layer emits many
// DataRows from: execution fills it, emitOutcome drains it. Keeping it here
// rather than streaming per row is what lets a multi-row scan deliver a whole
// result set through the same single-point seam the single-row Phase 8 path
// used, so the extended-protocol portal path needs no separate cursor. Zero
// rows is an empty result set, which a statement that matched nothing
// produces rather than a nil rows.
type execOutcome struct {
	tag        string
	rows       [][][]byte
	selectRows bool
	// notices carries non-fatal NoticeResponse messages the statement wants
	// the client to see, such as the warning for a SET LOCAL outside a
	// transaction block. Execution produces them; the wire layer emits them
	// before the CommandComplete so a client sees them in order.
	notices []noticeInfo
	// paramStatus carries the ParameterStatus updates a SET produced for a
	// variable PostgreSQL reports over the wire (application_name, TimeZone,
	// ...). The wire layer sends them with the statement's result so a client
	// that changed a reported setting learns the engine accepted it.
	paramStatus [][2]string
}

// noticeInfo is one NoticeResponse: its SQLSTATE code and message text. The
// severity is always WARNING; no supported statement raises a notice at any
// other severity.
type noticeInfo struct {
	code string
	msg  string
}

// isDDL reports whether a statement changes the schema.
func (k StmtKind) isDDL() bool {
	return k == StmtCreateTable || k == StmtDropTable
}

// isTxnControl reports whether a statement opens, ends or discards a transaction
// block. Such a statement is never refused for being inside a failed block, and
// its own failure must not poison the block.
func (k StmtKind) isTxnControl() bool {
	return k == StmtBegin || k == StmtCommit || k == StmtRollback
}

// readsRows reports whether a statement runs through the row executor, and so
// takes a store: SELECT and the three writes. EXPLAIN, ANALYZE and DDL have
// their own paths and never touch catalog rows as data.
func (k StmtKind) readsRows() bool {
	return k == StmtSelect || k == StmtInsert || k == StmtUpdate || k == StmtDelete
}

// usesCatalogRows reports whether a plan addresses a catalog table rather than
// the implicit "tellstone" table. A catalog statement carries the table's schema
// (SELECT/INSERT/UPDATE/DELETE) or, for a join, the two catalog tables; the
// implicit table carries neither.
func (p *Plan) usesCatalogRows() bool {
	return p.Join != nil || p.Schema != nil
}

// mvccStore builds the versioned view a catalog statement executes against.
//
// Inside a transaction block the view reads at the block's start timestamp and
// buffers its writes into the block, so a later commit stamps them together.
// Autocommit reads at the latest snapshot and needs no timestamp of its own;
// an autocommit write allocates one commit timestamp for the statement, from
// the serialized oracle, so its version orders correctly against every other
// write in the cluster.
func (s *Server) mvccStore(c *pgConn, kind StmtKind) (Store, error) {
	if c.txn != nil {
		return &mvccView{
			base:     c.txn.base,
			overlay:  c.txn,
			commit:   s.commit,
			snapshot: c.txn.startTS,
		}, nil
	}
	view := &mvccView{
		base:     s.store,
		commit:   s.commit,
		snapshot: math.MaxUint64,
	}
	if kind != StmtSelect {
		ts, err := s.nextTS()
		if err != nil {
			return nil, err
		}
		view.writeTS = ts
	}
	return view, nil
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

// authKeys is the set of RBAC keys a plan is checked against, with the table
// each names for the denial message. authKey covers a single table; a join adds
// its second table, because a session allowed to read one side must not learn
// the other side's rows by joining it with a table it owns.
func authKeys(plan *Plan, params []paramVal) (keys [][]byte, tables []string, err error) {
	if plan.Join == nil {
		key, err := authKey(plan, params)
		if err != nil {
			return nil, nil, err
		}
		return [][]byte{key}, []string{plan.Table}, nil
	}
	tables = joinTables(plan)
	keys = make([][]byte, len(tables))
	for i, t := range tables {
		keys[i] = []byte(TablePrefix(DefaultDB, t))
	}
	return keys, tables, nil
}

// joinTables names the tables a join reads, in the order their keys are
// given: the two it joins, and the tables of the join it is built on, when the
// left side is itself a join. A statement that joins three tables reads three,
// and a session must be allowed to read each of them.
func joinTables(plan *Plan) []string {
	if plan.Join == nil {
		return nil
	}
	tables := []string{plan.Join.Left, plan.Join.Right}
	if plan.Join.Inner != nil {
		tables = append(joinTables(plan.Join.Inner), tables...)
	}
	return tables
}

// authorize applies the session's command bit and key prefixes to a plan and
// records the attempt. It is a no-op for a session with no authentication.
//
// It is deliberately separate from execute because the extended protocol needs
// it earlier: Describe of a prepared statement has to report parameter and result
// types, which means reading the table's schema, and that must not happen for a
// statement the session is not allowed to run.
func (s *Server) authorize(c *pgConn, plan *Plan, params []paramVal) error {
	// Whether the plan reads a virtual catalog relation is settled here,
	// before the session's grants are consulted: a catalog read is not a
	// read of the client's own data, and it has no key to match a grant
	// against. The decision is also needed by the execution path, which asks
	// the store for the projection, so it is recorded on the plan.
	s.markVirtual(plan)
	if c.auth == nil || c.auth.session == nil {
		return nil
	}
	// A statement that names no table has no key to authorize against: SET,
	// SHOW, a FROM-less SELECT and an empty query touch no catalog data, so
	// there is nothing a role could be denied. They are accepted regardless
	// of the session's grants, matching a client's expectation that a
	// connection probe works.
	switch plan.Kind {
	case StmtSet, StmtShow, StmtSelectExpr, StmtEmpty:
		return nil
	}
	// A read of the virtual catalog relations is not a read of the client's
	// own data: it exposes the engine's schema, which every session may
	// inspect. The grants name tables, and a catalog relation is not one of
	// them, so there is no key to match a grant against.
	if plan.Virtual {
		return nil
	}
	keys, tables, err := authKeys(plan, params)
	if err != nil {
		return err
	}
	cmd, name := plan.Kind.command()
	for i, key := range keys {
		if !c.auth.session.IsAllowed(cmd, key) {
			if s.policy != nil {
				s.policy.LogDenied(c.user, c.remoteAddr, name, string(key))
			}
			s.auditDenied(c, name, key)
			return &pgError{code: errInsufficientPrivilege, msg: "permission denied for table " + tables[i]}
		}
	}
	c.auth.session.CountCommand()
	for _, key := range keys {
		s.auditCommand(c, name, key)
	}
	return nil
}

// execute runs a translated plan with the bound parameters. RBAC enforcement
// mirrors the binary frontend: the session's command bit and key prefixes are
// consulted on every data statement.
//
// It is the wrapper that gives a transaction block its PostgreSQL failure
// semantics: any error raised by a data statement inside the block poisons it,
// and every later statement is refused until the block ends. Transaction
// control statements are exempt -- their own failure must not poison a block
// they are about to end.
func (s *Server) execute(c *pgConn, plan *Plan, params []paramVal) (*execOutcome, error) {
	out, err := s.executeStmt(c, plan, params)
	if err != nil && c.txn != nil && !plan.Kind.isTxnControl() {
		c.txn.failed = true
	}
	return out, err
}

// executeStmt dispatches one statement. A transaction block executes against the
// connection's overlay store (see txn.go), so its writes are buffered until
// COMMIT and its reads see its own uncommitted effects.
func (s *Server) executeStmt(c *pgConn, plan *Plan, params []paramVal) (*execOutcome, error) {
	switch plan.Kind {
	case StmtBegin:
		return s.execBegin(c)
	case StmtCommit:
		return s.execCommit(c)
	case StmtRollback:
		return s.execRollback(c)
	case StmtSet:
		return s.execSet(c, plan)
	case StmtShow:
		return s.execShow(c, plan)
	case StmtSelectExpr:
		return s.execSelectExpr(c, plan)
	}

	// A block that has already failed refuses every statement until it ends.
	// This is PostgreSQL's rule and it is what makes a half-applied statement
	// inside a block unusable: the client cannot continue on a state it cannot
	// see, so it is told to roll back.
	if c.txn != nil && c.txn.failed {
		return nil, &pgError{
			code: errInFailedSQLTransaction,
			msg:  "current transaction is aborted, commands ignored until end of transaction block",
		}
	}

	// DDL is dispatched before the RBAC and transaction machinery below, both
	// of which are row-oriented: the RBAC check needs a key predicate, and a
	// catalog write is not a row write. A schema change inside a transaction
	// block is refused rather than silently applied outside the block, because
	// the store cannot roll a catalog mutation back.
	if plan.Kind.isDDL() {
		if c.txn != nil {
			return nil, &pgError{
				code: errFeatureNotSupported,
				msg:  "schema changes are not supported inside a transaction block",
			}
		}
		return s.execDDL(c, plan)
	}

	if err := s.authorize(c, plan, params); err != nil {
		return nil, err
	}

	// The table is resolved here, after the RBAC decision above, so a session
	// without permission learns nothing about which tables exist.
	if err := s.resolvePlan(plan); err != nil {
		return nil, err
	}

	st := c.store()
	// A statement that reads or writes catalog-table rows runs against an MVCC
	// view instead of the raw store, so it resolves column values at a snapshot
	// and stamps its writes with a commit timestamp (see mvcc.go). The implicit
	// "tellstone" table is a raw single-key table shared with the binary
	// frontend and keeps the plain store.
	if plan.Virtual {
		// A virtual catalog relation is projected from the live catalog rather
		// than stored, so the statement reads it through the store that serves
		// the projection. The MVCC view is not used: a projection has no
		// versions, and it is not a snapshot of a table.
		st = newCatalogStore(st, s.catalog(), DefaultDB, c.catalogUser())
	} else if plan.Kind.readsRows() && plan.usesCatalogRows() {
		var err error
		if st, err = s.mvccStore(c, plan.Kind); err != nil {
			return nil, err
		}
	}
	switch plan.Kind {
	case StmtExplain:
		return s.execExplain(c, plan)
	case StmtAnalyze:
		return s.execAnalyze(plan)
	case StmtSelect:
		return s.execSelect(c, st, plan, params)
	case StmtInsert:
		return s.execInsert(st, plan, params)
	case StmtUpdate:
		return s.execUpdate(st, plan, params)
	case StmtDelete:
		return s.execDelete(st, plan, params)
	default:
		return nil, &pgError{code: errFeatureNotSupported, msg: "unhandled statement kind"}
	}
}

// execBegin opens a transaction block. A BEGIN inside an already-open block is
// a no-op that keeps the existing transaction, matching PostgreSQL's warning
// behaviour without surfacing the warning: the block's identity and start
// timestamp do not change.
func (s *Server) execBegin(c *pgConn) (*execOutcome, error) {
	if c.txn != nil {
		return &execOutcome{tag: "BEGIN"}, nil
	}
	ts, err := s.nextTS()
	if err != nil {
		return nil, err
	}
	c.txn = newTxnState(ts, s.store)
	return &execOutcome{tag: "BEGIN"}, nil
}

// execCommit ends a block, applying its write set after conflict detection. A
// COMMIT outside a block is a successful no-op, as in PostgreSQL. A COMMIT of a
// failed block reports ROLLBACK and applies nothing.
func (s *Server) execCommit(c *pgConn) (*execOutcome, error) {
	if c.txn == nil {
		return &execOutcome{tag: "COMMIT"}, nil
	}
	t := c.txn
	c.txn = nil
	if t.failed {
		return &execOutcome{tag: "ROLLBACK"}, nil
	}
	if err := s.commitTxn(t); err != nil {
		return nil, err
	}
	return &execOutcome{tag: "COMMIT"}, nil
}

// execRollback discards a block's buffered writes. A ROLLBACK outside a block
// is a successful no-op.
func (s *Server) execRollback(c *pgConn) (*execOutcome, error) {
	c.txn = nil
	return &execOutcome{tag: "ROLLBACK"}, nil
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

// execSelectExpr evaluates a FROM-less SELECT (Stage A). It reads no store: the
// result is a target list of constant and session-value expressions, evaluated
// against the connection and the statement time. One row is always produced,
// which is what a client's startup probe (SELECT 1) expects.
func (s *Server) execSelectExpr(c *pgConn, plan *Plan) (*execOutcome, error) {
	row, err := evalSelectExprs(plan.Exprs, c, time.Now())
	if err != nil {
		return nil, err
	}
	return &execOutcome{tag: "SELECT 1", rows: [][][]byte{row}, selectRows: true}, nil
}

func (s *Server) execSelect(c *pgConn, st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execSelectRow(st, plan, params)
	}
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	val, found, serr := st.GetErr(string(key))
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if !found {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	cols := make([]Column, len(plan.Cols))
	for i, col := range plan.Cols {
		cols[i] = Column{Name: col, Type: implicitColumnType(col)}
	}
	keys, hidden, err := implicitSortKeys(plan, cols)
	if err != nil {
		return nil, err
	}
	row := make([][]byte, len(plan.Cols)+len(hidden))
	cell := func(name string) []byte {
		if name == colValue {
			return encodeByteaText(val)
		}
		return key
	}
	for i, col := range plan.Cols {
		if col == colKey || col == colValue {
			row[i] = cell(col)
		}
	}
	for j, h := range hidden {
		row[len(plan.Cols)+j] = cell(h)
	}
	out := &execOutcome{tag: "SELECT 1", rows: [][][]byte{row}, selectRows: true}
	if err := finalizeResult(plan, out, cols, keys, len(plan.Cols)); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) execInsert(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execInsertRow(st, plan, params)
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
		if serr := st.Set(string(key), val, 0); serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	case ConflictDoNothing:
		// The existing row must survive, so the write may only create. A
		// conflicting key is a successful no-op, reported as zero rows.
		applied, serr := st.SetIfAbsent(string(key), val, 0)
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
		applied, serr := st.SetIfAbsent(string(key), val, 0)
		if serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		if !applied {
			return nil, &pgError{code: errDuplicateKey, msg: `duplicate key value violates unique constraint "tellstone"`}
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	}
}

func (s *Server) execUpdate(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execUpdateRow(st, plan, params)
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
	applied, serr := st.SetIfPresent(string(key), val, 0)
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if !applied {
		return &execOutcome{tag: "UPDATE 0"}, nil
	}
	return &execOutcome{tag: "UPDATE 1"}, nil
}

func (s *Server) execDelete(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema != nil {
		return s.execDeleteRow(st, plan, params)
	}
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	ok, serr := st.Delete(string(key))
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

// execSelectRow runs a SELECT against a catalog table.
//
// A point lookup is still the fast path: the row is read by its key range and
// projected, with the D5 residual filter (a predicate the lookup could not
// absorb) evaluated on the row's storage bytes. A range or full scan uses the
// Phase 11 scan engine instead, which assembles rows from the key stream,
// filters them in the callback, and delivers them in batches. A join is a
// third shape, executed in join.go: two scans through one hash table.
func (s *Server) execSelectRow(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	sch := plan.Schema
	switch {
	case plan.Phys == nil:
		// A resolved catalog SELECT always carries a plan; a nil one is a bug
		// that must not read the row as if unguided.
		return nil, &pgError{code: errInternal, msg: "SELECT has no physical plan"}
	case plan.Phys.Kind == PlanHashJoin:
		return s.execJoin(st, plan, params)
	case plan.Phys.Kind == PlanRangeScan, plan.Phys.Kind == PlanFullScan:
		return s.execScan(st, plan, params)
	}
	if plan.grouped() {
		return s.execSelectRowGrouped(st, plan, params)
	}
	// CASE targets compile before the lookup, so a condition's errors surface
	// whether or not the addressed row exists.
	ccases, caseAt, err := s.compileCases(plan, sch, params)
	if err != nil {
		return nil, err
	}
	// The equality a point lookup walks is on the physical plan: translate only
	// sets the plan key for a predicate it can prove to be a single equality,
	// while a lookup with a residual filter arrives as an AND.
	rowID, err := canonicalRowID(plan.Phys.Key, sch, params)
	if err != nil {
		return nil, err
	}
	cells, found, err := readRow(st, sch, rowID)
	if err != nil {
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	if !found {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	// A residual filter decides whether the one row the lookup addresses
	// qualifies. Before Phase 11 this was refused; now it is evaluated on the
	// row's storage bytes (D5), with no decode.
	if plan.Phys.Filter != nil {
		filt, err := s.compileFilter(plan.Phys.Filter, sch, params)
		if err != nil {
			return nil, err
		}
		if filt.alwaysFalse() || !filt.eval(cells) {
			return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
		}
	}
	row := make([][]byte, len(plan.Cols))
	cols := make([]Column, len(plan.Cols))
	for i, name := range plan.Cols {
		if ci, isCase := caseAt[i]; isCase {
			cell, err := wireCell(ccases[ci].typ, ccases[ci].eval(cells))
			if err != nil {
				return nil, &pgError{code: errDataCorrupt, msg: fmt.Sprintf("column %q: %v", name, err)}
			}
			cols[i] = Column{Name: name, Type: ccases[ci].typ}
			row[i] = cell
			continue
		}
		idx, ok := sch.columnIndex(name)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", name)}
		}
		cols[i] = sch.Columns[idx]
		cell, err := wireCell(sch.Columns[idx].Type, cells[idx].value)
		if err != nil {
			// The bytes are in the store but do not decode as the column's
			// type, which means the row was written by something other than this
			// write path. Reporting it beats returning a wrong value.
			return nil, &pgError{code: errDataCorrupt, msg: fmt.Sprintf("column %q: %v", name, err)}
		}
		row[i] = cell
	}
	// A point lookup returns one row, but ORDER BY over a column the projection
	// does not return, DISTINCT and OFFSET/LIMIT still decide what that one row
	// is, so the same resolution and finalization a scan does run here too.
	keys, hidden, err := tableSortKeys(plan, sch, cols)
	if err != nil {
		return nil, err
	}
	if len(hidden) > 0 {
		wide := make([][]byte, len(plan.Cols)+len(hidden))
		copy(wide, row)
		row = wide
		for j, ord := range hidden {
			cell, err := wireCell(sch.Columns[ord].Type, cells[ord].value)
			if err != nil {
				return nil, &pgError{code: errDataCorrupt, msg: fmt.Sprintf("column %q: %v", sch.Columns[ord].Name, err)}
			}
			row[len(plan.Cols)+j] = cell
		}
	}
	out := &execOutcome{tag: "SELECT 1", rows: [][][]byte{row}, selectRows: true}
	if err := finalizeResult(plan, out, cols, keys, len(plan.Cols)); err != nil {
		return nil, err
	}
	return out, nil
}

// execSelectRowGrouped folds the one row a point lookup addresses. A missing
// or filtered-out row is an empty input to the fold, which still yields the
// single row of an ungrouped aggregate or no rows at all under GROUP BY.
func (s *Server) execSelectRowGrouped(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	sch := plan.Schema
	rowID, err := canonicalRowID(plan.Phys.Key, sch, params)
	if err != nil {
		return nil, err
	}
	cells, found, err := readRow(st, sch, rowID)
	if err != nil {
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	if found && plan.Phys.Filter != nil {
		filt, err := s.compileFilter(plan.Phys.Filter, sch, params)
		if err != nil {
			return nil, err
		}
		if filt.alwaysFalse() || !filt.eval(cells) {
			found = false
		}
	}
	proj := make([]int, 0, len(plan.Cols))
	cols := make([]Column, 0, len(plan.Cols))
	for i, name := range plan.Cols {
		if plan.isAggPos(i) {
			continue
		}
		idx, ok := sch.columnIndex(name)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", name)}
		}
		proj = append(proj, idx)
		cols = append(cols, sch.Columns[idx])
	}
	refCell := func(qual, col string) (int, Column, error) {
		si, err := tableOrderColumn(sch, plan, qual, col)
		if err != nil {
			return 0, Column{}, err
		}
		for i, p := range proj {
			if p == si {
				return i, sch.Columns[si], nil
			}
		}
		proj = append(proj, si)
		cols = append(cols, sch.Columns[si])
		return len(proj) - 1, sch.Columns[si], nil
	}
	gw, err := resolveGroupWork(plan, refCell)
	if err != nil {
		return nil, err
	}
	var rows [][][]byte
	if found {
		wide := make([][]byte, len(proj))
		for i, si := range proj {
			w, werr := wireCell(sch.Columns[si].Type, cells[si].value)
			if werr != nil {
				return nil, &pgError{code: errDataCorrupt, msg: fmt.Sprintf("column %q: %v", sch.Columns[si].Name, werr)}
			}
			wide[i] = w
		}
		rows = [][][]byte{wide}
	}
	out := &execOutcome{selectRows: true}
	if err := gw.apply(plan, out, rows); err != nil {
		return nil, err
	}
	return out, nil
}

// execScan runs a full-table or primary-key-range SELECT end to end, filling
// the outcome's result set from the scan's batches.
func (s *Server) execScan(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.grouped() {
		return s.execScanGrouped(st, plan, params)
	}
	sch := plan.Schema
	phys := plan.Phys
	// CASE targets compile first, before any early answer, so a condition's
	// errors surface whether or not the range holds a row.
	ccases, caseAt, err := s.compileCases(plan, sch, params)
	if err != nil {
		return nil, err
	}
	// A range that provably covers nothing (its bound overflowed the column's
	// domain) or a filter that admits no row is answered without touching the
	// store.
	if phys.EmptyRange {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	// The projection skips the CASE positions: their cells are computed by the
	// flush into the chunk's trailing columns, not read from the schema.
	proj := make([]int, 0, len(plan.Cols))
	cols := make([]Column, len(plan.Cols))
	for i, name := range plan.Cols {
		if ci, isCase := caseAt[i]; isCase {
			cols[i] = Column{Name: name, Type: ccases[ci].typ}
			continue
		}
		idx, ok := sch.columnIndex(name)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", name)}
		}
		proj = append(proj, idx)
		cols[i] = sch.Columns[idx]
	}
	// ORDER BY is resolved now, while the schema is known: a key the projection
	// returns indexes cols as built above, and a key it does not becomes a
	// hidden trailing column the sort reads and the client never sees.
	keys, hidden, err := tableSortKeys(plan, sch, cols)
	if err != nil {
		return nil, err
	}
	for _, ord := range hidden {
		proj = append(proj, ord)
		cols = append(cols, sch.Columns[ord])
	}
	vis := len(plan.Cols)
	filt, err := s.compileFilter(phys.Filter, sch, params)
	if err != nil {
		return nil, err
	}
	if filt.alwaysFalse() {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	// wireAt maps each chunk column onto its position in the wire row. The
	// chunk is laid out as [projected columns: visible, then hidden sort
	// keys][CASE cells at the trailing edge]; the wire row is [visible columns
	// in select-list order][hidden sort keys]. Without CASE the map is the
	// identity and the plain drain serves unchanged.
	wireAt := make([]int, 0, len(proj)+len(ccases))
	for i := range plan.Cols {
		if _, isCase := caseAt[i]; isCase {
			continue
		}
		wireAt = append(wireAt, i)
	}
	for j := range hidden {
		wireAt = append(wireAt, len(plan.Cols)+j)
	}
	for k := range ccases {
		wireAt = append(wireAt, plan.Cases[k].Pos)
	}
	out := &execOutcome{selectRows: true}
	// As the scan fills each batch, drain it into the outcome's result set:
	// each surviving cell is rendered to its wire form exactly once, and the
	// bytes are the sink's (they outlive the scan's reused buffers).
	err = newScan(st, phys, sch, proj, filt, ccases).run(func(c *Chunk) error {
		if len(ccases) == 0 {
			return drainChunk(c, cols, out)
		}
		return drainChunkMapped(c, cols, wireAt, out)
	})
	if err != nil {
		return nil, &pgError{code: errDataCorrupt, msg: err.Error()}
	}
	if err := finalizeResult(plan, out, cols, keys, vis); err != nil {
		return nil, err
	}
	return out, nil
}

// execScanGrouped runs a grouped SELECT over a full-table or range scan. The
// scan materializes the plain projection plus, as hidden trailing cells, the
// columns the aggregates and grouping keys read; the fold then turns those
// rows into one output row per group, and the ordering, DISTINCT and bounding
// apply to the folded result. A range or filter that admits no row still
// produces a result: an ungrouped aggregate over an empty input is one row of
// counts and NULLs, which only the fold can compute.
func (s *Server) execScanGrouped(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	sch := plan.Schema
	phys := plan.Phys
	proj := make([]int, 0, len(plan.Cols))
	cols := make([]Column, 0, len(plan.Cols))
	for i, name := range plan.Cols {
		if plan.isAggPos(i) {
			continue
		}
		idx, ok := sch.columnIndex(name)
		if !ok {
			return nil, &pgError{code: errUndefinedColumn, msg: fmt.Sprintf("column %q does not exist", name)}
		}
		proj = append(proj, idx)
		cols = append(cols, sch.Columns[idx])
	}
	refCell := func(qual, col string) (int, Column, error) {
		si, err := tableOrderColumn(sch, plan, qual, col)
		if err != nil {
			return 0, Column{}, err
		}
		for i, p := range proj {
			if p == si {
				return i, sch.Columns[si], nil
			}
		}
		proj = append(proj, si)
		cols = append(cols, sch.Columns[si])
		return len(proj) - 1, sch.Columns[si], nil
	}
	gw, err := resolveGroupWork(plan, refCell)
	if err != nil {
		return nil, err
	}
	out := &execOutcome{selectRows: true}
	var rows [][][]byte
	if !phys.EmptyRange {
		filt, err := s.compileFilter(phys.Filter, sch, params)
		if err != nil {
			return nil, err
		}
		if !filt.alwaysFalse() {
			err = newScan(st, phys, sch, proj, filt, nil).run(func(c *Chunk) error {
				return drainChunk(c, cols, out)
			})
			if err != nil {
				return nil, &pgError{code: errDataCorrupt, msg: err.Error()}
			}
			rows = out.rows
		}
	}
	if err := gw.apply(plan, out, rows); err != nil {
		return nil, err
	}
	return out, nil
}

// newScan builds the scanning operator for one side of an access method: the
// seek prefix the store starts from, the row assembler that turns the key
// stream into cells, and a fresh batch for the projection.
//
// The store seeks by prefix, so the scan's prefix must be shared by every key
// it wants, and a lower row bound need not be a prefix of the rows after it.
// The longest common prefix of the plan's two row bounds is the tightest such
// prefix; the callback's bound filters do the rest, so the yielded set is exact
// either way. A single-table scan and each side of a join share this operator;
// they differ only in what they do with each batch.
func newScan(st Store, phys *physicalPlan, sch *Schema, proj []int, filt *compiledNode, cases []*compiledCase) *tableScan {
	beg := phys.Lo
	if beg == nil {
		// A plan without a lower bound must not scan the store from its start,
		// which would run into the catalog subtree. The table prefix is always
		// the conservative bound.
		beg = []byte(TablePrefix(sch.DB, sch.Table))
	}
	seek := commonPrefix(beg, phys.Hi)
	if len(seek) == 0 {
		// The bounds share nothing (they cannot, in practice -- both start with
		// the table prefix); scanning must not start at the store's root.
		seek = []byte(TablePrefix(sch.DB, sch.Table))
	}
	ts := &tableScan{
		store: st,
		sch:   sch,
		seek:  seek,
		beg:   beg,
		hi:    phys.Hi,
		proj:  proj,
		filt:  filt,
		cases: cases,
		as: &rowAssembler{
			sch:    sch,
			prefix: TablePrefix(sch.DB, sch.Table),
			cells:  make([]rowValue, len(sch.Columns)),
			owned:  make([][]byte, len(sch.Columns)),
			proj:   proj,
			filt:   filt,
			cases:  cases,
		},
	}
	ts.chunk = newChunk(int(ts.projLen()))
	return ts
}

// newChunk prepares a scan batch wide enough for proj columns and the whole
// batch, with a data buffer sized for a typical row.
func newChunk(ncols int) *Chunk {
	c := &Chunk{NCols: uint16(ncols)}
	c.ensureNulls(uint16(ncols))
	c.data = make([]byte, 0, ncols*64*BatchRows)
	return c
}

// drainChunk renders one filled batch into the outcome's result set, walking
// the chunk's cell framing and converting each present cell to its wire text.
// A NULL cell stays nil; a present empty string stays an empty cell. cols gives
// every cell its column declaration: a single-table scan passes the schema's
// columns by projection, and a join passes the join projection's declarations,
// which may come from either side's schema.
func drainChunk(c *Chunk, cols []Column, out *execOutcome) error {
	return drainChunkMapped(c, cols, nil, out)
}

// drainChunkMapped renders one filled batch into the outcome's result set,
// with an explicit chunk-column-to-wire-column map. A nil map is the identity,
// which is what every layout without CASE needs; a scan that computes CASE
// cells passes the map, because its chunk carries those cells at the trailing
// edge while the wire row wants them at their select-list position.
func drainChunkMapped(c *Chunk, cols []Column, wireAt []int, out *execOutcome) error {
	off := 0
	n := len(cols)
	if wireAt != nil {
		n = int(c.NCols)
	}
	for row := uint32(0); row < c.Rows; row++ {
		wire := make([][]byte, len(cols))
		for j := 0; j < n; j++ {
			dst := j
			if wireAt != nil {
				dst = wireAt[j]
			}
			if c.isNull(row, uint32(j)) {
				wire[dst] = nil
				continue
			}
			l := binary.BigEndian.Uint32(c.data[off:])
			off += 4
			cell, err := wireCell(cols[dst].Type, c.data[off:off+int(l)])
			if err != nil {
				return fmt.Errorf("column %q: %v", cols[dst].Name, err)
			}
			off += int(l)
			wire[dst] = cell
		}
		out.rows = append(out.rows, wire)
	}
	return nil
}

// execInsertRow writes a multi-column row, failing on a duplicate row id.
func (s *Server) execInsertRow(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
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
	if err := insertRow(st, sch, rowID, cells); err != nil {
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
// execUpdateRow rewrites columns of rows matched by the plan.
func (s *Server) execUpdateRow(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Phys != nil && (plan.Phys.Kind == PlanRangeScan || plan.Phys.Kind == PlanFullScan) {
		res, err := s.execUpdateMulti(st, plan, params)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	if err := refuseMultiRow(plan.Phys); err != nil {
		return nil, err
	}
	rowID, err := plan.rowID(params)
	if err != nil {
		return nil, err
	}
	cells, err := plan.buildCells(params, false)
	if err != nil {
		return nil, err
	}
	existed, err := updateRow(st, plan.Schema, rowID, cells)
	if err != nil {
		if errors.Is(err, ErrNotNull) {
			return nil, &pgError{code: errNotNullViolation, msg: err.Error()}
		}
		if errors.Is(err, ErrBadSchema) {
			return nil, &pgError{code: errSyntax, msg: err.Error()}
		}
		return nil, &pgError{code: errIoError, msg: err.Error()}
	}
	if existed {
		return &execOutcome{tag: "UPDATE 1"}, nil
	}
	return &execOutcome{tag: "UPDATE 0"}, nil
}

// execDeleteRow removes a row and every one of its column keys.
// execDeleteRow removes rows matched by the plan.
func (s *Server) execDeleteRow(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Phys != nil && (plan.Phys.Kind == PlanRangeScan || plan.Phys.Kind == PlanFullScan) {
		res, err := s.execDeleteMulti(st, plan, params)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	if err := refuseMultiRow(plan.Phys); err != nil {
		return nil, err
	}
	rowID, err := plan.rowID(params)
	if err != nil {
		return nil, err
	}
	ok, serr := deleteRow(st, plan.Schema, rowID)
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
	return canonicalRowID(p.Key, p.Schema, params)
}

// canonicalRowID renders a primary-key equality's value as the row id a catalog
// row addresses. It is rowID's second half, which the scan engine also needs:
// a point lookup with a residual filter carries its equality on the physical
// plan's Key, not the plan's, because translate populates the plan key only for
// a predicate it can prove is a single equality.
func canonicalRowID(v ValRef, sch *Schema, params []paramVal) (string, error) {
	key, err := v.resolve(params)
	if err != nil {
		return "", err
	}
	if len(key) == 0 {
		return "", &pgError{code: errNotNullViolation, msg: "primary key must not be empty"}
	}
	pk := sch.Columns[sch.PrimaryKey]
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
	// Whether the plan reads a virtual catalog relation is settled here as
	// well as in the authorization path, because the plan may be resolved
	// without one -- an EXPLAIN's inner plan is resolved on its own.
	s.markVirtual(plan)
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
		// A join's left side carries its qualifier on the join, not on the
		// plan, because the plan's own table name is only one of the two.
		qual := plan.TableSchema
		if plan.Join != nil {
			qual = plan.Join.LeftSchema
		}
		sch, err := s.resolveRelation(qual, plan.Table)
		if err != nil {
			return err
		}
		plan.Schema = sch
	}
	switch plan.Kind {
	case StmtInsert, StmtUpdate, StmtDelete:
		if plan.Virtual {
			// The catalog relations are projected from the live catalog, so
			// there is nothing to write them to. The refusal is explicit
			// rather than the accident of a failed write.
			return &pgError{code: errFeatureNotSupported, msg: "the catalog relations are read-only"}
		}
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
	// A grouped statement's result columns include aggregates, whose types no
	// schema declares; RowDescription needs them before any row exists.
	if err := resolveAggOIDs(plan); err != nil {
		return err
	}
	return nil
}

// resolveRelation resolves a named relation to the schema it is read with: a
// virtual catalog relation when the statement names one, a table of the
// engine's own catalog otherwise.
//
// The two are told apart by the qualifier, and by the catalog itself when the
// statement did not qualify the name: PostgreSQL searches pg_catalog ahead of
// the search path, so an unqualified name that matches no table of the
// engine's is looked for there.
func (s *Server) resolveRelation(qual, table string) (*Schema, error) {
	if isCatalogSchema(qual) {
		rel := lookupCatalogRelation(qual, table)
		if rel == nil {
			return nil, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("relation %q does not exist", qualifiedName(qual, table))}
		}
		return rel.asSchema(), nil
	}
	if qual == "" {
		sch, err := s.catalog().Get(table)
		if err == nil {
			return sch, nil
		}
		if !errors.Is(err, ErrNoSuchTable) {
			return nil, s.schemaError(err)
		}
		if rel := lookupUnqualified(table); rel != nil {
			return rel.asSchema(), nil
		}
		return nil, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("relation %q does not exist", table)}
	}
	sch, err := s.catalog().Get(table)
	if err != nil {
		if errors.Is(err, ErrNoSuchTable) {
			return nil, &pgError{code: errUndefinedTable, msg: fmt.Sprintf("relation %q does not exist", table)}
		}
		return nil, s.schemaError(err)
	}
	return sch, nil
}

// relationIsVirtual reports whether a named relation is one of the virtual
// catalog relations rather than a table of the engine's own catalog.
//
// It is the decision the access control needs before the relation is resolved:
// a catalog read touches no table of the client's own, so it is not the kind
// of read a grant can refuse.
func (s *Server) relationIsVirtual(qual, table string) bool {
	switch {
	case isCatalogSchema(qual):
		return true
	case qual != "":
		return false
	default:
		if _, err := s.catalog().Get(table); err == nil {
			return false
		}
		return lookupUnqualified(table) != nil
	}
}

// markVirtual records on a plan whether it reads a virtual catalog relation.
// It is idempotent, and it is called from both the authorization and the
// resolution path because the authorization happens first and has to know.
func (s *Server) markVirtual(plan *Plan) {
	if plan.Virtual {
		return
	}
	if plan.Join != nil {
		if s.relationIsVirtual(plan.Join.LeftSchema, plan.Join.Left) ||
			s.relationIsVirtual(plan.Join.RightSchema, plan.Join.Right) {
			plan.Virtual = true
		}
		return
	}
	if s.relationIsVirtual(plan.TableSchema, plan.Table) {
		plan.Virtual = true
	}
}

// qualifiedName renders a relation's qualified name for an error message.
func qualifiedName(qual, table string) string {
	if qual == "" {
		return table
	}
	return qual + "." + table
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
	// A join is planned against both tables rather than one, and cannot share
	// the single-table decision below: planJoin resolves the ON operands, the
	// WHERE clause and the projection against two schemas (join.go).
	if plan.Join != nil {
		return s.planJoin(plan)
	}
	if plan.Schema == nil {
		return nil
	}
	// A qualified column name arrives from `WHERE users.id = 1` or
	// `SELECT users.id, users.*`. The qualifier named this table at
	// translation, and a single-table statement has no static need to keep it:
	// the projection and the predicate are expressed in bare names here, the
	// way the planner and the executor look columns up. A qualifier naming
	// some other table is left in place, to be refused with 42703 rather than
	// silently read from the wrong table.
	var err error
	plan.Cols, err = stripOwnQualifier(plan.Cols, plan.Schema, plan.TableAlias)
	if err != nil {
		return err
	}
	filter, err := plan.predicate()
	if err != nil {
		return err
	}
	// The point-lookup path is already carried on the plan as WhereCol/Key and
	// is what this phase executes. It is planned too, so EXPLAIN reports it and
	// the refusal below has something to compare against.
	filter, err = mapExprCols(filter, func(col string) (string, error) {
		return stripColumnQualifier(col, plan.Table, plan.TableAlias), nil
	})
	if err != nil {
		return err
	}
	if err := plan.translateCases(); err != nil {
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

// stripColumnQualifier drops a table qualifier a column name carries when it
// names table, and leaves the name alone otherwise: `users.id` with table
// "users" becomes `id`, and `other.id` stays `other.id` so the planner refuses
// it with 42703 instead of reading the bare name from the wrong table. The
// comparison is case-insensitive, matching PostgreSQL's normalization of
// identifiers (`WHERE USERS.ID = 1` names the same column).
// stripColumnQualifier drops a qualifier that names this table.
//
// The table's alias counts as its name. PostgreSQL hides the table's own name
// behind the alias, so a client that wrote `FROM pg_catalog.pg_class c` names
// the relation "c" and never "pg_class"; a client that wrote `FROM users u`
// names it "u". A qualifier naming neither is left in place, to be refused
// with 42703 rather than silently read from the wrong table.
func stripColumnQualifier(col string, tables ...string) string {
	if i := strings.IndexByte(col, '.'); i > 0 {
		for _, t := range tables {
			if t != "" && strings.EqualFold(col[:i], t) {
				return col[i+1:]
			}
		}
	}
	return col
}

// stripOwnQualifier normalizes a single-table projection the way
// stripColumnQualifier normalizes one filter column. A qualifier naming this
// table is dropped (`users.id` becomes `id`), and a qualified star naming it
// expands to the table's columns. The star rules are the target list's own,
// applied a second time now that the qualifier can be recognized: one star and
// nothing else, or the projection is ambiguous.
func stripOwnQualifier(cols []string, sch *Schema, alias ...string) ([]string, error) {
	if len(cols) == 0 {
		return cols, nil
	}
	if len(cols) == 1 && cols[0] == "*" {
		// A bare "*" is expanded by resolvePlan after planning, and has no
		// qualifier to strip.
		return cols, nil
	}
	if strings.HasSuffix(cols[0], ".*") {
		if len(cols) != 1 {
			return nil, fmt.Errorf("%w: * cannot be combined with named columns", errUnsupported)
		}
		if q := cols[0][:len(cols[0])-2]; qualifierNames(q, sch.Table, alias...) {
			return sch.RowIDColumns(), nil
		}
		// A starred qualifier naming some other table is left for column
		// resolution to refuse with 42703.
		return cols, nil
	}
	// Drop the qualifier, deduped afterwards: the target list dedupes by the
	// name as written, and `id, users.id` survives that and would otherwise
	// emit the same column twice.
	seen := map[string]bool{}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		c = stripColumnQualifier(c, append([]string{sch.Table}, alias...)...)
		if seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	return out, nil
}

// qualifierNames reports whether a qualifier names the relation: its own name
// or the alias it was given.
func qualifierNames(qual, table string, alias ...string) bool {
	if strings.EqualFold(qual, table) {
		return true
	}
	for _, a := range alias {
		if a != "" && strings.EqualFold(qual, a) {
			return true
		}
	}
	return false
}

// predicate returns the statement's WHERE clause as an expression tree.
//
// The errors it reports are statement-shape errors, not store failures, so
// they are mapped to their SQLSTATE here rather than at the wire, where a
// plain error's fallback is the I/O code. A WHERE clause the translator cannot
// read is a syntax-class rejection (42601), and reporting it as an I/O error
// would send the client looking at the connection instead of the query.
func (p *Plan) predicate() (Expr, error) {
	if p.Where == nil {
		return nil, nil
	}
	filter, err := translateExpr(p.Where)
	if err != nil {
		return nil, errOrSyntax(err)
	}
	return filter, nil
}

// translateCases translates every CASE condition once the schema is known and
// strips the statement's own table qualifier from the column references it
// found, the same two steps the WHERE clause takes in planAccess. A condition
// that cannot be translated reports through errOrSyntax, so a client sees the
// same 42601 it would see for an unsupported WHERE at the same moment.
func (p *Plan) translateCases() error {
	for i := range p.Cases {
		if err := p.translateCaseConditions(&p.Cases[i], func(col string) (string, error) {
			col = stripColumnQualifier(col, p.Table)
			if p.TableAlias != "" {
				col = stripColumnQualifier(col, p.TableAlias)
			}
			return col, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// translateCaseConditions translates the conditions of a CASE and of the CASEs
// nested inside it, resolving their columns with the function given.
func (p *Plan) translateCaseConditions(ct *caseTarget, resolve func(string) (string, error)) error {
	for j := range ct.Whens {
		e, err := translateExpr(ct.Whens[j].cond)
		if err != nil {
			return errOrSyntax(err)
		}
		e, err = mapExprCols(e, resolve)
		if err != nil {
			return err
		}
		ct.Whens[j].expr = e
		if ct.Whens[j].sub != nil {
			if err := p.translateCaseConditions(ct.Whens[j].sub, resolve); err != nil {
				return err
			}
		}
	}
	if ct.ElseSub != nil {
		if err := p.translateCaseConditions(ct.ElseSub, resolve); err != nil {
			return err
		}
	}
	return nil
}

// executable reports whether this phase can run a plan.
//
// Only a point lookup can. The wire protocol here returns a single DataRow, so a
// plan that can match many rows has nowhere to put them: truncating at one row
// would be a silent wrong answer, and returning only the first would be the
// same answer dressed as a complete one. Phase 11 adds the streaming pipeline
// that a range or a scan can be delivered through.
func (p *physicalPlan) executable() bool {
	return p != nil && p.Kind == PlanPointLookup && !p.EmptyRange && p.Filter == nil
}

// execExplain renders the wrapped statement's plan without running it.
func (s *Server) execExplain(c *pgConn, plan *Plan) (*execOutcome, error) {
	// The inner statement is authorized before its plan is resolved so a
	// session without permission learns nothing about which tables exist.
	if err := s.authorize(c, plan.Inner, nil); err != nil {
		return nil, err
	}
	if err := s.resolvePlan(plan.Inner); err != nil {
		return nil, err
	}
	lines := Explain(plan.Inner.Phys, s.statsFor, time.Now())
	text := strings.Join(textLines(lines), "\n")
	return &execOutcome{tag: "EXPLAIN", rows: [][][]byte{{[]byte(text)}}, selectRows: true}, nil
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
	if p.Kind == PlanPointLookup {
		// The lookup addresses one row, but whether that row qualifies needs
		// the filter evaluator a scan would apply. Returning the row would
		// answer the wrong question whenever the residual predicate is false,
		// so the statement is refused rather than approximated.
		return &pgError{
			code: errFeatureNotSupported,
			msg:  "plan PointLookup cannot be executed: residual filter evaluation is deferred to Phase 11",
		}
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
