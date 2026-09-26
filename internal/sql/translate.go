/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: translate.go
Description: Maps a single SQL statement onto the implicit tellstone table. The
PostgreSQL parser (pg_query_go) turns the statement into a protobuf parse tree,
which is then narrowed to the supported shapes: single-row CRUD with an equality
predicate on the key column, and BEGIN/COMMIT/ROLLBACK. Anything outside those
shapes is rejected explicitly rather than approximated.
*/
package sql

import (
	"errors"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// The implicit Phase 8 table. Full DDL and additional types are Phase 9.
const (
	tableName = "tellstone"
	colKey    = "key"
	colValue  = "value"

	// PostgreSQL type OIDs used in RowDescription / ParameterDescription.
	oidText  = 25
	oidBytea = 17
)

var (
	errUnsupported = errors.New("unsupported statement")
	errNoWhere     = errors.New("statement on tellstone requires a WHERE key = <value> filter")
)

// StmtKind is the operational class a translated statement performs.
type StmtKind uint8

const (
	StmtSelect StmtKind = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtBegin
	StmtCommit
	StmtRollback
)

// ValRef is a bound-value reference: either a literal extracted from the query
// text (simple Q protocol) or a PostgreSQL parameter reference (extended
// protocol), resolved from the Bind message at execution time.
type ValRef struct {
	Literal []byte
	Param   int // 1-based parameter number; 0 with Literal used
}

func (v ValRef) ref(param bool, n int, lit []byte) ValRef {
	if param {
		return ValRef{Param: n}
	}
	return ValRef{Literal: lit}
}

// Plan is the parsed form of one supported statement. Columns echo the SQL
// projection for SELECT; Key/Value carry the WHERE-key predicate and the
// INSERT/UPDATE payload respectively.
type Plan struct {
	Kind StmtKind
	Cols []string // StmtSelect: requested columns in request order (deduped)
	Key  ValRef
	Val  ValRef
	Tag  string // CommandComplete tag for transaction statements
	// Conflict selects the INSERT's conflict behavior, carried from the
	// ON CONFLICT clause: bare/insert-only raises a duplicate-key error,
	// ConflictDoNothing leaves the existing row alone, and ConflictDoUpdate
	// overwrites it.
	Conflict ConflictAction
}

// ConflictAction is the INSERT conflict behavior the translator extracted from
// the ON CONFLICT clause.
type ConflictAction uint8

const (
	// ConflictRaise is the default: a plain INSERT on an existing key is a
	// duplicate-key violation.
	ConflictRaise ConflictAction = iota
	// ConflictDoNothing is ON CONFLICT DO NOTHING: an existing row is
	// preserved and the statement still reports success.
	ConflictDoNothing
	// ConflictDoUpdate is ON CONFLICT DO UPDATE SET value = excluded.value.
	ConflictDoUpdate
)

// Translate parses a single SQL statement and maps it onto the implicit
// tellstone table. Exactly one statement is accepted so the simple-query path
// emits one ResultSet per Q message, matching libpq semantics.
func Translate(sql string) (*Plan, error) {
	if len(sql) == 0 {
		return nil, errUnsupported
	}
	res, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	if len(res.Stmts) != 1 {
		return nil, fmt.Errorf("%w: exactly one statement per query is supported (got %d)", errUnsupported, len(res.Stmts))
	}
	node := res.Stmts[0].GetStmt()
	switch {
	case node.GetSelectStmt() != nil:
		return translateSelect(node.GetSelectStmt())
	case node.GetInsertStmt() != nil:
		return translateInsert(node.GetInsertStmt())
	case node.GetUpdateStmt() != nil:
		return translateUpdate(node.GetUpdateStmt())
	case node.GetDeleteStmt() != nil:
		return translateDelete(node.GetDeleteStmt())
	case node.GetTransactionStmt() != nil:
		return translateTransaction(node.GetTransactionStmt())
	default:
		return nil, fmt.Errorf("%w: statement is not a supported data command", errUnsupported)
	}
}

func checkTable(rv *pg_query.RangeVar) error {
	if rv == nil || rv.GetRelname() != tableName {
		name := ""
		if rv != nil {
			name = rv.GetRelname()
		}
		return &pgError{code: errUndefinedTable, msg: fmt.Sprintf("relation %q does not exist", name)}
	}
	return nil
}

// columnRef resolves a single-column reference (e.g. key or value). It returns
// ("*", true) for a bare star and ("", false) when the node is not a plain
// single-column reference of the supported shape.
func columnRef(node *pg_query.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	if node.GetAStar() != nil {
		return "*", true
	}
	cr := node.GetColumnRef()
	if cr == nil {
		return "", false
	}
	fields := cr.GetFields()
	if len(fields) != 1 {
		return "", false
	}
	if fields[0].GetAStar() != nil {
		return "*", true
	}
	sv := fields[0].GetString_()
	if sv == nil {
		return "", false
	}
	return sv.GetSval(), true
}

// valueRef extracts a literal or parameter reference from a node. column names
// the column the value feeds, so a NULL literal can be reported as the
// not-null violation it is rather than as an unsupported expression.
func valueRef(node *pg_query.Node, column string) (ValRef, error) {
	if node == nil {
		return ValRef{}, errUnsupported
	}
	if pr := node.GetParamRef(); pr != nil {
		return ValRef{Param: int(pr.GetNumber())}, nil
	}
	if ac := node.GetAConst(); ac != nil {
		if ac.GetIsnull() {
			return ValRef{}, &pgError{code: errNotNullViolation, msg: fmt.Sprintf("null value in column %q violates not-null constraint", column)}
		}
		sv := ac.GetSval()
		if sv == nil {
			return ValRef{}, fmt.Errorf("%w: only string expressions are supported", errUnsupported)
		}
		return ValRef{Literal: []byte(sv.GetSval())}, nil
	}
	return ValRef{}, fmt.Errorf("%w: only literals and parameters are supported", errUnsupported)
}

// keyPredicate requires a single equality predicate on the key column. This is
// the only supported WHERE shape for Phase 8.
func keyPredicate(where *pg_query.Node) (ValRef, error) {
	if where == nil {
		return ValRef{}, errNoWhere
	}
	ae := where.GetAExpr()
	if ae == nil || ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
		return ValRef{}, fmt.Errorf("%w: only equality predicates on key are supported", errUnsupported)
	}
	names := ae.GetName()
	if len(names) != 1 || names[0].GetString_() == nil || names[0].GetString_().GetSval() != "=" {
		return ValRef{}, fmt.Errorf("%w: only equality predicates on key are supported", errUnsupported)
	}
	if n, _ := columnRef(ae.GetLexpr()); n != colKey {
		return ValRef{}, fmt.Errorf("%w: only the key column may be filtered", errUnsupported)
	}
	return valueRef(ae.GetRexpr(), colKey)
}

func translateSelect(ss *pg_query.SelectStmt) (*Plan, error) {
	if ss.GetIntoClause() != nil {
		return nil, fmt.Errorf("%w: SELECT ... INTO is not supported", errUnsupported)
	}
	from := ss.GetFromClause()
	if len(from) != 1 {
		return nil, fmt.Errorf("%w: SELECT requires exactly one FROM relation", errUnsupported)
	}
	if err := checkTable(from[0].GetRangeVar()); err != nil {
		return nil, err
	}
	key, err := keyPredicate(ss.GetWhereClause())
	if err != nil {
		return nil, err
	}
	var cols []string
	seen := map[string]bool{}
	for _, t := range ss.GetTargetList() {
		rt := t.GetResTarget()
		if rt == nil {
			return nil, fmt.Errorf("%w: unsupported target expression", errUnsupported)
		}
		var names []string
		if name, ok := columnRef(rt.GetVal()); ok && name == "*" {
			names = []string{colKey, colValue}
		} else {
			if !ok || (name != colKey && name != colValue) {
				return nil, fmt.Errorf("%w: column %q is not available on %s", errUnsupported, name, tableName)
			}
			names = []string{name}
		}
		for _, n := range names {
			if !seen[n] {
				seen[n] = true
				cols = append(cols, n)
			}
		}
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%w: SELECT target list is empty", errUnsupported)
	}
	return &Plan{Kind: StmtSelect, Cols: cols, Key: key}, nil
}

func translateInsert(is *pg_query.InsertStmt) (*Plan, error) {
	if err := checkTable(is.GetRelation()); err != nil {
		return nil, err
	}
	// Column list must be exactly {key, value} in either order.
	var keyPos, valPos = -1, -1
	for i, c := range is.GetCols() {
		n := c.GetResTarget().GetName()
		switch n {
		case colKey:
			keyPos = i
		case colValue:
			valPos = i
		default:
			return nil, fmt.Errorf("%w: column %q is not writable on %s", errUnsupported, n, tableName)
		}
	}
	if keyPos < 0 || valPos < 0 {
		return nil, fmt.Errorf("%w: INSERT must provide both key and value", errUnsupported)
	}
	sn := is.GetSelectStmt().GetSelectStmt()
	if sn == nil {
		return nil, fmt.Errorf("%w: only INSERT ... VALUES is supported", errUnsupported)
	}
	vals := sn.GetValuesLists()
	if len(vals) != 1 {
		return nil, fmt.Errorf("%w: exactly one VALUES row is supported", errUnsupported)
	}
	items := vals[0].GetList().GetItems()
	if len(items) != 2 {
		return nil, fmt.Errorf("%w: INSERT must provide exactly key and value", errUnsupported)
	}
	// ON CONFLICT is translated by action, because the two forms mean opposite
	// things on the store: DO NOTHING must preserve the existing row, while DO
	// UPDATE must overwrite it.
	conflict, err := translateOnConflict(is.GetOnConflictClause())
	if err != nil {
		return nil, err
	}
	key, err := valueRef(items[keyPos], colKey)
	if err != nil {
		return nil, err
	}
	val, err := valueRef(items[valPos], colValue)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: StmtInsert, Key: key, Val: val, Conflict: conflict}, nil
}

// translateOnConflict maps an ON CONFLICT clause onto a ConflictAction. Only
// DO NOTHING and the canonical DO UPDATE SET value = excluded.value are
// supported; anything else is rejected rather than silently downgraded to a
// plain overwrite, which would corrupt rows the statement promised to keep.
func translateOnConflict(oc *pg_query.OnConflictClause) (ConflictAction, error) {
	if oc == nil {
		return ConflictRaise, nil
	}
	// The arbiter only has to name the one primary key; a named constraint or
	// any other column has no corresponding index on the implicit table.
	if err := checkConflictTarget(oc.GetInfer()); err != nil {
		return ConflictRaise, err
	}
	switch oc.GetAction() {
	case pg_query.OnConflictAction_ONCONFLICT_NOTHING:
		if oc.GetWhereClause() != nil {
			return ConflictRaise, fmt.Errorf("%w: ON CONFLICT DO NOTHING with a WHERE clause is not supported", errUnsupported)
		}
		return ConflictDoNothing, nil
	case pg_query.OnConflictAction_ONCONFLICT_UPDATE:
		if oc.GetWhereClause() != nil {
			return ConflictRaise, fmt.Errorf("%w: ON CONFLICT DO UPDATE with a WHERE clause is not supported", errUnsupported)
		}
		if err := checkExcludedUpsert(oc.GetTargetList()); err != nil {
			return ConflictRaise, err
		}
		return ConflictDoUpdate, nil
	default:
		return ConflictRaise, fmt.Errorf("%w: unsupported ON CONFLICT action %s", errUnsupported, oc.GetAction())
	}
}

// checkConflictTarget accepts an absent arbiter or one naming the key column,
// which is the only conflict target the implicit table has. A nil infer clause
// carries no target, so nothing is checked.
func checkConflictTarget(infer *pg_query.InferClause) error {
	if infer == nil {
		return nil
	}
	if infer.GetConname() != "" {
		return fmt.Errorf("%w: ON CONFLICT ON CONSTRAINT is not supported on %s", errUnsupported, tableName)
	}
	elems := infer.GetIndexElems()
	if len(elems) == 0 {
		return nil
	}
	if len(elems) != 1 || elems[0].GetIndexElem().GetName() != colKey {
		return fmt.Errorf("%w: ON CONFLICT supports only the key column as conflict target", errUnsupported)
	}
	return nil
}

// checkExcludedUpsert accepts only SET value = excluded.value: an assignment
// that reads another column or a literal would not behave like an upsert of the
// proposed row. The parser spells excluded.value as a two-field column
// reference, "excluded" then "value".
func checkExcludedUpsert(targets []*pg_query.Node) error {
	if len(targets) != 1 {
		return fmt.Errorf("%w: ON CONFLICT DO UPDATE must assign exactly the value column", errUnsupported)
	}
	rt := targets[0].GetResTarget()
	if rt == nil || rt.GetName() != colValue {
		return fmt.Errorf("%w: ON CONFLICT DO UPDATE may only assign the value column", errUnsupported)
	}
	cb := rt.GetVal().GetColumnRef()
	if cb == nil || !isExcludedValue(cb) {
		return fmt.Errorf("%w: ON CONFLICT DO UPDATE supports only value = excluded.value", errUnsupported)
	}
	return nil
}

// isExcludedValue reports whether a column reference is excluded.value.
func isExcludedValue(cb *pg_query.ColumnRef) bool {
	fields := cb.GetFields()
	if len(fields) != 2 {
		return false
	}
	first, second := fields[0].GetString_(), fields[1].GetString_()
	return first != nil && second != nil &&
		first.GetSval() == "excluded" && second.GetSval() == colValue
}

func translateUpdate(us *pg_query.UpdateStmt) (*Plan, error) {
	if err := checkTable(us.GetRelation()); err != nil {
		return nil, err
	}
	key, err := keyPredicate(us.GetWhereClause())
	if err != nil {
		return nil, err
	}
	var val ValRef
	set := false
	for _, t := range us.GetTargetList() {
		rt := t.GetResTarget()
		if rt == nil {
			return nil, fmt.Errorf("%w: unsupported SET expression", errUnsupported)
		}
		if rt.GetName() != colValue {
			return nil, fmt.Errorf("%w: only the value column may be updated", errUnsupported)
		}
		if set {
			return nil, fmt.Errorf("%w: multiple value assignments are unsupported", errUnsupported)
		}
		if val, err = valueRef(rt.GetVal(), colValue); err != nil {
			return nil, err
		}
		set = true
	}
	if !set {
		return nil, fmt.Errorf("%w: UPDATE requires a value assignment", errUnsupported)
	}
	return &Plan{Kind: StmtUpdate, Key: key, Val: val}, nil
}

func translateDelete(ds *pg_query.DeleteStmt) (*Plan, error) {
	if err := checkTable(ds.GetRelation()); err != nil {
		return nil, err
	}
	key, err := keyPredicate(ds.GetWhereClause())
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: StmtDelete, Key: key}, nil
}

func translateTransaction(ts *pg_query.TransactionStmt) (*Plan, error) {
	switch ts.GetKind() {
	case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN:
		return &Plan{Kind: StmtBegin, Tag: "BEGIN"}, nil
	case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
		return &Plan{Kind: StmtCommit, Tag: "COMMIT"}, nil
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		return &Plan{Kind: StmtRollback, Tag: "ROLLBACK"}, nil
	default:
		return nil, fmt.Errorf("%w: only BEGIN/COMMIT/ROLLBACK transactions are accepted", errUnsupported)
	}
}

// resultColOIDs returns the result-column type OIDs for a SELECT projection.
func resultColOIDs(cols []string) []int32 {
	oids := make([]int32, len(cols))
	for i, c := range cols {
		switch c {
		case colKey:
			oids[i] = oidText
		case colValue:
			oids[i] = oidBytea
		}
	}
	return oids
}
