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
	"strconv"
	"strings"

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
	// StmtCreateTable and StmtDropTable are Phase 9 DDL. They translate to
	// catalog writes rather than row keys, so they carry a schema or a table
	// name on the plan instead of a key and a value.
	StmtCreateTable
	StmtDropTable
)

// ValRef is a bound-value reference: either a literal extracted from the query
// text (simple Q protocol) or a PostgreSQL parameter reference (extended
// protocol), resolved from the Bind message at execution time.
type ValRef struct {
	Literal []byte
	Param   int // 1-based parameter number; 0 with Literal used
	// Null marks an explicit SQL NULL. It is a distinct state rather than an
	// absent literal because on a catalog table NULL is a legitimate value for
	// a nullable column, and only the schema can say whether it is allowed. The
	// implicit table declares both its columns NOT NULL, so there the same NULL
	// is a violation -- which is why the two paths resolve NULL differently.
	Null bool
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
	// WhereCol names the column a statement filtered on. The executor checks
	// it against the table's primary key, which is the only column that
	// identifies a row.
	WhereCol string
	// Schema is the table definition a StmtCreateTable will publish, and the
	// table's definition on a catalog-backed DML plan. A DML plan carries it
	// when the statement names a catalog table rather than the implicit
	// tellstone table, which is what selects the row-oriented execution path
	// in exec.go. It stays nil for the implicit table, so the two storage
	// models remain distinguishable at the point of execution.
	Schema *Schema
	// Table names the table a DDL statement acts on, and the table a
	// catalog-backed DML statement acts on.
	Table string
	// Columns and Values are a multi-column statement's column list and its
	// values, positionally paired. They are used only on the catalog path; the
	// implicit table carries its single key and value on Key and Val.
	Columns []string
	Values  []ValRef
	// IfNotExists and IfExists carry the corresponding DDL clauses, which turn
	// a missing or duplicate object from an error into a notice.
	IfNotExists bool
	IfExists    bool
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
	case node.GetCreateStmt() != nil:
		return translateCreate(node.GetCreateStmt())
	case node.GetDropStmt() != nil:
		return translateDrop(node.GetDropStmt())
	case node.GetIndexStmt() != nil:
		// Named explicitly because the phase 9 limitation is likely to be hit:
		// this layout has no secondary index, so an accepted CREATE INDEX would
		// be a promise the engine cannot keep.
		return nil, fmt.Errorf("%w: CREATE INDEX is not supported in phase 9", errUnsupported)
	case node.GetViewStmt() != nil:
		return nil, fmt.Errorf("%w: CREATE VIEW is not supported in phase 9", errUnsupported)
	case node.GetAlterTableStmt() != nil:
		return nil, fmt.Errorf("%w: ALTER TABLE is not supported in phase 9", errUnsupported)
	default:
		return nil, fmt.Errorf("%w: statement is not a supported data command", errUnsupported)
	}
}

// dmlTarget resolves the relation a DML statement names.
//
// Translation deliberately does NOT decide whether the table exists. It cannot:
// Translate is a pure function with no store, and a table it has never seen may
// have been created by another client a moment earlier. An earlier version
// answered "relation does not exist" for any name but the implicit one, which
// reported a table the catalog had just accepted as missing. Existence is
// therefore settled at execution against the live catalog, which can answer it
// truthfully.
func dmlTarget(rv *pg_query.RangeVar) (table string, implicit bool, err error) {
	if rv == nil {
		return "", false, fmt.Errorf("%w: statement has no relation", errUnsupported)
	}
	name := rv.GetRelname()
	if err := ValidateTableName(name); err != nil {
		return "", false, err
	}
	// A schema qualifier is rejected rather than ignored: dropping it would
	// point a statement at a different table than the one the client named.
	if s := rv.GetSchemaname(); s != "" &&
		!strings.EqualFold(s, DefaultDB) && !strings.EqualFold(s, "public") {
		return "", false, fmt.Errorf("%w: schema %q is not available; only %s exists", errUnsupported, s, DefaultDB)
	}
	if name == tableName {
		return name, true, nil
	}
	if isReservedCatalogTable(name) {
		return "", false, fmt.Errorf("%w: %q names a reserved table", errUnsupported, name)
	}
	return name, false, nil
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
// valueRefOrNull is valueRef for a column whose type is not yet known. A NULL
// literal is reported as a state rather than raised as a not-null violation,
// because only the column's declared nullability can decide that, and only the
// catalog has it.
func valueRefOrNull(node *pg_query.Node, column string) (ValRef, error) {
	if ac := node.GetAConst(); ac != nil {
		if ac.GetIsnull() {
			return ValRef{Null: true}, nil
		}
		// A non-string constant is rendered to its text form and left for the
		// column type to read. A number or boolean arriving as a literal
		// carries no type of its own here -- `1` is text until something says
		// otherwise -- and the column is that something. Rejecting it as
		// "only string expressions" would make a bigint primary key
		// unreachable, since its natural literal is not a string.
		switch {
		case ac.GetIval() != nil:
			return ValRef{Literal: []byte(strconv.FormatInt(int64(ac.GetIval().GetIval()), 10))}, nil
		case ac.GetFval() != nil:
			return ValRef{Literal: []byte(ac.GetFval().GetFval())}, nil
		case ac.GetBoolval() != nil:
			return ValRef{Literal: []byte(strconv.FormatBool(ac.GetBoolval().GetBoolval()))}, nil
		}
	}
	// The parser folds a unary minus into the constant but leaves a unary plus
	// as an operator applied to one, so "+7" arrives as an expression where
	// "-7" arrives as a literal. PostgreSQL treats them alike, so the sign is
	// applied here rather than reported as an unsupported expression.
	//
	// A prefix operator arrives with its left operand absent and the value in
	// the right slot, so the operand is whichever side is present. Reading it
	// from a fixed side would silently take the empty one.
	if ae := node.GetAExpr(); ae != nil {
		names := ae.GetName()
		if ae.GetKind() == pg_query.A_Expr_Kind_AEXPR_OP && len(names) == 1 &&
			names[0].GetString_() != nil {
			lexpr, rexpr := ae.GetLexpr(), ae.GetRexpr()
			if (lexpr == nil) != (rexpr == nil) {
				if op := names[0].GetString_().GetSval(); op == "+" || op == "-" {
					operand := rexpr
					if operand == nil {
						operand = lexpr
					}
					inner, err := valueRefOrNull(operand, column)
					if err != nil || inner.Null {
						return inner, err
					}
					if op == "+" {
						return inner, nil
					}
					if n, err := strconv.ParseInt(string(inner.Literal), 10, 64); err == nil {
						return ValRef{Literal: []byte(strconv.FormatInt(-n, 10))}, nil
					}
					if f, err := strconv.ParseFloat(string(inner.Literal), 64); err == nil {
						return ValRef{Literal: []byte(strconv.FormatFloat(-f, 'g', -1, 64))}, nil
					}
					return ValRef{}, fmt.Errorf("%w: unary minus needs a numeric operand", errUnsupported)
				}
			}
		}
	}
	if node.GetTypeCast() != nil {
		// A cast of a literal, which is how the parser spells a typed constant.
		// The cast target is not a type check this phase needs, so it is
		// unwrapped and the column type decides.
		return valueRefOrNull(node.GetTypeCast().GetArg(), column)
	}
	if pr := node.GetParamRef(); pr != nil {
		return ValRef{Param: int(pr.GetNumber())}, nil
	}
	if be := node.GetBooleanTest(); be != nil {
		switch be.GetBooltesttype() {
		case pg_query.BoolTestType_IS_TRUE:
			return ValRef{Literal: []byte("true")}, nil
		case pg_query.BoolTestType_IS_FALSE:
			return ValRef{Literal: []byte("false")}, nil
		}
	}
	return valueRef(node, column)
}

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
// rowPredicate reads the WHERE clause of a statement that addresses one row. It
// returns the filtered column and its value; the executor checks that the
// column is the table's primary key, which is the only column that identifies
// a row.
//
// The column check needs the schema and so cannot happen here for a catalog
// table, but the shape is still checked here: a predicate that is not an
// equality on a plain column reference is not addressable however the schema
// turns out.
func rowPredicate(where *pg_query.Node) (col string, ref ValRef, err error) {
	if where == nil {
		return "", ValRef{}, errNoWhere
	}
	ae := where.GetAExpr()
	if ae == nil || ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
		return "", ValRef{}, fmt.Errorf("%w: only equality predicates are supported", errUnsupported)
	}
	names := ae.GetName()
	if len(names) != 1 || names[0].GetString_() == nil || names[0].GetString_().GetSval() != "=" {
		return "", ValRef{}, fmt.Errorf("%w: only equality predicates are supported", errUnsupported)
	}
	n, ok := columnRef(ae.GetLexpr())
	if !ok {
		return "", ValRef{}, fmt.Errorf("%w: the filtered side must be a plain column", errUnsupported)
	}
	ref, err = valueRefOrNull(ae.GetRexpr(), n)
	if err != nil {
		return "", ValRef{}, err
	}
	return n, ref, nil
}

func translateSelect(ss *pg_query.SelectStmt) (*Plan, error) {
	if ss.GetIntoClause() != nil {
		return nil, fmt.Errorf("%w: SELECT ... INTO is not supported", errUnsupported)
	}
	if ss.GetGroupClause() != nil || ss.GetHavingClause() != nil || ss.GetLimitCount() != nil ||
		ss.GetLimitOffset() != nil || ss.GetSortClause() != nil || ss.GetDistinctClause() != nil ||
		len(ss.GetLockingClause()) > 0 {
		// A projection and an equality filter are the only shapes answerable
		// by reading one row range. Anything that can change which rows
		// qualify, or that folds rows together, is refused rather than
		// ignored, because ignoring it would answer a different question from
		// the one asked.
		return nil, fmt.Errorf("%w: grouping, ordering, limiting and DISTINCT are not supported", errUnsupported)
	}
	from := ss.GetFromClause()
	if len(from) != 1 {
		return nil, fmt.Errorf("%w: SELECT requires exactly one FROM relation", errUnsupported)
	}
	table, implicit, err := dmlTarget(from[0].GetRangeVar())
	if err != nil {
		return nil, err
	}
	whereCol, key, err := rowPredicate(ss.GetWhereClause())
	if err != nil {
		return nil, err
	}
	plan := &Plan{Kind: StmtSelect, Table: table, WhereCol: whereCol, Key: key}

	// A bare star cannot be expanded here: the column list lives in the
	// catalog, which only the executor can read. It is carried as "*" and
	// expanded against the schema once that is known.
	star := false
	seen := map[string]bool{}
	for _, t := range ss.GetTargetList() {
		rt := t.GetResTarget()
		if rt == nil {
			return nil, fmt.Errorf("%w: unsupported target expression", errUnsupported)
		}
		if rt.GetName() != "" {
			return nil, fmt.Errorf("%w: output column aliases are not supported", errUnsupported)
		}
		name, ok := columnRef(rt.GetVal())
		if !ok {
			return nil, fmt.Errorf("%w: only plain column references may be selected", errUnsupported)
		}
		if name == "*" {
			if star {
				continue
			}
			star = true
			plan.Cols = []string{"*"}
			continue
		}
		if star {
			return nil, fmt.Errorf("%w: * cannot be combined with named columns", errUnsupported)
		}
		if !seen[name] {
			seen[name] = true
			plan.Cols = append(plan.Cols, name)
		}
	}
	if len(plan.Cols) == 0 {
		return nil, fmt.Errorf("%w: SELECT target list is empty", errUnsupported)
	}
	if implicit {
		// The implicit table has exactly two columns and stricter projection
		// rules that predate the catalog.
		plan.Table = tableName
		if err := checkImplicitSelect(plan, whereCol); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

// checkImplicitSelect applies the implicit table's projection rules: its only
// two columns, and a filter on key. Kept separate so the catalog path does not
// inherit the implicit table's restrictions by accident.
func checkImplicitSelect(plan *Plan, whereCol string) error {
	if whereCol != colKey {
		return fmt.Errorf("%w: only the key column may be filtered", errUnsupported)
	}
	if len(plan.Cols) == 1 && plan.Cols[0] == "*" {
		plan.Cols = []string{colKey, colValue}
		return nil
	}
	for _, c := range plan.Cols {
		if c != colKey && c != colValue {
			return fmt.Errorf("%w: column %q is not available on %s", errUnsupported, c, tableName)
		}
	}
	return nil
}

func translateInsert(is *pg_query.InsertStmt) (*Plan, error) {
	table, implicit, err := dmlTarget(is.GetRelation())
	if err != nil {
		return nil, err
	}
	if is.GetReturningList() != nil {
		return nil, fmt.Errorf("%w: RETURNING is not supported", errUnsupported)
	}
	if is.GetOnConflictClause() != nil && !implicit {
		// The conflict actions on a catalog table are decided by the row's
		// primary key claim, which is one store operation. DO NOTHING needs a
		// check that is atomic with the write, and DO UPDATE needs a read of
		// the existing row, neither of which the row path can promise without
		// a transaction. Refusing is honest; accepting would race.
		return nil, fmt.Errorf("%w: ON CONFLICT is not supported on catalog tables", errUnsupported)
	}
	vals := is.GetSelectStmt().GetSelectStmt().GetValuesLists()
	if len(vals) == 0 {
		return nil, fmt.Errorf("%w: only INSERT ... VALUES is supported", errUnsupported)
	}
	if len(vals) != 1 {
		return nil, fmt.Errorf("%w: exactly one VALUES row is supported", errUnsupported)
	}
	items := vals[0].GetList().GetItems()

	// The column list is optional in SQL, in which case the values line up with
	// the table's columns. That cannot be resolved without the schema, so the
	// absence is carried and settled at execution.
	var names []string
	for _, c := range is.GetCols() {
		rt := c.GetResTarget()
		if rt == nil || rt.GetName() == "" {
			return nil, fmt.Errorf("%w: INSERT column list must name columns", errUnsupported)
		}
		names = append(names, rt.GetName())
	}
	if len(names) == 0 {
		if implicit {
			return nil, fmt.Errorf("%w: INSERT must name the key and value columns", errUnsupported)
		}
		names = nil
	}
	if len(names) != len(items) {
		return nil, fmt.Errorf("%w: INSERT names %d columns but supplies %d values", errUnsupported, len(names), len(items))
	}
	refs := make([]ValRef, len(items))
	for i, item := range items {
		col := ""
		if names != nil {
			col = names[i]
		}
		refs[i], err = valueRefOrNull(item, col)
		if err != nil {
			return nil, err
		}
	}

	if implicit {
		return translateImplicitInsert(is, table, names, refs)
	}
	return &Plan{Kind: StmtInsert, Table: table, Columns: names, Values: refs}, nil
}

// translateImplicitInsert keeps the Phase 8 shape for the implicit table, whose
// single key and value ride on the plan's Key and Val fields.
func translateImplicitInsert(is *pg_query.InsertStmt, table string, names []string, refs []ValRef) (*Plan, error) {
	var keyPos, valPos = -1, -1
	for i, n := range names {
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
	conflict, err := translateOnConflict(is.GetOnConflictClause())
	if err != nil {
		return nil, err
	}
	if refs[keyPos].Null || refs[valPos].Null {
		return nil, &pgError{code: errNotNullViolation, msg: "null value in column \"" + colKey + "\" violates not-null constraint"}
	}
	return &Plan{
		Kind: StmtInsert, Table: table, WhereCol: colKey,
		Key: refs[keyPos], Val: refs[valPos], Conflict: conflict,
	}, nil
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
	table, implicit, err := dmlTarget(us.GetRelation())
	if err != nil {
		return nil, err
	}
	if us.GetReturningList() != nil {
		return nil, fmt.Errorf("%w: RETURNING is not supported", errUnsupported)
	}
	if len(us.GetFromClause()) > 0 {
		return nil, fmt.Errorf("%w: UPDATE ... FROM is not supported", errUnsupported)
	}
	whereCol, key, err := rowPredicate(us.GetWhereClause())
	if err != nil {
		return nil, err
	}
	if key.Null {
		return nil, &pgError{code: errNotNullViolation, msg: "null value in the filter violates not-null constraint"}
	}
	plan := &Plan{Kind: StmtUpdate, Table: table, WhereCol: whereCol, Key: key}

	// The SET list is a set of column assignments. Each must be a plain
	// column = literal or parameter; an expression would need evaluation this
	// path has nowhere to put.
	var names []string
	var refs []ValRef
	seen := map[string]bool{}
	for _, t := range us.GetTargetList() {
		rt := t.GetResTarget()
		if rt == nil || rt.GetName() == "" {
			return nil, fmt.Errorf("%w: SET must assign to a named column", errUnsupported)
		}
		if seen[rt.GetName()] {
			return nil, fmt.Errorf("%w: column %q is assigned more than once", errUnsupported, rt.GetName())
		}
		seen[rt.GetName()] = true
		ref, err := valueRefOrNull(rt.GetVal(), rt.GetName())
		if err != nil {
			return nil, err
		}
		names = append(names, rt.GetName())
		refs = append(refs, ref)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: UPDATE requires at least one assignment", errUnsupported)
	}
	if implicit {
		if whereCol != colKey {
			return nil, fmt.Errorf("%w: only the key column may be filtered", errUnsupported)
		}
		if len(names) != 1 || names[0] != colValue {
			return nil, fmt.Errorf("%w: only the value column may be updated on %s", errUnsupported, tableName)
		}
		if refs[0].Null {
			return nil, &pgError{code: errNotNullViolation, msg: "null value in column \"" + colValue + "\" violates not-null constraint"}
		}
		plan.Val = refs[0]
		return plan, nil
	}
	plan.Columns = names
	plan.Values = refs
	return plan, nil
}

func translateDelete(ds *pg_query.DeleteStmt) (*Plan, error) {
	table, implicit, err := dmlTarget(ds.GetRelation())
	if err != nil {
		return nil, err
	}
	if ds.GetReturningList() != nil {
		return nil, fmt.Errorf("%w: RETURNING is not supported", errUnsupported)
	}
	if len(ds.GetUsingClause()) > 0 {
		return nil, fmt.Errorf("%w: DELETE ... USING is not supported", errUnsupported)
	}
	whereCol, key, err := rowPredicate(ds.GetWhereClause())
	if err != nil {
		return nil, err
	}
	if key.Null {
		return nil, &pgError{code: errNotNullViolation, msg: "null value in the filter violates not-null constraint"}
	}
	if implicit && whereCol != colKey {
		return nil, fmt.Errorf("%w: only the key column may be filtered", errUnsupported)
	}
	return &Plan{Kind: StmtDelete, Table: table, WhereCol: whereCol, Key: key}, nil
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
