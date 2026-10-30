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
	// StmtExplain wraps a statement and renders its plan instead of running it.
	// The wrapped statement lives on Inner, and is planned but never executed.
	StmtExplain
	// StmtAnalyze collects local row-count statistics for the optimizer.
	// Phase 10 keeps those local rather than replicated (ADR-014 decision 5).
	StmtAnalyze
	// StmtSet and StmtShow read or write the session's GUC state (Stage A).
	// They touch no store: SET records a value on the connection and SHOW
	// reads it back, which is what lets a client's startup and introspection
	// sequence proceed against an engine that has no settings catalog yet.
	StmtSet
	StmtShow
	// StmtSelectExpr is a FROM-less SELECT: a target list of constant or
	// session-value expressions with no relation to read. It is how a client
	// probes the connection (`SELECT 1`, `SELECT version()`), so it must
	// answer without a catalog.
	StmtSelectExpr
	// StmtEmpty is a Parse whose query string is blank. PostgreSQL accepts
	// it and answers EmptyQueryResponse at Execute rather than failing the
	// statement, and a client that parses an empty string deliberately (a
	// validation round-trip) reads that answer as protocol health, not as
	// SQL to run.
	StmtEmpty
)

// producesRows reports whether plans of this kind answer with a result set on
// the wire. Describe consults it to choose RowDescription over NoData before
// Execute has run: it is the plan, not the execution outcome, that has to say
// whether column definitions are coming. A kind that returns true here must
// also set execOutcome.selectRows when it runs, or the reverse -- a DataRow
// without a preceding RowDescription -- breaks protocol clients such as
// pgJDBC, which close the connection on it.
func (k StmtKind) producesRows() bool {
	switch k {
	case StmtSelect, StmtSelectExpr, StmtShow, StmtExplain:
		return true
	default:
		return false
	}
}

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
	// TableSchema is the schema a relation was qualified with, empty when the
	// statement did not qualify it. It is what tells a virtual catalog
	// relation ("pg_catalog.pg_class") from a table of the engine's own
	// catalog ("public.users"), which the statement's name alone cannot.
	TableSchema string
	// Virtual marks a plan that reads one of the virtual catalog relations.
	// A catalog read is not a data read: it touches no table of the client's
	// own, so it is not subject to the access control that governs the data.
	Virtual bool
	// TableAlias is the FROM alias of a single-table SELECT, empty when the
	// statement did not alias its table. ORDER BY resolves a qualified
	// reference against it the way PostgreSQL does -- an alias hides the table
	// name -- so `FROM t AS x ORDER BY x.k` and `FROM t ORDER BY t.k` both
	// resolve, and a qualifier naming neither is a missing FROM-clause entry.
	TableAlias string
	// Where is the statement's WHERE clause as a parse-tree node, kept so the
	// planner can build the predicate at execution time once the schema is
	// known. Translation cannot plan: it has no schema.
	Where *pg_query.Node
	// Phys is the access method chosen for a data statement, resolved in
	// resolvePlan once the schema is known. EXPLAIN renders it; a statement that
	// is not a point lookup is refused at execution because the wire protocol
	// returns one row (ADR-014 decision 3).
	Phys *physicalPlan
	// Inner is the statement an StmtExplain wraps. It is planned exactly as it
	// would be on its own, which is the only way the plan a client reads can be
	// the plan it would get.
	Inner *Plan
	// AnalyzeTables names the tables an StmtAnalyze collects. Empty means every
	// table in the catalog, which is what a bare ANALYZE means.
	AnalyzeTables []string
	// Columns and Values are a multi-column statement's column list and its
	// values, positionally paired. They are used only on the catalog path; the
	// implicit table carries its single key and value on Key and Val.
	Columns []string
	Values  []ValRef
	// IfNotExists and IfExists carry the corresponding DDL clauses, which turn
	// a missing or duplicate object from an error into a notice.
	IfNotExists bool
	IfExists    bool
	// Join carries the parsed FROM join of a StmtSelect whose FROM is one.
	// It is nil for every other statement, and the executor plans it against
	// the live catalog once both tables' schemas are known -- translation
	// cannot, because a table named here may be created by another client a
	// moment after this function returns.
	Join *joinSpec
	// GUC carries the session-variable payload of a StmtSet or StmtShow: the
	// variable name (empty for RESET ALL / SHOW ALL) and, for SET, the literal
	// value the client supplied. It is resolved and applied at execution, on
	// the connection, so translation stays a pure function of the parse tree.
	GUC *gucPlan
	// Exprs is a FROM-less SELECT's target list, one expression per output
	// column, evaluated at execution with no relation behind it.
	Exprs []*selectExpr
	// Aggs carries a SELECT's aggregate functions, one per aggregate target
	// or HAVING reference, in the order they were parsed. Pos is the output
	// position the function's value is returned at, or -1 when the aggregate
	// exists only to evaluate HAVING and is not projected.
	Aggs []aggSpec
	// GroupCols carries the GROUP BY items: a column reference as written, or
	// an ordinal position into the select list. A query with either this or
	// Aggs is grouped: its rows are folded into one output row per group (or,
	// without GROUP BY, one row for the whole input), and every projected
	// column that is not an aggregate must be one of the grouping keys.
	GroupCols []groupSpec
	// Having is the HAVING predicate, evaluated per group after the
	// aggregates are computed. It compares one aggregate against a literal.
	Having *havingSpec
	// Cases carries a SELECT's CASE target expressions, one per CASE target,
	// in the order they were parsed. Pos is the output position the branch's
	// value is returned at. The conditions are translated when the schema is
	// known, exactly as the WHERE clause is, because translation has no schema
	// to resolve a column against.
	Cases []caseTarget
	// ColOIDs carries the result-column type OIDs for a statement whose
	// projection is not a catalog column: a FROM-less SELECT's expressions and
	// SHOW's single text column. It is nil for every other statement, whose
	// OIDs the RowDescription derives from the schema as before.
	ColOIDs []int32
	// OutNames carries the output column names a client is told, which are the
	// projection's aliases where one was written (`SELECT id AS i`) and the
	// projection itself otherwise. It is nil when no alias differs, so the
	// RowDescription keeps reading names from Cols.
	OutNames []string
	// OrderBy is the statement's ORDER BY, each item already reduced to the
	// output column it sorts on or to a schema column the projection does not
	// return. It is nil when the statement has no ORDER BY.
	OrderBy []orderKey
	// Distinct folds equal output rows together before the result is returned.
	Distinct bool
	// Limit and Offset bound the result after ordering and DISTINCT. HasLimit
	// distinguishes a written `LIMIT 0` from an absent LIMIT, and a `LIMIT ALL`
	// parses as a null constant and so leaves HasLimit false.
	HasLimit  bool
	HasOffset bool
	Limit     int64
	Offset    int64
}

// grouped reports whether the statement folds its input rows: an aggregate in
// the target list or a GROUP BY item makes the result one row per group (or,
// without GROUP BY, one row for the whole input) rather than one row per input
// row. The executor branches on it to choose between the plain materialization
// and the aggregate path, and ORDER BY resolves against the output columns
// only, because grouping has already erased every other column.
func (p *Plan) grouped() bool {
	return len(p.GroupCols) > 0 || len(p.Aggs) > 0
}

// orderKey is one ORDER BY item, reduced as far as translation can reduce it.
//
// An ordinal is resolved here: PostgreSQL reads it as a position in the select
// list, which translation knows. A column reference is not resolved here --
// translation has no schema, and PostgreSQL resolves ORDER BY against both the
// select list and the input relation -- so it rides as Src and is resolved by
// the executor, which does have both. Exactly one of Out and Src is set.
type orderKey struct {
	// Out is the zero-based index of the output column the item sorts on, or
	// -1 when the item names a column that still has to be resolved.
	Out int
	// Src is the ORDER BY reference as written: "id", "u.id" or a star's
	// relatives are refused above, so this is always a plain column reference.
	Src string
	// Desc orders descending; NullsFirst defaults the way PostgreSQL's does,
	// NULLs last ascending and first descending, when the clause says nothing.
	Desc       bool
	NullsFirst bool
}

// gucPlan is the parsed form of one SET/RESET/SHOW: the variable name and,
// for SET, the value to store. A nil Value on a SET means RESET-equivalent
// (VAR_SET_DEFAULT or VAR_RESET), and Name empty means the whole set (RESET
// ALL / SHOW ALL).
type gucPlan struct {
	Name  string
	Value string
	// HasValue distinguishes SET x = '' from RESET x: both carry an empty
	// Value, but only the former stores an empty string.
	HasValue bool
	// IsLocal marks SET LOCAL, which PostgreSQL scopes to the current
	// transaction block and discards at COMMIT/ROLLBACK.
	IsLocal bool
	// IsReset marks RESET / RESET ALL / SET x = DEFAULT, which restores the
	// variable's default rather than storing a client value.
	IsReset bool
	// IsShow marks SHOW, whose plan evaluates to the variable's current value.
	IsShow bool
	// Multi carries the name/value pairs of a multi-variable SET, the shape
	// `SET SESSION CHARACTERISTICS AS TRANSACTION ...` parses into. Each pair is
	// applied as an ordinary variable of that name, so SHOW round-trips them.
	Multi []gucPair
}

// gucPair is one name/value assignment of a multi-variable SET.
type gucPair struct {
	Name  string
	Value string
}

// selectExpr is one output expression of a FROM-less SELECT. Only the shapes a
// startup probe uses are representable: a constant literal, a named function
// such as version(), or a transaction-dependent value function such as
// current_timestamp. Everything else is refused at translation, before a
// relation-less evaluator could guess at it.
type selectExpr struct {
	// Name is the output column's name (the alias, or PostgreSQL's default
	// label for the expression).
	Name string
	// Kind selects how the value is produced at execution.
	Kind selectExprKind
	// Literal is the rendered text of a constant, already wire-encoded for
	// its type. Function names the function a FuncExpr calls.
	Literal string
	Func    string
	// FuncArg carries the argument of the one supported function that takes
	// one: current_schemas' include-implicit flag. Every other supported
	// function is called with no arguments, where this stays false.
	FuncArg bool
	// OID is the result-column type OID the RowDescription advertises.
	OID int32
	// Null marks an explicit SQL NULL literal.
	Null bool
}

type selectExprKind uint8

const (
	// selectExprLiteral is a constant: number, string, boolean or NULL.
	selectExprLiteral selectExprKind = iota
	// selectExprFunc is a supported scalar function with no arguments
	// (version(), current_database(), ...).
	selectExprFunc
	// selectExprNow is a transaction/statement timestamp value function, which
	// has no argument but a value that changes between statements.
	selectExprNow
)

// joinSpec is the parsed form of one supported FROM join: two tables, their
// aliases as the statement writes them (or their names when unaliased), and
// the two ON operands as columnRef read them -- qualified or bare. Which side
// each operand names is a schema question, so it is settled in the planner;
// only the shapes are checked at translation, where a refusal costs no catalog
// read.
type joinSpec struct {
	Left, Right string
	// LeftSchema and RightSchema are the schema qualifiers of the two sides,
	// empty when the statement did not qualify them. They are what tell a
	// virtual catalog relation from a table of the engine's own catalog.
	LeftSchema, RightSchema string
	LeftAlias, RightAlias   string
	LKey, RKey              string
	// OnRest is the ON clause's remainder: the conditions of the join
	// condition that are not the hash equality, as the parse tree wrote
	// them. They are translated once the schemas are known, because a name
	// in them says which side it belongs to only against those schemas.
	OnRest *pg_query.Node
	// Inner is the plan of the left side when that side is itself a join: a
	// statement that joins three tables is planned as a chain of two joins,
	// and the first of them is this plan. The executor runs it and joins
	// its rows with the right side's.
	Inner *Plan
	// Preserve says which sides keep their rows when the equality finds no
	// match. INNER preserves neither, LEFT the left, RIGHT the right, FULL
	// both.
	Preserve JoinPreserve
}

// JoinPreserve is a join's outer-join kind: which sides of the equality keep
// their rows when nothing on the other side matches. It is the whole of the
// outer-join semantics this engine carries -- everything else about a join,
// the hash, the merged row, the filter and the projection, is shared across
// the four kinds.
type JoinPreserve uint8

const (
	// PreserveNone is an INNER join: only matched pairs are rows.
	PreserveNone JoinPreserve = iota
	// PreserveLeft is a LEFT [OUTER] JOIN: unmatched left rows are rows, with
	// the right side's cells NULL.
	PreserveLeft
	// PreserveRight is a RIGHT [OUTER] JOIN: unmatched right rows are rows,
	// with the left side's cells NULL.
	PreserveRight
	// PreserveBoth is a FULL [OUTER] JOIN: both of the above.
	PreserveBoth
)

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

// Translate parses a single SQL statement. The extended protocol's Parse
// message carries exactly one statement, so this refuses a multi-statement
// string rather than silently running only the first. The simple-query path
// uses TranslateAll, which runs every statement a Q message contains.
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
	return translateNode(res.Stmts[0].GetStmt())
}

// parsedStatements is the parse tree of a simple-query string. The simple-query
// path caches this instead of translated Plans so every execution builds a
// fresh plan against the live catalog; a cached Plan would keep the schema it
// was first resolved with even after another server changes the catalog.
type parsedStatements []*pg_query.RawStmt

// ParseAll parses a simple-query string into its statements without translating
// them. It is the cacheable half of TranslateAll.
func ParseAll(sql string) (parsedStatements, error) {
	if len(strings.TrimSpace(sql)) == 0 {
		return nil, nil
	}
	res, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return parsedStatements(res.Stmts), nil
}

// TranslateParsed translates statements previously returned by ParseAll. Each
// call produces a fresh plan, so nothing resolved against the catalog (schema,
// expanded projection, physical plan) is carried over from an earlier call.
func TranslateParsed(stmts parsedStatements) ([]*Plan, error) {
	plans := make([]*Plan, 0, len(stmts))
	for _, raw := range stmts {
		plan, err := translateNode(raw.GetStmt())
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// TranslateAll parses a simple-query string into one plan per statement, in
// the order the client sent them. The simple-query protocol may carry several
// statements separated by semicolons, and answers each with its own
// CommandComplete/ResultSet; the caller iterates the returned slice. An empty
// or semicolon-only string parses to zero statements, which the caller reports
// as an EmptyQueryResponse rather than an error.
func TranslateAll(sql string) ([]*Plan, error) {
	stmts, err := ParseAll(sql)
	if err != nil {
		return nil, err
	}
	return TranslateParsed(stmts)
}

// translateNode maps one parse-tree statement onto a Plan.
func translateNode(node *pg_query.Node) (*Plan, error) {
	switch {
	case node.GetSelectStmt() != nil:
		return translateSelect(node.GetSelectStmt())
	case node.GetInsertStmt() != nil:
		return translateInsert(node.GetInsertStmt())
	case node.GetUpdateStmt() != nil:
		return translateUpdate(node.GetUpdateStmt())
	case node.GetDeleteStmt() != nil:
		return translateDelete(node.GetDeleteStmt())
	case node.GetExplainStmt() != nil:
		return translateExplain(node.GetExplainStmt())
	case node.GetVacuumStmt() != nil:
		return translateVacuum(node.GetVacuumStmt())
	case node.GetTransactionStmt() != nil:
		return translateTransaction(node.GetTransactionStmt())
	case node.GetVariableSetStmt() != nil:
		return translateVariableSet(node.GetVariableSetStmt())
	case node.GetVariableShowStmt() != nil:
		return translateVariableShow(node.GetVariableShowStmt())
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
func dmlTarget(rv *pg_query.RangeVar) (table string, qual string, implicit bool, err error) {
	if rv == nil {
		return "", "", false, fmt.Errorf("%w: statement has no relation", errUnsupported)
	}
	name := rv.GetRelname()
	if err := ValidateTableName(name); err != nil {
		return "", "", false, err
	}
	// A schema qualifier is rejected rather than ignored: dropping it would
	// point a statement at a different table than the one the client named. The
	// two PostgreSQL system schemas are the exception -- they name the virtual
	// catalog relations, which the executor resolves.
	if s := rv.GetSchemaname(); s != "" &&
		!strings.EqualFold(s, DefaultDB) && !strings.EqualFold(s, "public") &&
		!strings.EqualFold(s, "pg_catalog") && !strings.EqualFold(s, "information_schema") {
		return "", "", false, fmt.Errorf("%w: schema %q is not available; only %s exists", errUnsupported, s, DefaultDB)
	}
	qual = strings.ToLower(rv.GetSchemaname())
	if name == tableName {
		return name, qual, true, nil
	}
	if isReservedCatalogTable(name) {
		return "", "", false, fmt.Errorf("%w: %q names a reserved table", errUnsupported, name)
	}
	return name, qual, false, nil
}

// columnRef resolves a column reference: "key" for a bare column,
// "table.column" for a qualified one, "table.*" for a qualified star and "*"
// for a bare one. It returns ("", false) when the node is not a plain
// single-column reference of the supported shape.
//
// The qualified forms exist because a join writes them constantly (`u.id =
// o.uid`) and refusing them would refuse every join there is. On a
// single-table statement they are not special: planAccess strips the
// qualifier when it matches the table, so `WHERE users.id = 1` plans exactly
// like `WHERE id = 1`, and a qualifier naming some other table is left in
// place to fail column resolution with 42703 rather than silently reading the
// bare name from the wrong table. On a join the qualifier is not stripped --
// it is what tells the planner which side the column belongs to.
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
	switch len(fields) {
	case 1:
		if fields[0].GetAStar() != nil {
			return "*", true
		}
		sv := fields[0].GetString_()
		if sv == nil {
			return "", false
		}
		return sv.GetSval(), true
	case 2:
		first := fields[0].GetString_()
		if first == nil {
			return "", false
		}
		if fields[1].GetAStar() != nil {
			return first.GetSval() + ".*", true
		}
		second := fields[1].GetString_()
		if second == nil {
			return "", false
		}
		return first.GetSval() + "." + second.GetSval(), true
	}
	return "", false
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
	if tc := node.GetTypeCast(); tc != nil {
		// A cast of a literal, which is how the parser spells a typed constant.
		// The cast target is not a type check this phase needs, so it is
		// unwrapped and the column type decides -- except for a cast to an
		// object identifier, where the target names the type the value is
		// read as.
		if oid, ok, err := castToOID(tc); err != nil {
			return ValRef{}, err
		} else if ok {
			return ValRef{Literal: []byte(strconv.FormatInt(int64(oid), 10))}, nil
		}
		return valueRefOrNull(tc.GetArg(), column)
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

// castToOID reads a cast to an object identifier -- `::regclass` or `::oid`
// -- and resolves the relation it names to the identifier the catalog gives
// that relation.
//
// The identifier is the one the catalog reports: a virtual relation's own
// identifier for a system relation, and the derived identifier for a table of
// the engine's own catalog. A client writes the cast to name a relation
// (`'pg_class'::regclass`), and the value it compares against is the
// identifier the catalog itself reports for that relation, so both sides of
// the comparison agree.
//
// A cast whose target is not an identifier type, or whose argument is not a
// name, is not this function's business: it reports ok=false, and the caller
// unwraps the cast as it unwraps any other.
func castToOID(tc *pg_query.TypeCast) (int32, bool, error) {
	names := tc.GetTypeName().GetNames()
	if len(names) == 0 {
		return 0, false, nil
	}
	last := names[len(names)-1].GetString_()
	if last == nil {
		return 0, false, nil
	}
	target := strings.ToLower(last.GetSval())
	if target != "regclass" && target != "oid" {
		return 0, false, nil
	}
	ac := tc.GetArg().GetAConst()
	if ac == nil {
		return 0, false, nil
	}
	if ac.GetIval() != nil {
		return int32(ac.GetIval().GetIval()), true, nil
	}
	if ac.GetSval() == nil {
		return 0, false, nil
	}
	name := ac.GetSval().GetSval()
	// handle qualified regclass like schema.relation
	if strings.Contains(name, ".") {
		parts := strings.SplitN(name, ".", 2)
		schema := strings.Trim(parts[0], "\"")
		rname := strings.Trim(parts[1], "\"")
		if rel := lookupCatalogRelation(schema, rname); rel != nil {
			return rel.oid, true, nil
		}
		return objectOID(rname), true, nil
	}
	if rel := lookupCatalogRelation("", name); rel != nil {
		return rel.oid, true, nil
	}
	if rel := lookupCatalogRelation(schemaPGCatalog, name); rel != nil {
		return rel.oid, true, nil
	}
	if strings.Contains(name, ".") {
		parts := strings.SplitN(name, ".", 2)
		schema := strings.Trim(parts[0], "\"")
		rname := strings.Trim(parts[1], "\"")
		if rel := lookupCatalogRelation(schema, rname); rel != nil {
			return rel.oid, true, nil
		}
		return 0, false, fmt.Errorf("relation %q does not exist", name)
	}
	// A table of the engine's own catalog is named the way the catalog
	// reports it: under the public schema, with the identifier the derived
	// naming gives it.
	return objectOID(name), true, nil
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
// rowPredicate extracts the single-column equality that identifies one row.
//
// It is opportunistic: a WHERE clause it cannot read is reported as "no single
// row predicate" rather than as an error, because the planner is the thing that
// decides what a predicate means. Reporting the error here would make a range
// scan unplannable -- the translator would reject it before the optimizer ever
// saw it, and the optimizer is exactly where a range is supposed to be
// recognised. The planner re-validates the clause and raises the real error for
// a predicate neither can express.
//
// The fields are only consumed on the point-lookup path, which is reached only
// when this function succeeded, so tolerating other shapes costs nothing.
func rowPredicate(where *pg_query.Node) (col string, ref ValRef, err error) {
	col, ref, err = singleRowPredicate(where)
	if errors.Is(err, errUnsupported) || errors.Is(err, errNoWhere) {
		// Not a single-row predicate -- or none at all. The planner reads the
		// clause and reports something more useful than this function can: an
		// unfiltered catalog query is a full scan, and "requires a WHERE key"
		// was true only while a scan was impossible to plan.
		return "", ValRef{}, nil
	}
	return col, ref, err
}

// rowPredicateFor picks the strict or the tolerant reader. The implicit table
// predates the planner and keeps its old behaviour.
func rowPredicateFor(table string, implicit bool, where *pg_query.Node) (string, ValRef, error) {
	if implicit {
		return singleRowPredicate(where)
	}
	return rowPredicate(where)
}

// singleRowPredicate is rowPredicate without the tolerance, for the implicit
// tellstone table. That table has no catalog schema and so is never planned; its
// single key column is the whole of its access path, and a predicate it cannot
// read has to be refused rather than deferred.
func singleRowPredicate(where *pg_query.Node) (col string, ref ValRef, err error) {
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

// refuseSelectModifiers rejects the SELECT clauses this engine cannot carry out.
// ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY and HAVING are parsed by the
// callers instead of being refused here, because the executor can apply them
// to a materialized result; everything left changes which rows qualify or how
// they are computed, and is refused rather than ignored, because ignoring it
// would answer a different question from the one asked.
func refuseSelectModifiers(ss *pg_query.SelectStmt) error {
	if ss.GetIntoClause() != nil {
		return fmt.Errorf("%w: SELECT ... INTO is not supported", errUnsupported)
	}
	if len(ss.GetLockingClause()) > 0 || len(ss.GetWindowClause()) > 0 ||
		ss.GetWithClause() != nil || ss.GetValuesLists() != nil {
		return fmt.Errorf("%w: window, VALUES and locking clauses are not supported", errUnsupported)
	}
	return nil
}

// selectTargets reads a SELECT's target list into a projection, deduped by the
// name as written, and into the names the client is told. The two differ only
// where an alias was written (`SELECT id AS i`): projection names resolve
// columns against the schema, output names describe the result, and conflating
// them would either fail to resolve the alias or announce a column the schema
// does not have.
//
// An aggregate call becomes an aggSpec at the position it occupies; its
// projection name is the function name, which is the name PostgreSQL returns
// for it, and it is never deduplicated, because two count(*) targets are two
// result columns however identical their values will be.
//
// A bare star stays "*"; a qualified star ("t.*") rides as a name, because which
// table it expands to is known only when the schema is. The star rules are
// shared by single-table and join statements so one path cannot accept a shape
// the other refuses.
func selectTargets(list []*pg_query.Node) (cols []string, out []string, aggs []aggSpec, cases []caseTarget, err error) {
	var aliased bool
	star := false
	seen := map[string]bool{}
	for _, t := range list {
		rt := t.GetResTarget()
		if rt == nil {
			return nil, nil, nil, nil, fmt.Errorf("%w: unsupported target expression", errUnsupported)
		}
		label := rt.GetName()
		if fc := rt.GetVal().GetFuncCall(); fc != nil {
			spec, aerr := aggregateTarget(fc)
			if aerr != nil {
				return nil, nil, nil, nil, aerr
			}
			if star {
				return nil, nil, nil, nil, fmt.Errorf("%w: * cannot be combined with named columns", errUnsupported)
			}
			spec.Pos = len(cols)
			cols = append(cols, spec.Func)
			out = append(out, labelOr(label, spec.Func))
			if label != "" && label != spec.Func {
				aliased = true
			}
			aggs = append(aggs, spec)
			continue
		}
		if ce := rt.GetVal().GetCaseExpr(); ce != nil {
			if star {
				return nil, nil, nil, nil, fmt.Errorf("%w: * cannot be combined with named columns", errUnsupported)
			}
			ct, cerr := translateCaseTarget(ce, rt.GetName(), len(cols))
			if cerr != nil {
				return nil, nil, nil, nil, cerr
			}
			cols = append(cols, ct.Name)
			out = append(out, labelOr(label, ct.Name))
			if label != "" && label != ct.Name {
				aliased = true
			}
			cases = append(cases, ct)
			continue
		}
		if ac := rt.GetVal().GetAConst(); ac != nil {
			// A constant in the target list: the column is there, and every
			// row of it holds the same value. A client's introspection
			// queries write them to fill a column they have no value for.
			if star {
				return nil, nil, nil, nil, fmt.Errorf("%w: * cannot be combined with named columns", errUnsupported)
			}
			lit, cerr := caseLitFrom(rt.GetVal())
			if cerr != nil {
				return nil, nil, nil, nil, cerr
			}
			typ, terr := unifyCaseLits([]*caseLit{&lit}, nil)
			if terr != nil {
				return nil, nil, nil, nil, terr
			}
			ct := caseTarget{Name: labelOr(label, "null"), Pos: len(cols), Type: typ}
			ct.Else = &lit
			cols = append(cols, ct.Name)
			out = append(out, labelOr(label, ct.Name))
			if label != "" && label != ct.Name {
				aliased = true
			}
			cases = append(cases, ct)
			continue
		}
		name, ok := columnRef(rt.GetVal())
		if !ok {
			return nil, nil, nil, nil, fmt.Errorf("%w: only plain column references, aggregates and CASE expressions may be selected", errUnsupported)
		}
		if label != "" && name == "*" {
			return nil, nil, nil, nil, fmt.Errorf("%w: a star cannot be aliased", errUnsupported)
		}
		if name == "*" {
			if star {
				continue
			}
			if len(cases) > 0 {
				return nil, nil, nil, nil, fmt.Errorf("%w: * cannot be combined with named columns", errUnsupported)
			}
			star = true
			cols, out = []string{"*"}, []string{"*"}
			continue
		}
		if star {
			return nil, nil, nil, nil, fmt.Errorf("%w: * cannot be combined with named columns", errUnsupported)
		}
		if !seen[name] {
			seen[name] = true
			cols = append(cols, name)
			out = append(out, labelOr(label, name))
			if label != "" && label != name {
				aliased = true
			}
		}
	}
	if len(cols) == 0 {
		return nil, nil, nil, nil, fmt.Errorf("%w: SELECT target list is empty", errUnsupported)
	}
	if !aliased {
		out = nil
	}
	return cols, out, aggs, cases, nil
}

// caseTarget is one CASE expression in a SELECT target list.
//
// Only the shape a client-compatibility transcript can emit is accepted
// (ADR-016 decision 4): each branch result must be a text, integer or boolean
// literal (or NULL), and the condition is either a boolean expression
// (searched CASE) or a comparison of a plain column against a literal (simple
// CASE, desugared into the same boolean form PostgreSQL defines it as). The
// conditions are translated when the schema is known, exactly as the WHERE
// clause is.
type caseTarget struct {
	// Name is the output column name: the alias when one was written, else
	// "case", which is what PostgreSQL calls an unlabeled CASE.
	Name string
	// Pos is the output position the CASE's value is returned at.
	Pos int
	// Whens holds one entry per WHEN branch, in written order.
	Whens []caseWhen
	// Else is the ELSE branch's literal, or nil when no ELSE was written --
	// either way an unmatched CASE is NULL, so the executor treats a missing
	// ELSE as a NULL result.
	Else *caseLit
	// ElseSub is the ELSE branch's own CASE, when the branch is one: a
	// client's metadata queries nest a CASE inside a CASE.
	ElseSub *caseTarget
	// Type is the unified result type of the branch literals, inferred the
	// way PostgreSQL's parse analysis unifies them: NULL branches carry no
	// type, an untyped string literal adopts the other type (and must parse
	// as it, or fail the same way), and two real types that cannot be matched
	// are refused.
	Type ColumnType
}

// caseWhen is one WHEN branch of a CASE: the condition as written (a boolean
// expression for a searched CASE, the literal operand value for a simple one,
// already desugared into a comparison by translateCaseTarget) and the branch's
// result literal. cond/expr are filled in planAccess, once the schema can
// resolve the condition's columns.
type caseWhen struct {
	cond *pg_query.Node
	lit  caseLit
	// sub is the branch's own CASE, when the branch is one rather than a
	// literal.
	sub  *caseTarget
	expr Expr
}

// caseLit is a CASE branch's result literal. kind is 's' for an untyped
// string literal (unknown in PostgreSQL's terms), 'i' for an integer, 'b' for
// a boolean; unification may rewrite a string literal into the type the
// other branches demand.
type caseLit struct {
	null bool
	kind byte
	s    string
	i    int64
	b    bool
}

// caseName is the output name PostgreSQL gives an unlabeled CASE.
const caseName = "case"

// translateCaseTarget reads one CASE parse node into a caseTarget: branch
// literals are parsed and unified here, at parse time, because their errors
// (an unusable result type, mixed types) are analysis errors PostgreSQL
// reports before the statement ever runs.
func translateCaseTarget(ce *pg_query.CaseExpr, label string, pos int) (caseTarget, error) {
	ct := caseTarget{Name: labelOr(label, caseName), Pos: pos}
	whens := ce.GetArgs()
	if len(whens) == 0 {
		return ct, fmt.Errorf("%w: CASE needs at least one WHEN branch", errUnsupported)
	}
	// A simple CASE compares its operand against each WHEN value; the
	// comparison is the same column-equality a searched condition would carry,
	// so it is desugared here into the same A_Expr shape and goes through the
	// same translator, folds and all (`CASE x WHEN NULL` never matches).
	var operand *pg_query.Node
	var operandExpr *pg_query.Node
	if arg := ce.GetArg(); arg != nil {
		if _, ok := plainColumn(arg); ok {
			operand = arg
		} else {
			// The operand is an expression rather than a column. A simple
			// CASE compares its operand with each WHEN value, so an
			// expression operand is compared with the value the branch
			// writes -- which, for a boolean expression, is what the
			// branch's truth value says.
			operandExpr = arg
		}
	}
	lits := make([]*caseLit, 0, len(whens)+1)
	// subs holds the branches that are a CASE of their own. Their types are
	// known once they are translated, and they join the result type the
	// literals do.
	subs := make([]*caseTarget, 0, len(whens)+1)
	for _, w := range whens {
		cw := w.GetCaseWhen()
		if cw == nil {
			return ct, fmt.Errorf("%w: unsupported CASE branch", errUnsupported)
		}
		cond := cw.GetExpr()
		switch {
		case operand != nil:
			cond = equalsNode(operand, cw.GetExpr())
		case operandExpr != nil:
			// The branch's value is compared with the operand, so it must
			// be a boolean: the condition is the operand, or its negation.
			v, err := caseLitFrom(cw.GetExpr())
			if err != nil {
				return ct, err
			}
			if v.kind != 'b' {
				return ct, fmt.Errorf("%w: a CASE with an expression operand compares it with a boolean", errUnsupported)
			}
			if v.b {
				cond = operandExpr
			} else {
				cond = notNode(operandExpr)
			}
		case cond == nil:
			return ct, fmt.Errorf("%w: CASE branch has no condition", errUnsupported)
		}
		// The branch's result is a literal or a CASE of its own: a client's
		// metadata queries nest one inside the other.
		if sub := cw.GetResult().GetCaseExpr(); sub != nil {
			st, err := translateCaseTarget(sub, "", 0)
			if err != nil {
				return ct, err
			}
			ct.Whens = append(ct.Whens, caseWhen{cond: cond, sub: &st})
			subs = append(subs, ct.Whens[len(ct.Whens)-1].sub)
			continue
		}
		lit, err := caseLitFrom(cw.GetResult())
		if err != nil {
			return ct, err
		}
		ct.Whens = append(ct.Whens, caseWhen{cond: cond, lit: lit})
		lits = append(lits, &ct.Whens[len(ct.Whens)-1].lit)
	}
	if sub := ce.GetDefresult().GetCaseExpr(); sub != nil {
		st, err := translateCaseTarget(sub, "", 0)
		if err != nil {
			return ct, err
		}
		ct.ElseSub = &st
		subs = append(subs, ct.ElseSub)
	} else {
		elseLit, err := caseLitFrom(ce.GetDefresult())
		if err != nil {
			return ct, err
		}
		if ce.GetDefresult() == nil {
			elseLit = caseLit{null: true}
		}
		ct.Else = &elseLit
		lits = append(lits, ct.Else)
	}
	typ, err := unifyCaseLits(lits, subs)
	if err != nil {
		return ct, err
	}
	ct.Type = typ
	return ct, nil
}

// notNode builds `NOT expr` as the parse-tree node a simple CASE's boolean
// operand is negated into.
func notNode(expr *pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: &pg_query.BoolExpr{
		Boolop: pg_query.BoolExprType_NOT_EXPR,
		Args:   []*pg_query.Node{expr},
	}}}
}

// equalsNode builds `operand = value` as the parse-tree node a simple CASE's
// WHEN desugars into. PostgreSQL defines a simple CASE as exactly this
// comparison per branch.
func equalsNode(operand, value *pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: &pg_query.A_Expr{
		Kind:  pg_query.A_Expr_Kind_AEXPR_OP,
		Name:  []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "="}}}},
		Lexpr: operand,
		Rexpr: value,
	}}}
}

// caseLitFrom parses one CASE result node. Only literals are accepted: a
// column reference, an operator, a cast or a parameter would need the schema
// or a runtime value to type, and none of them appears in the client
// statements this phase is built from (ADR-016 decision 4).
func caseLitFrom(n *pg_query.Node) (caseLit, error) {
	if n == nil {
		return caseLit{null: true}, nil
	}
	ac := n.GetAConst()
	if ac == nil {
		return caseLit{}, fmt.Errorf("%w: CASE result expressions must be literals", errUnsupported)
	}
	if ac.GetIsnull() {
		return caseLit{null: true}, nil
	}
	if sv := ac.GetSval(); sv != nil {
		return caseLit{kind: 's', s: sv.GetSval()}, nil
	}
	if iv := ac.GetIval(); iv != nil {
		return caseLit{kind: 'i', i: int64(iv.GetIval())}, nil
	}
	if bv := ac.GetBoolval(); bv != nil {
		return caseLit{kind: 'b', b: bv.GetBoolval()}, nil
	}
	if fv := ac.GetFval(); fv != nil {
		// pg_query's protobuf carries machine integers as int32, so an
		// integer literal wider than int4 arrives as a textual float node.
		// PostgreSQL still types such a constant integer (bigint when it
		// exceeds int4); a genuinely fractional literal is a float CASE,
		// which this build does not support.
		if x, err := strconv.ParseInt(fv.GetFval(), 10, 64); err == nil {
			return caseLit{kind: 'i', i: x}, nil
		}
		return caseLit{}, fmt.Errorf("%w: CASE result expressions must be text, integer or boolean literals", errUnsupported)
	}
	return caseLit{}, fmt.Errorf("%w: CASE result expressions must be literals", errUnsupported)
}

// unifyCaseLits infers a CASE's result type from its branch literals and
// rewrites untyped string literals into the type the integer or boolean
// branches demand, the way PostgreSQL's parse analysis coerces unknown
// literals. The failure modes and their SQLSTATEs are PostgreSQL's: a string
// that cannot parse as the unified type is 22P02 at analysis time (whether or
// not the branch is ever taken), and two real types that cannot be matched are
// 42804.
func unifyCaseLits(lits []*caseLit, subs []*caseTarget) (ColumnType, error) {
	typeName := func(k byte) string {
		switch k {
		case 'i':
			return "integer"
		case 'b':
			return "boolean"
		default:
			return "text"
		}
	}
	coerceStr := func(l *caseLit, k byte) error {
		switch k {
		case 'i':
			x, err := strconv.ParseInt(strings.TrimSpace(l.s), 10, 64)
			if err != nil {
				return &pgError{code: errInvalidTextRepresentation,
					msg: fmt.Sprintf("invalid input syntax for type integer: %q", l.s)}
			}
			l.kind, l.i = 'i', x
		case 'b':
			switch strings.ToLower(strings.TrimSpace(l.s)) {
			case "t", "true", "y", "yes", "on", "1":
				l.kind, l.b = 'b', true
			case "f", "false", "n", "no", "off", "0":
				l.kind, l.b = 'b', false
			default:
				return &pgError{code: errInvalidTextRepresentation,
					msg: fmt.Sprintf("invalid input syntax for type boolean: %q", l.s)}
			}
		}
		return nil
	}
	var fam byte
	pending := make([]*caseLit, 0, len(lits))
	for _, l := range lits {
		if l.null {
			continue
		}
		k := l.kind
		if k == 's' && fam != 0 && fam != 's' {
			// An untyped string beside the family already chosen: it must
			// parse as that type, here, or PostgreSQL's analysis error.
			if err := coerceStr(l, fam); err != nil {
				return 0, err
			}
			continue
		}
		if k != 's' && fam == 's' {
			// The family just became real; every untyped string seen so far
			// has to parse as it, in order.
			for _, pl := range pending {
				if err := coerceStr(pl, k); err != nil {
					return 0, err
				}
			}
			pending = pending[:0]
			fam = k
			continue
		}
		if fam == 0 || fam == 's' {
			fam = k
		}
		if k == 's' {
			pending = append(pending, l)
			continue
		}
		if k != fam {
			return 0, &pgError{code: errDatatypeMismatch,
				msg: fmt.Sprintf("CASE types %s and %s cannot be matched", typeName(k), typeName(fam))}
		}
	}
	// A branch that is a CASE of its own contributes its own type, which is
	// known: it cannot be coerced into another type, so it is matched against
	// the family the literals chose rather than converted into it.
	for _, sub := range subs {
		k := byte('s')
		switch sub.Type {
		case TypeInt, TypeBigInt:
			k = 'i'
		case TypeBool:
			k = 'b'
		}
		if fam == 0 {
			fam = k
			continue
		}
		if k == fam {
			continue
		}
		return 0, &pgError{code: errDatatypeMismatch,
			msg: fmt.Sprintf("CASE types %s and %s cannot be matched", typeName(k), typeName(fam))}
	}
	if fam == 'i' {
		for _, l := range lits {
			if l.null {
				continue
			}
			if l.i > 2147483647 || l.i < -2147483648 {
				return TypeBigInt, nil
			}
		}
		return TypeInt, nil
	}
	if fam == 'b' {
		return TypeBool, nil
	}
	// Only untyped strings (or no typed branch at at all): a text CASE.
	return TypeVarchar, nil
}

// applySelectModifiers reads the clauses refuseSelectModifiers lets through --
// DISTINCT, GROUP BY, HAVING, ORDER BY, LIMIT and OFFSET -- onto the plan.
//
// Only shapes it can carry out exactly are accepted. DISTINCT ON, a GROUP BY
// item that is an expression rather than a column or a position, a HAVING
// predicate that is not one aggregate compared against one literal, an ORDER
// BY item that is an expression rather than a column or a position, ORDER BY
// ... USING, and a LIMIT that is not a literal are refused rather than dropped,
// because dropping any of them answers a different question from the one asked.
func applySelectModifiers(ss *pg_query.SelectStmt, plan *Plan, out, cols []string, aggs []aggSpec) error {
	if ds := ss.GetDistinctClause(); len(ds) > 0 {
		// A bare DISTINCT parses as one entry carrying no expression; an entry
		// that carries one is a DISTINCT ON, whose tie-breaking this engine has
		// no way to reproduce.
		if len(ds) != 1 || ds[0].GetNode() != nil {
			return fmt.Errorf("%w: DISTINCT ON is not supported", errUnsupported)
		}
		plan.Distinct = true
	}
	plan.Aggs = aggs
	for _, g := range ss.GetGroupClause() {
		if ac := g.GetAConst(); ac != nil {
			if ac.GetIsnull() || ac.GetIval() == nil || ac.GetIval().GetIval() < 1 || int(ac.GetIval().GetIval()) > len(cols) {
				return fmt.Errorf("%w: GROUP BY position %d is not in select list", errUnsupported, ac.GetIval().GetIval())
			}
			plan.GroupCols = append(plan.GroupCols, groupSpec{Pos: int(ac.GetIval().GetIval()) - 1})
			continue
		}
		name, ok := columnRef(g)
		if !ok || name == "*" {
			return fmt.Errorf("%w: GROUP BY must be a column or a position", errUnsupported)
		}
		plan.GroupCols = append(plan.GroupCols, groupSpec{Col: name})
	}
	if h := ss.GetHavingClause(); h != nil {
		spec, err := havingOf(h, plan)
		if err != nil {
			return err
		}
		plan.Having = &spec
	}
	names := out
	if names == nil {
		names = cols
	}
	for _, s := range ss.GetSortClause() {
		key, err := orderKeyOf(s, len(names), plan)
		if err != nil {
			return err
		}
		plan.OrderBy = append(plan.OrderBy, key)
	}
	if n := ss.GetLimitCount(); n != nil {
		v, set, err := constCount(n, "LIMIT")
		if err != nil {
			return err
		}
		plan.HasLimit, plan.Limit = set, v
	}
	if n := ss.GetLimitOffset(); n != nil {
		v, set, err := constCount(n, "OFFSET")
		if err != nil {
			return err
		}
		plan.HasOffset, plan.Offset = set, v
	}
	return nil
}

// orderKeyOf reduces one ORDER BY item: a position becomes an output index, an
// aggregate call that names a projected aggregate becomes the position that
// aggregate is returned at, and a column reference is carried as written for
// the executor to resolve against the select list and the input relation
// together, which is what PostgreSQL resolves it against.
func orderKeyOf(s *pg_query.Node, nOut int, plan *Plan) (orderKey, error) {
	sb := s.GetSortBy()
	if sb == nil || sb.GetNode() == nil {
		return orderKey{}, fmt.Errorf("%w: unsupported ORDER BY item", errUnsupported)
	}
	if len(sb.GetUseOp()) > 0 || sb.GetSortbyDir() == pg_query.SortByDir_SORTBY_USING {
		return orderKey{}, fmt.Errorf("%w: ORDER BY ... USING is not supported", errUnsupported)
	}
	key := orderKey{Out: -1, Desc: sb.GetSortbyDir() == pg_query.SortByDir_SORTBY_DESC}
	switch sb.GetSortbyNulls() {
	case pg_query.SortByNulls_SORTBY_NULLS_FIRST:
		key.NullsFirst = true
	case pg_query.SortByNulls_SORTBY_NULLS_LAST:
		key.NullsFirst = false
	default:
		key.NullsFirst = key.Desc
	}
	if fc := sb.GetNode().GetFuncCall(); fc != nil {
		// `ORDER BY count(*)` sorts by the aggregate the select list already
		// computes, which is the only aggregate an ORDER BY item may name: a
		// call that matches no projected aggregate has no value to sort by.
		spec, err := aggregateTarget(fc)
		if err != nil {
			return orderKey{}, err
		}
		for _, a := range plan.Aggs {
			if a.Pos >= 0 && a.Func == spec.Func && a.Arg == spec.Arg {
				key.Out = a.Pos
				return key, nil
			}
		}
		return orderKey{}, fmt.Errorf("%w: ORDER BY aggregate must appear in the select list", errUnsupported)
	}
	if ac := sb.GetNode().GetAConst(); ac != nil {
		if ac.GetIsnull() || ac.GetIval() == nil {
			return orderKey{}, fmt.Errorf("%w: ORDER BY position must be an integer literal", errUnsupported)
		}
		pos := ac.GetIval().GetIval()
		if pos < 1 || pos > int32(nOut) {
			return orderKey{}, fmt.Errorf("%w: ORDER BY position %d is not in select list", errUnsupported, pos)
		}
		key.Out = int(pos) - 1
		return key, nil
	}
	name, ok := columnRef(sb.GetNode())
	if !ok || name == "*" {
		return orderKey{}, fmt.Errorf("%w: ORDER BY must be a column or a position", errUnsupported)
	}
	key.Src = name
	return key, nil
}

// constCount reads a LIMIT/OFFSET's operand. A written null (`LIMIT ALL`) sets
// no bound, which is what PostgreSQL treats it as; anything that is not a
// non-negative integer literal is refused rather than approximated.
func constCount(n *pg_query.Node, clause string) (v int64, set bool, err error) {
	ac := n.GetAConst()
	switch {
	case ac == nil:
		return 0, false, fmt.Errorf("%w: %s must be an integer literal", errUnsupported, clause)
	case ac.GetIsnull():
		return 0, false, nil
	case ac.GetIval() == nil || ac.GetIval().GetIval() < 0:
		return 0, false, fmt.Errorf("%w: %s must be a non-negative integer literal", errUnsupported, clause)
	}
	return int64(ac.GetIval().GetIval()), true, nil
}

func translateSelect(ss *pg_query.SelectStmt) (*Plan, error) {
	from := ss.GetFromClause()
	if len(from) == 0 {
		return translateSelectExpr(ss)
	}
	if err := refuseSelectModifiers(ss); err != nil {
		return nil, err
	}
	if len(from) != 1 {
		if lr, rr := commaJoinSides(from); lr != nil && rr != nil {
			return translateCommaJoinSelect(ss, lr, rr)
		}
		// A FROM clause that lists a table and a join of two more is a join
		// of three tables: `FROM a, b LEFT JOIN c ON ...`. It is the shape
		// a client's introspection query has, and it is planned as a chain
		// of two joins: the two tables the WHERE clause joins first, and
		// the third joined to their result.
		if len(from) == 2 {
			if rv := from[0].GetRangeVar(); rv != nil {
				if je := from[1].GetJoinExpr(); je != nil {
					return translateThreeTableSelect(ss, rv, je)
				}
			}
		}
		// A FROM clause that lists three tables is a comma join of three:
		// `FROM a, b, c`. It is planned as a chain of two inner joins, the
		// same chain the other three-table shape is, with both keys taken
		// from the WHERE clause.
		if len(from) == 3 {
			if a := from[0].GetRangeVar(); a != nil {
				if b := from[1].GetRangeVar(); b != nil {
					if c := from[2].GetRangeVar(); c != nil {
						return translateThreeCommaSelect(ss, a, b, c)
					}
				}
			}
		}
		return nil, fmt.Errorf("%w: SELECT requires exactly one FROM relation", errUnsupported)
	}
	if je := from[0].GetJoinExpr(); je != nil {
		return translateJoinSelect(ss, je)
	}
	rv := from[0].GetRangeVar()
	table, qual, implicit, err := dmlTarget(rv)
	if err != nil {
		return nil, err
	}
	whereCol, key, err := rowPredicateFor(table, implicit, ss.GetWhereClause())
	if err != nil {
		return nil, err
	}
	cols, out, aggs, cases, err := selectTargets(ss.GetTargetList())
	if err != nil {
		return nil, err
	}
	plan := &Plan{Kind: StmtSelect, Table: table, TableSchema: qual, TableAlias: rv.GetAlias().GetAliasname(),
		WhereCol: whereCol, Key: key, Where: ss.GetWhereClause(), Cols: cols, OutNames: out}
	if err := applySelectModifiers(ss, plan, out, cols, aggs); err != nil {
		return nil, err
	}

	if implicit {
		// The implicit table has exactly two columns and stricter projection
		// rules that predate the catalog.
		plan.Table = tableName
		if err := checkImplicitSelect(plan, whereCol); err != nil {
			return nil, err
		}
	}
	if len(cases) > 0 {
		// The fold path has no slot for a per-row computed position: every
		// non-grouped output cell is either a schema column or an aggregate,
		// and CASE inside the fold is a different question than this phase
		// answers. Refusing keeps the shape honest rather than half-planned.
		if plan.grouped() {
			return nil, fmt.Errorf("%w: CASE in a query with GROUP BY, HAVING or aggregates is not supported", errUnsupported)
		}
		plan.Cases = cases
		// The result type of a literal-only CASE is known here, without a
		// schema: the RowDescription reads these OIDs directly, the way a
		// FROM-less SELECT's expressions carry theirs.
		plan.ColOIDs = make([]int32, len(cols))
		for i := range cases {
			plan.ColOIDs[cases[i].Pos] = oidOf(cases[i].Type)
		}
	}
	return plan, nil
}

// translateJoinSelect reads a FROM that is exactly one join: two tables, an
// equality, and nothing else, of a kind the hash join preserves correctly --
// INNER, LEFT, RIGHT or FULL. Every other join shape is refused here rather
// than planned wrong: NATURAL and USING need name inference that can silently
// pick the wrong column, a nested join needs a planner that can choose which
// side to build, and an ON clause that is not a single column equality is not
// hashable at all.
//
// The shapes are checked here because translation can settle them without a
// catalog read; which table each ON operand and each WHERE column names is
// settled later, in planJoin, against the live schemas.
func translateJoinSelect(ss *pg_query.SelectStmt, je *pg_query.JoinExpr) (*Plan, error) {
	if err := refuseSelectModifiers(ss); err != nil {
		return nil, err
	}
	if je.GetIsNatural() {
		return nil, fmt.Errorf("%w: NATURAL JOIN is not supported", errUnsupported)
	}
	var preserve JoinPreserve
	switch je.GetJointype() {
	case pg_query.JoinType_JOIN_INNER:
		preserve = PreserveNone
	case pg_query.JoinType_JOIN_LEFT:
		preserve = PreserveLeft
	case pg_query.JoinType_JOIN_RIGHT:
		preserve = PreserveRight
	case pg_query.JoinType_JOIN_FULL:
		preserve = PreserveBoth
	default:
		return nil, fmt.Errorf("%w: only INNER, LEFT, RIGHT and FULL JOIN are supported", errUnsupported)
	}
	if len(je.GetUsingClause()) > 0 {
		return nil, fmt.Errorf("%w: JOIN ... USING is not supported", errUnsupported)
	}
	if je.GetQuals() == nil {
		// CROSS JOIN parses as jointype INNER with a nil quals, so it lands
		// here rather than on the type check above.
		return nil, fmt.Errorf("%w: JOIN requires an ON condition", errUnsupported)
	}
	if je.GetRarg().GetJoinExpr() != nil {
		// A join whose right side is a join is a shape the planner does not
		// read: the chain it plans runs left to right, and the right side
		// is the side that is read last.
		return nil, fmt.Errorf("%w: nested joins are not supported", errUnsupported)
	}
	if je.GetLarg().GetJoinExpr() != nil {
		// A join whose left side is a join is the shape a client writes when
		// it chains joins: `a LEFT JOIN b ON ... LEFT JOIN c ON ...`. It is
		// planned as the chain it reads: the first join is planned, and the
		// second joins the third table to its result.
		return translateChainedJoinSelect(ss, je)
	}
	ae := je.GetQuals().GetAExpr()
	if ae == nil || ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP ||
		len(ae.GetName()) != 1 || ae.GetName()[0].GetString_().GetSval() != "=" {
		// The ON clause may name more than the equality: a client's
		// introspection queries write `ON a.x = b.y AND b.z = 0`, where the
		// first equality is what the hash join is built on and the rest is a
		// condition on the matched pair. The equality is found among the
		// clause's conjuncts; the rest ride along as the match filter.
		key, rest, ok := joinOnEquality(je.GetQuals())
		if !ok {
			return nil, fmt.Errorf("%w: ON must be a single equality between two columns", errUnsupported)
		}
		return translateJoinSelectWithKey(ss, je, preserve, key.l, key.r, rest)
	}
	lkey, ok := plainColumn(ae.GetLexpr())
	if !ok {
		return nil, fmt.Errorf("%w: ON must be a single equality between two columns", errUnsupported)
	}
	rkey, ok := plainColumn(ae.GetRexpr())
	if !ok {
		return nil, fmt.Errorf("%w: ON must be a single equality between two columns", errUnsupported)
	}
	return translateJoinSelectWithKey(ss, je, preserve, lkey, rkey, nil)
}

// translateJoinSelectWithKey translates a join whose hash key is known: the
// two columns the equality compares, and the rest of the ON clause.
func translateJoinSelectWithKey(ss *pg_query.SelectStmt, je *pg_query.JoinExpr, preserve JoinPreserve, lkey, rkey string, onRest *pg_query.Node) (*Plan, error) {
	lr := je.GetLarg().GetRangeVar()
	rr := je.GetRarg().GetRangeVar()
	if lr == nil || rr == nil {
		return nil, fmt.Errorf("%w: JOIN sides must be simple tables", errUnsupported)
	}
	ltable, lqual, limplicit, err := dmlTarget(lr)
	if err != nil {
		return nil, err
	}
	rtable, rqual, rimplicit, err := dmlTarget(rr)
	if err != nil {
		return nil, err
	}
	if limplicit || rimplicit {
		return nil, fmt.Errorf("%w: the implicit table cannot be joined", errUnsupported)
	}

	lalias := lr.GetAlias().GetAliasname()
	if lalias == "" {
		lalias = ltable
	}
	ralias := rr.GetAlias().GetAliasname()
	if ralias == "" {
		ralias = rtable
	}
	if strings.EqualFold(lalias, ralias) {
		return nil, fmt.Errorf("%w: table name %q specified more than once", errUnsupported, lalias)
	}
	cols, out, aggs, cases, err := selectTargets(ss.GetTargetList())
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Kind:     StmtSelect,
		Table:    ltable,
		Where:    ss.GetWhereClause(),
		Cols:     cols,
		OutNames: out,
		Cases:    cases,
		Join: &joinSpec{
			Left: ltable, Right: rtable,
			LeftSchema: lqual, RightSchema: rqual,
			LeftAlias: lalias, RightAlias: ralias,
			LKey: lkey, RKey: rkey,
			OnRest:   onRest,
			Preserve: preserve,
		},
	}
	if err := applySelectModifiers(ss, plan, out, cols, aggs); err != nil {
		return nil, err
	}
	return plan, nil
}

// translateChainedJoinSelect translates a join whose left side is itself a
// join: `a LEFT JOIN b ON ... LEFT JOIN c ON ...`.
//
// The chain is planned as the statement reads it: the first join is planned on
// its own, projecting the whole merged row, and the second join takes that
// result as its left side. The second join's ON clause may name a column of
// either of the first join's tables, because its merged row keeps their
// names.
func translateChainedJoinSelect(ss *pg_query.SelectStmt, je *pg_query.JoinExpr) (*Plan, error) {
	if err := refuseSelectModifiers(ss); err != nil {
		return nil, err
	}
	inner, err := translateJoinSelect(ss, je.GetLarg().GetJoinExpr())
	if err != nil {
		return nil, err
	}
	// The inner join is the left side of the outer one, so it projects the
	// whole merged row rather than the statement's target list.
	inner.Cols = []string{"*"}
	inner.OutNames = nil
	inner.Where = nil
	rr := je.GetRarg().GetRangeVar()
	if rr == nil {
		return nil, fmt.Errorf("%w: JOIN sides must be simple tables", errUnsupported)
	}
	rtable, rqual, rimplicit, err := dmlTarget(rr)
	if err != nil {
		return nil, err
	}
	if rimplicit {
		return nil, fmt.Errorf("%w: the implicit table cannot be joined", errUnsupported)
	}
	ralias := rr.GetAlias().GetAliasname()
	if ralias == "" {
		ralias = rtable
	}
	// The ON clause's equality is the key; the rest of the clause rides
	// along as the match filter.
	key, onRest, ok := joinOnEquality(je.GetQuals())
	if !ok {
		return nil, fmt.Errorf("%w: ON must be a single equality between two columns", errUnsupported)
	}
	cols, out, aggs, cases, err := selectTargets(ss.GetTargetList())
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Kind:     StmtSelect,
		Table:    inner.Table,
		Where:    ss.GetWhereClause(),
		Cols:     cols,
		OutNames: out,
		Cases:    cases,
		Join: &joinSpec{
			Inner:       inner,
			Right:       rtable,
			RightSchema: rqual,
			RightAlias:  ralias,
			LKey:        key.l,
			RKey:        key.r,
			OnRest:      onRest,
			Preserve:    preserveOf(je),
		},
	}
	if err := applySelectModifiers(ss, plan, out, cols, aggs); err != nil {
		return nil, err
	}
	return plan, nil
}

// preserveOf reads a join's shape: which of its sides survives a row that
// matches nothing.
func preserveOf(je *pg_query.JoinExpr) JoinPreserve {
	switch je.GetJointype() {
	case pg_query.JoinType_JOIN_LEFT:
		return PreserveLeft
	case pg_query.JoinType_JOIN_RIGHT:
		return PreserveRight
	case pg_query.JoinType_JOIN_FULL:
		return PreserveBoth
	default:
		return PreserveNone
	}
}

// commaJoinSides reports the two tables of a comma join -- a FROM clause that
// lists more than one table with no join operator between them -- when the
// list is exactly that: two plain tables. The condition that joins them lives
// in the WHERE clause, which is the shape PostgreSQL's own metadata queries
// use, so the join is planned from the WHERE's equality rather than from an ON
// clause.
func commaJoinSides(from []*pg_query.Node) (*pg_query.RangeVar, *pg_query.RangeVar) {
	if len(from) != 2 {
		return nil, nil
	}
	lr := from[0].GetRangeVar()
	rr := from[1].GetRangeVar()
	if lr == nil || rr == nil {
		return nil, nil
	}
	return lr, rr
}

// translateThreeCommaSelect translates a FROM clause that lists three tables:
// `FROM a, b, c`.
//
// It is planned as a chain of two inner joins, like the other three-table
// shape: the first joins the two tables the first equality names, the second
// joins the third table to their result. Both keys come from the WHERE
// clause, because a comma join is written that way.
func translateThreeCommaSelect(ss *pg_query.SelectStmt, a, b, c *pg_query.RangeVar) (*Plan, error) {
	if err := refuseSelectModifiers(ss); err != nil {
		return nil, err
	}
	inner, err := translateInnerJoin(ss, a, b)
	if err != nil {
		return nil, err
	}
	ctable, cqual, cimplicit, err := dmlTarget(c)
	if err != nil {
		return nil, err
	}
	if cimplicit {
		return nil, fmt.Errorf("%w: the implicit table cannot be joined", errUnsupported)
	}
	calias := c.GetAlias().GetAliasname()
	if calias == "" {
		calias = ctable
	}
	// The second join's key is the equality that joins the third table to the
	// first two.
	lkey, rkey, ok := equalityBetween(ss.GetWhereClause(),
		[]string{inner.Join.LeftAlias, inner.Join.RightAlias}, []string{calias})
	if !ok {
		return nil, fmt.Errorf("%w: a comma join of three tables needs an equality between the third table and the first two", errUnsupported)
	}
	cols, out, aggs, cases, err := selectTargets(ss.GetTargetList())
	if err != nil {
		return nil, err
	}
	// The WHERE clause's conditions are split between the two joins: the ones
	// that name only the first join's tables are decided there, and the rest
	// are decided after the second join, against the whole row.
	innerWhere, outerWhere := splitJoinWhere(ss.GetWhereClause(), inner.Join.LeftAlias, inner.Join.RightAlias)
	inner.Where = innerWhere
	plan := &Plan{
		Kind:     StmtSelect,
		Table:    inner.Table,
		Where:    outerWhere,
		Cols:     cols,
		OutNames: out,
		Cases:    cases,
		Join: &joinSpec{
			Inner:       inner,
			Right:       ctable,
			RightSchema: cqual,
			RightAlias:  calias,
			LKey:        lkey,
			RKey:        rkey,
			Preserve:    PreserveNone,
		},
	}
	if err := applySelectModifiers(ss, plan, out, cols, aggs); err != nil {
		return nil, err
	}
	return plan, nil
}

// translateCommaJoinSelect translates a FROM clause that lists two tables:
// the cross product PostgreSQL's comma join is, with the equality that joins
// them taken from the WHERE clause.
//
// The equality is found by walking the WHERE clause's conjunctions for a
// comparison between two columns. Which of the two columns belongs to which
// side is not settled here -- a bare name could belong to either, and only the
// schemas know -- so the two names are carried in the order they were written
// and the planner resolves them against the live schemas.
func translateCommaJoinSelect(ss *pg_query.SelectStmt, lr, rr *pg_query.RangeVar) (*Plan, error) {
	if err := refuseSelectModifiers(ss); err != nil {
		return nil, err
	}
	ltable, lqual, limplicit, err := dmlTarget(lr)
	if err != nil {
		return nil, err
	}
	rtable, rqual, rimplicit, err := dmlTarget(rr)
	if err != nil {
		return nil, err
	}
	if limplicit || rimplicit {
		return nil, fmt.Errorf("%w: the implicit table cannot be joined", errUnsupported)
	}
	lkey, rkey, ok := commaJoinKey(ss.GetWhereClause())
	if !ok {
		return nil, fmt.Errorf("%w: a comma join needs an equality between the two tables in its WHERE clause", errUnsupported)
	}
	lalias := lr.GetAlias().GetAliasname()
	if lalias == "" {
		lalias = ltable
	}
	ralias := rr.GetAlias().GetAliasname()
	if ralias == "" {
		ralias = rtable
	}
	if strings.EqualFold(lalias, ralias) {
		return nil, fmt.Errorf("%w: table name %q specified more than once", errUnsupported, lalias)
	}
	cols, out, aggs, cases, err := selectTargets(ss.GetTargetList())
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Kind:     StmtSelect,
		Table:    ltable,
		Where:    ss.GetWhereClause(),
		Cols:     cols,
		OutNames: out,
		Cases:    cases,
		Join: &joinSpec{
			Left: ltable, Right: rtable,
			LeftSchema: lqual, RightSchema: rqual,
			LeftAlias: lalias, RightAlias: ralias,
			LKey: lkey, RKey: rkey,
			Preserve: PreserveNone,
		},
	}
	if err := applySelectModifiers(ss, plan, out, cols, aggs); err != nil {
		return nil, err
	}
	return plan, nil
}

// translateThreeTableSelect translates a FROM clause that joins three tables:
// one table, and a join of two more (`FROM a, b LEFT JOIN c ON ...`).
//
// The statement is planned as a chain of two joins: the first joins the two
// tables the WHERE clause joins -- the comma join -- and the second joins the
// third table to their result. The chain is what the executor runs: a join
// whose left side is itself a join.
//
// The reordering is sound for the shapes it accepts. The comma join is an
// inner join, so it only removes rows, and the join it precedes keeps every
// row it is given (adding NULLs where nothing matched) rather than removing
// any: the two orders therefore keep the same rows, and the join conditions
// name the same columns in both.
func translateThreeTableSelect(ss *pg_query.SelectStmt, ra *pg_query.RangeVar, je *pg_query.JoinExpr) (*Plan, error) {
	if err := refuseSelectModifiers(ss); err != nil {
		return nil, err
	}
	if je.GetIsNatural() {
		return nil, fmt.Errorf("%w: NATURAL JOIN is not supported", errUnsupported)
	}
	if je.GetLarg().GetJoinExpr() != nil || je.GetRarg().GetJoinExpr() != nil {
		return nil, fmt.Errorf("%w: nested joins are not supported", errUnsupported)
	}
	rb := je.GetLarg().GetRangeVar()
	rc := je.GetRarg().GetRangeVar()
	if rb == nil || rc == nil {
		return nil, fmt.Errorf("%w: JOIN sides must be simple tables", errUnsupported)
	}
	var preserve JoinPreserve
	switch je.GetJointype() {
	case pg_query.JoinType_JOIN_INNER:
		preserve = PreserveNone
	case pg_query.JoinType_JOIN_LEFT:
		preserve = PreserveLeft
	case pg_query.JoinType_JOIN_RIGHT:
		preserve = PreserveRight
	case pg_query.JoinType_JOIN_FULL:
		preserve = PreserveBoth
	default:
		return nil, fmt.Errorf("%w: only INNER, LEFT, RIGHT and FULL JOIN are supported", errUnsupported)
	}
	if len(je.GetUsingClause()) > 0 {
		return nil, fmt.Errorf("%w: JOIN ... USING is not supported", errUnsupported)
	}
	if je.GetQuals() == nil {
		return nil, fmt.Errorf("%w: JOIN requires an ON condition", errUnsupported)
	}
	// The first join's two tables: the one the FROM lists and the left side
	// of the join. The equality that joins them is the one the WHERE clause
	// carries, because that is how a comma join is written.
	inner, err := translateInnerJoin(ss, ra, rb)
	if err != nil {
		return nil, err
	}
	// The second join's key is the ON clause's equality, and the rest of the
	// ON clause rides along as the match filter.
	key, onRest, ok := joinOnEquality(je.GetQuals())
	if !ok {
		return nil, fmt.Errorf("%w: ON must be a single equality between two columns", errUnsupported)
	}
	ctable, cqual, cimplicit, err := dmlTarget(rc)
	if err != nil {
		return nil, err
	}
	if cimplicit {
		return nil, fmt.Errorf("%w: the implicit table cannot be joined", errUnsupported)
	}
	calias := rc.GetAlias().GetAliasname()
	if calias == "" {
		calias = ctable
	}
	cols, out, aggs, cases, err := selectTargets(ss.GetTargetList())
	if err != nil {
		return nil, err
	}
	// The WHERE clause's conditions are split between the two joins: the ones
	// that name only the first join's tables are decided there, and the rest
	// are decided after the second join, against the whole row.
	innerWhere, outerWhere := splitJoinWhere(ss.GetWhereClause(), inner.Join.LeftAlias, inner.Join.RightAlias)
	inner.Where = innerWhere
	plan := &Plan{
		Kind:     StmtSelect,
		Table:    inner.Table,
		Where:    outerWhere,
		Cols:     cols,
		OutNames: out,
		Cases:    cases,
		Join: &joinSpec{
			Inner:       inner,
			Right:       ctable,
			RightSchema: cqual,
			RightAlias:  calias,
			LKey:        key.l,
			RKey:        key.r,
			OnRest:      onRest,
			Preserve:    preserve,
		},
	}
	if err := applySelectModifiers(ss, plan, out, cols, aggs); err != nil {
		return nil, err
	}
	return plan, nil
}

// translateInnerJoin builds the plan of the first join of a three-table
// statement: the two tables the WHERE clause joins, projected as they are.
//
// The projection is the whole merged row, because the second join reads the
// first join's result as its left side.
func translateInnerJoin(ss *pg_query.SelectStmt, lr, rr *pg_query.RangeVar) (*Plan, error) {
	ltable, lqual, limplicit, err := dmlTarget(lr)
	if err != nil {
		return nil, err
	}
	rtable, rqual, rimplicit, err := dmlTarget(rr)
	if err != nil {
		return nil, err
	}
	if limplicit || rimplicit {
		return nil, fmt.Errorf("%w: the implicit table cannot be joined", errUnsupported)
	}
	lalias := lr.GetAlias().GetAliasname()
	if lalias == "" {
		lalias = ltable
	}
	ralias := rr.GetAlias().GetAliasname()
	if ralias == "" {
		ralias = rtable
	}
	if strings.EqualFold(lalias, ralias) {
		return nil, fmt.Errorf("%w: table name %q specified more than once", errUnsupported, lalias)
	}
	// The key is the equality that joins these two tables. The columns are
	// named with the tables' qualifiers, because a statement that joins
	// three tables has an equality between each pair of them, and only the
	// qualifier says which pair an equality joins.
	lkey, rkey, ok := equalityBetween(ss.GetWhereClause(), []string{lalias}, []string{ralias})
	if !ok {
		return nil, fmt.Errorf("%w: a comma join needs an equality between the two tables in its WHERE clause", errUnsupported)
	}
	return &Plan{
		Kind:        StmtSelect,
		Table:       ltable,
		TableSchema: lqual,
		Cols:        []string{"*"},
		Join: &joinSpec{
			Left: ltable, Right: rtable,
			LeftSchema: lqual, RightSchema: rqual,
			LeftAlias: lalias, RightAlias: ralias,
			LKey: lkey, RKey: rkey,
			Preserve: PreserveNone,
		},
	}, nil
}

// splitJoinWhere divides a WHERE clause between the two joins of a
// three-table statement: the conditions that name only the first join's
// tables are decided by that join, and the rest by the second.
//
// A condition naming a column of the third table cannot be decided by the
// first join, which does not have that table; a condition naming a bare
// column could name any of the three, so it is left to the second join,
// which has them all. The split is by qualifier, which is what the statement
// wrote: a column of the first join's tables is written with that table's
// name or its alias.
func splitJoinWhere(where *pg_query.Node, leftAlias, rightAlias string) (inner, outer *pg_query.Node) {
	if where == nil {
		return nil, nil
	}
	conjuncts := []*pg_query.Node{where}
	if be := where.GetBoolExpr(); be != nil && be.GetBoolop() == pg_query.BoolExprType_AND_EXPR {
		conjuncts = be.GetArgs()
	}
	var innerArgs, outerArgs []*pg_query.Node
	for _, arg := range conjuncts {
		if conditionNamesOnly(arg, leftAlias, rightAlias) {
			innerArgs = append(innerArgs, arg)
			continue
		}
		outerArgs = append(outerArgs, arg)
	}
	return andNode(innerArgs), andNode(outerArgs)
}

// conditionNamesOnly reports whether every column a condition names belongs to
// one of the two qualifiers given.
//
// A bare column names a column of whichever table has it, so it is never
// known to name only the two tables: the condition is left to the join that
// has all of them.
func conditionNamesOnly(node *pg_query.Node, qualifiers ...string) bool {
	found := false
	ok := walkParseColumns(node, func(name string) bool {
		i := strings.IndexByte(name, '.')
		if i <= 0 {
			// A bare column: it may name a column of any table.
			return false
		}
		q := name[:i]
		for _, qual := range qualifiers {
			if strings.EqualFold(q, qual) {
				found = true
				return true
			}
		}
		return false
	})
	return ok && found
}

// walkParseColumns calls fn for every column reference in a parse tree, and
// reports whether every one of them was accepted.
//
// The walk covers the expression kinds a condition can be built from: a
// comparison, a boolean combination, a null test, a LIKE, a function call and
// a cast. A kind the walk does not know makes the condition's columns
// unknown, which sends the condition to the join that has every table.
func walkParseColumns(node *pg_query.Node, fn func(name string) bool) bool {
	if node == nil {
		return true
	}
	if col, ok := plainColumn(node); ok {
		return fn(col)
	}
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			if !walkParseColumns(arg, fn) {
				return false
			}
		}
		return true
	}
	if ae := node.GetAExpr(); ae != nil {
		if !walkParseColumns(ae.GetLexpr(), fn) {
			return false
		}
		return walkParseColumns(ae.GetRexpr(), fn)
	}
	if nt := node.GetNullTest(); nt != nil {
		return walkParseColumns(nt.GetArg(), fn)
	}
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.GetArgs() {
			if !walkParseColumns(arg, fn) {
				return false
			}
		}
		return true
	}
	if tc := node.GetTypeCast(); tc != nil {
		return walkParseColumns(tc.GetArg(), fn)
	}
	return true
}

// andNode joins a list of conditions into one conjunction, or reports none.
func andNode(args []*pg_query.Node) *pg_query.Node {
	switch len(args) {
	case 0:
		return nil
	case 1:
		return args[0]
	}
	return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: &pg_query.BoolExpr{
		Boolop: pg_query.BoolExprType_AND_EXPR,
		Args:   args,
	}}}
}

// commaJoinKey finds the equality that joins the two tables of a comma join:
// the first conjunction of the WHERE clause that compares two columns. The
// comparison is read from the parse tree rather than from the expression tree,
// because the expression tree resolves a column against a schema and the
// schema is not known yet.
func commaJoinKey(where *pg_query.Node) (string, string, bool) {
	if where == nil {
		return "", "", false
	}
	// The conjunctions of the WHERE clause: the whole clause when it is a
	// single condition, the AND's arguments when it is a conjunction.
	conjuncts := []*pg_query.Node{where}
	if be := where.GetBoolExpr(); be != nil && be.GetBoolop() == pg_query.BoolExprType_AND_EXPR {
		conjuncts = be.GetArgs()
	}
	for _, arg := range conjuncts {
		ae := arg.GetAExpr()
		if ae == nil || ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
			continue
		}
		names := ae.GetName()
		if len(names) != 1 || names[0].GetString_() == nil || names[0].GetString_().GetSval() != "=" {
			continue
		}
		left, ok := plainColumn(ae.GetLexpr())
		if !ok {
			continue
		}
		right, ok := plainColumn(ae.GetRexpr())
		if !ok {
			continue
		}
		return left, right, true
	}
	return "", "", false
}

// equalityBetween finds in a WHERE clause an equality between a column of one
// of the left qualifiers and a column of one of the right qualifiers, and
// reports the two column names in the order the qualifiers were given.
//
// The qualifiers are the names the statement may use for a table: its alias,
// or its name when it has none. A bare column names a column of whichever
// table has it, so it is not a match: which table it belongs to is a question
// only the schemas can answer.
func equalityBetween(where *pg_query.Node, leftQuals, rightQuals []string) (string, string, bool) {
	if where == nil {
		return "", "", false
	}
	conjuncts := []*pg_query.Node{where}
	if be := where.GetBoolExpr(); be != nil && be.GetBoolop() == pg_query.BoolExprType_AND_EXPR {
		conjuncts = be.GetArgs()
	}
	for _, arg := range conjuncts {
		ae := arg.GetAExpr()
		if ae == nil || ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
			continue
		}
		names := ae.GetName()
		if len(names) != 1 || names[0].GetString_() == nil || names[0].GetString_().GetSval() != "=" {
			continue
		}
		left, ok := plainColumn(ae.GetLexpr())
		if !ok {
			continue
		}
		right, ok := plainColumn(ae.GetRexpr())
		if !ok {
			continue
		}
		lq, lok := qualifierOf(left)
		rq, rok := qualifierOf(right)
		if !lok || !rok {
			continue
		}
		if hasQualifier(leftQuals, lq) && hasQualifier(rightQuals, rq) {
			return left, right, true
		}
		if hasQualifier(leftQuals, rq) && hasQualifier(rightQuals, lq) {
			return right, left, true
		}
	}
	return "", "", false
}

// qualifierOf splits a qualified column name into its table and its column.
func qualifierOf(name string) (string, bool) {
	i := strings.IndexByte(name, '.')
	if i <= 0 || i == len(name)-1 {
		return "", false
	}
	return name[:i], true
}

// hasQualifier reports whether a qualifier is one of the ones given.
func hasQualifier(quals []string, q string) bool {
	for _, qual := range quals {
		if strings.EqualFold(qual, q) {
			return true
		}
	}
	return false
}

// joinOnEquality finds the equality a join's ON clause is hashed on among the
// clause's conjuncts, and reports the rest of the clause.
//
// The equality is the one that compares two columns of the two sides. A
// conjunct that compares a column with a literal is a condition on the
// matched pair rather than the key: it cannot narrow the hash, but it does
// decide which pairs are joined.
func joinOnEquality(quals *pg_query.Node) (key struct{ l, r string }, rest *pg_query.Node, ok bool) {
	conjuncts := []*pg_query.Node{quals}
	if be := quals.GetBoolExpr(); be != nil && be.GetBoolop() == pg_query.BoolExprType_AND_EXPR {
		conjuncts = be.GetArgs()
	}
	var others []*pg_query.Node
	for _, arg := range conjuncts {
		ae := arg.GetAExpr()
		if ae == nil || ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
			others = append(others, arg)
			continue
		}
		names := ae.GetName()
		if len(names) != 1 || names[0].GetString_() == nil || names[0].GetString_().GetSval() != "=" {
			others = append(others, arg)
			continue
		}
		left, lok := plainColumn(ae.GetLexpr())
		right, rok := plainColumn(ae.GetRexpr())
		if !lok || !rok {
			others = append(others, arg)
			continue
		}
		if ok {
			// A second equality is a condition on the matched pair, not a
			// second key: the hash join is built on one.
			others = append(others, arg)
			continue
		}
		key.l, key.r = left, right
		ok = true
	}
	if !ok {
		return key, nil, false
	}
	if len(others) == 0 {
		return key, nil, true
	}
	if len(others) == 1 {
		return key, others[0], true
	}
	return key, &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: &pg_query.BoolExpr{
		Boolop: pg_query.BoolExprType_AND_EXPR,
		Args:   others,
	}}}, true
}

// two columns, and a filter on key. Kept separate so the catalog path does not
// inherit the implicit table's restrictions by accident.
func checkImplicitSelect(plan *Plan, whereCol string) error {
	if whereCol != colKey {
		return fmt.Errorf("%w: only the key column may be filtered", errUnsupported)
	}
	if plan.grouped() {
		return fmt.Errorf("%w: the implicit table does not support aggregates or GROUP BY", errUnsupported)
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
	table, qual, implicit, err := dmlTarget(is.GetRelation())
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
	// Only a statement that named its columns has two lists to compare. Without
	// a column list the values are positional and names is nil, so comparing its
	// length would report "names 0 columns but supplies N values" and refuse the
	// ordinary INSERT INTO t VALUES (...). The arity is checked against the
	// schema in buildCells instead, which is the only place the column count is
	// actually known.
	if names != nil && len(names) != len(items) {
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
	return &Plan{Kind: StmtInsert, Table: table, TableSchema: qual, Columns: names, Values: refs}, nil
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
	table, qual, implicit, err := dmlTarget(us.GetRelation())
	if err != nil {
		return nil, err
	}
	if us.GetReturningList() != nil {
		return nil, fmt.Errorf("%w: RETURNING is not supported", errUnsupported)
	}
	if len(us.GetFromClause()) > 0 {
		return nil, fmt.Errorf("%w: UPDATE ... FROM is not supported", errUnsupported)
	}
	whereCol, key, err := rowPredicateFor(table, implicit, us.GetWhereClause())
	if err != nil {
		return nil, err
	}
	if key.Null {
		return nil, &pgError{code: errNotNullViolation, msg: "null value in the filter violates not-null constraint"}
	}
	plan := &Plan{Kind: StmtUpdate, Table: table, TableSchema: qual, WhereCol: whereCol, Key: key,
		Where: us.GetWhereClause()}

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
	table, qual, implicit, err := dmlTarget(ds.GetRelation())
	if err != nil {
		return nil, err
	}
	if ds.GetReturningList() != nil {
		return nil, fmt.Errorf("%w: RETURNING is not supported", errUnsupported)
	}
	if len(ds.GetUsingClause()) > 0 {
		return nil, fmt.Errorf("%w: DELETE ... USING is not supported", errUnsupported)
	}
	whereCol, key, err := rowPredicateFor(table, implicit, ds.GetWhereClause())
	if err != nil {
		return nil, err
	}
	if key.Null {
		return nil, &pgError{code: errNotNullViolation, msg: "null value in the filter violates not-null constraint"}
	}
	if implicit && whereCol != colKey {
		return nil, fmt.Errorf("%w: only the key column may be filtered", errUnsupported)
	}
	return &Plan{Kind: StmtDelete, Table: table, TableSchema: qual, WhereCol: whereCol, Key: key,
		Where: ds.GetWhereClause()}, nil
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

// translateVariableSet reads SET / SET SESSION / SET LOCAL / RESET / RESET
// ALL / SET x = DEFAULT into a gucPlan. Every variable name is accepted: this
// engine has no settings catalog to validate against, and a client that sets
// an unknown name must still see the statement succeed, exactly as PostgreSQL
// accepts a placeholder for an unrecognised custom GUC. Unknown names are
// simply never read back except by SHOW.
func translateVariableSet(vs *pg_query.VariableSetStmt) (*Plan, error) {
	switch vs.GetKind() {
	case pg_query.VariableSetKind_VAR_RESET_ALL:
		return &Plan{Kind: StmtSet, GUC: &gucPlan{IsReset: true}}, nil
	case pg_query.VariableSetKind_VAR_RESET:
		return &Plan{Kind: StmtSet, GUC: &gucPlan{Name: vs.GetName(), IsReset: true}}, nil
	case pg_query.VariableSetKind_VAR_SET_VALUE:
		val, err := gucValue(vs.GetArgs())
		if err != nil {
			return nil, err
		}
		return &Plan{Kind: StmtSet, GUC: &gucPlan{
			Name:     vs.GetName(),
			Value:    val,
			HasValue: true,
			IsLocal:  vs.GetIsLocal(),
		}}, nil
	case pg_query.VariableSetKind_VAR_SET_DEFAULT:
		return &Plan{Kind: StmtSet, GUC: &gucPlan{Name: vs.GetName(), IsReset: true}}, nil
	case pg_query.VariableSetKind_VAR_SET_CURRENT:
		// SET x TO CURRENT_USER and friends: PostgreSQL resolves the keyword
		// to a value we do not model. Accepting it silently would store the
		// wrong string, so it is refused rather than guessed at.
		return nil, fmt.Errorf("%w: SET ... TO CURRENT is not supported", errUnsupported)
	case pg_query.VariableSetKind_VAR_SET_MULTI:
		pairs, err := gucPairs(vs.GetArgs())
		if err != nil {
			return nil, err
		}
		return &Plan{Kind: StmtSet, GUC: &gucPlan{Multi: pairs, IsLocal: vs.GetIsLocal()}}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported SET form", errUnsupported)
	}
}

// gucPairs reads a multi-variable SET's arguments, which are DefElem nodes each
// naming one variable and carrying its value.
func gucPairs(args []*pg_query.Node) ([]gucPair, error) {
	if len(args) == 0 {
		return nil, nil
	}
	pairs := make([]gucPair, 0, len(args))
	for _, a := range args {
		de := a.GetDefElem()
		if de == nil {
			return nil, fmt.Errorf("%w: unsupported SET CHARACTERISTICS form", errUnsupported)
		}
		val, err := gucNodeValue(de.GetArg())
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, gucPair{Name: de.GetDefname(), Value: val})
	}
	return pairs, nil
}

// gucValue renders a SET's value argument as text. A SET carries a
// comma-separated list of A_Const or TypeCast nodes; only the single-value
// form a client uses for a plain assignment is accepted. Booleans arrive as
// bare identifiers (SET x = on), which the parser hands back as a TypeCast-less
// ColumnRef, so that shape is read here too.
func gucValue(args []*pg_query.Node) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	if len(args) > 1 {
		return "", fmt.Errorf("%w: SET accepts a single value", errUnsupported)
	}
	return gucNodeValue(args[0])
}

// gucNodeValue renders one SET value node as the text the variable stores.
func gucNodeValue(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", nil
	}
	if c := node.GetAConst(); c != nil {
		return aConstText(c)
	}
	if tc := node.GetTypeCast(); tc != nil {
		inner, err := gucNodeValue(tc.GetArg())
		if err != nil {
			return "", err
		}
		// A cast such as 'on'::boolean renders to its inner literal; the
		// declared target type only matters for validation this engine does
		// not do.
		return inner, nil
	}
	if cr := node.GetColumnRef(); cr != nil {
		// SET x = on / off / true / false arrive as bare identifiers.
		if fields := cr.GetFields(); len(fields) == 1 {
			if s := fields[0].GetString_(); s != nil {
				return s.GetSval(), nil
			}
		}
	}
	return "", fmt.Errorf("%w: unsupported SET value expression", errUnsupported)
}

// translateVariableShow reads SHOW <name> / SHOW ALL into a gucPlan. SHOW ALL
// returns PostgreSQL's three columns (name, setting, description); SHOW <name>
// returns a single column named after the variable.
func translateVariableShow(vs *pg_query.VariableShowStmt) (*Plan, error) {
	name := vs.GetName()
	if strings.EqualFold(name, "all") {
		return &Plan{Kind: StmtShow, GUC: &gucPlan{IsShow: true},
			Cols:    []string{"name", "setting", "description"},
			ColOIDs: []int32{oidText, oidText, oidText}}, nil
	}
	return &Plan{Kind: StmtShow, GUC: &gucPlan{Name: name, IsShow: true},
		Cols: []string{name}, ColOIDs: []int32{oidText}}, nil
}

// translateSelectExpr reads a FROM-less SELECT: a target list of constant and
// session-value expressions. It exists so a client's startup probe
// (SELECT 1, SELECT version(), SELECT current_schema()) is answered without a
// catalog. Any clause that would require a relation (WHERE, GROUP BY, ...) is
// refused, because there is nothing to read.
func translateSelectExpr(ss *pg_query.SelectStmt) (*Plan, error) {
	if len(ss.GetFromClause()) != 0 {
		return nil, fmt.Errorf("%w: SELECT WITH a relation must name a table", errUnsupported)
	}
	if ss.GetWhereClause() != nil || len(ss.GetGroupClause()) != 0 ||
		ss.GetHavingClause() != nil || len(ss.GetSortClause()) != 0 ||
		len(ss.GetDistinctClause()) != 0 || ss.GetLimitCount() != nil ||
		ss.GetLimitOffset() != nil || len(ss.GetWindowClause()) != 0 ||
		ss.GetWithClause() != nil {
		return nil, fmt.Errorf("%w: a FROM-less SELECT supports only a target list", errUnsupported)
	}
	targets := ss.GetTargetList()
	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: SELECT has no output columns", errUnsupported)
	}
	exprs := make([]*selectExpr, 0, len(targets))
	cols := make([]string, 0, len(targets))
	oids := make([]int32, 0, len(targets))
	for _, t := range targets {
		rt := t.GetResTarget()
		if rt == nil {
			return nil, fmt.Errorf("%w: unsupported SELECT target", errUnsupported)
		}
		ex, err := selectExprOf(rt)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, ex)
		cols = append(cols, ex.Name)
		oids = append(oids, ex.OID)
	}
	return &Plan{Kind: StmtSelectExpr, Exprs: exprs, Cols: cols, ColOIDs: oids}, nil
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

// translateExplain wraps a statement in a plan request.
//
// EXPLAIN ANALYZE is refused rather than accepted-and-ignored. Silently
// dropping the ANALYZE would report a plan for a query that never ran, and a
// client that asked to measure would believe it had measured; running it would
// need an execution path that returns more than one row. Both are worse than an
// error that names the reason.
func translateExplain(es *pg_query.ExplainStmt) (*Plan, error) {
	for _, opt := range es.GetOptions() {
		if d := opt.GetDefElem(); d != nil && d.GetDefname() == "analyze" {
			return nil, fmt.Errorf("%w: EXPLAIN ANALYZE would have to execute the statement, and this phase cannot return a multi-row result; use EXPLAIN alone", errUnsupported)
		}
	}
	inner, err := explainInner(es.GetQuery())
	if err != nil {
		return nil, err
	}
	// The projection is fixed: one text column. Declaring it here rather than
	// leaving Cols empty matters because RowDescription reports len(Cols) as the
	// field count, while the rendered plan is one cell wide -- a mismatch a real
	// client rejects outright ("unexpected field count in D message"), and one
	// no in-process test can see, because it never parses the description.
	return &Plan{Kind: StmtExplain, Inner: inner, Cols: []string{"QUERY PLAN"}}, nil
}

// explainInner translates the node an EXPLAIN wraps. EXPLAIN of anything but a
// data command is refused, because the plan being asked for is a row-access
// plan and there is no such thing for DDL or a transaction.
func explainInner(n *pg_query.Node) (*Plan, error) {
	switch {
	case n.GetSelectStmt() != nil:
		return translateSelect(n.GetSelectStmt())
	case n.GetInsertStmt() != nil:
		return translateInsert(n.GetInsertStmt())
	case n.GetUpdateStmt() != nil:
		return translateUpdate(n.GetUpdateStmt())
	case n.GetDeleteStmt() != nil:
		return translateDelete(n.GetDeleteStmt())
	}
	return nil, fmt.Errorf("%w: EXPLAIN supports SELECT, INSERT, UPDATE and DELETE", errUnsupported)
}

// translateVacuum accepts ANALYZE and refuses VACUUM.
//
// Both arrive as the same node, distinguished by IsVacuumcmd. VACUUM reclaims
// space this layout does not have -- there is no free-list to compact, because
// a delete removes keys outright -- so accepting it would promise something the
// store cannot do. ANALYZE reads statistics and is the half that is real.
func translateVacuum(vs *pg_query.VacuumStmt) (*Plan, error) {
	if vs.GetIsVacuumcmd() {
		return nil, fmt.Errorf("%w: VACUUM is not supported; this layout deletes keys outright and has nothing to reclaim", errUnsupported)
	}
	plan := &Plan{Kind: StmtAnalyze}
	for _, r := range vs.GetRels() {
		vr := r.GetVacuumRelation()
		if vr == nil {
			continue
		}
		name := vr.GetRelation().GetRelname()
		if err := ValidateTableName(name); err != nil {
			return nil, err
		}
		plan.AnalyzeTables = append(plan.AnalyzeTables, name)
	}
	return plan, nil
}
