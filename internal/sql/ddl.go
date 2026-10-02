/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: ddl.go
Description: Translates CREATE TABLE and DROP TABLE parse trees into the
catalog operations that carry them out. Translation is separated from execution
because it is pure: the parse tree decides what was asked for, and the catalog
decides whether it can be done, so a rejected CREATE TABLE never reaches the
store.

The supported shape is deliberately narrow, and each rejection names what is
missing rather than reporting a generic syntax error. A client that gets
"composite PRIMARY KEY is not supported" can act on it; one that gets
"syntax error at or near ..." cannot.
*/
package sql

import (
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// translateCreateTable maps a CreateStmt onto a Schema. It reports the schema
// plus whether IF NOT EXISTS was written, which the caller needs because the
// two cases differ only after the catalog is consulted: without the clause a
// duplicate is an error, with it the statement succeeds as a notice.
func translateCreateTable(st *pg_query.CreateStmt) (*Schema, bool, error) {
	if st == nil {
		return nil, false, errUnsupported
	}
	if st.GetAccessMethod() != "" && !strings.EqualFold(st.GetAccessMethod(), "heap") {
		// A USING clause names a storage engine this layout has no equivalent
		// for; accepting and ignoring it would store rows in a way the client
		// did not ask for.
		return nil, false, fmt.Errorf("%w: USING %s is not supported", errUnsupported, st.GetAccessMethod())
	}
	if len(st.GetInhRelations()) > 0 {
		return nil, false, fmt.Errorf("%w: table inheritance is not supported", errUnsupported)
	}
	if len(st.GetOptions()) > 0 {
		// WITH (...) names storage parameters. Accepting and ignoring them
		// would let a client believe it had tuned storage.
		return nil, false, fmt.Errorf("%w: table options are not supported", errUnsupported)
	}
	if st.GetOfTypename() != nil {
		return nil, false, fmt.Errorf("%w: CREATE TABLE ... OF is not supported", errUnsupported)
	}
	rv := st.GetRelation()
	if rv == nil {
		return nil, false, fmt.Errorf("%w: CREATE TABLE without a table name", errUnsupported)
	}
	switch rv.GetRelpersistence() {
	case "", "p":
	case "t":
		// TEMP and UNLOGGED tables would need a session and a durability
		// distinction the catalog has nowhere to record.
		return nil, false, fmt.Errorf("%w: TEMP tables are not supported", errUnsupported)
	case "u":
		return nil, false, fmt.Errorf("%w: UNLOGGED tables are not supported", errUnsupported)
	default:
		return nil, false, fmt.Errorf("%w: table persistence %q is not supported", errUnsupported, rv.GetRelpersistence())
	}

	s := &Schema{DB: DefaultDB, Table: rv.GetRelname(), PrimaryKey: -1}
	if rv.GetSchemaname() != "" && !strings.EqualFold(rv.GetSchemaname(), DefaultDB) &&
		!strings.EqualFold(rv.GetSchemaname(), "public") {
		// Phase 9 has one database subtree, so an explicitly qualified name
		// must name that subtree or it is asking for storage that does not
		// exist. Silently dropping the qualifier would let two clients think
		// they have separate tables when they share one.
		return nil, false, fmt.Errorf("%w: schema %q is not available; only %s exists", errUnsupported, rv.GetSchemaname(), DefaultDB)
	}
	if isReservedCatalogTable(s.Table) {
		return nil, false, fmt.Errorf("%w: %q names a reserved table", errUnsupported, s.Table)
	}

	// A primary key can arrive two ways: inline on the column, or as a
	// separate table constraint. Both set the same field, and a table that
	// declares it twice is rejected rather than one silently winning.
	inlinePK := -1
	tablePK := -1

	for _, elt := range st.GetTableElts() {
		switch {
		case elt.GetColumnDef() != nil:
			col, idx, err := translateColumnDef(elt.GetColumnDef(), len(s.Columns))
			if err != nil {
				return nil, false, err
			}
			if idx >= 0 {
				if inlinePK >= 0 {
					return nil, false, fmt.Errorf("%w: more than one PRIMARY KEY", ErrCompositePrimaryKey)
				}
				inlinePK = idx
			}
			s.Columns = append(s.Columns, col)
		case elt.GetConstraint() != nil:
			idx, err := translateTableConstraint(elt.GetConstraint(), s.Columns)
			if err != nil {
				return nil, false, err
			}
			if idx >= 0 {
				if tablePK >= 0 || inlinePK >= 0 {
					return nil, false, fmt.Errorf("%w: more than one PRIMARY KEY", ErrCompositePrimaryKey)
				}
				tablePK = idx
			}
		case elt.GetIndexStmt() != nil:
			return nil, false, fmt.Errorf("%w: indexes are not supported in phase 9", errUnsupported)
		default:
			return nil, false, fmt.Errorf("%w: unsupported table element", errUnsupported)
		}
	}

	switch {
	case len(s.Columns) == 0:
		return nil, false, fmt.Errorf("%w: CREATE TABLE with no columns", errUnsupported)
	case tablePK >= 0:
		s.PrimaryKey = tablePK
	case inlinePK >= 0:
		s.PrimaryKey = inlinePK
	default:
		// The primary key is the row id (ADR-013 decision 2), so a table
		// without one has no way to address a row. This is the single most
		// likely reason a client's first CREATE TABLE is refused, so the
		// message says what to do about it.
		return nil, false, fmt.Errorf("%w: every table needs one", ErrNoPrimaryKey)
	}
	s.Columns[s.PrimaryKey].Nullable = false
	// Validate here rather than at Create time: the catalog would report the
	// same failure, but by then the message no longer names the statement the
	// client sent, and a duplicate column is a property of the DDL text.
	if err := s.Validate(); err != nil {
		return nil, false, err
	}
	return s, st.GetIfNotExists(), nil
}

// translateColumnDef maps one column definition, returning the column and its
// index if the definition declared the primary key inline (idx >= 0), or -1
// when it did not.
func translateColumnDef(cd *pg_query.ColumnDef, ordinal int) (Column, int, error) {
	name := cd.GetColname()
	if err := ValidateColumnName(name); err != nil {
		return Column{}, -1, err
	}
	typ, err := columnTypeOf(cd.GetTypeName())
	if err != nil {
		return Column{}, -1, fmt.Errorf("column %q: %w", name, err)
	}
	// A column with a serial type or an identity clause would need a sequence
	// to draw ids from. The row id is generated from the proposer's TSO instead
	// (guardrail 4), so accepting a serial would imply a second id source that
	// does not exist.
	if cd.GetIdentity() != "" {
		return Column{}, -1, fmt.Errorf("%w: column %q: identity columns are not supported", errUnsupported, name)
	}
	if cd.GetGenerated() != "" {
		return Column{}, -1, fmt.Errorf("%w: column %q: generated columns are not supported", errUnsupported, name)
	}
	if cd.GetIsFromType() {
		return Column{}, -1, fmt.Errorf("%w: column %q: table inheritance is not supported", errUnsupported, name)
	}

	col := Column{Name: name, Type: typ, Nullable: !cd.GetIsNotNull()}
	pk := -1

	var sawDefault bool
	for _, con := range cd.GetConstraints() {
		c := con.GetConstraint()
		if c == nil {
			continue
		}
		switch c.GetContype() {
		case pg_query.ConstrType_CONSTR_PRIMARY:
			pk = ordinal
			col.Nullable = false
		case pg_query.ConstrType_CONSTR_NOTNULL:
			col.Nullable = false
		case pg_query.ConstrType_CONSTR_DEFAULT:
			// A column DEFAULT reaches the raw parse tree as a constraint, not
			// on ColumnDef.RawDefault: that field is filled in later by the
			// analyser. Reading the wrong field would silently drop every
			// default the client declared, which is why this is read here.
			if sawDefault {
				return Column{}, -1, fmt.Errorf("%w: column %q: more than one DEFAULT", errUnsupported, name)
			}
			sawDefault = true
			def, err := defaultValueOf(typ, name, c.GetRawExpr())
			if err != nil {
				return Column{}, -1, err
			}
			col.Default = def
		case pg_query.ConstrType_CONSTR_NULL:
			col.Nullable = true
		case pg_query.ConstrType_CONSTR_UNIQUE, pg_query.ConstrType_CONSTR_CHECK:
			// UNIQUE and CHECK cannot be enforced by this key layout: a
			// uniqueness check would need a second index and a CHECK would need
			// an expression evaluator. Neither can be stored in the catalog and
			// honoured later, so accepting them would make the catalog a record
			// of constraints the engine does not apply.
			return Column{}, -1, fmt.Errorf("%w: column %q: %s constraints are not supported", errUnsupported, name, constraintName(c.GetContype()))
		case pg_query.ConstrType_CONSTR_FOREIGN, pg_query.ConstrType_CONSTR_EXCLUSION:
			return Column{}, -1, fmt.Errorf("%w: column %q: %s constraints are not supported", errUnsupported, name, constraintName(c.GetContype()))
		}
	}

	return col, pk, nil
}

// translateTableConstraint maps a table-level constraint, returning the column
// index it designates as the primary key, or -1 if it is not a primary key the
// layout can use.
func translateTableConstraint(c *pg_query.Constraint, declared []Column) (int, error) {
	switch c.GetContype() {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		keys := c.GetKeys()
		if len(keys) == 0 {
			// A PRIMARY KEY with no key list only happens for the degenerate
			// forms the parser allows; it cannot name a row.
			return -1, fmt.Errorf("%w: PRIMARY KEY with no columns", ErrBadSchema)
		}
		if len(keys) > 1 {
			// A composite key would need a single row id encoding a tuple, and
			// the order-preserving integer encoding in layout.go is defined for
			// scalars. Refusing is cheaper than inventing a tuple encoding now
			// and having to keep it forever.
			return -1, fmt.Errorf("%w: got %d columns", ErrCompositePrimaryKey, len(keys))
		}
		name := ""
		if str := keys[0].GetString_(); str != nil {
			name = str.GetSval()
		}
		// A quoted key preserves the client's capitalisation, which the
		// parser hands back verbatim; match the declared column exactly first
		// and only then case-insensitively.
		for i := range declared {
			if declared[i].Name == name {
				return i, nil
			}
		}
		for i := range declared {
			if strings.EqualFold(declared[i].Name, name) {
				return i, nil
			}
		}
		return -1, fmt.Errorf("%w: PRIMARY KEY references undeclared column %q", ErrBadSchema, name)
	case pg_query.ConstrType_CONSTR_UNIQUE, pg_query.ConstrType_CONSTR_CHECK,
		pg_query.ConstrType_CONSTR_FOREIGN, pg_query.ConstrType_CONSTR_EXCLUSION:
		return -1, fmt.Errorf("%w: %s constraints are not supported", errUnsupported, constraintName(c.GetContype()))
	}
	return -1, nil
}

func constraintName(t pg_query.ConstrType) string {
	switch t {
	case pg_query.ConstrType_CONSTR_UNIQUE:
		return "UNIQUE"
	case pg_query.ConstrType_CONSTR_CHECK:
		return "CHECK"
	case pg_query.ConstrType_CONSTR_FOREIGN:
		return "FOREIGN KEY"
	case pg_query.ConstrType_CONSTR_EXCLUSION:
		return "EXCLUDE"
	case pg_query.ConstrType_CONSTR_PRIMARY:
		return "PRIMARY KEY"
	}
	return "UNKNOWN"
}

// serialTypes are the auto-increment spellings PostgreSQL expands into a column
// plus a sequence.
var serialTypes = map[string]bool{
	"serial": true, "bigserial": true, "smallserial": true, "serial2": true,
	"serial4": true, "serial8": true,
}

// outOfScopeTypes are types that exist in SQL and are recognised as such but
// have no Phase 9 encoding. They are listed so the error can say "not supported
// in phase 9" instead of implying the type was never heard of, which would read
// as a typo when it is a real gap.
var outOfScopeTypes = map[string]bool{
	"numeric": true, "decimal": true, "smallint": true, "int2": true,
	"date": true, "time": true, "uuid": true, "char": true, "bpchar": true,
	"interval": true, "money": true, "inet": true, "cidr": true, "xml": true,
	"jsonpath": true, "tsvector": true,
}

// columnTypeOf resolves a parsed type name onto a ColumnType, rejecting the
// shapes that look like a supported type but are not: arrays, setof, and a
// type carrying a type modifier that changes its meaning. varchar(10) is the
// one modifier accepted, and it is dropped, because this layout stores the
// encoded length rather than enforcing a declared maximum.
func columnTypeOf(tn *pg_query.TypeName) (ColumnType, error) {
	if tn == nil {
		return TypeInvalid, fmt.Errorf("%w: column has no type", ErrBadSchema)
	}
	if tn.GetSetof() {
		return TypeInvalid, fmt.Errorf("%w: SETOF types are not supported", errUnsupported)
	}
	if len(tn.GetArrayBounds()) > 0 {
		return TypeInvalid, fmt.Errorf("%w: array types are not supported", errUnsupported)
	}
	names := tn.GetNames()
	if len(names) == 0 {
		// The parser resolved the name to an OID without keeping the
		// identifier, which happens for types it knows only by oid.
		return TypeInvalid, fmt.Errorf("%w: column type %d is not recognised", errUnsupported, tn.GetTypeOid())
	}
	last := names[len(names)-1].GetString_()
	if last == nil {
		return TypeInvalid, fmt.Errorf("%w: column type is not a plain name", errUnsupported)
	}
	name := last.GetSval()
	// pg_catalog.int4 is how the parser spells a built-in; strip the qualifier
	// so it resolves through the same table as the unqualified spelling.
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	if _, isSerial := serialTypes[name]; isSerial {
		// serial expands to a column plus a sequence in PostgreSQL. Phase 9
		// draws generated row ids from the proposer's TSO instead
		// (guardrail 4), so a sequence-backed id would be a second, competing
		// source of row identity.
		return TypeInvalid, fmt.Errorf("%w: type %s is not supported; a PRIMARY KEY is generated from the proposer's TSO", errUnsupported, name)
	}
	typ, err := ParseColumnType(name)
	if err != nil {
		if _, known := outOfScopeTypes[name]; known {
			return TypeInvalid, fmt.Errorf("%w: type %s is not supported in phase 9", errUnsupported, name)
		}
		return TypeInvalid, err
	}
	if mod := tn.GetTypmods(); len(mod) > 0 && typ != TypeVarchar {
		// numeric(10,2) or timestamp(3) mean something specific that this
		// encoding does not carry, so accepting the type and dropping the
		// modifier would promise precision the store does not keep.
		return TypeInvalid, fmt.Errorf("%w: type modifier on %s is not supported", errUnsupported, name)
	}
	return typ, nil
}

// defaultValueOf encodes a column DEFAULT. Only literals and unary negation are
// accepted, because a default that depended on the current time or a sequence
// would have to be evaluated per insert on the proposer and would then be
// non-deterministic across replicas.
func defaultValueOf(typ ColumnType, column string, node *pg_query.Node) ([]byte, error) {
	if ac := node.GetAConst(); ac != nil {
		// A quoted literal reaches us as text, and the column type decides how to
		// read it — the same decision encodeTextAs makes for a bound parameter or
		// an extended-protocol value. Reusing it here is what keeps the two paths
		// from disagreeing about what a literal means: encoding DEFAULT '\x4142'
		// with EncodeValue directly would store the four literal characters
		// "\x4142" where an INSERT of the same text stores the byte 0x41 0x42, so
		// the same value would compare differently depending on how it arrived.
		if sv := ac.GetSval(); sv != nil && !ac.GetIsnull() {
			enc, err := encodeTextAs(typ, []byte(sv.GetSval()))
			if err != nil {
				return nil, fmt.Errorf("column %q default: %w", column, err)
			}
			return enc, nil
		}
		v, err := literalValue(ac)
		if err != nil {
			return nil, fmt.Errorf("column %q default: %w", column, err)
		}
		enc, err := EncodeValue(typ, coerceLiteral(typ, v))
		if err != nil {
			return nil, fmt.Errorf("column %q default: %w", column, err)
		}
		return enc, nil
	}
	if ue := node.GetTypeCast(); ue != nil {
		// A cast such as 0::bigint is how the parser represents a typed
		// literal in some positions; unwrap it and re-check against the column.
		return defaultValueOf(typ, column, ue.GetArg())
	}
	if be := node.GetBooleanTest(); be != nil {
		switch be.GetBooltesttype() {
		case pg_query.BoolTestType_IS_TRUE:
			enc, err := EncodeValue(typ, true)
			if err != nil {
				return nil, fmt.Errorf("column %q default: %w", column, err)
			}
			return enc, nil
		case pg_query.BoolTestType_IS_FALSE:
			enc, err := EncodeValue(typ, false)
			if err != nil {
				return nil, fmt.Errorf("column %q default: %w", column, err)
			}
			return enc, nil
		}
	}
	return nil, fmt.Errorf("%w: column %q: only literal defaults are supported", errUnsupported, column)
}

// translateDropTable maps a DropStmt onto a table name, reporting whether
// IF EXISTS was written. Only DROP TABLE is accepted: dropping an index or a
// sequence would refer to objects this phase cannot create in the first place.
func translateDropTable(st *pg_query.DropStmt) (string, bool, error) {
	if st == nil {
		return "", false, errUnsupported
	}
	if st.GetRemoveType() != pg_query.ObjectType_OBJECT_TABLE {
		return "", false, fmt.Errorf("%w: only DROP TABLE is supported", errUnsupported)
	}
	if st.GetConcurrent() {
		return "", false, fmt.Errorf("%w: DROP TABLE CONCURRENTLY is not supported", errUnsupported)
	}
	objs := st.GetObjects()
	if len(objs) != 1 {
		return "", false, fmt.Errorf("%w: expected one table name, got %d", errUnsupported, len(objs))
	}
	names := objs[0].GetList()
	if names == nil || len(names.Items) != 1 {
		return "", false, fmt.Errorf("%w: expected one table name", errUnsupported)
	}
	// The object list holds a bare name node rather than a RangeVar, so a
	// schema qualifier is not represented and there is nothing to check.
	sv, ok := names.Items[0].Node.(*pg_query.Node_String_)
	if !ok {
		return "", false, fmt.Errorf("%w: expected a table name", errUnsupported)
	}
	name := sv.String_.GetSval()
	// The same reserved names CREATE refuses. The implicit tellstone table has
	// no catalog entry, so a DROP of it would find nothing to remove and either
	// fail confusingly or, once the implicit table is moved into the catalog,
	// remove the table every other statement in phase 8 depends on.
	if isReservedCatalogTable(name) {
		return "", false, fmt.Errorf("%w: %q names a reserved table", errUnsupported, name)
	}
	return name, st.GetMissingOk(), nil
}

// coerceLiteral applies the column type to a literal the parser handed back as
// text. A quoted constant has no type of its own -- '1.5' is the same token
// whether it becomes a float or a varchar -- so the declared column type is the
// only thing that can say which it is. Without this, DEFAULT 1.5 on a float
// column would be read as the string "1.5" and rejected.
func coerceLiteral(typ ColumnType, v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	switch typ {
	case TypeInt, TypeBigInt:
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return n
		}
	case TypeFloat:
		if f, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
			return f
		}
	case TypeBool:
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "true", "t":
			return true
		case "false", "f":
			return false
		}
	}
	return v
}

// literalValue reads a constant from the parse tree into the Go value
// EncodeValue expects. The parser hands back a string, a float, a boolean or a
// bit string, and the integer spellings arrive as strings, so the conversion is
// explicit here rather than relying on the caller having parsed the SQL text
// correctly.
func literalValue(ac *pg_query.A_Const) (any, error) {
	if ac.GetIsnull() {
		// A NULL default is rejected: it is indistinguishable from "no
		// default" in the catalog, where nil means absent, so storing it would
		// silently turn a declared NULL default into a required column.
		return nil, fmt.Errorf("%w: NULL defaults are not supported", errUnsupported)
	}
	if bs := ac.GetBoolval(); bs != nil {
		return bs.GetBoolval(), nil
	}
	if f := ac.GetFval(); f != nil {
		return f.GetFval(), nil
	}
	if bi := ac.GetIval(); bi != nil {
		return int64(bi.GetIval()), nil
	}
	sv := ac.GetSval()
	if sv == nil {
		return nil, fmt.Errorf("%w: constant is not a supported literal", errUnsupported)
	}
	s := sv.GetSval()
	// A bare numeric literal in a string field: the parser classifies unquoted
	// digits as ival, but a quoted '123' stays a string, and the column type
	// decides which it should be. Try the numeric read and let EncodeValue be
	// the judge of whether the column accepted it.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	return s, nil
}

// translateCreate maps a CreateStmt onto a StmtCreateTable plan.
func translateCreate(st *pg_query.CreateStmt) (*Plan, error) {
	s, ifNotExists, err := translateCreateTable(st)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: StmtCreateTable, Schema: s, Table: s.Table, IfNotExists: ifNotExists}, nil
}

// translateDrop maps a DropStmt onto a StmtDropTable plan. The object type is
// checked in translateDropTable, so a DROP INDEX reaches the same "only DROP
// TABLE is supported" message whichever node it arrived as.
func translateDrop(st *pg_query.DropStmt) (*Plan, error) {
	table, ifExists, err := translateDropTable(st)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: StmtDropTable, Table: table, IfExists: ifExists}, nil
}
