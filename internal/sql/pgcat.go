/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: pgcat.go
Description: The virtual pg_catalog and information_schema relations (ADR-016
Stage C). They are projected at query time from the live table catalog, so a
client's introspection reads the engine's real schema rather than a copy of it
that could drift.

The relations are read-only. A client may read them; it may not write them, and
a write is refused rather than silently dropped, because a catalog that accepts
a row it never reports is worse than one that refuses it.

The shape of the emulation follows what the clients actually ask for. A
PostgreSQL client enumerating a database reads pg_namespace, pg_class,
pg_attribute, pg_type, pg_index, pg_constraint and pg_attrdef, and reads
information_schema.tables, information_schema.columns and
information_schema.schemata for the SQL-standard view of the same facts. The
relations below carry those columns; the ones no client in scope reads (the
statistics families, the exotic catalogs) are present but empty, which is what
ADR-016 decision D7 allows.
*/
package sql

import (
	"hash/fnv"
	"sort"
	"strings"
)

// The schema names a client sees. The engine's own default database is called
// "tellstone", but a PostgreSQL client expects its tables under "public", which
// is the schema a fresh database has and the one a client's search_path names
// by default. The catalogs therefore publish the engine's tables under
// "public"; the internal name stays an implementation detail of the keyspace.
const (
	schemaPGCatalog = "pg_catalog"
	schemaInfo      = "information_schema"
	schemaPublic    = "public"
)

// The fixed object identifiers PostgreSQL gives the schemas it always has.
// They are stable constants rather than allocated ones so that a client's
// cached identifier for a schema survives a restart.
const (
	oidPGCatalogNamespace = 11
	oidPublicNamespace    = 2200
	oidInfoNamespace      = 13000
	oidPublicDatabase     = 16400
)

// The object identifiers of the catalog relations themselves. They sit in a
// range no user object can reach, because a user object's identifier is
// derived from its name (see objectOID) and is kept below this range.
const catalogOIDBase = 900000

// catalogColumn is one column of a virtual relation.
type catalogColumn struct {
	name     string
	typ      ColumnType
	nullable bool
}

// catalogRelation is one virtual relation: its shape, the column that names a
// row, and the rows it yields.
type catalogRelation struct {
	schema  string
	name    string
	columns []catalogColumn
	// key names the columns that identify a row. A single-column key is the
	// relation's primary key: a point lookup on it addresses one row, and the
	// column's value is the row's identifier. A composite key identifies a row
	// too, but no single column does, so the relation is read by a scan and
	// its row identifier is derived from all of them.
	key []string
	// oid is the relation's identifier in pg_class.
	oid  int32
	rows func(c *catalogContext) ([][]rowValue, error)
}

// asSchema renders the relation as the schema the row path reads it with.
//
// The primary key is the relation's single-column key when it has one. A
// relation whose key is composite has no primary key: no single column
// identifies a row, so the planner reads it with a scan rather than a point
// lookup, and the row's identifier is derived from the whole key.
func (r *catalogRelation) asSchema() *Schema {
	cols := make([]Column, len(r.columns))
	for i, c := range r.columns {
		cols[i] = Column{Name: c.name, Type: c.typ, Nullable: c.nullable}
	}
	pk := -1
	if name := r.pk(); name != "" {
		pk = r.columnIndex(name)
	}
	return &Schema{DB: r.schema, Table: r.name, Columns: cols, PrimaryKey: pk}
}

// pk names the relation's single-column key, empty when it has none.
func (r *catalogRelation) pk() string {
	if len(r.key) == 1 {
		return r.key[0]
	}
	return ""
}

// columnIndex finds a column by name.
func (r *catalogRelation) columnIndex(name string) int {
	for i := range r.columns {
		if r.columns[i].name == name {
			return i
		}
	}
	return -1
}

// catalogContext carries what the row builders need: the live catalog, and the
// session's view of the database and user names.
type catalogContext struct {
	cat  *Catalog
	db   string
	user string
}

// cc declares a catalog column that is never null.
func cc(name string, typ ColumnType) catalogColumn { return catalogColumn{name: name, typ: typ} }

// ccn declares a catalog column that may be null.
func ccn(name string, typ ColumnType) catalogColumn {
	return catalogColumn{name: name, typ: typ, nullable: true}
}

// catRow builds one row from named cells. A column the map does not mention
// is null, which is how a relation declares an absent value without every
// builder having to name every column.
func catRow(cols []catalogColumn, cells map[string]rowValue) []rowValue {
	row := make([]rowValue, len(cols))
	for i, c := range cols {
		if v, ok := cells[c.name]; ok {
			row[i] = v
		}
	}
	return row
}

// The cell builders. A null cell is the absence of a value rather than a
// zero-length one: the storage layout stores a null column as the absence of
// its key, so a null has to be built the same way here.
func catText(s string) rowValue {
	v, _ := EncodeValue(TypeVarchar, s)
	return rowValue{value: v, set: true}
}

func catInt(n int64) rowValue {
	v, _ := EncodeValue(TypeInt, n)
	return rowValue{value: v, set: true}
}

func catBig(n int64) rowValue {
	v, _ := EncodeValue(TypeBigInt, n)
	return rowValue{value: v, set: true}
}

func catFloat(f float64) rowValue {
	v, _ := EncodeValue(TypeFloat, f)
	return rowValue{value: v, set: true}
}

func catBool(b bool) rowValue {
	v, _ := EncodeValue(TypeBool, b)
	return rowValue{value: v, set: true}
}

// catNull is a null cell: set is false, so the column is not written.
func catNull() rowValue { return rowValue{} }

// catalogRelations is the registry of virtual relations, built once.
//
// It is assigned in init rather than in its declaration because a relation's
// row builder names the registry: pg_class lists the catalog relations
// themselves, so the registry is referred to from inside the functions that
// build it. Go's initialization order cannot express that as a declaration,
// because a declaration's initializer and the functions it calls would depend
// on each other.
var catalogRelations []*catalogRelation

func init() { catalogRelations = buildCatalogRelations() }

// buildCatalogRelations assembles the registry. It is a function rather than a
// literal so the relation list stays readable as a table.
func buildCatalogRelations() []*catalogRelation {
	rels := []*catalogRelation{
		relPGNamespace(),
		relPGClass(),
		relPGAttribute(),
		relPGType(),
		relPGAttrdef(),
		relPGIndex(),
		relPGConstraint(),
		relPGDatabase(),
		relPGDescription(),
		relPGRoles(),
		relPGAm(),
		relPGTablespace(),
		relPGSettings(),
		relPGProc(),
		relPGInherits(),
		relPGCollation(),
		relPGEnum(),
		relPGRange(),
		relPGTables(),
		relPGViews(),
		relPGMatviews(),
		relPGSequences(),
		relPGStatUserTables(),
		relInfoSchemata(),
		relInfoTables(),
		relInfoColumns(),
		relInfoViews(),
		relInfoTableConstraints(),
		relInfoKeyColumnUsage(),
		relInfoReferentialConstraints(),
		relInfoCheckConstraints(),
		relInfoRoutines(),
		relInfoParameters(),
		relInfoSequences(),
		relInfoDomains(),
		relInfoCharacterSets(),
		relInfoTablePrivileges(),
		relInfoColumnPrivileges(),
		relInfoEnabledRoles(),
		relInfoApplicableRoles(),
	}
	for i, r := range rels {
		r.oid = catalogOIDBase + int32(i)
	}
	return rels
}

// lookupCatalogRelation finds a virtual relation by schema and name. An empty
// schema searches pg_catalog first, then information_schema, which is the
// order PostgreSQL's implicit search path uses.
func lookupCatalogRelation(schema, name string) *catalogRelation {
	name = strings.ToLower(name)
	if schema == "" {
		return lookupUnqualified(name)
	}
	schema = strings.ToLower(schema)
	for _, r := range catalogRelations {
		if r.schema == schema && r.name == name {
			return r
		}
	}
	return nil
}

// lookupUnqualified finds the relation an unqualified name names. PostgreSQL
// searches pg_catalog ahead of the search path, so an unqualified name that
// matches a pg_catalog relation names that relation. The information_schema
// is not in the search path, so its relations are reachable only when they
// are qualified.
func lookupUnqualified(name string) *catalogRelation {
	name = strings.ToLower(name)
	for _, r := range catalogRelations {
		if r.schema == schemaPGCatalog && r.name == name {
			return r
		}
	}
	return nil
}

// isCatalogSchema reports whether a schema name names one of the two system
// schemas.
func isCatalogSchema(name string) bool {
	return strings.EqualFold(name, schemaPGCatalog) || strings.EqualFold(name, schemaInfo)
}

// objectOID derives a stable object identifier from an object's name.
//
// PostgreSQL assigns identifiers from a counter that is persisted with the
// catalog, so an identifier survives a rename and is never reused. Deriving one
// from the name gives the property a client depends on most -- the identifier
// of a table does not change between two connections, or between a restart and
// the session after it -- without a counter that has to be written on every
// DDL. The identifiers of user objects live below catalogOIDBase so that they
// cannot collide with the catalog's own.
func objectOID(name string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	// The high bit is cleared so the value is a positive 31-bit identifier,
	// which is what a client's signed integer can hold.
	return int32(h.Sum32()&0x7fffffff)%(catalogOIDBase-16384) + 16384
}

// userTables lists the engine's tables with their schemas, in name order.
func (c *catalogContext) userTables() ([]*Schema, error) {
	names, err := c.cat.Tables()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	out := make([]*Schema, 0, len(names))
	for _, n := range names {
		sch, err := c.cat.Get(n)
		if err != nil {
			// A table whose catalog entry cannot be read is skipped rather
			// than failing the whole introspection: the client asked for the
			// list of tables, and one unreadable entry is not a reason to
			// answer with nothing.
			continue
		}
		out = append(out, sch)
	}
	return out, nil
}

// The PostgreSQL type identifiers and names Tellstone's column types map to.
// They are the identifiers a client uses to name a column's type, so they are
// the ones PostgreSQL itself uses.
const (
	pgOIDBool        = 16
	pgOIDBytea       = 17
	pgOIDName        = 19
	pgOIDInt8        = 20
	pgOIDInt2        = 21
	pgOIDInt4        = 23
	pgOIDText        = 25
	pgOIDOid         = 26
	pgOIDJSON        = 114
	pgOIDFloat4      = 700
	pgOIDFloat8      = 701
	pgOIDBPChar      = 1042
	pgOIDVarchar     = 1043
	pgOIDTimestamp   = 1114
	pgOIDTimestamptz = 1184
	pgOIDNumeric     = 1700
	pgOIDRegProc     = 24
	pgOIDRegClass    = 2205
	pgOIDRegType     = 2206
	pgOIDJSONB       = 3802
	pgOIDUUID        = 2950
)

// pgTypeOf names the PostgreSQL type a Tellstone column type is reported as.
func pgTypeOf(t ColumnType) (oid int32, name string, len int16, byval bool) {
	switch t {
	case TypeInt:
		return pgOIDInt4, "int4", 4, true
	case TypeBigInt:
		return pgOIDInt8, "int8", 8, false
	case TypeFloat:
		return pgOIDFloat8, "float8", 8, false
	case TypeVarchar:
		return pgOIDText, "text", -1, false
	case TypeBytes:
		return pgOIDBytea, "bytea", -1, false
	case TypeTimestamp:
		return pgOIDTimestamptz, "timestamptz", 8, false
	case TypeJSONB:
		return pgOIDJSONB, "jsonb", -1, false
	case TypeBool:
		return pgOIDBool, "bool", 1, true
	default:
		return pgOIDText, "text", -1, false
	}
}

// sqlTypeOf names the SQL-standard type name information_schema.columns
// reports for a Tellstone column type.
func sqlTypeOf(t ColumnType) string {
	switch t {
	case TypeInt:
		return "integer"
	case TypeBigInt:
		return "bigint"
	case TypeFloat:
		return "double precision"
	case TypeVarchar:
		return "text"
	case TypeBytes:
		return "bytea"
	case TypeTimestamp:
		return "timestamp with time zone"
	case TypeJSONB:
		return "jsonb"
	case TypeBool:
		return "boolean"
	default:
		return "text"
	}
}

// The standard types every catalog carries, so that a client building its type
// cache finds the types it asks for.
type pgTypeRow struct {
	oid      int32
	name     string
	len      int16
	byval    bool
	typType  string
	category string
	elem     int32
	array    int32
}

// standardTypes are the types a client expects to find in pg_type.
var standardTypes = []pgTypeRow{
	{pgOIDBool, "bool", 1, true, "b", "B", 0, 0},
	{pgOIDBytea, "bytea", -1, false, "b", "U", 0, 0},
	{pgOIDName, "name", 64, false, "b", "S", 0, 0},
	{pgOIDInt8, "int8", 8, false, "b", "N", 0, 0},
	{pgOIDInt2, "int2", 2, true, "b", "N", 0, 0},
	{pgOIDInt4, "int4", 4, true, "b", "N", 0, 0},
	{pgOIDText, "text", -1, false, "b", "S", 0, 0},
	{pgOIDOid, "oid", 4, true, "b", "N", 0, 0},
	{pgOIDJSON, "json", -1, false, "b", "U", 0, 0},
	{pgOIDFloat4, "float4", 4, true, "b", "N", 0, 0},
	{pgOIDFloat8, "float8", 8, false, "b", "N", 0, 0},
	{pgOIDBPChar, "bpchar", -1, false, "b", "S", 0, 0},
	{pgOIDVarchar, "varchar", -1, false, "b", "S", 0, 0},
	{pgOIDTimestamp, "timestamp", 8, false, "b", "D", 0, 0},
	{pgOIDTimestamptz, "timestamptz", 8, false, "b", "D", 0, 0},
	{pgOIDNumeric, "numeric", -1, false, "b", "N", 0, 0},
	{pgOIDRegProc, "regproc", 4, true, "b", "N", 0, 0},
	{pgOIDRegClass, "regclass", 4, true, "b", "N", 0, 0},
	{pgOIDRegType, "regtype", 4, true, "b", "N", 0, 0},
	{pgOIDJSONB, "jsonb", -1, false, "b", "U", 0, 0},
	{pgOIDUUID, "uuid", 16, false, "b", "U", 0, 0},
}
