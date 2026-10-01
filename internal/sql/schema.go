/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: schema.go
Description: The table catalog: what a CREATE TABLE means, how a schema is
validated and stored, and how the DDL parse tree becomes one. A schema is one
blob at MetaTableKey, so publishing a table is a single store write rather than
a sequence that could be observed half-applied.

The blob is a versioned binary encoding rather than JSON or the PG parser's own
protobuf for three reasons. It has to be small, because it is read on every DML
statement against the table. It has to be deterministic, so two nodes that
adopt the same schema produce byte-identical values and a state-machine
replay cannot diverge on encoding alone. And it has to be readable without the
cgo parser linked in, which rules out reusing the parse tree.
*/
package sql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/Saxy/Tellstone/internal/cluster"
	"github.com/Saxy/Tellstone/internal/keyspace"
)

// Column is one declared column, in declaration order. Ordinal is implied by
// position in Schema.Columns rather than stored, so the two cannot disagree.
type Column struct {
	Name string
	Type ColumnType
	// Nullable is false for a PRIMARY KEY column and for any column declared
	// NOT NULL.
	Nullable bool
	// Default is the encoded default value, or nil when the column has no
	// default. It is stored already encoded by EncodeValue so that applying a
	// default at write time is a copy rather than a re-encode, and so a default
	// that cannot be represented is rejected at DDL time.
	Default []byte
}

// Schema is a table definition as stored in the catalog.
type Schema struct {
	DB    string
	Table string
	// Columns are in declaration order. Ordinals are therefore stable, which
	// is what lets a row be rebuilt from a range scan by position.
	Columns []Column
	// PrimaryKey is the index into Columns of the primary key column. Phase 9
	// is deliberately single-column: a composite key has no row-id encoding
	// yet, and a half-supported one would silently drop ordering.
	PrimaryKey int
}

// Catalog errors, distinct so the executor can pick the right SQLSTATE.
var (
	// ErrNoSuchTable reports a lookup for a table with no catalog entry.
	ErrNoSuchTable = errors.New("sql: table does not exist")
	// ErrTableExists reports CREATE TABLE without IF NOT EXISTS on a name
	// already in the catalog.
	ErrTableExists = errors.New("sql: table already exists")
	// ErrSchemaCorrupt reports a catalog blob that is not a readable schema.
	// It is deliberately fatal-looking: silently treating an unreadable
	// catalog as an absent table would let a DDL statement overwrite it.
	ErrSchemaCorrupt = errors.New("sql: schema catalog entry is corrupt")
	// ErrTableNotEmpty reports a DROP of a table that still has rows. Dropping
	// the catalog entry alone would leave the rows' keys behind with nothing
	// describing them, so the refusal is deliberate rather than a fault.
	ErrTableNotEmpty = errors.New("sql: table is not empty")
	// ErrEmptinessUnverifiable reports that a DROP TABLE could not be proven
	// safe because the cluster could not complete the range read. It is
	// deliberately distinct from ErrTableNotEmpty: a client whose table has rows
	// should not be told the cluster failed to answer, and both are distinct
	// from a drop that succeeded.
	ErrEmptinessUnverifiable = errors.New("sql: cannot verify that the table is empty")
	// ErrNoPrimaryKey reports a table with no PRIMARY KEY, which Phase 9
	// requires because the key column is the row id.
	ErrNoPrimaryKey = errors.New("sql: table requires a PRIMARY KEY")
	// ErrCompositePrimaryKey reports a multi-column PRIMARY KEY.
	ErrCompositePrimaryKey = errors.New("sql: composite PRIMARY KEY is not supported")
	// ErrDuplicateColumn reports a repeated column name.
	ErrDuplicateColumn = errors.New("sql: duplicate column name")
	// ErrBadSchema reports a schema that fails validation.
	ErrBadSchema = errors.New("sql: invalid schema")
)

// MaxColumns bounds a table definition. It is generous for Phase 9 and exists
// because the catalog blob length is bounded by the store's value limit, and a
// pathological CREATE TABLE would otherwise fail later as an opaque write
// error rather than as a rejected statement.
const MaxColumns = 1024

// noDefault is the length prefix meaning "this column has no default". A
// separate sentinel rather than a flag bit is what lets an empty default be
// stored: with a flag, a present-but-empty default and a truncated entry are the
// same two bytes, and the reader has to guess which it is looking at. With a
// sentinel length, zero unambiguously means "present and empty".
const noDefault = 1<<16 - 1

// maxBlobField is the largest name or default value a catalog entry can carry.
// The bound is the length prefix minus the sentinel. Names and defaults are
// length-prefixed, so a field past this bound would have its prefix silently
// truncated and decode as a different, shorter value. Validate rejects the
// oversized field as a statement error instead, which is the only place it can
// still be attributed to the DDL that caused it.
const maxBlobField = noDefault - 1

// PrimaryKeyName names the primary key column.
func (s *Schema) PrimaryKeyName() string { return s.Columns[s.PrimaryKey].Name }

// Column looks up a column by name, reporting whether it exists.
func (s *Schema) Column(name string) (*Column, bool) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i], true
		}
	}
	return nil, false
}

// RowPrefix is the key range covering every row of the table, which is what a
// full table scan walks.
func (s *Schema) RowPrefix() string { return TablePrefix(s.DB, s.Table) }

// Validate checks a schema before it reaches the store. Every rejection here is
// a statement error the client can fix, so they are reported rather than
// repaired: a catalog that silently dropped a column would make later reads
// return a row shaped differently from the one the client inserted.
func (s *Schema) Validate() error {
	if s == nil {
		return fmt.Errorf("%w: nil schema", ErrBadSchema)
	}
	if err := keyspace.ValidateDatabaseName(s.DB); err != nil {
		return fmt.Errorf("%w: %w", ErrBadSchema, err)
	}
	if err := ValidateTableName(s.Table); err != nil {
		return fmt.Errorf("%w: %w", ErrBadSchema, err)
	}
	if len(s.Columns) == 0 {
		return fmt.Errorf("%w: table has no columns", ErrBadSchema)
	}
	if len(s.Columns) > MaxColumns {
		return fmt.Errorf("%w: %d columns exceeds the %d column limit", ErrBadSchema, len(s.Columns), MaxColumns)
	}
	seen := make(map[string]bool, len(s.Columns))
	for _, c := range s.Columns {
		if err := ValidateColumnName(c.Name); err != nil {
			return fmt.Errorf("%w: %w", ErrBadSchema, err)
		}
		if len(c.Name) > maxBlobField {
			return fmt.Errorf("%w: column name is %d bytes, over the %d byte limit", ErrBadSchema, len(c.Name), maxBlobField)
		}
		lowered := strings.ToLower(c.Name)
		if seen[lowered] {
			// SQL identifiers are case-insensitive unless quoted, so "Name"
			// and "name" would collide in a row's column keys even though they
			// are distinct strings.
			return fmt.Errorf("%w: %q", ErrDuplicateColumn, c.Name)
		}
		seen[lowered] = true
		if c.Type == TypeInvalid {
			return fmt.Errorf("%w: column %q has no type", ErrBadSchema, c.Name)
		}
		if len(c.Default) > maxBlobField {
			return fmt.Errorf("%w: column %q default is %d bytes, over the %d byte limit", ErrBadSchema, c.Name, len(c.Default), maxBlobField)
		}
		if c.Default != nil {
			// A default has to be readable back as its own type, or every
			// insert using it would produce a row that cannot be decoded.
			if _, err := DecodeValue(c.Type, c.Default); err != nil {
				return fmt.Errorf("%w: column %q default: %w", ErrBadSchema, c.Name, err)
			}
		}
	}
	switch {
	case s.PrimaryKey < 0 || s.PrimaryKey >= len(s.Columns):
		return fmt.Errorf("%w: primary key index %d out of range", ErrBadSchema, s.PrimaryKey)
	}
	if s.Columns[s.PrimaryKey].Default != nil {
		// A defaulted primary key is how generated ids would work, but Phase 9
		// has no proposer-side id source to draw on yet (ADR-013 decision 4),
		// so allowing it would let a table accept rows it cannot key.
		return fmt.Errorf("%w: PRIMARY KEY column %q cannot have a default", ErrBadSchema, s.Columns[s.PrimaryKey].Name)
	}
	return nil
}

// schemaMagic tags a catalog blob so a value that is not a schema is detected
// instead of decoded into something plausible. The keyspace is shared with user
// data above the meta prefix, so "is this ours" has to be answered by the bytes.
const schemaMagic = "TSCH"

// schemaVersion is the blob format version. It is checked on read, so a future
// format change can be introduced without misreading old entries, and bumped
// deliberately rather than inferred.
const schemaVersion = 1

// EncodeSchema serialises a schema. The output is deterministic: same schema in,
// same bytes out, on any node. That matters because the catalog blob is
// replicated, and a replay that re-encoded it differently would look like a
// divergence even though the schema is identical.
func EncodeSchema(s *Schema) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 128+len(s.Columns)*32)
	buf = append(buf, schemaMagic...)
	buf = append(buf, schemaVersion)
	buf = appendU16(buf, uint16(len(s.Columns)))
	buf = appendU16(buf, uint16(s.PrimaryKey))
	buf = appendString(buf, s.DB)
	buf = appendString(buf, s.Table)
	for _, c := range s.Columns {
		var flags byte
		if !c.Nullable {
			flags |= 1 << 0
		}
		buf = append(buf, flags)
		buf = append(buf, byte(c.Type))
		buf = appendString(buf, c.Name)
		if c.Default == nil {
			buf = appendU16(buf, noDefault)
		} else {
			// A present but empty default is length zero, which the sentinel
			// keeps distinct from absent.
			buf = appendBytes(buf, c.Default)
		}
	}
	return buf, nil
}

// DecodeSchema parses a catalog blob and validates the result. Validation runs
// on read as well as on write: a blob that decoded into a shape the writer
// should have rejected means the writer is older or newer than this reader, and
// trusting it would surface as a confusing failure much later.
func DecodeSchema(db, table string, b []byte) (*Schema, error) {
	if len(b) < len(schemaMagic)+1 {
		return nil, fmt.Errorf("%w: %d byte entry is too short", ErrSchemaCorrupt, len(b))
	}
	if string(b[:len(schemaMagic)]) != schemaMagic {
		return nil, fmt.Errorf("%w: missing magic", ErrSchemaCorrupt)
	}
	if v := b[len(schemaMagic)]; v != schemaVersion {
		return nil, fmt.Errorf("%w: unsupported blob version %d", ErrSchemaCorrupt, v)
	}
	p := &schemaParser{b: b, off: len(schemaMagic) + 1}
	s := &Schema{DB: db, Table: table, PrimaryKey: -1}
	ncols, err := p.u16()
	if err != nil {
		return nil, err
	}
	pk, err := p.u16()
	if err != nil {
		return nil, err
	}
	s.PrimaryKey = int(pk)
	if s.DB, err = p.str(); err != nil {
		return nil, err
	}
	if s.Table, err = p.str(); err != nil {
		return nil, err
	}
	// The blob carries its own names so a moved or renamed entry is detectable,
	// but the key it was read from is authoritative for lookup.
	if s.DB != db || s.Table != table {
		return nil, fmt.Errorf("%w: entry for %s/%s holds %s/%s", ErrSchemaCorrupt, db, table, s.DB, s.Table)
	}
	if int(ncols) > MaxColumns {
		return nil, fmt.Errorf("%w: %d columns exceeds the limit", ErrSchemaCorrupt, ncols)
	}
	s.Columns = make([]Column, 0, ncols)
	for i := 0; i < int(ncols); i++ {
		flags, err := p.byte()
		if err != nil {
			return nil, err
		}
		typ, err := p.byte()
		if err != nil {
			return nil, err
		}
		name, err := p.str()
		if err != nil {
			return nil, err
		}
		c := Column{Name: name, Type: ColumnType(typ), Nullable: flags&(1<<0) == 0}
		if flags&^uint8(1<<0) != 0 {
			return nil, fmt.Errorf("%w: column %q has unknown flags %#x", ErrSchemaCorrupt, name, flags)
		}
		n, err := p.u16()
		if err != nil {
			return nil, err
		}
		switch {
		case n == noDefault:
			// Absent, which leaves Default nil.
		case n > maxBlobField:
			return nil, fmt.Errorf("%w: column %q default length %d is not decodable", ErrSchemaCorrupt, name, n)
		default:
			def, err := p.bytesOfLen(int(n))
			if err != nil {
				return nil, err
			}
			// An empty slice, not nil: the sentinel already established that a
			// default is present, and Default nil means absent everywhere else.
			if def == nil {
				def = []byte{}
			}
			c.Default = def
		}
		s.Columns = append(s.Columns, c)
	}
	if p.off != len(b) {
		// Trailing bytes mean the writer's view of the format differs from the
		// reader's. Ignoring them would hide a compatibility break.
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrSchemaCorrupt, len(b)-p.off)
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSchemaCorrupt, err)
	}
	return s, nil
}

// schemaParser is a bounds-checked reader. Every read validates, so a truncated
// or hostile blob yields an error instead of a panic in a query path.
type schemaParser struct {
	b   []byte
	off int
}

func (p *schemaParser) need(n int) error {
	if n < 0 || p.off+n > len(p.b) {
		return fmt.Errorf("%w: truncated at offset %d", ErrSchemaCorrupt, p.off)
	}
	return nil
}

func (p *schemaParser) byte() (byte, error) {
	if err := p.need(1); err != nil {
		return 0, err
	}
	v := p.b[p.off]
	p.off++
	return v, nil
}

func (p *schemaParser) u16() (uint16, error) {
	if err := p.need(2); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint16(p.b[p.off:])
	p.off += 2
	return v, nil
}

func (p *schemaParser) bytes() ([]byte, error) {
	n, err := p.u16()
	if err != nil {
		return nil, err
	}
	if err := p.need(int(n)); err != nil {
		return nil, err
	}
	// A zero-length field returns nil rather than an empty slice so that a
	// column with no default compares equal to one decoded with Default nil,
	// which is what Validate and EncodeValue both test.
	if n == 0 {
		return nil, nil
	}
	v := p.b[p.off : p.off+int(n)]
	p.off += int(n)
	return v, nil
}

// bytesOfLen reads exactly n bytes, allowing n to be zero.
func (p *schemaParser) bytesOfLen(n int) ([]byte, error) {
	if err := p.need(n); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	v := p.b[p.off : p.off+n]
	p.off += n
	return v, nil
}

func (p *schemaParser) str() (string, error) {
	v, err := p.bytes()
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func appendU16(b []byte, v uint16) []byte { return append(b, byte(v>>8), byte(v)) }

// appendString writes a length-prefixed string. The length is a uint16, so
// Validate has to keep names and defaults within it; the check there is what
// turns an oversized name into a rejected statement instead of a truncated
// length prefix that decodes as garbage.
func appendString(b []byte, s string) []byte { return appendBytes(b, []byte(s)) }

func appendBytes(b []byte, v []byte) []byte {
	b = appendU16(b, uint16(len(v)))
	return append(b, v...)
}

// Catalog reads and writes table schemas. It holds no cache: the catalog is one
// small read per statement against a local key, and a cache would have to be
// invalidated across the store's conditional-write semantics, which is a
// correctness problem for no measured gain.
type Catalog struct {
	store Store
	db    string
}

// NewCatalog returns a catalog over db in the given store.
func NewCatalog(store Store, db string) *Catalog {
	if db == "" {
		db = DefaultDB
	}
	return &Catalog{store: store, db: db}
}

// DB names the database subtree this catalog manages.
func (c *Catalog) DB() string { return c.db }

// Get loads a table's schema, reporting ErrNoSuchTable when the catalog has no
// entry for it.
func (c *Catalog) Get(table string) (*Schema, error) {
	if err := ValidateTableName(table); err != nil {
		return nil, err
	}
	raw, ok, err := c.store.GetErr(MetaTableKey(c.db, table))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoSuchTable, table)
	}
	return DecodeSchema(c.db, table, raw)
}

// Exists reports whether a table has a catalog entry, without decoding it.
func (c *Catalog) Exists(table string) (bool, error) {
	if err := ValidateTableName(table); err != nil {
		return false, err
	}
	_, ok, err := c.store.GetErr(MetaTableKey(c.db, table))
	return ok, err
}

// Tables lists the table names in the catalog. It is a prefix scan of the meta
// subtree, so it depends on the range read added to Store in Phase 9.
func (c *Catalog) Tables() ([]string, error) {
	prefix := MetaTablePrefix(c.db)
	var names []string
	_, err := c.store.ScanPrefix(prefix, func(key, _ []byte) bool {
		names = append(names, strings.TrimPrefix(string(key), prefix))
		return true
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

// Create publishes a schema. The write is conditional on absence so two
// concurrent CREATE TABLE statements for the same name cannot both succeed: the
// loser gets ErrTableExists instead of overwriting the winner's definition
// while rows written against it are still present.
//
// When the store can replicate a schema mutation as its own log entry, that
// path is used: a DDL entry states the table's shape explicitly, so a replica
// applying it needs no context beyond the log. The conditional-create guarantee
// is the same either way, so this changes how the intent travels rather than
// whether concurrent CREATEs can both win.
func (c *Catalog) Create(s *Schema) error {
	if err := s.Validate(); err != nil {
		return err
	}
	blob, err := EncodeSchema(s)
	if err != nil {
		return err
	}
	key := MetaTableKey(c.db, s.Table)

	if ds, ok := c.store.(DDLStore); ok {
		applied, err := ds.CreateTable(context.Background(), key, blob)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("%w: %q", ErrTableExists, s.Table)
		}
		return nil
	}

	applied, err := c.store.SetIfAbsent(key, blob, 0)
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: %q", ErrTableExists, s.Table)
	}
	return nil
}

// Drop removes a table's catalog entry. It refuses to do so if rows remain, so
// that DROP TABLE cannot orphan live data: the caller is expected to have
// already deleted the rows (or to be using it on an empty table). A catalog
// without this check would let a later CREATE TABLE of the same name inherit
// rows it knows nothing about.
func (c *Catalog) Drop(table string) error {
	if err := ValidateTableName(table); err != nil {
		return err
	}
	if _, err := c.Get(table); err != nil {
		return err
	}
	key := MetaTableKey(c.db, table)

	// The DDL path proves emptiness inside the proposing node, before the entry
	// exists. That ordering is deliberate and is why it must not be done here as
	// well: a check that runs, then a proposal, leaves a window in which a row
	// can be inserted, and running the check twice only shrinks the window rather
	// than closing it. Doing it on the proposing side keeps one check in the one
	// place where it is adjacent to the entry it protects.
	if ds, ok := c.store.(DDLStore); ok {
		if err := ds.DropTable(context.Background(), key); err != nil {
			return translateDDLError(table, err)
		}
		return nil
	}

	remaining, err := c.RowCount(table)
	if err != nil {
		return err
	}
	if remaining > 0 {
		return fmt.Errorf("%w: table %q still has rows", ErrTableNotEmpty, table)
	}
	ok, err := c.store.Delete(key)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %q", ErrNoSuchTable, table)
	}
	return nil
}

// RowCount counts the rows currently stored for a table by walking the table's
// row range and counting distinct row ids. It is a scan, so it is O(rows) and
// is only used on the DROP path and by tests; a row count maintained
// incrementally would need to survive crash recovery, which is a Phase 10
// concern.
func (c *Catalog) RowCount(table string) (int, error) {
	if err := ValidateTableName(table); err != nil {
		return 0, err
	}
	prefix := TablePrefix(c.db, table)
	rows := 0
	var last string
	_, err := c.store.ScanPrefix(prefix, func(key, _ []byte) bool {
		rest := strings.TrimPrefix(string(key), prefix)
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			// A key inside the table range with no column separator is not a
			// row this catalog can account for.
			return true
		}
		id := rest[:slash]
		if id != last {
			rows++
			last = id
		}
		return true
	})
	return rows, err
}

// RowIDColumns returns the column names a row range yields, in the order they
// appear in the scan. Column keys are appended in column order by the write
// path, so this is the ordinal order a reader can trust.
func (s *Schema) RowIDColumns() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}

// DefaultFor returns a column's encoded default, or nil when it has none.
func (s *Schema) DefaultFor(column string) ([]byte, bool) {
	c, ok := s.Column(column)
	if !ok || c.Default == nil {
		return nil, false
	}
	return c.Default, true
}

// isReservedCatalogTable reports whether a name collides with the implicit
// Phase 8 table or the meta subtree. Those keyspaces are already claimed, so a
// user table of the same name would either shadow the implicit table or land
// inside the control subtree.
func isReservedCatalogTable(name string) bool {
	return name == tableName || strings.HasPrefix(name, metaPrefix) || strings.HasPrefix(name, "*")
}

// translateDDLError maps a cluster-layer DDL refusal onto this package's
// sentinel errors, so the SQLSTATE a client receives does not depend on which
// store happened to execute the statement.
//
// A store that replicates DDL reports emptiness refusals with its own errors,
// and they are wrapped rather than passed through: internal/sql deliberately
// does not import internal/cluster, so without this the same refusal would reach
// a client as errIoError (a server fault) in cluster mode and as 0A000 (a
// feature limitation) in standalone mode.
func translateDDLError(table string, err error) error {
	switch {
	case cluster.IsTableNotEmpty(err):
		return fmt.Errorf("%w: table %q still has rows", ErrTableNotEmpty, table)
	case errors.Is(err, cluster.ErrEmptinessUnverifiable):
		// Distinct from "not empty" on purpose: this one is a cluster that could
		// not answer, so reporting it as "your table has rows" would send the
		// client off deleting rows that are not the problem.
		return fmt.Errorf("%w: table %q: %w", ErrEmptinessUnverifiable, table, err)
	case cluster.IsConditionNotMet(err):
		return fmt.Errorf("%w: %q", ErrTableExists, table)
	default:
		return err
	}
}

// CreateIfNotExists publishes a schema, reporting whether it was created. This
// is the IF NOT EXISTS form: a name already in the catalog is left alone and the
// statement succeeds, matching the SQL clause rather than the bare Create
// behaviour of raising an error.
func (c *Catalog) CreateIfNotExists(s *Schema) (bool, error) {
	if err := s.Validate(); err != nil {
		return false, err
	}
	blob, err := EncodeSchema(s)
	if err != nil {
		return false, err
	}
	key := MetaTableKey(c.db, s.Table)
	if ds, ok := c.store.(DDLStore); ok {
		return ds.CreateTable(context.Background(), key, blob)
	}
	applied, err := c.store.SetIfAbsent(key, blob, 0)
	if err != nil {
		return false, err
	}
	return applied, nil
}

// DropIfExists removes a table's catalog entry, reporting whether there was one
// to remove. This is the IF EXISTS form of Drop. The "table still has rows"
// refusal still applies: IF EXISTS waives the missing-table case, not the one
// where dropping would orphan live data.
func (c *Catalog) DropIfExists(table string) (bool, error) {
	if err := ValidateTableName(table); err != nil {
		return false, err
	}
	if err := c.Drop(table); err != nil {
		if errors.Is(err, ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// columnIndex looks up a column's ordinal, which is the position its cell takes
// in a row. Callers need the ordinal rather than the *Column because a range
// scan returns columns in name order and the ordinal is what re-imposes the
// schema's order.
func (s *Schema) columnIndex(name string) (int, bool) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return i, true
		}
	}
	return 0, false
}
