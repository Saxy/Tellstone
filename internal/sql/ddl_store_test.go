/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: ddl_store_test.go
Description: Tests for the DDLStore seam — the path that makes CREATE and DROP
TABLE travel as dedicated replicated entries rather than conditional key writes.

These tests exist because the two store implementations must be
indistinguishable to a client. The same DROP TABLE has to produce the same
SQLSTATE whether it ran against an in-memory store or a Raft-replicated cluster,
and the seam is the only place where that equivalence can silently drift: the
cluster store reports its refusals with internal/cluster's error values, and
nothing in the type system connects them to this package's sentinels.

Authors:

	Maximilian Hagen
*/
package sql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/cluster"
)

// ddlRecordingStore implements both Store and DDLStore, so the catalog takes
// the DDL path. It records which seam methods were called and what they carried,
// which is what lets a test assert that the DDL opcodes really are the ones
// carrying the schema rather than a SetIfAbsent that happens to work.
type ddlRecordingStore struct {
	mu   sync.Mutex
	data map[string][]byte

	// Rows present under a table's row prefix, which is what the drop guard
	// consults.
	rows map[string]bool

	createCalls int
	dropCalls   int
	lastSchema  []byte
	lastKey     string

	// createApplied models the conditional create: false means the name was
	// taken.
	createApplied bool
	// guardErr is returned by the drop guard as "cannot determine".
	guardErr error
}

func newDDLRecordingStore() *ddlRecordingStore {
	return &ddlRecordingStore{data: map[string][]byte{}, rows: map[string]bool{}, createApplied: true}
}

func (s *ddlRecordingStore) GetErr(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (s *ddlRecordingStore) Set(key string, value []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append([]byte(nil), value...)
	return nil
}

func (s *ddlRecordingStore) ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	snapshot := make([][2][]byte, 0, len(keys))
	for _, k := range keys {
		snapshot = append(snapshot, [2][]byte{[]byte(k), append([]byte(nil), s.data[k]...)})
	}
	s.mu.Unlock()
	for _, kv := range snapshot {
		if !fn(kv[0], kv[1]) {
			break
		}
	}
	return len(snapshot), nil
}

func (s *ddlRecordingStore) SetIfAbsent(key string, value []byte, _ time.Duration) (bool, error) {
	panic("SetIfAbsent must not be used when the store implements DDLStore")
}

func (s *ddlRecordingStore) SetIfPresent(key string, value []byte, _ time.Duration) (bool, error) {
	panic("SetIfPresent must not be used when the store implements DDLStore")
}

func (s *ddlRecordingStore) Delete(key string) (bool, error) {
	panic("Delete must not be used when the store implements DDLStore")
}

func (s *ddlRecordingStore) CreateTable(ctx context.Context, key string, schema []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createCalls++
	s.lastKey, s.lastSchema = key, append([]byte(nil), schema...)
	if !s.createApplied {
		return false, nil
	}
	s.data[key] = append([]byte(nil), schema...)
	return true, nil
}

func (s *ddlRecordingStore) DropTable(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropCalls++
	if s.guardErr != nil {
		return fmt.Errorf("%w: %w", cluster.ErrEmptinessUnverifiable, s.guardErr)
	}
	// Derive the row prefix the same way the cluster path does, so the fake
	// models the real guard rather than a guessed one.
	prefix, err := cluster.RowPrefixFor(key)
	if err != nil {
		return err
	}
	for r := range s.rows {
		if strings.HasPrefix(r, prefix) {
			return fmt.Errorf("%w: %s", cluster.ErrTableNotEmpty, prefix)
		}
	}
	delete(s.data, key)
	return nil
}

// ddlSchema builds a minimal valid schema for a one-column table.
func ddlSchema(t *testing.T, table string) *Schema {
	t.Helper()
	return &Schema{
		DB:         DefaultDB,
		Table:      table,
		Columns:    []Column{{Name: "id", Type: TypeBigInt}},
		PrimaryKey: 0,
	}
}

func TestCreateUsesDDLSeamWhenAvailable(t *testing.T) {
	store := newDDLRecordingStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Create(ddlSchema(t, "users")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if store.createCalls != 1 {
		t.Fatalf("CreateTable called %d times, want 1 (the DDL path was not taken)", store.createCalls)
	}
	want := MetaTableKey(DefaultDB, "users")
	if store.lastKey != want {
		t.Fatalf("DDL key = %q, want %q", store.lastKey, want)
	}
	// The schema blob must arrive intact, since it is what a replica applies.
	got, err := cat.Get("users")
	if err != nil {
		t.Fatalf("Get after Create: %v", err)
	}
	if got.Table != "users" || len(got.Columns) != 1 || got.Columns[0].Name != "id" {
		t.Fatalf("round-tripped schema = %+v, want a one-column id table", got)
	}
}

func TestCreateDuplicateThroughDDLSeam(t *testing.T) {
	store := newDDLRecordingStore()
	store.createApplied = false // the name is taken
	cat := NewCatalog(store, DefaultDB)
	err := cat.Create(ddlSchema(t, "users"))
	if !errors.Is(err, ErrTableExists) {
		t.Fatalf("error = %v, want ErrTableExists", err)
	}
}

func TestCreateIfNotExistsUsesDDLSeam(t *testing.T) {
	store := newDDLRecordingStore()
	store.createApplied = false
	cat := NewCatalog(store, DefaultDB)
	created, err := cat.CreateIfNotExists(ddlSchema(t, "users"))
	if err != nil {
		t.Fatalf("CreateIfNotExists: %v", err)
	}
	if created {
		t.Fatal("reported a create that the store refused")
	}
	if store.createCalls != 1 {
		t.Fatalf("CreateTable called %d times, want 1", store.createCalls)
	}
}

func TestDropUsesDDLSeamAndRefusesNonEmpty(t *testing.T) {
	store := newDDLRecordingStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Create(ddlSchema(t, "users")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	store.rows["tellstone/users/1/id"] = true

	err := cat.Drop("users")
	if !errors.Is(err, ErrTableNotEmpty) {
		t.Fatalf("error = %v, want ErrTableNotEmpty", err)
	}
	if store.dropCalls != 1 {
		t.Fatalf("DropTable called %d times, want 1", store.dropCalls)
	}
	// The table must still exist: a refused drop leaves the definition alone.
	if _, err := cat.Get("users"); err != nil {
		t.Fatalf("Get after refused drop: %v", err)
	}
}

// The mapping from the cluster layer's sentinel to this package's sentinel is
// the whole reason translateDDLError exists. Without it a non-empty drop in
// cluster mode falls through schemaError's default and reaches the client as an
// IO error (a server fault) instead of the feature limitation it actually is.
func TestDropMapsClusterRefusalToFeatureLimitation(t *testing.T) {
	store := newDDLRecordingStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Create(ddlSchema(t, "users")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	store.rows["tellstone/users/1/id"] = true

	err := cat.Drop("users")
	if !errors.Is(err, ErrTableNotEmpty) {
		t.Fatalf("error = %v, want it to satisfy sql.ErrTableNotEmpty", err)
	}
	// The client-facing message names the table, not the internal prefix.
	if !strings.Contains(err.Error(), `"users"`) {
		t.Fatalf("error %q does not name the table", err.Error())
	}
	// It must not leak the cluster package's wording to the client.
	if strings.Contains(err.Error(), "cluster ddl:") {
		t.Fatalf("error %q leaks the cluster layer's wording", err.Error())
	}
}

// An unverifiable check is a different failure from "your table has rows", and
// collapsing the two would send a client off deleting rows that are not the
// problem. It maps to an internal fault instead, because retrying is the correct
// response.
func TestDropMapsUnverifiableToInternalError(t *testing.T) {
	store := newDDLRecordingStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Create(ddlSchema(t, "users")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	store.guardErr = errors.New("region 3: no leader")

	err := cat.Drop("users")
	if errors.Is(err, ErrTableNotEmpty) {
		t.Fatalf("unverifiable was reported as a non-empty table: %v", err)
	}
	if !errors.Is(err, ErrEmptinessUnverifiable) {
		t.Fatalf("error = %v, want ErrEmptinessUnverifiable", err)
	}
	srv := &Server{}
	if pgErr, ok := srv.schemaError(err).(*pgError); !ok || pgErr.code != errInternal {
		t.Fatalf("SQLSTATE = %+v, want %v (errInternal)", pgErr, errInternal)
	}
}

func TestDropSucceedsWhenEmptyThroughDDLSeam(t *testing.T) {
	store := newDDLRecordingStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Create(ddlSchema(t, "users")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := cat.Drop("users"); err != nil {
		t.Fatalf("Drop on an empty table: %v", err)
	}
	if _, err := cat.Get("users"); !errors.Is(err, ErrNoSuchTable) {
		t.Fatalf("Get after Drop = %v, want ErrNoSuchTable", err)
	}
}

func TestDropMissingTableReportsNoSuchTable(t *testing.T) {
	store := newDDLRecordingStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Drop("nosuch"); !errors.Is(err, ErrNoSuchTable) {
		t.Fatalf("error = %v, want ErrNoSuchTable", err)
	}
	// The guard must not run for a table that does not exist.
	if store.dropCalls != 0 {
		t.Fatalf("DropTable called %d times for a missing table, want 0", store.dropCalls)
	}
}

// A store without the DDL capability must keep working, and must not be asked
// for the DDL methods. This is the standalone path.
func TestPlainStoreStillUsesConditionalWrite(t *testing.T) {
	store := newTestStore()
	cat := NewCatalog(store, DefaultDB)
	if err := cat.Create(ddlSchema(t, "users")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, ok := store.data[MetaTableKey(DefaultDB, "users")]; !ok {
		t.Fatal("plain store did not receive the catalog entry")
	}
}
