package sql

/*
Tests that the SQL row path is safe against the real storage engine.

The row helpers are written against the Store seam, and the seam's ScanPrefix
callback is not free to do anything: an engine-backed scan holds the engine's
read lock for the duration of the callback, so a write issued from inside it
needs the write lock the read lock is holding. Nothing in the Store contract
says so, which is exactly why it has to be tested against the real engine
rather than against a test double that happens to materialise its results.
*/

import (
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
)

// engineStore adapts a storage.Engine to the Store seam, the way a standalone
// deployment wires one. It deliberately adds no buffering of its own, so the
// scan callback runs wherever the engine runs it.
type engineStore struct {
	e *storage.Engine
}

func newEngineStore(t *testing.T) *engineStore {
	t.Helper()
	return &engineStore{e: storage.NewEngine(0, 0, 0, testLogger(), nil)}
}

func (s *engineStore) GetErr(key string) ([]byte, bool, error) {
	v, ok := s.e.Get(key)
	return v, ok, nil
}

func (s *engineStore) Set(key string, value []byte, _ time.Duration) error {
	return s.e.Set(key, value, 0)
}

func (s *engineStore) SetIfAbsent(key string, value []byte, ttl time.Duration) (bool, error) {
	out, err := s.e.SetIfAbsent(key, value, ttl)
	return out.Applied, err
}

func (s *engineStore) SetIfPresent(key string, value []byte, ttl time.Duration) (bool, error) {
	out, err := s.e.SetIfPresent(key, value, ttl)
	return out.Applied, err
}

func (s *engineStore) Delete(key string) (bool, error) {
	return s.e.Delete(key), nil
}

func (s *engineStore) ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error) {
	return s.e.ScanPrefix(prefix, fn), nil
}

func (s *engineStore) remaining(prefix string) int {
	n := 0
	s.e.ScanPrefix(prefix, func(_, _ []byte) bool {
		n++
		return true
	})
	return n
}

// TestRowIsOneKey pins the packed-row invariant (Phase 5 D1-A): a row is a
// single key holding a packed value, so insertRow writes exactly one key and
// deleteRow removes exactly one key. This replaces the old sweep/compensate
// hazards, which do not exist because there is no per-column key set to sweep
// and no partial write to compensate.
func TestRowIsOneKey(t *testing.T) {
	store := newEngineStore(t)
	srv := NewServer("127.0.0.1:0", store, nil, nil, nil, nil, nil, testLogger(), false)
	t.Cleanup(srv.Close)

	sch := &Schema{
		DB: DefaultDB, Table: "t", PrimaryKey: 0,
		Columns: []Column{
			{Name: "id", Type: TypeBigInt},
			{Name: "a", Type: TypeVarchar},
			{Name: "b", Type: TypeVarchar},
			{Name: "c", Type: TypeVarchar},
		},
	}
	if err := sch.Validate(); err != nil {
		t.Fatalf("schema: %v", err)
	}
	id, err := EncodeValue(TypeBigInt, int64(7))
	if err != nil {
		t.Fatalf("encode pk: %v", err)
	}
	cells := make(rowCells, len(sch.Columns))
	for i := range cells {
		cells[i] = rowValue{value: []byte("v"), set: true}
	}
	cells[sch.PrimaryKey] = rowValue{value: id, set: true}
	if err := insertRow(srv.store, sch, "7", cells); err != nil {
		t.Fatalf("insertRow: %v", err)
	}
	if got := store.remaining(RowPrefix(sch.DB, sch.Table, "7")); got != 1 {
		t.Fatalf("row has %d keys, want 1 (packed row)", got)
	}

	cells2, found, err := readRow(srv.store, sch, "7", nil)
	if err != nil || !found {
		t.Fatalf("readRow: found=%v err=%v", found, err)
	}
	for i := range cells2 {
		if !cells2[i].set {
			t.Fatalf("cell %d is not set", i)
		}
		want := "v"
		if i == sch.PrimaryKey {
			want = string(id)
		}
		if string(cells2[i].value) != want {
			t.Fatalf("cell %d = %q, want %q", i, cells2[i].value, want)
		}
	}

	if ok, err := deleteRow(srv.store, sch, "7"); err != nil || !ok {
		t.Fatalf("deleteRow: ok=%v err=%v", ok, err)
	}
	if got := store.remaining(RowPrefix(sch.DB, sch.Table, "7")); got != 0 {
		t.Fatalf("delete left %d keys, want 0", got)
	}
}
