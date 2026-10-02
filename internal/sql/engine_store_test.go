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

// TestSweepRowDoesNotDeadlockUnderEngineLock is the regression test for the
// hazard: a sweep that deletes from inside the scan callback would block
// forever here, because the callback runs under the engine's read lock and
// Delete needs the write lock. The test asserts completion rather than a value,
// because a deadlock shows up as a test timeout.
func TestSweepRowDoesNotDeadlockUnderEngineLock(t *testing.T) {
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
	// The primary key cell carries the column's order-preserving encoding, the
	// same bytes insertRow would write for a real statement.
	id, err := EncodeValue(TypeBigInt, int64(7))
	if err != nil {
		t.Fatalf("encode pk: %v", err)
	}
	cells := make(rowCells, len(sch.Columns))
	for i := range cells {
		cells[i] = rowValue{value: []byte("v"), set: true}
	}
	cells[sch.PrimaryKey] = rowValue{value: id, set: true}
	if err := srv.insertRow(sch, "7", cells); err != nil {
		t.Fatalf("insertRow: %v", err)
	}
	// Four column keys, so the sweep has something to delete.
	if got := store.remaining(RowPrefix(sch.DB, sch.Table, "7")); got != 4 {
		t.Fatalf("row has %d keys, want 4", got)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = srv.sweepRow(sch, "7")
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("sweepRow did not return: it is deleting from inside the scan callback, " +
			"which deadlocks against the engine's read lock")
	}

	if got := store.remaining(RowPrefix(sch.DB, sch.Table, "7")); got != 1 {
		t.Fatalf("sweep left %d keys, want only the primary key", got)
	}
}

// The same hazard on the other mutating path: insertRow compensates a partial
// write by sweeping, and must not do it under the scan lock either.
func TestCompensateRowDoesNotDeadlockUnderEngineLock(t *testing.T) {
	store := newEngineStore(t)
	srv := NewServer("127.0.0.1:0", store, nil, nil, nil, nil, nil, testLogger(), false)
	t.Cleanup(srv.Close)

	sch := &Schema{
		DB: DefaultDB, Table: "t", PrimaryKey: 0,
		Columns: []Column{{Name: "id", Type: TypeBigInt}, {Name: "a", Type: TypeVarchar}},
	}
	if err := sch.Validate(); err != nil {
		t.Fatalf("schema: %v", err)
	}
	id, err := EncodeValue(TypeBigInt, int64(1))
	if err != nil {
		t.Fatalf("encode pk: %v", err)
	}
	cells := rowCells{
		{value: id, set: true},
		{value: []byte("x"), set: true},
	}
	if err := srv.insertRow(sch, "1", cells); err != nil {
		t.Fatalf("insertRow: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.compensateRow(sch, "1")
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("compensateRow did not return: it swept from inside a scan callback")
	}
	if got := store.remaining(RowPrefix(sch.DB, sch.Table, "1")); got != 0 {
		t.Fatalf("compensation left %d keys, want 0", got)
	}
}
