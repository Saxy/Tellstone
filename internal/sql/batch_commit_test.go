/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: batch_commit_test.go
Description: Tests that COMMIT uses the optional BatchStore capability: one
ApplyBatch call for the whole write set, with the batch path's error surfaced to
the client.
*/
package sql

import (
	"errors"
	"sync"
	"testing"
)

// batchRecordingStore is a testStore that also implements BatchStore, recording
// each ApplyBatch call so a test can assert the commit took the batch path.
type batchRecordingStore struct {
	*testStore
	mu      sync.Mutex
	batches [][]BatchWrite
	err     error
}

func (s *batchRecordingStore) ApplyBatch(writes []BatchWrite) error {
	if s.err != nil {
		return s.err
	}
	cp := make([]BatchWrite, len(writes))
	copy(cp, writes)
	s.mu.Lock()
	s.batches = append(s.batches, cp)
	s.mu.Unlock()
	for _, w := range writes {
		if w.Delete {
			_, _ = s.testStore.Delete(w.Key)
			continue
		}
		_ = s.testStore.Set(w.Key, w.Value, 0)
	}
	return nil
}

func (s *batchRecordingStore) batchSnapshot() [][]BatchWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]BatchWrite, len(s.batches))
	copy(out, s.batches)
	return out
}

func newBatchTestServer(t *testing.T, store Store) *Server {
	t.Helper()
	srv := NewServer("127.0.0.1:0", store, nil, nil, nil, nil, nil, testLogger(), false)
	addr, err := srv.Start()
	if err != nil {
		t.Fatalf("start sql server: %v", err)
	}
	t.Cleanup(srv.Close)
	t.Logf("sql server on %s", addr)
	return srv
}

// TestCommitTxnUsesBatchStore drives a three-row transaction and checks it
// reaches the store as one ApplyBatch call rather than three Set calls.
func TestCommitTxnUsesBatchStore(t *testing.T) {
	store := &batchRecordingStore{testStore: newTestStore()}
	srv := newBatchTestServer(t, store)

	c := dialServer(t, srv.Addr())
	c.startupTrust("default")

	for i, key := range []string{"b1", "b2", "b3"} {
		queryOK(t, c, `INSERT INTO tellstone (key, value) VALUES ('`+key+`', 'v`+string(rune('0'+i))+`')`, "INSERT 0 1")
	}

	queryOK(t, c, `BEGIN`, "BEGIN")
	for _, key := range []string{"b1", "b2", "b3"} {
		queryOK(t, c, `UPDATE tellstone SET value = 'committed' WHERE key = '`+key+`'`, "UPDATE 1")
	}
	queryOK(t, c, `COMMIT`, "COMMIT")

	batches := store.batchSnapshot()
	if len(batches) != 1 {
		t.Fatalf("ApplyBatch called %d times, want 1", len(batches))
	}
	if len(batches[0]) != 3 {
		t.Fatalf("batch carried %d writes, want 3", len(batches[0]))
	}
	for _, w := range batches[0] {
		if w.Delete {
			t.Fatalf("unexpected delete for %q", w.Key)
		}
		if string(w.Value) != "committed" {
			t.Fatalf("write %q value = %q, want committed", w.Key, w.Value)
		}
	}
	for _, key := range []string{"b1", "b2", "b3"} {
		v, ok, err := store.GetErr(key)
		if err != nil {
			t.Fatalf("GetErr(%q): %v", key, err)
		}
		if !ok || string(v) != "committed" {
			t.Fatalf("%s = %q, %v; want committed, true", key, v, ok)
		}
	}
}

// TestCommitTxnBatchStoreError checks a batch failure surfaces as a client error
// instead of being reported as a successful COMMIT.
func TestCommitTxnBatchStoreError(t *testing.T) {
	store := &batchRecordingStore{
		testStore: newTestStore(),
		err:       errors.New("injected batch failure"),
	}
	srv := newBatchTestServer(t, store)

	c := dialServer(t, srv.Addr())
	c.startupTrust("default")

	queryOK(t, c, `INSERT INTO tellstone (key, value) VALUES ('e1', 'v1')`, "INSERT 0 1")
	queryOK(t, c, `BEGIN`, "BEGIN")
	queryOK(t, c, `UPDATE tellstone SET value = 'v2' WHERE key = 'e1'`, "UPDATE 1")
	queryErr(t, c, `COMMIT`, errIoError)

	// The failed commit must not have changed the committed value.
	v, ok, err := store.GetErr("e1")
	if err != nil {
		t.Fatalf("GetErr: %v", err)
	}
	if !ok || string(v) != "v1" {
		t.Fatalf("e1 = %q, %v; want v1, true", v, ok)
	}
}
