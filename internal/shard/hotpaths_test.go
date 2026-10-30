package shard

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func newActorShard(t *testing.T) *Shard {
	t.Helper()
	s := New(1, time.Millisecond, 64, 0, nil, nil)
	s.EnableActor(false)
	t.Cleanup(s.Close)
	return s
}

func TestActorSetGetDelete(t *testing.T) {
	s := newActorShard(t)
	if r := s.Execute("SET", "k", []byte("v"), 0); r.Err != nil {
		t.Fatal(r.Err)
	}
	if r := s.Execute("GET", "k", nil, 0); !r.OK || string(r.Value) != "v" {
		t.Fatalf("get after set: ok=%v value=%q", r.OK, r.Value)
	}
	if r := s.Execute("DEL", "k", nil, 0); !r.OK {
		t.Fatal("expected DEL of existing key to report deletion")
	}
	if r := s.Execute("GET", "k", nil, 0); r.OK {
		t.Fatal("key still present after DEL")
	}
}

// A write and a read of the same key issued back to back must observe program
// order, even when they are queued into the same batch.
func TestActorPreservesProgramOrder(t *testing.T) {
	s := newActorShard(t)
	for i := 0; i < 1000; i++ {
		v := []byte(fmt.Sprintf("v%d", i))
		s.Execute("SET", "k", v, 0)
		if r := s.Execute("GET", "k", nil, 0); string(r.Value) != string(v) {
			t.Fatalf("iteration %d: read %q, want %q", i, r.Value, v)
		}
	}
}

func TestActorConcurrentWriters(t *testing.T) {
	s := newActorShard(t)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("w%d:%d", w, i)
				if r := s.Execute("SET", key, []byte("x"), 0); r.Err != nil {
					t.Errorf("set %s: %v", key, r.Err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < 8; w++ {
		for i := 0; i < 200; i++ {
			if r := s.Execute("GET", fmt.Sprintf("w%d:%d", w, i), nil, 0); !r.OK {
				t.Fatalf("missing w%d:%d", w, i)
			}
		}
	}
}

func TestActorRejectsMutationAfterClose(t *testing.T) {
	s := New(1, time.Millisecond, 64, 0, nil, nil)
	s.EnableActor(false)
	if r := s.Execute("SET", "k", []byte("v"), 0); r.Err != nil {
		t.Fatal(r.Err)
	}
	s.Close()
	s.Close() // idempotent
	if r := s.Execute("SET", "k", []byte("v2"), 0); r.Err != ErrShardStopped {
		t.Fatalf("SET after close: err=%v, want ErrShardStopped", r.Err)
	}
}

func TestLockFreeMapBasic(t *testing.T) {
	m := NewLockFreeMap(16)
	m.Put([]byte("a"), []byte("1"))
	if v, ok := m.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("got %q %v", v, ok)
	}
	if _, ok := m.Get([]byte("missing")); ok {
		t.Fatal("expected miss")
	}
	m.Put([]byte("a"), []byte("2"))
	if v, _ := m.Get([]byte("a")); string(v) != "2" {
		t.Fatalf("after update got %q", v)
	}
}

// Keys inserted far past the initial bucket count must all remain reachable
// after the table has grown several times.
func TestLockFreeMapResizeKeepsAllKeys(t *testing.T) {
	m := NewLockFreeMap(16)
	const n = 100000
	for i := 0; i < n; i++ {
		m.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("val-%d", i)))
	}
	if got := len(m.table.Load().buckets); got < n {
		t.Fatalf("table did not grow: %d buckets for %d keys", got, n)
	}
	for i := 0; i < n; i++ {
		v, ok := m.Get([]byte(fmt.Sprintf("key-%d", i)))
		if !ok || string(v) != fmt.Sprintf("val-%d", i) {
			t.Fatalf("key-%d: got %q %v", i, v, ok)
		}
	}
}

func TestLockFreeMapConcurrentReadersDuringWrites(t *testing.T) {
	m := NewLockFreeMap(16)
	m.Put([]byte("stable"), []byte("yes"))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if v, ok := m.Get([]byte("stable")); !ok || string(v) != "yes" {
					t.Errorf("reader lost stable key: %q %v", v, ok)
					return
				}
			}
		}()
	}
	for i := 0; i < 50000; i++ {
		m.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	close(stop)
	wg.Wait()
}

func TestRCUMapBasic(t *testing.T) {
	m := NewRCUMap()
	m.Put("a", []byte("1"))
	if v, ok := m.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("got %q %v", v, ok)
	}
	if _, ok := m.Get("missing"); ok {
		t.Fatal("expected miss")
	}
}

func TestAlignToCacheLine(t *testing.T) {
	if AlignToCacheLine(1) != 64 || AlignToCacheLine(64) != 64 || AlignToCacheLine(65) != 128 {
		t.Fatal("AlignToCacheLine rounding is wrong")
	}
}

func TestActorWithCorePinning(t *testing.T) {
	s := New(3, time.Millisecond, 64, 0, nil, nil)
	s.EnableActor(true)
	t.Cleanup(s.Close)
	if r := s.Execute("SET", "k", []byte("v"), 0); r.Err != nil {
		t.Fatal(r.Err)
	}
	if r := s.Execute("GET", "k", nil, 0); string(r.Value) != "v" {
		t.Fatalf("got %q", r.Value)
	}
}
