package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestItemTablePutLoadRemove(t *testing.T) {
	tb := newItemTable(0)
	if _, ok := tb.load("a"); ok {
		t.Fatal("empty table reported a hit")
	}
	if _, existed := tb.put("a", Item{Value: []byte("1"), Version: 1}); existed {
		t.Fatal("first put reported an existing key")
	}
	old, existed := tb.put("a", Item{Value: []byte("2"), Version: 2})
	if !existed || string(old.Value) != "1" || old.Version != 1 {
		t.Fatalf("update returned old=%+v existed=%v", old, existed)
	}
	if it, ok := tb.load("a"); !ok || string(it.Value) != "2" || it.Version != 2 {
		t.Fatalf("load after update: %+v %v", it, ok)
	}
	if it, ok := tb.remove("a"); !ok || string(it.Value) != "2" {
		t.Fatalf("remove: %+v %v", it, ok)
	}
	if _, ok := tb.load("a"); ok {
		t.Fatal("key still present after remove")
	}
	if _, ok := tb.remove("a"); ok {
		t.Fatal("second remove reported a hit")
	}
}

// Every key must survive the grows that happen as the table fills.
func TestItemTableGrowKeepsEveryKey(t *testing.T) {
	tb := newItemTable(0)
	const n = 50000
	for i := 0; i < n; i++ {
		tb.put(fmt.Sprintf("k%06d", i), Item{Value: []byte{byte(i)}, Version: uint64(i + 1)})
	}
	if len(tb.arr.Load().heads) < n {
		t.Fatalf("table did not grow to hold %d keys", n)
	}
	for i := 0; i < n; i++ {
		it, ok := tb.load(fmt.Sprintf("k%06d", i))
		if !ok || it.Version != uint64(i+1) {
			t.Fatalf("k%06d: %+v %v", i, it, ok)
		}
	}
	seen := 0
	tb.each(func(string, Item) { seen++ })
	if seen != n {
		t.Fatalf("each visited %d entries, want %d", seen, n)
	}
}

// Readers that never take a lock must only ever see a value some writer stored,
// and a key that was never removed must never disappear while writers churn
// around it, including across grows.
func TestItemTableLockFreeReadersDuringWrites(t *testing.T) {
	tb := newItemTable(0)
	tb.put("pinned", Item{Value: []byte("pinned-value"), Version: 1})

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
				it, ok := tb.load("pinned")
				if !ok || string(it.Value) != "pinned-value" {
					t.Errorf("pinned key lost or torn: %+v %v", it, ok)
					return
				}
				tb.load(fmt.Sprintf("churn-%d", it.Version%512))
			}
		}()
	}
	for i := 0; i < 30000; i++ {
		tb.put(fmt.Sprintf("churn-%d", i%512), Item{Value: []byte("v"), Version: uint64(i)})
		if i%3 == 0 {
			tb.remove(fmt.Sprintf("churn-%d", (i+7)%512))
		}
	}
	close(stop)
	wg.Wait()
}

// Engine-level: Get runs without the write lock, so it must stay correct while
// Set, Delete and conditional writes race on the same keys, and the index must
// still agree with the table afterwards.
func TestEngineConcurrentGetAndWrites(t *testing.T) {
	e := NewEngine(0, 0, 0, nil, nil)
	defer e.Close()
	const keys = 64
	for i := 0; i < keys; i++ {
		if err := e.Set(fmt.Sprintf("row:%02d", i), []byte("seed"), 0); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				k := fmt.Sprintf("row:%02d", (g*17+i)%keys)
				switch i % 4 {
				case 0:
					_ = e.Set(k, []byte(fmt.Sprintf("w%d-%d", g, i)), 0)
				case 1:
					e.Delete(k)
				case 2:
					_, _ = e.SetIfAbsent(k, []byte("abs"), 0)
				default:
					_, _ = e.SetIfPresent(k, []byte("pres"), 0)
				}
			}
		}(g)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20000; i++ {
				v, ok := e.Get(fmt.Sprintf("row:%02d", i%keys))
				if ok && len(v) == 0 {
					t.Errorf("Get returned an empty value for a live key")
					return
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()

	indexAgrees(t, e, "after concurrent Get/Set/Delete")
	e.wmu.Lock()
	e.items.each(func(k string, _ Item) {
		if !strings.HasPrefix(k, "row:") {
			t.Errorf("unexpected key %q", k)
		}
	})
	e.wmu.Unlock()
}
