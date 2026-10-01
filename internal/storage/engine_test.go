package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// TestEngine_RejectsOverlongKeys covers the boundary where the front-coded
// B+Tree's uint16 suffix length overflows. A key of 65536 bytes truncated to a
// length of 0 on the way in, so the entry was written under a key that was not
// the one supplied: the write reported success and the key could then never be
// read or deleted again. Every entry point has to refuse the key instead.
func TestEngine_RejectsOverlongKeys(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	atLimit := string(bytes.Repeat([]byte("k"), MaxKeyLen))
	tooLong := atLimit + "x"

	if err := engine.Set(atLimit, []byte("v"), 0); err != nil {
		t.Fatalf("a key of exactly MaxKeyLen was refused: %v", err)
	}
	if err := engine.Set(tooLong, []byte("v"), 0); !errors.Is(err, ErrKeyTooLong) {
		t.Errorf("Set(%d bytes) = %v, want ErrKeyTooLong", len(tooLong), err)
	}
	if _, err := engine.SetIfAbsent(tooLong, []byte("v"), 0); !errors.Is(err, ErrKeyTooLong) {
		t.Errorf("SetIfAbsent(%d bytes) = %v, want ErrKeyTooLong", len(tooLong), err)
	}
	if _, err := engine.SetIfPresent(tooLong, []byte("v"), 0); !errors.Is(err, ErrKeyTooLong) {
		t.Errorf("SetIfPresent(%d bytes) = %v, want ErrKeyTooLong", len(tooLong), err)
	}
	if err := engine.SetRaw(tooLong, []byte("v"), 0); !errors.Is(err, ErrKeyTooLong) {
		t.Errorf("SetRaw(%d bytes) = %v, want ErrKeyTooLong", len(tooLong), err)
	}
	// SetFromBuffer splits a buffer into key and value, so it has to check the
	// key half rather than the whole buffer.
	buf := append([]byte(tooLong), []byte("value")...)
	if err := engine.SetFromBuffer(buf, len(tooLong), 0); !errors.Is(err, ErrKeyTooLong) {
		t.Errorf("SetFromBuffer with a %d-byte key = %v, want ErrKeyTooLong", len(tooLong), err)
	}

	// A refused write must leave nothing behind: the key still absent, and the
	// scan must not surface a truncated entry under some other key.
	if _, ok := engine.Get(tooLong); ok {
		t.Error("a refused key is present")
	}
	hits := engine.ScanPrefix(tooLong[:MaxKeyLen], func(k, _ []byte) bool {
		t.Logf("scan surfaced %d-byte key", len(k))
		return true
	})
	if hits != 1 {
		t.Fatalf("scan found %d keys, want only the one written at MaxKeyLen", hits)
	}
}

// TestEngine_SetCopiesAliasedKeyAndValue reproduces the data-corruption bug where the
// engine retained a key/value that aliased a transient network read buffer (the server's
// networkHandler derives a zero-copy unsafe string key straight from gnet's buffer). After
// the buffer is reused, the stored key and value must remain intact, which requires Set to
// clone the key and copy the plaintext value before retaining them.
func TestEngine_SetCopiesAliasedKeyAndValue(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	// One backing buffer holding both key ("key1") and value ("VALUE!"), mimicking a frame
	// sliced directly out of the network ring buffer.
	buf := []byte("key1VALUE!")
	keyBytes := buf[:4]
	valBytes := buf[4:]
	aliasKey := *(*string)(unsafe.Pointer(&keyBytes)) // exactly what server.networkHandler does

	if err := engine.Set(aliasKey, valBytes, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Reuse/overwrite the buffer the way gnet does after c.Discard.
	copy(buf, []byte("XXXXyyyyyy"))

	got, found := engine.Get("key1")
	if !found {
		t.Fatal("stored key was corrupted by buffer reuse: key1 not found")
	}
	if string(got) != "VALUE!" {
		t.Fatalf("stored value was corrupted by buffer reuse: got %q want %q", got, "VALUE!")
	}
}

// TestEngine_DeleteReportsExistence verifies Delete reports whether the key was
// present, and that an expired key is evicted physically but reported as absent
// (the lazy-eviction semantics Get used to drive the DEL count).
func TestEngine_DeleteReportsExistence(t *testing.T) {
	engine := NewEngine(0, 0, 0, nil, nil)
	defer engine.Close()

	if engine.Delete("missing") {
		t.Error("Delete(missing) = true, want false")
	}
	if err := engine.Set("live", []byte("v"), 0); err != nil {
		t.Fatalf("set live: %v", err)
	}
	if !engine.Delete("live") {
		t.Error("Delete(live) = false, want true")
	}

	if err := engine.Set("expired", []byte("v"), 10*time.Millisecond); err != nil {
		t.Fatalf("set expired: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if engine.Delete("expired") {
		t.Error("Delete(expired) = true, want false")
	}
	if _, found := engine.Get("expired"); found {
		t.Error("expired key left behind by Delete")
	}
}

func TestEngine_TableDriven(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	type testCase struct {
		name          string
		op            string // "SET", "GET", "DELETE"
		key           string
		value         []byte
		ttl           time.Duration
		expectFound   bool
		expectValue   []byte
		sleepBeforeOp time.Duration // Simuliert das Verstreichen von Zeit (wichtig für TTL)
	}

	tests := []testCase{
		{
			name:        "Set and Get typical key-value pair",
			op:          "SET",
			key:         "user:1000",
			value:       []byte(`{"name":"Max"}`),
			ttl:         0,
			expectFound: true,
			expectValue: []byte(`{"name":"Max"}`),
		},
		{
			name:        "Get existing key from previous step",
			op:          "GET",
			key:         "user:1000",
			expectFound: true,
			expectValue: []byte(`{"name":"Max"}`),
		},
		{
			name:        "Overwrite existing key (Update)",
			op:          "SET",
			key:         "user:1000",
			value:       []byte(`{"name":"Maximilian"}`),
			ttl:         0,
			expectFound: true,
			expectValue: []byte(`{"name":"Maximilian"}`),
		},
		{
			name:        "Get non-existent key",
			op:          "GET",
			key:         "ghost_key",
			expectFound: false,
			expectValue: nil,
		},
		{
			name:        "Delete existing key",
			op:          "DELETE",
			key:         "user:1000",
			expectFound: false,
			expectValue: nil,
		},
		{
			name:        "Set empty key and empty value",
			op:          "SET",
			key:         "",
			value:       []byte(""),
			ttl:         0,
			expectFound: true,
			expectValue: []byte(""),
		},
		{
			name:          "Set with short TTL - Key still valid",
			op:            "SET",
			key:           "ttl:live",
			value:         []byte("alive"),
			ttl:           50 * time.Millisecond,
			sleepBeforeOp: 0,
			expectFound:   true,
			expectValue:   []byte("alive"),
		},
		{
			name:          "Set with short TTL - Key expired (Lazy Eviction)",
			op:            "SET",
			key:           "ttl:expire",
			value:         []byte("dead"),
			ttl:           10 * time.Millisecond,
			sleepBeforeOp: 20 * time.Millisecond, // Länger als TTL gewartet
			expectFound:   false,
			expectValue:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.sleepBeforeOp > 0 {
				time.Sleep(tc.sleepBeforeOp)
			}
			switch tc.op {
			case "SET":
				engine.Set(tc.key, tc.value, tc.ttl)
				// Wenn wir direkt danach prüfen wollen (wie in manchen Testfällen)
				if tc.expectFound {
					val, found := engine.Get(tc.key)
					if !found {
						t.Errorf("expected key %q to be found", tc.key)
					}
					if !bytes.Equal(val, tc.expectValue) {
						t.Errorf("expected %q, got %q", tc.expectValue, val)
					}
				}
			case "GET":
				val, found := engine.Get(tc.key)
				if found != tc.expectFound {
					t.Errorf("expected found=%t, got %t", tc.expectFound, found)
				}
				if !bytes.Equal(val, tc.expectValue) {
					t.Errorf("expected %q, got %q", tc.expectValue, val)
				}
			case "DELETE":
				engine.Delete(tc.key)
				_, found := engine.Get(tc.key)
				if found {
					t.Errorf("expected key %q to be deleted", tc.key)
				}
			}
		})
	}
}

// TestEngine_NewDisabledEviction verifies that an invalid interval or zero slot count
// disables active eviction (NoOpChronometer) instead of panicking. The engine must still
// be fully usable; expired keys are then reclaimed lazily on access.
func TestEngine_NewDisabledEviction(t *testing.T) {
	t.Run("interval <= 0 disables active eviction", func(t *testing.T) {
		engine := NewEngine(0, 100, 0, nil, nil)
		defer engine.Close()
		if _, ok := engine.Chronometer().(*NoOpChronometer); !ok {
			t.Errorf("expected NoOpChronometer when interval <= 0, got %T", engine.Chronometer())
		}
		engine.Set("k", []byte("v"), 0)
		if v, found := engine.Get("k"); !found || string(v) != "v" {
			t.Errorf("engine should remain usable with eviction disabled; got %q found=%t", v, found)
		}
	})

	t.Run("numSlots == 0 disables active eviction", func(t *testing.T) {
		engine := NewEngine(1*time.Second, 0, 0, nil, nil)
		defer engine.Close()
		if _, ok := engine.Chronometer().(*NoOpChronometer); !ok {
			t.Errorf("expected NoOpChronometer when numSlots == 0, got %T", engine.Chronometer())
		}
	})
}

func FuzzEngine_Operations(f *testing.F) {
	// Seed-Daten bereitstellen
	f.Add("normal_key", []byte("normal_value"))
	f.Add("", []byte("")) // Edge-Cases
	f.Add("special_#!@*&_chars", []byte{0x00, 0xFF, 0xDE, 0xAD})
	engine := NewEngine(50*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()
	f.Fuzz(func(t *testing.T, key string, value []byte) {
		engine.Set(key, value, 0)
		got, found := engine.Get(key)
		if !found {
			t.Errorf("fuzzing error: key %q was set but not found", key)
		}
		if !bytes.Equal(got, value) {
			t.Errorf("fuzzing data corruption: sent %v, got %v", value, got)
		}
		engine.Delete(key)
		_, found = engine.Get(key)
		if found {
			t.Errorf("fuzzing error: key %q should have been deleted", key)
		}
	})
}

// TestEngine_Scan verifies range scans return exactly the entries in
// [start, end) sorted lexicographically, skipping expired entries.
func TestEngine_Scan(t *testing.T) {
	engine := NewEngine(0, 0, 0, nil, nil)
	defer engine.Close()

	keys := []string{"user:alice", "user:bob", "user:carol", "admin:root", "user:dave"}
	for i, k := range keys {
		if err := engine.Set(k, []byte{byte(i)}, 0); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	t.Run("full range", func(t *testing.T) {
		var got []string
		engine.Scan(nil, nil, func(key string, _ []byte) { got = append(got, key) })
		want := []string{"admin:root", "user:alice", "user:bob", "user:carol", "user:dave"}
		if len(got) != len(want) {
			t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("key[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("prefix range", func(t *testing.T) {
		var got []string
		engine.Scan([]byte("user:"), []byte("user:\xff"), func(key string, _ []byte) { got = append(got, key) })
		want := []string{"user:alice", "user:bob", "user:carol", "user:dave"}
		if len(got) != len(want) {
			t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("key[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("bounded range", func(t *testing.T) {
		var got []string
		engine.Scan([]byte("user:b"), []byte("user:d"), func(key string, _ []byte) { got = append(got, key) })
		want := []string{"user:bob", "user:carol"}
		if len(got) != len(want) {
			t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("key[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("excludes expired", func(t *testing.T) {
		if err := engine.Set("user:expired", []byte("x"), 10*time.Millisecond); err != nil {
			t.Fatalf("set expired: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
		var got []string
		engine.Scan(nil, nil, func(key string, _ []byte) { got = append(got, key) })
		for _, k := range got {
			if k == "user:expired" {
				t.Errorf("expired key %q was returned by Scan", k)
			}
		}
	})
}

// TestEngine_SetIfAbsentIsAtomic checks that the create-if-absent precondition
// and the write happen in one critical section. Racing N writers on one key must
// yield exactly one successful create, which is what lets the SQL frontend
// report a duplicate key instead of silently overwriting.
func TestEngine_SetIfAbsentIsAtomic(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	const writers = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	applied := 0
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := engine.SetIfAbsent("race", []byte{byte(i)}, 0)
			if err != nil {
				t.Errorf("SetIfAbsent: %v", err)
				return
			}
			if out.Applied {
				mu.Lock()
				applied++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if applied != 1 {
		t.Fatalf("SetIfAbsent applied %d writes, want exactly 1", applied)
	}
	if _, ok := engine.Get("race"); !ok {
		t.Fatal("winning write did not land")
	}
}

// TestEngine_SetIfPresentIsAtomic checks the mirror precondition: a row that is
// absent must never be created by an UPDATE.
func TestEngine_SetIfPresentIsAtomic(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	if out, err := engine.SetIfPresent("missing", []byte("x"), 0); err != nil || out.Applied {
		t.Fatalf("SetIfPresent on absent key: applied=%v err=%v, want false, nil", out.Applied, err)
	}
	if _, present := engine.Get("missing"); present {
		t.Fatal("SetIfPresent created a key that did not exist")
	}
	if err := engine.Set("present", []byte("old"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if out, err := engine.SetIfPresent("present", []byte("new"), 0); err != nil || !out.Applied {
		t.Fatalf("SetIfPresent on present key: applied=%v err=%v, want true, nil", out.Applied, err)
	}
	if v, _ := engine.Get("present"); string(v) != "new" {
		t.Fatalf("value = %q, want %q", v, "new")
	}
}

// TestEngine_ConditionalSetTreatsExpiredAsAbsent pins the liveness rule: an
// expired-but-resident entry counts as absent, so it neither blocks a create nor
// satisfies an update.
func TestEngine_ConditionalSetTreatsExpiredAsAbsent(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	if err := engine.Set("k", []byte("v"), 5*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if out, err := engine.SetIfAbsent("k", []byte("fresh"), 0); err != nil || !out.Applied {
		t.Fatalf("SetIfAbsent over an expired key: applied=%v err=%v, want true, nil", out.Applied, err)
	}
	if err := engine.Set("k2", []byte("v"), 5*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if out, err := engine.SetIfPresent("k2", []byte("nope"), 0); err != nil || out.Applied {
		t.Fatalf("SetIfPresent over an expired key: applied=%v err=%v, want false, nil", out.Applied, err)
	}
}

// TestEngine_RestoreIfSparesConcurrentWriter pins the reason a conditional
// write reports a version. The shard applies the write to memory first and the
// durability record second; if that record fails it rolls the memory change
// back. A plain rollback would delete or overwrite whatever a competing writer
// stored in between, so the rollback must no-op once the version moved on.
func TestEngine_RestoreIfSparesConcurrentWriter(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	// SETNX creates the key, then a competing writer replaces it.
	first, err := engine.SetIfAbsent("k", []byte("first"), 0)
	if err != nil || !first.Applied {
		t.Fatalf("SetIfAbsent: applied=%v err=%v", first.Applied, err)
	}
	if _, err := engine.SetIfPresent("k", []byte("second"), 0); err != nil {
		t.Fatalf("SetIfPresent: %v", err)
	}

	// The late rollback of the first write must not touch the newer value.
	if engine.RestoreIf("k", first) {
		t.Error("RestoreIf rolled back a write another writer had already replaced")
	}
	if v, ok := engine.Get("k"); !ok || string(v) != "second" {
		t.Fatalf("value = %q ok=%v, want %q", v, ok, "second")
	}
}

// TestEngine_RestoreIfUndoesOwnWrite is the ordinary case: nothing else wrote
// the key, so the failed write is rolled back and a create is removed again.
func TestEngine_RestoreIfUndoesOwnWrite(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	out, err := engine.SetIfAbsent("k", []byte("v"), 0)
	if err != nil || !out.Applied {
		t.Fatalf("SetIfAbsent: applied=%v err=%v", out.Applied, err)
	}
	if !engine.RestoreIf("k", out) {
		t.Fatal("RestoreIf did not roll back its own write")
	}
	if _, ok := engine.Get("k"); ok {
		t.Fatal("rolled-back create left the key behind")
	}
	// A second rollback of the same token must not remove a later write.
	if out2, err := engine.SetIfAbsent("k", []byte("again"), 0); err != nil || !out2.Applied {
		t.Fatalf("SetIfAbsent after rollback: applied=%v err=%v", out2.Applied, err)
	}
	if engine.RestoreIf("k", out) {
		t.Error("a spent token rolled back a later write")
	}
	if v, ok := engine.Get("k"); !ok || string(v) != "again" {
		t.Fatalf("value = %q ok=%v, want %q", v, ok, "again")
	}
}

// TestEngine_RestoreIfRestoresPreviousValueAndTTL covers the update case: the
// replaced entry comes back with its own value and its own expiration, not as a
// permanent key.
func TestEngine_RestoreIfRestoresPreviousValueAndTTL(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	if err := engine.Set("k", []byte("old"), 2*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	out, err := engine.SetIfPresent("k", []byte("new"), 0)
	if err != nil || !out.Applied {
		t.Fatalf("SetIfPresent: applied=%v err=%v", out.Applied, err)
	}
	if !out.PrevOK || string(out.Prev.Value) != "old" {
		t.Fatalf("Prev = %q (ok=%v), want %q", out.Prev.Value, out.PrevOK, "old")
	}
	if out.Prev.Expiration.IsZero() {
		t.Fatal("Prev lost the original TTL, so a rollback would make the key permanent")
	}
	if !engine.RestoreIf("k", out) {
		t.Fatal("RestoreIf did not roll back its own update")
	}
	if v, ok := engine.Get("k"); !ok || string(v) != "old" {
		t.Fatalf("value = %q ok=%v, want %q", v, ok, "old")
	}
	// The restored TTL must still fire rather than pinning the key forever.
	time.Sleep(2200 * time.Millisecond)
	if _, ok := engine.Get("k"); ok {
		t.Fatal("restored key outlived its original TTL")
	}
}

// TestEngine_RestoreIfKeepsAccountingBalanced guards the memory ceiling: a
// rolled-back write must give back exactly the bytes it took, or a long run of
// durability failures would leak the engine's budget.
func TestEngine_RestoreIfKeepsAccountingBalanced(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100, 0, nil, nil)
	defer engine.Close()

	const n = 200
	for i := 0; i < n; i++ {
		out, err := engine.SetIfAbsent(fmt.Sprintf("k%d", i), bytes.Repeat([]byte("x"), 512), 0)
		if err != nil || !out.Applied {
			t.Fatalf("SetIfAbsent: applied=%v err=%v", out.Applied, err)
		}
		if !engine.RestoreIf(fmt.Sprintf("k%d", i), out) {
			t.Fatalf("RestoreIf %d did not roll back", i)
		}
	}
	if got := engine.AllocatedBytes(); got != 0 {
		t.Fatalf("memory usage after %d rolled-back writes = %d, want 0", n, got)
	}
	if got := engine.KeyCount(); got != 0 {
		t.Fatalf("key count after %d rolled-back writes = %d, want 0", n, got)
	}
}
