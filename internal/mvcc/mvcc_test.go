package mvcc

import (
	"testing"
)

// TestAppendRoundTrip pins the append-only encoding: Append folds a legacy value
// into the timestamp-0 frame, appends a new frame, and Resolve reads it back.
func TestAppendRoundTrip(t *testing.T) {
	// Legacy raw value -> versioned with frame 0 + new frame.
	raw := []byte("hello")
	v := Append(raw, 10, []byte("world"), false, 0)

	if !Versioned(v) {
		t.Fatal("Append did not produce a versioned value")
	}
	if got, ok := Resolve(v, 0); !ok || string(got) != "hello" {
		t.Fatalf("Resolve@0 = %q,%v want %q,true", got, ok, "hello")
	}
	if got, ok := Resolve(v, 10); !ok || string(got) != "world" {
		t.Fatalf("Resolve@10 = %q,%v want %q,true", got, ok, "world")
	}
	if got, ok := Resolve(v, 5); !ok || string(got) != "hello" {
		t.Fatalf("Resolve@5 = %q,%v want %q,true", got, ok, "hello")
	}
}

// TestAppendIsOrderedAndLatest pins that appended frames are monotonic and
// Latest reports the newest.
func TestAppendIsOrderedAndLatest(t *testing.T) {
	v := Append(nil, 10, []byte("a"), false, 0)
	v = Append(v, 20, []byte("b"), false, 0)
	v = Append(v, 30, []byte("c"), false, 0)

	if got := Latest(v); got != 30 {
		t.Fatalf("Latest = %d, want 30", got)
	}
	if got, ok := Resolve(v, 25); !ok || string(got) != "b" {
		t.Fatalf("Resolve@25 = %q,%v want %q,true", got, ok, "b")
	}
}

// TestCompactDropsOldFrames pins that compact keeps the floor (newest frame at
// or before the horizon) and every frame after it, dropping the rest.
func TestCompactDropsOldFrames(t *testing.T) {
	v := Append(nil, 10, []byte("a"), false, 0)
	v = Append(v, 20, []byte("b"), false, 0)
	v = Append(v, 30, []byte("c"), false, 0)
	v = Append(v, 40, []byte("d"), false, 0)

	// Horizon 25: the floor is frame@20; frames@10 dropped, 20/30/40 kept.
	c := compact(v, 25)
	if got, ok := Resolve(c, 15); ok || got != nil {
		t.Fatalf("Resolve@15 after compact = %q,%v, want absent", got, ok)
	}
	if got, ok := Resolve(c, 25); !ok || string(got) != "b" {
		t.Fatalf("Resolve@25 after compact = %q,%v want %q", got, ok, "b")
	}
	if got, ok := Resolve(c, 35); !ok || string(got) != "c" {
		t.Fatalf("Resolve@35 after compact = %q,%v want %q", got, ok, "c")
	}
	if got, ok := Resolve(c, 40); !ok || string(got) != "d" {
		t.Fatalf("Resolve@40 after compact = %q,%v want %q", got, ok, "d")
	}
}

// TestCompactNoDropWhenNoFloor pins that a horizon older than every frame drops
// nothing (there is no floor frame to keep).
func TestCompactNoDropWhenNoFloor(t *testing.T) {
	v := Append(nil, 10, []byte("a"), false, 0)
	v = Append(v, 20, []byte("b"), false, 0)

	// Horizon 5: no frame at or before 5, so nothing is dropped.
	c := compact(v, 5)
	if got, ok := Resolve(c, 10); !ok || string(got) != "a" {
		t.Fatalf("Resolve@10 = %q,%v want %q", got, ok, "a")
	}
}

// TestAppendTombstone pins that a tombstone append resolves absent at the commit
// timestamp but keeps the older value visible at earlier snapshots.
func TestAppendTombstone(t *testing.T) {
	v := Append(nil, 10, []byte("a"), false, 0)
	v = Append(v, 20, nil, true, 0)

	if got, ok := Resolve(v, 10); !ok || string(got) != "a" {
		t.Fatalf("Resolve@10 = %q,%v want %q", got, ok, "a")
	}
	if got, ok := Resolve(v, 20); ok || got != nil {
		t.Fatalf("Resolve@20 = %q,%v, want absent", got, ok)
	}
}
