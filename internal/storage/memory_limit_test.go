package storage

import (
	"errors"
	"testing"
)

// A replacement must be charged its net change, not its full size. Before this
// fix, an update that shrank an existing entry was rejected once the engine was
// at its ceiling, because the check added the new entry to the already-counted
// old one.
func TestEngine_ReplacementUsesNetMemoryDelta(t *testing.T) {
	const maxBytes = 11 // key "k" (1) + old value (10)

	t.Run("shrinking replacement at ceiling", func(t *testing.T) {
		e := NewEngine(0, 0, maxBytes, nil, nil)
		defer e.Close()

		if err := e.Set("k", []byte("0123456789"), 0); err != nil {
			t.Fatalf("initial set: %v", err)
		}
		if got := e.AllocatedBytes(); got != maxBytes {
			t.Fatalf("allocated = %d, want %d", got, maxBytes)
		}
		if err := e.Set("k", []byte("x"), 0); err != nil {
			t.Fatalf("shrinking replacement rejected: %v", err)
		}
		if got := e.AllocatedBytes(); got != 2 {
			t.Fatalf("allocated after shrink = %d, want 2", got)
		}
		if v, ok := e.Get("k"); !ok || string(v) != "x" {
			t.Fatalf("value = %q (present=%v), want %q", v, ok, "x")
		}
	})

	t.Run("same-size replacement at ceiling", func(t *testing.T) {
		e := NewEngine(0, 0, maxBytes, nil, nil)
		defer e.Close()

		if err := e.Set("k", []byte("0123456789"), 0); err != nil {
			t.Fatalf("initial set: %v", err)
		}
		if err := e.Set("k", []byte("abcdefghij"), 0); err != nil {
			t.Fatalf("same-size replacement rejected: %v", err)
		}
		if got := e.AllocatedBytes(); got != maxBytes {
			t.Fatalf("allocated = %d, want %d", got, maxBytes)
		}
		if v, ok := e.Get("k"); !ok || string(v) != "abcdefghij" {
			t.Fatalf("value = %q (present=%v), want the replacement", v, ok)
		}
	})

	t.Run("growing replacement still rejected", func(t *testing.T) {
		e := NewEngine(0, 0, maxBytes, nil, nil)
		defer e.Close()

		if err := e.Set("k", []byte("0123456789"), 0); err != nil {
			t.Fatalf("initial set: %v", err)
		}
		err := e.Set("k", []byte("01234567890"), 0)
		if !errors.Is(err, ErrEngineFull) {
			t.Fatalf("growing replacement error = %v, want ErrEngineFull", err)
		}
		if v, ok := e.Get("k"); !ok || string(v) != "0123456789" {
			t.Fatalf("rejected write changed the value: %q (present=%v)", v, ok)
		}
		if got := e.AllocatedBytes(); got != maxBytes {
			t.Fatalf("allocated after rejection = %d, want %d", got, maxBytes)
		}
	})
}

func TestEngine_ConditionalShrinkingReplacementAtCeiling(t *testing.T) {
	const maxBytes = 11
	e := NewEngine(0, 0, maxBytes, nil, nil)
	defer e.Close()

	if err := e.Set("k", []byte("0123456789"), 0); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	out, err := e.SetIfPresent("k", []byte("x"), 0)
	if err != nil {
		t.Fatalf("SetIfPresent shrinking replacement rejected: %v", err)
	}
	if !out.Applied {
		t.Fatal("SetIfPresent did not apply")
	}
	if got := e.AllocatedBytes(); got != 2 {
		t.Fatalf("allocated = %d, want 2", got)
	}
}

func TestEngine_SetFromBufferShrinkingReplacementAtCeiling(t *testing.T) {
	const maxBytes = 11
	e := NewEngine(0, 0, maxBytes, nil, nil)
	defer e.Close()

	if err := e.SetFromBuffer([]byte("k0123456789"), 1, 0); err != nil {
		t.Fatalf("initial SetFromBuffer: %v", err)
	}
	if err := e.SetFromBuffer([]byte("kx"), 1, 0); err != nil {
		t.Fatalf("SetFromBuffer shrinking replacement rejected: %v", err)
	}
	if got := e.AllocatedBytes(); got != 2 {
		t.Fatalf("allocated = %d, want 2", got)
	}
	if v, ok := e.Get("k"); !ok || string(v) != "x" {
		t.Fatalf("value = %q (present=%v), want %q", v, ok, "x")
	}
}

func TestEngine_SetRawShrinkingReplacementAtCeiling(t *testing.T) {
	const maxBytes = 11
	e := NewEngine(0, 0, maxBytes, nil, nil)
	defer e.Close()

	if err := e.SetRaw("k", []byte("0123456789"), 0); err != nil {
		t.Fatalf("initial SetRaw: %v", err)
	}
	if err := e.SetRaw("k", []byte("x"), 0); err != nil {
		t.Fatalf("SetRaw shrinking replacement rejected: %v", err)
	}
	if got := e.AllocatedBytes(); got != 2 {
		t.Fatalf("allocated = %d, want 2", got)
	}
	if v, ok := e.Get("k"); !ok || string(v) != "x" {
		t.Fatalf("value = %q (present=%v), want %q", v, ok, "x")
	}
}
