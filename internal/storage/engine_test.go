package storage

import (
	"bytes"
	"testing"
	"time"
	"unsafe"
)

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
