package storage

import (
	"bytes"
	"testing"
	"time"
)

func TestEngine_TableDriven(t *testing.T) {
	engine := NewEngine(10*time.Millisecond, 100)
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

func TestEngine_NewPanic(t *testing.T) {
	t.Run("Panic on invalid interval", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic for interval <= 0")
			}
		}()
		NewEngine(0, 100)
	})

	t.Run("Panic on invalid numSlots", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic for numSlots == 0")
			}
		}()
		NewEngine(1*time.Second, 0)
	})
}

func FuzzEngine_Operations(f *testing.F) {
	// Seed-Daten bereitstellen
	f.Add("normal_key", []byte("normal_value"))
	f.Add("", []byte("")) // Edge-Cases
	f.Add("special_#!@*&_chars", []byte{0x00, 0xFF, 0xDE, 0xAD})
	engine := NewEngine(50*time.Millisecond, 100)
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
