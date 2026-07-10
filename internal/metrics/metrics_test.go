package metrics

import (
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/storage"
)

// TestCollectorEngineSnapshot verifies that the engine snapshot reflects
// the values reported by the storage engine after a few operations.
func TestCollectorEngineSnapshot(t *testing.T) {
	// Create a simple storage engine with a minimal chronometer (interval 1ms, 1 slot).
	eng := storage.NewEngine(time.Millisecond, 1, 0, log.NewNoOpLogger(), nil)
	// Perform a basic Set operation to affect counters.
	if err := eng.Set("key1", []byte("value"), 0); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	// Perform a Get to increase hit/miss counters.
	if _, ok := eng.Get("key1"); !ok {
		t.Fatalf("expected key to exist after Set")
	}
	// Create a dummy network server (no handler, no activity).
	srv := network.NewServer("", 0, nil, nil, log.NewNoOpLogger())

	col := NewCollector(eng, srv, log.NewNoOpLogger())
	snap := col.GetEngineSnapshot()
	if snap.KeyCount != 1 {
		t.Fatalf("expected KeyCount 1, got %d", snap.KeyCount)
	}
	if snap.TotalCommands == 0 {
		t.Fatalf("expected TotalCommands > 0 after Set/Get")
	}
	// Snapshot fields that should be non‑negative.
	if snap.AllocatedBytes == 0 {
		t.Fatalf("AllocatedBytes should be >0 after storing a value")
	}
	// Ensure no panic when calling GetNetworkSnapshot.
	netSnap := col.GetNetworkSnapshot()
	_ = netSnap // silence unused variable warning
}
