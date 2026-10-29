package shard

import (
	"golang.org/x/sys/unix"
	"runtime"
)

// CorePinner manages core pinning for shards
// Optional feature to keep affinity locked down when needed
type CorePinner struct {
	coreID int
	pinned bool
}

// NewCorePinner creates a new core pinner
func NewCorePinner(coreID int) *CorePinner {
	return &CorePinner{
		coreID: coreID,
	}
}

// Pin pins the current goroutine to a specific core
func (cp *CorePinner) Pin() error {
	if cp.pinned {
		return nil
	}

	// Lock the current OS thread
	runtime.LockOSThread()

	// Set affinity to the specified core
	cpuSet := unix.CPUSet{}
	cpuSet.Set(cp.coreID)

	if err := unix.SchedSetaffinity(0, &cpuSet); err != nil {
		runtime.UnlockOSThread()
		return err
	}

	cp.pinned = true
	return nil
}

// Unpin unpins the current goroutine from a specific core
func (cp *CorePinner) Unpin() {
	if cp.pinned {
		runtime.UnlockOSThread()
		cp.pinned = false
	}
}
