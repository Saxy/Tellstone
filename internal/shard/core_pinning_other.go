//go:build !linux

package shard

import "errors"

// CorePinner is a no-op on platforms that do not expose CPU affinity, keeping
// the shard package buildable everywhere.
type CorePinner struct {
	coreID int
	pinned bool
}

// NewCorePinner creates a new core pinner.
func NewCorePinner(coreID int) *CorePinner {
	return &CorePinner{coreID: coreID}
}

// Pin reports that CPU affinity is unavailable off Linux instead of claiming
// the goroutine was pinned when nothing happened.
func (cp *CorePinner) Pin() error {
	return errors.New("shard: CPU affinity is not supported on this platform")
}

// Unpin is a no-op off Linux.
func (cp *CorePinner) Unpin() {
	cp.pinned = false
}
