//go:build !linux

package shard

// CorePinner is a no-op on platforms that do not expose CPU affinity, keeping
// the shard package buildable everywhere while --pin-shards stays a no-op.
type CorePinner struct {
	coreID int
	pinned bool
}

// NewCorePinner creates a new core pinner.
func NewCorePinner(coreID int) *CorePinner {
	return &CorePinner{coreID: coreID}
}

// Pin is a no-op off Linux.
func (cp *CorePinner) Pin() error {
	cp.pinned = true
	return nil
}

// Unpin is a no-op off Linux.
func (cp *CorePinner) Unpin() {
	cp.pinned = false
}
