//go:build linux

package shard

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestCorePinnerPinUnpin(t *testing.T) {
	var allowed unix.CPUSet
	if err := unix.SchedGetaffinity(0, &allowed); err != nil {
		t.Skipf("cannot read allowed affinity set: %v", err)
	}
	cpu := -1
	for i := 0; i < len(allowed)*64; i++ {
		if allowed.IsSet(i) {
			cpu = i
			break
		}
	}
	if cpu < 0 {
		t.Skip("process has no allowed CPU in its affinity set")
	}
	cp := NewCorePinner(cpu)
	if err := cp.Pin(); err != nil {
		t.Fatalf("pin CPU %d: %v", cpu, err)
	}
	cp.Unpin()
}
