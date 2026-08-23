/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: proposals.go
Description: Proposal tracking for synchronous Raft writes. Each write
proposal is tagged with a unique 8-byte ID prepended to the log entry data.
The readyLoop detects committed entries bearing a proposal ID and signals
the waiting goroutine. This lets the write path block until the quorum has
committed the entry and the FSM has applied it locally.

Wire format for tagged entries:

	[8B proposal ID (big-endian uint64)][encoded SET/DEL payload]

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// proposalIDSize is the 8-byte prefix prepended to tagged proposals.
const proposalIDSize = 8

// proposalTracker manages pending synchronous proposals. Each proposal is
// assigned a monotonically increasing ID. The readyLoop signals completion
// when the corresponding committed entry is applied.
type proposalTracker struct {
	nextID  atomic.Uint64
	pending sync.Map // map[uint64]chan error
}

// newProposalTracker creates a new proposal tracker.
func newProposalTracker() *proposalTracker {
	return &proposalTracker{}
}

// add registers a new pending proposal and returns its unique ID. The
// returned channel receives nil on success or the FSM apply error, then is
// closed.
func (pt *proposalTracker) add() (uint64, chan error) {
	id := pt.nextID.Add(1)
	ch := make(chan error, 1)
	pt.pending.Store(id, ch)
	return id, ch
}

// complete signals that a proposal has been committed and applied. It sends
// the apply error (nil on success) then closes the channel. Safe to call for
// unknown IDs (no-op).
func (pt *proposalTracker) complete(id uint64, err error) {
	if v, ok := pt.pending.LoadAndDelete(id); ok {
		ch := v.(chan error)
		ch <- err
		close(ch)
	}
}

// remove cancels a pending proposal (e.g. on context timeout). The channel
// is deleted but NOT closed — the readyLoop may still send to it, and
// receiving from a closed channel returns the zero value, which is safe.
func (pt *proposalTracker) remove(id uint64) {
	pt.pending.Delete(id)
}

// tagProposal prepends the 8-byte proposal ID to the data payload.
func tagProposal(id uint64, data []byte) []byte {
	tagged := make([]byte, proposalIDSize+len(data))
	binary.BigEndian.PutUint64(tagged[:proposalIDSize], id)
	copy(tagged[proposalIDSize:], data)
	return tagged
}

// extractProposalID extracts the proposal ID from a tagged entry's data.
// Returns 0 and false if the data is nil, too short, or the high bit is
// set (reserved for untagged entries where the first byte is an opcode 0x01
// or 0x02).
func extractProposalID(data []byte) (uint64, bool) {
	if len(data) < proposalIDSize {
		return 0, false
	}
	id := binary.BigEndian.Uint64(data[:proposalIDSize])
	// OpSet (0x01) and OpDel (0x02) have high bit clear, so a valid
	// proposal ID will always have the high bit set (IDs start at 1, so
	// the first ID is 0x0000000000000001). To avoid collision with
	// untagged entries, we use a convention: proposal IDs have bit 63 set.
	// Since we Add(1) starting from 0, the first ID is 1 — we set bit 63
	// when storing so the tagged format is:
	//   taggedID = id | (1 << 63)
	// This ensures the first byte is always >= 0x80, never confused with
	// OpSet/OpDel.
	if id&(1<<63) == 0 {
		return 0, false
	}
	return id &^ (1 << 63), true
}

// tagID sets the high bit to distinguish tagged entries from raw opcodes.
func tagID(id uint64) uint64 {
	return id | (1 << 63)
}
