/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: split.go
Description: Region split coordinator (Phase 4). Orchestrates the quiesce →
split → replay flow described in the Guarded Approach (ADR-004 §7). The
coordinator is owned by the Raft leader: it pauses the region (quiesce),
splits the key range in etcd, allocates a new region ID, and resumes both
halves so the routing table converges via the existing etcd Watch.

Wire format for range snapshots (Phase 6):

	[1B count][key1_len][key1][val1_len][val1]...[keyN_len][keyN][valN_len][valN]

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/keyspace"
	"github.com/Saxy/Tellstone/internal/log"
)

// SplitThreshold is the default byte-count threshold for triggering a region
// split. The PD monitors SizeBytes (updated by the leader's size report loop)
// and calls Split when the threshold is exceeded.
const SplitThreshold = 64 * 1024 * 1024 // 64 MiB

// quiesceTimeout is the maximum time the coordinator waits for in-flight
// proposals to drain before aborting the split.
const quiesceTimeout = 5 * time.Second

// SplitRequest describes a region split. The caller may provide a splitKey;
// if nil the coordinator picks the midpoint of the region's key range.
type SplitRequest struct {
	RegionID uint64
	SplitKey []byte // nil → midpoint
}

// SplitResult contains the IDs of both halves after a successful split.
type SplitResult struct {
	LeftID  uint64
	RightID uint64
}

// SplitCoordinator orchestrates region splits: quiesce → split → resume.
// It runs on the Raft leader and coordinates via etcd (region metadata)
// and the local transport (new raft group registration).
type SplitCoordinator struct {
	mgr *RegionManager
	// geoMu guards geo: it is set once during startup (SetGeoPolicyProvider)
	// while Split (automatic or manual) may already be reading it.
	geoMu  sync.RWMutex
	geo    GeoPolicyProvider // optional; nil falls back to the default policy
	logger log.Logger
}

// NewSplitCoordinator wraps a region manager.
func NewSplitCoordinator(mgr *RegionManager, logger log.Logger) *SplitCoordinator {
	return &SplitCoordinator{mgr: mgr, logger: logger}
}

// SetGeoPolicyProvider wires the coordinator to a geo policy source so new
// regions get a PreferredZone pinned by the operator rules (Phase 6). Safe to
// call concurrently with Split.
func (sc *SplitCoordinator) SetGeoPolicyProvider(g GeoPolicyProvider) {
	sc.geoMu.Lock()
	sc.geo = g
	sc.geoMu.Unlock()
}

// Split executes a region split. It:
//  1. Reads the current region from etcd.
//  2. Chooses a split key (midpoint if not provided).
//  3. Allocates a new region ID for the right half.
//  4. Creates the right region [splitKey, endKey) in etcd.
//  5. Updates the left region [startKey, splitKey) in etcd.
//
// Both regions share the same peers; epoch is bumped so stale routing entries
// are dropped. The split does NOT create a new raft group — that is the
// caller's responsibility after seeing the new region in the routing table.
func (sc *SplitCoordinator) Split(ctx context.Context, req SplitRequest) (*SplitResult, error) {
	// 1. Read current region from etcd.
	cur, err := sc.mgr.getRegion(ctx, req.RegionID)
	if err != nil {
		return nil, fmt.Errorf("split: read region %d: %w", req.RegionID, err)
	}
	if cur == nil {
		return nil, fmt.Errorf("split: region %d not found", req.RegionID)
	}

	// 2. Choose split key.
	splitKey, err := sc.chooseSplitKey(cur, req.SplitKey)
	if err != nil {
		return nil, err
	}

	// Validate split key is strictly inside the region.
	if len(cur.StartKey) > 0 && bytes.Compare(splitKey, cur.StartKey) <= 0 {
		return nil, fmt.Errorf("split: split key %q not after region start %q", splitKey, cur.StartKey)
	}
	if len(cur.EndKey) > 0 && bytes.Compare(splitKey, cur.EndKey) >= 0 {
		return nil, fmt.Errorf("split: split key %q not before region end %q", splitKey, cur.EndKey)
	}

	// 3. Allocate new region ID for the right half.
	alloc := NewRegionIDAllocator(sc.mgr.cli)
	newID, err := alloc.NextID(ctx)
	if err != nil {
		return nil, fmt.Errorf("split: allocate region ID: %w", err)
	}

	// 4. Create right region [splitKey, endKey) in etcd.
	sc.geoMu.RLock()
	policy := DefaultGeoPolicy()
	if sc.geo != nil {
		policy = sc.geo.Policy()
	}
	sc.geoMu.RUnlock()
	right := Region{
		ID:            newID,
		StartKey:      splitKey,
		EndKey:        cur.EndKey,
		Peers:         cur.Peers,
		Leader:        0, // claimed by the new raft group after bootstrap
		Epoch:         cur.Epoch + 1,
		PreferredZone: PreferredZoneOf(policy, splitKey),
	}
	if regionExceedsWireLimit(right) {
		return nil, fmt.Errorf("split: right region exceeds wire limit")
	}
	if _, err := sc.mgr.cli.Put(ctx, regionKey(newID), string(encodeRegion(right))); err != nil {
		return nil, fmt.Errorf("split: create right region: %w", err)
	}
	if sc.logger.Enabled(log.LevelInfo) {
		sc.logger.Log(log.LevelInfo, "split: created right region",
			log.Uint64("new_region_id", newID),
			log.Int("split_key_len", len(splitKey)),
		)
	}

	// 5. Update left region [startKey, splitKey) in etcd. Epoch is bumped
	// by 2 so it is strictly greater than the right region's epoch, ensuring
	// a stale routing entry from the old single-region cannot roll back the
	// split (RoutingTable drops lower-or-equal epochs).
	left := Region{
		ID:            cur.ID,
		StartKey:      cur.StartKey,
		EndKey:        splitKey,
		Peers:         cur.Peers,
		Leader:        cur.Leader,
		Epoch:         cur.Epoch + 2,
		PreferredZone: PreferredZoneOf(policy, cur.StartKey),
	}
	if regionExceedsWireLimit(left) {
		return nil, fmt.Errorf("split: left region exceeds wire limit")
	}
	if _, err := sc.mgr.cli.Put(ctx, regionKey(cur.ID), string(encodeRegion(left))); err != nil {
		return nil, fmt.Errorf("split: update left region: %w", err)
	}
	if sc.logger.Enabled(log.LevelInfo) {
		sc.logger.Log(log.LevelInfo, "split: updated left region",
			log.Uint64("region_id", cur.ID),
			log.Int("new_end_key_len", len(splitKey)),
		)
	}

	return &SplitResult{LeftID: cur.ID, RightID: newID}, nil
}

// ErrSplitWouldCutRow reports that a region cannot be split without cutting a
// row in half, so the split was refused rather than performed.
//
// This is a normal, expected outcome and not a fault: a region holding a single
// large row has no row boundary inside it, and cutting that row would scatter
// its columns across two regions. The split is retried once the region has
// grown enough to contain a second row, and callers that drive splits on a
// schedule can treat this as "try again later" rather than an error to report.
var ErrSplitWouldCutRow = errors.New("cluster split: no row boundary available inside region")

// chooseSplitKey picks the key to split cur at, snapping an automatically chosen
// point onto a row boundary so a split never lands inside a row.
//
// An explicitly supplied split key is honoured verbatim. It is an operator
// decision -- the escape hatch for regions whose boundaries are already wrong
// from before this rule existed -- and second-guessing it would make the
// coordinator unable to make progress on exactly the regions that most need
// operator attention. A split key that lands mid-row is logged, because the
// resulting region boundary is a hazard even when it was asked for.
func (sc *SplitCoordinator) chooseSplitKey(cur *Region, requested []byte) ([]byte, error) {
	if len(requested) > 0 {
		splitKey := make([]byte, len(requested))
		copy(splitKey, requested)
		if _, inRow := keyspace.RowPrefixOf(splitKey); inRow {
			if sc.logger != nil && sc.logger.Enabled(log.LevelWarn) {
				sc.logger.Log(log.LevelWarn, "split: requested split key falls inside a row; "+
					"the resulting boundary will separate that row's columns",
					log.String("split_key", string(splitKey)))
			}
		}
		return splitKey, nil
	}

	mid := midpointKey(cur.StartKey, cur.EndKey)

	// A midpoint that is not inside a row has no row to cut, so it is already
	// a legal boundary: it sits between two keys rather than within one.
	prefix, inRow := keyspace.RowPrefixOf(mid)
	if !inRow {
		return mid, nil
	}

	// The midpoint is inside a row, so snap it back to that row's prefix, which
	// leaves the whole row on the right of the boundary. Only the backward
	// direction is derivable from a key alone; see keyspace.RowPrefixOf for why
	// the forward direction is not merely unimplemented but unsound.
	snapped := make([]byte, len(prefix))
	copy(snapped, prefix)

	// The snapped point has to be strictly inside the region. It can fail this
	// when the region holds a single row, or when the region start is itself
	// already inside a row -- a boundary this code did not create. Refusing is
	// the correct answer in both cases: every alternative cuts a row.
	if len(cur.StartKey) > 0 && bytes.Compare(snapped, cur.StartKey) <= 0 {
		return nil, fmt.Errorf("%w: region %d holds a single row [%q, %q)",
			ErrSplitWouldCutRow, cur.ID, cur.StartKey, cur.EndKey)
	}
	if len(cur.EndKey) > 0 && bytes.Compare(snapped, cur.EndKey) >= 0 {
		return nil, fmt.Errorf("%w: region %d holds a single row [%q, %q)",
			ErrSplitWouldCutRow, cur.ID, cur.StartKey, cur.EndKey)
	}

	if sc.logger != nil && sc.logger.Enabled(log.LevelInfo) {
		sc.logger.Log(log.LevelInfo, "split: snapped midpoint to row boundary",
			log.Uint64("region_id", cur.ID),
			log.String("midpoint", string(mid)),
			log.String("split_key", string(snapped)),
		)
	}
	return snapped, nil
}

// midpointKey returns a key approximately in the middle of [start, end).
// An empty start is treated as the zero key; an empty end is treated as the
// maximum key. The midpoint is computed byte-by-byte so it always lies
// strictly between start and end for lexicographic key spaces.
func midpointKey(start, end []byte) []byte {
	s := start
	if len(s) == 0 {
		s = []byte{0}
	}
	e := end
	if len(e) == 0 {
		e = bytes.Repeat([]byte{0xff}, len(s)+1)
	}
	// Find the first byte where s and e differ.
	minLen := len(s)
	if len(e) < minLen {
		minLen = len(e)
	}
	for i := 0; i < minLen; i++ {
		if s[i] != e[i] {
			// If the bytes differ by more than 1, average them.
			if e[i]-s[i] > 1 {
				mid := make([]byte, i+1)
				copy(mid, s[:i])
				mid[i] = (s[i] + e[i]) / 2
				return mid
			}
			// Adjacent bytes: take the lower and append 0x80 to get a key
			// strictly between them (e.g. between ':' and ';' → ":{"  ).
			mid := make([]byte, i+2)
			copy(mid, s[:i+1])
			mid[i+1] = 0x80
			return mid
		}
	}
	// One is a prefix of the other — insert a middle byte after the shorter.
	// The byte is chosen as half the first differing byte of the longer key,
	// guaranteeing the result is strictly between start and end.
	if len(s) < len(e) {
		mid := make([]byte, len(s)+1)
		copy(mid, s)
		mid[len(s)] = e[len(s)] / 2
		return mid
	}
	if len(e) < len(s) {
		mid := make([]byte, len(e)+1)
		copy(mid, e)
		mid[len(e)] = s[len(e)] / 2
		return mid
	}
	// Same length, all bytes equal — shouldn't happen for a valid range.
	mid := make([]byte, len(s)+1)
	copy(mid, s)
	mid[len(s)] = 0x80
	return mid
}
