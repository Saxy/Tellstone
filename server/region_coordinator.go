/*
Package server
Tellstone Cloud-Native In-Memory Database
File: region_coordinator.go
Description: Phase 4 server-side region coordinator. Hosts one Raft group node
per region over a single shared TCP transport (the bootstrap node's transport)
and performs region splits when a region's tracked size exceeds the configured
threshold. A region split is quiesce → etcd metadata split → host the new
right-hand region group → resume. Only the current leader of a region runs its
split; the etcd Watch + routing table converge on every node.

Authors:

	Maximilian Hagen
*/
package server

import (
	"context"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/cluster"
	"github.com/Saxy/Tellstone/internal/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// regionCooldown is how long a region is left alone after a split before the
// auto-split loop may consider it again. Guards against thrashing when the size
// counters are still reporting the pre-split total.
const regionCooldown = 30 * time.Second

// splitCooldownTimeout is how long the leader waits for in-flight proposals to
// drain before aborting a split.
const splitCooldownTimeout = 5 * time.Second

// electionTickDefault is the Raft election timeout in ticks used on the
// region's preferred zone. A shorter timeout means the raft node campaigns
// sooner and typically wins when the leader dies (ADR-006 soft preference).
const electionTickDefault = 10

// electionTickOffZone is the election timeout used on nodes outside the
// region's preferred zone. Doubling the timeout makes cross-zone nodes
// unlikely to become leader while a same-zone follower is alive, keeping
// leaders close to their data's "home" zone.
const electionTickOffZone = 20

// RegionCoordinator owns the per-region Raft group nodes on this process. It
// is cheap to run once per process; every region node shares the bootstrap
// node's TCP transport and the same local shard dispatcher (the engine is
// shared across regions on a node).
type RegionCoordinator struct {
	cli        *clientv3.Client
	nodeID     uint64
	host       *cluster.Node // bootstrap node; owns the shared transport
	mgr        *cluster.RegionManager
	rt         *cluster.RoutingTable
	splitter   *cluster.SplitCoordinator
	dispatcher cluster.Dispatcher
	peers      []cluster.Peer
	threshold  uint64
	tracker    *cluster.RegionSizeTracker
	resolver   cluster.RegionResolver
	logger     log.Logger
	// zone is this process's availability zone (--zone). When non-empty it
	// biases per-region raft election timeouts: nodes in a region's preferred
	// zone campaign first, so the leader stays in the data's "home" zone.
	zone string

	mu          sync.Mutex
	regionNodes map[uint64]*cluster.Node
	lastSplit   map[uint64]time.Time
	splitMu     sync.Mutex

	stopCh chan struct{}
	done   chan struct{}
}

// NewRegionCoordinator creates a coordinator bound to the bootstrap raft node
// and the PD etcd client. peers are the cluster's raft membership (ID + addr);
// threshold is the byte size above which a region auto-splits. tracker and
// resolver wire every hosted region node (including new split regions) into
// the size-accounting used for auto-split decisions; either may be nil.
func NewRegionCoordinator(cli *clientv3.Client, nodeID uint64, host *cluster.Node, mgr *cluster.RegionManager, dispatcher cluster.Dispatcher, peers []cluster.Peer, threshold uint64, logger log.Logger, tracker *cluster.RegionSizeTracker, resolver cluster.RegionResolver) *RegionCoordinator {
	rt := mgr.RoutingTable()
	return &RegionCoordinator{
		cli:        cli,
		nodeID:     nodeID,
		host:       host,
		mgr:        mgr,
		rt:         rt,
		splitter:   cluster.NewSplitCoordinator(mgr, logger),
		dispatcher: dispatcher,
		peers:      peers,
		threshold:  threshold,
		tracker:    tracker,
		resolver:   resolver,
		logger:     logger,
		regionNodes: map[uint64]*cluster.Node{
			1: host, // the bootstrap node hosts region 1
		},
		lastSplit: make(map[uint64]time.Time),
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// regionLeadershipProvider reports per-region Raft leadership so the region
// manager's leader-publication loop can update every region (not just the
// bootstrap region). A region with no group node yet reports not-leader. It
// also implements the single-group LeadershipProvider: IsLeader reports the
// bootstrap region (region 1), whose group is hosted by the bootstrap node.
type regionLeadershipProvider struct {
	coord *RegionCoordinator
}

func (p regionLeadershipProvider) IsLeader() bool { return p.IsLeaderFor(1) }

func (p regionLeadershipProvider) IsLeaderFor(regionID uint64) bool {
	if n := p.coord.NodeForRegion(regionID); n != nil {
		return n.IsLeader()
	}
	return false
}

// LeadershipProvider returns a per-region leadership signal for the region
// manager's leader-publication loop. The server swaps this onto the manager
// after construction (the manager and coordinator reference each other).
func (rc *RegionCoordinator) LeadershipProvider() cluster.LeadershipProvider {
	return regionLeadershipProvider{coord: rc}
}

// SetGeoPolicyProvider wires the coordinator's split logic to the geo policy
// source so split-created regions get zone-pinned placement (Phase 6).
func (rc *RegionCoordinator) SetGeoPolicyProvider(g cluster.GeoPolicyProvider) {
	rc.splitter.SetGeoPolicyProvider(g)
}

// SetZone sets this process's availability zone. When set, the coordinator
// biases per-region raft election timeouts so same-zone followers win leader
// elections (ADR-006 soft preference). Safe to call once before Run.
func (rc *RegionCoordinator) SetZone(zone string) {
	rc.zone = zone
}

// NodeForRegion returns the local Raft group node hosting region, or nil when
// this process does not host that region yet.
func (rc *RegionCoordinator) NodeForRegion(id uint64) *cluster.Node {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.regionNodes[id]
}

// electionTickFor returns the raft election timeout (in ticks) for a region
// hosted on this process. Soft zone preference: a node inside the region's
// preferred zone keeps the fast default tick so it campaigns and wins first;
// a node outside the preferred zone gets a doubled timeout and is unlikely to
// become leader while a same-zone follower is alive (ADR-006). Global or
// unknown zones get no bias.
func (rc *RegionCoordinator) electionTickFor(r cluster.Region) int {
	if rc.zone == "" || r.PreferredZone == "" || r.PreferredZone == cluster.GeoZoneGlobal {
		return electionTickDefault
	}
	if rc.zone == r.PreferredZone {
		return electionTickDefault
	}
	return electionTickOffZone
}

// HostRegion creates and starts a local Raft group node for region r, sharing
// the bootstrap node's transport. Idempotent: hosting an already-hosted region
// is a no-op, because the raft library bakes the election clock in when the
// node starts, so a PreferredZone change can only affect region nodes built
// after the update. Safe to call from any goroutine.
func (rc *RegionCoordinator) HostRegion(_ context.Context, r cluster.Region) error {
	rc.mu.Lock()
	if _, ok := rc.regionNodes[r.ID]; ok {
		rc.mu.Unlock()
		// The region node already exists. Its election tick was fixed at
		// creation time (cluster.Node has no post-start mutable bias: raft
		// bakes the election clock in at StartNode). A policy re-pin that
		// changes the preferred zone therefore affects only nodes created
		// for this region after the update, not one already running.
		return nil
	}
	rc.mu.Unlock()

	cfg := cluster.NodeConfig{
		NodeID:          rc.nodeID,
		GroupID:         r.ID,
		Peers:           rc.peers,
		ElectionTick:    rc.electionTickFor(r),
		HeartbeatTick:   1,
		TickInterval:    50 * time.Millisecond,
		Dispatcher:      rc.dispatcher,
		Logger:          rc.logger,
		Transport:       rc.host.Transport(),
		SharedTransport: true,
	}
	n, err := cluster.NewNode(cfg)
	if err != nil {
		return err
	}
	if err := n.Start(); err != nil {
		return err
	}
	if rc.tracker != nil {
		n.FSM().SetSizeTracker(rc.tracker, rc.resolver)
	}
	rc.mu.Lock()
	// Another goroutine may have won the race; stop the stale node we built.
	if _, ok := rc.regionNodes[r.ID]; ok {
		rc.mu.Unlock()
		n.Stop()
		return nil
	}
	rc.regionNodes[r.ID] = n
	rc.mu.Unlock()
	if rc.logger.Enabled(log.LevelInfo) {
		rc.logger.Log(log.LevelInfo, "region coordinator: hosting region",
			log.Uint64("region_id", r.ID),
			log.Int("peers", len(r.Peers)),
		)
	}
	return nil
}

// EnsureAllRegions reads every region from etcd and hosts any that this
// process does not yet serve. Recent splits are excluded so the new group can
// elect a leader before the watcher fires twice.
func (rc *RegionCoordinator) EnsureAllRegions(ctx context.Context) {
	resp, err := rc.cli.Get(ctx, cluster.RegionKeyPrefix(), clientv3.WithPrefix())
	if err != nil {
		return
	}
	for _, kv := range resp.Kvs {
		r, ok := cluster.DecodeRegion(kv.Value)
		if !ok {
			continue
		}
		if err := rc.HostRegion(ctx, r); err != nil {
			if rc.logger.Enabled(log.LevelWarn) {
				rc.logger.Log(log.LevelWarn, "region coordinator: host region failed",
					log.Uint64("region_id", r.ID),
					log.String("error", err.Error()),
				)
			}
		}
	}
}

// Split executes a region split on this node, which must be the region leader.
// It quiesces the region's Raft group, splits the key range in etcd, hosts the
// new right-hand region, then resumes. It returns the split result or an
// error (including ErrNotLeader; the caller should retry elsewhere).
func (rc *RegionCoordinator) Split(ctx context.Context, req cluster.SplitRequest) (*cluster.SplitResult, error) {
	rc.splitMu.Lock()
	defer rc.splitMu.Unlock()

	regionNode := rc.NodeForRegion(req.RegionID)
	if regionNode != nil && !regionNode.IsLeader() {
		return nil, cluster.ErrNotLeader
	}

	// Quiesce so no in-flight proposals interleave with the split.
	quiesced := true
	if regionNode != nil {
		if !regionNode.Quiesce(splitCooldownTimeout) {
			regionNode.Resume()
			return nil, ctx.Err()
		}
	}
	defer func() {
		if quiesced && regionNode != nil {
			regionNode.Resume()
		}
	}()

	result, err := rc.splitter.Split(ctx, req)
	if err != nil {
		return nil, err
	}

	// Host the new right-hand region group locally; every node converges via
	// its own coordinator loop and the etcd watcher.
	if err := rc.HostRegion(ctx, cluster.Region{ID: result.RightID}); err != nil {
		quiesced = false // do not resume the old group: the split partly failed
		return nil, err
	}

	rc.mu.Lock()
	rc.lastSplit[req.RegionID] = time.Now()
	rc.mu.Unlock()

	if rc.logger.Enabled(log.LevelInfo) {
		rc.logger.Log(log.LevelInfo, "region coordinator: split complete",
			log.Uint64("region_id", req.RegionID),
			log.Uint64("right_region_id", result.RightID),
		)
	}
	return result, nil
}

// Run starts the coordinator's background loop: periodically converge region
// hosting and auto-split regions that exceed the threshold. Returns when ctx
// is cancelled.
func (rc *RegionCoordinator) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	rc.EnsureAllRegions(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rc.EnsureAllRegions(ctx)
			rc.autoSplit(ctx)
		}
	}
}

// autoSplit checks every region's reported size and — when this node is the
// region leader and the size exceeds the threshold — triggers a split.
func (rc *RegionCoordinator) autoSplit(ctx context.Context) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.threshold == 0 {
		return
	}
	resp, err := rc.cli.Get(context.Background(), cluster.RegionKeyPrefix(), clientv3.WithPrefix())
	if err != nil {
		return
	}
	for _, kv := range resp.Kvs {
		r, ok := cluster.DecodeRegion(kv.Value)
		if !ok {
			continue
		}
		if r.SizeBytes <= rc.threshold {
			continue
		}
		if rc.regionNodes[r.ID] == nil {
			continue // not yet hosted locally; the hosting loop will catch it
		}
		if !rc.regionNodes[r.ID].IsLeader() {
			continue
		}
		if cooldown, ok := rc.lastSplit[r.ID]; ok && time.Since(cooldown) < regionCooldown {
			continue
		}
		if rc.logger.Enabled(log.LevelInfo) {
			rc.logger.Log(log.LevelInfo, "region coordinator: size threshold exceeded, splitting",
				log.Uint64("region_id", r.ID),
				log.Uint64("size_bytes", r.SizeBytes),
				log.Uint64("threshold", rc.threshold),
			)
		}
		go func(id uint64) {
			splitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, serr := rc.Split(splitCtx, cluster.SplitRequest{RegionID: id}); serr != nil {
				if rc.logger.Enabled(log.LevelWarn) {
					rc.logger.Log(log.LevelWarn, "region coordinator: auto split failed",
						log.Uint64("region_id", id),
						log.String("error", serr.Error()),
					)
				}
			}
		}(r.ID)
	}
}

// Stop stops all non-bootstrap region nodes (the bootstrap node is stopped by
// the server's normal shutdown path).
func (rc *RegionCoordinator) Stop() {
	rc.mu.Lock()
	nodes := make([]*cluster.Node, 0, len(rc.regionNodes))
	for id, n := range rc.regionNodes {
		if id == 1 || n == rc.host {
			continue
		}
		nodes = append(nodes, n)
	}
	rc.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
}
