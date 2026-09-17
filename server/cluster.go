/*
Package server
Tellstone Cloud-Native In-Memory Database
File: cluster.go
Description: Cluster mode integration. Provides the Dispatcher adapter that
bridges the Raft FSM's committed entries to the shared-nothing shard layer,
and a cluster-aware store that routes write operations through Raft consensus
when --cluster-mode is active. Reads are always served locally for low latency.

Authors:

	Maximilian Hagen
*/
package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/cluster"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/router"
	"github.com/Saxy/Tellstone/internal/shard"
)

// shardDispatcher implements cluster.Dispatcher by routing committed Raft
// entries to the correct local shard via the router's placement hash. Reusing
// Router.ShardID guarantees replicated data lands in the same shard on every
// node — a private copy of the hash could silently diverge from the request
// routing path.
type shardDispatcher struct {
	shards []*shard.Shard
	router *router.Router
	logger log.Logger
}

func newShardDispatcher(shards []*shard.Shard, logger log.Logger) *shardDispatcher {
	return &shardDispatcher{shards: shards, router: router.New(shards), logger: logger}
}

// Dispatch routes a decoded operation to the local shard that owns the key.
func (d *shardDispatcher) Dispatch(key string, op byte, value []byte, ttl time.Duration) error {
	var opStr string
	switch op {
	case cluster.OpSet:
		opStr = shard.CmdSet
	case cluster.OpDel:
		opStr = shard.CmdDel
	default:
		if d.logger.Enabled(log.LevelWarn) {
			d.logger.Log(log.LevelWarn, "cluster dispatcher: unknown op",
				log.Uint("op", uint32(op)),
				log.String("key", key),
			)
		}
		return nil
	}
	target := d.shards[d.router.ShardID(key)]
	shardIdx := target.ID
	resp := target.Execute(opStr, key, value, ttl)
	if resp.Err != nil {
		if d.logger.Enabled(log.LevelError) {
			d.logger.Log(log.LevelError, "cluster dispatcher: shard execute failed",
				log.String("error", resp.Err.Error()),
				log.String("op", opStr),
				log.String("key", key),
				log.Uint("shard_id", uint32(shardIdx)),
				log.Bool("ok", resp.OK),
			)
		}
		return resp.Err
	}
	if d.logger.Enabled(log.LevelDebug) {
		d.logger.Log(log.LevelDebug, "cluster dispatcher: shard execute ok",
			log.String("op", opStr),
			log.String("key", key),
			log.Uint("shard_id", uint32(shardIdx)),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	return nil
}

// localReader is the seam clusterStore reads through. The production
// implementation is *RouterStore (reads from the shared-nothing shard layer);
// the manual split test injects a lightweight in-memory key store.
type localReader interface {
	Get(key string) ([]byte, bool)
}

// clusterStore wraps a RouterStore and routes writes through Raft when
// cluster mode is active. Phase 3/4 routing: a write is proposed directly
// against the local Raft group of the region that owns the key; when this node
// is not that region's leader, the write is forwarded to the region leader
// resolved from the local routing table. Reads go to the local engine after a
// linearizable ReadIndex round-trip against the owning region's local group;
// if that round-trip cannot complete in time, the local value is served as a
// best-effort fallback and may be slightly stale (read-anywhere).
type clusterStore struct {
	local       localReader
	node        *cluster.Node
	coord       *RegionCoordinator
	rt          *cluster.RoutingTable
	mgr         *cluster.RegionManager
	geo         *cluster.GeoManager // Phase 6: zone registry + policy for read routing (nil = not geo)
	clientZone  string              // Phase 6: zone of this node (the read's client zone)
	logger log.Logger
	// geoForward metrics (Phase 6 step 7): a forwarded write whose region
	// leader sits in a different zone than this node pays cross-zone latency.
	// Counters are lock-free atomics read by the /metrics scrape.
	crossZoneForwards  atomic.Uint64
	crossZoneLatencyNs atomic.Uint64
	sameZoneForwards   atomic.Uint64
}

// GeoCrossZoneForwards reports how many writes were forwarded to a region
// leader in a different zone than this node.
func (cs *clusterStore) GeoCrossZoneForwards() uint64 {
	return cs.crossZoneForwards.Load()
}

// GeoCrossZoneForwardLatencyNanosTotal is the cumulative latency (ns) spent
// forwarding writes to cross-zone region leaders.
func (cs *clusterStore) GeoCrossZoneForwardLatencyNanosTotal() uint64 {
	return cs.crossZoneLatencyNs.Load()
}

// GeoSameZoneForwards reports how many writes were forwarded to a region
// leader in this node's own zone.
func (cs *clusterStore) GeoSameZoneForwards() uint64 {
	return cs.sameZoneForwards.Load()
}

func newClusterStore(local localReader, node *cluster.Node, rt *cluster.RoutingTable, mgr *cluster.RegionManager, coord *RegionCoordinator, logger log.Logger) *clusterStore {
	return &clusterStore{local: local, node: node, coord: coord, rt: rt, mgr: mgr, logger: logger}
}

// SetGeo wires the Phase 6 geo sources into the cluster store after the
// cluster's GeoManager is online. From then on reads resolve the owning
// region through FindInZone, preferring a member that sits in the request's
// zone when the region is zone-pinned (ADR-006 §Read Routing). A nil geo
// manager keeps the plain zone-agnostic path.
func (cs *clusterStore) SetGeo(g *cluster.GeoManager, clientZone string) {
	cs.geo = g
	cs.clientZone = clientZone
}

// nodeForRegion returns the local Raft group node that hosts the region owning
// the key's route, falling back to the bootstrap node when unavailable.
func (cs *clusterStore) nodeForRegion(route *cluster.RegionRoute) *cluster.Node {
	if cs.coord != nil && route != nil {
		if n := cs.coord.NodeForRegion(route.ID); n != nil {
			return n
		}
	}
	return cs.node
}

func (cs *clusterStore) Get(key string) ([]byte, bool) {
	// Linearizable read: wait (bounded) until this replica has applied at least
	// the Raft commit index observed by ReadIndex, then serve from the local
	// engine. On timeout/failure we fall back to the local value (best-effort,
	// may be slightly stale) rather than failing the read.
	var rn *cluster.Node
	var route *cluster.RegionRoute
	if cs.geo != nil && cs.clientZone != "" {
		route = cs.rt.FindInZone([]byte(key), cs.clientZone, cs.geo.Zones())
	} else {
		route = cs.rt.Find([]byte(key))
	}
	if route != nil {
		rn = cs.nodeForRegion(route)
	}
	if rn == nil {
		rn = cs.node
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rn.LinearizableRead(ctx); err != nil {
		if cs.logger.Enabled(log.LevelWarn) {
			cs.logger.Log(log.LevelWarn, "cluster store: linearizable read failed, serving local (best-effort, may be stale)",
				log.String("error", err.Error()),
				log.String("key", key),
			)
		}
	}
	val, ok := cs.local.Get(key)
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: GET",
			log.String("key", key),
			log.Bool("found", ok),
			log.Int("value_len", len(val)),
		)
	}
	return val, ok
}

// routeWritePropose proposes a single log-entry write (or a chunk chain) on
// the region group that owns the key, forwarding to the region leader when
// this node is not the leader. It is resilient to a stale or unreachable
// region leader (D4 churn): on a failed forward it refreshes the routing table
// and retries, and it waits briefly for the leader to be claimed before
// reporting CLUSTERNOTREADY. payloads holds one entry (fast path) or a chunk
// chain for large values.
func (cs *clusterStore) routeWrite(key string, payloads [][]byte) error {
	const (
		maxAttempts = 6
		backoff     = 200 * time.Millisecond
		// Overall cap for a single Set/Delete so a run of stale/unreachable
		// leaders cannot block the caller indefinitely.
		writeBudget = 8 * time.Second
	)
	chunked := len(payloads) > 1
	deadline := time.Now().Add(writeBudget)
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		route := cs.rt.Find([]byte(key))
		if route == nil {
			return fmt.Errorf("CLUSTERDOWN: no region covers key %q", key)
		}
		if route.Leader == 0 {
			cs.refreshRouting()
			if attempt < maxAttempts-1 {
				time.Sleep(backoff)
			}
			continue
		}
		regionNode := cs.nodeForRegion(route)
		// Bound each attempt by the remaining overall budget.
		budget := time.Until(deadline)
		if budget <= 0 {
			break
		}
		if budget > 5*time.Second {
			budget = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		var err error
		if route.Leader == cs.nodeID() {
			if chunked {
				err = regionNode.ProposeChunked(ctx, payloads)
			} else {
				err = regionNode.ProposeAndWait(ctx, payloads[0])
			}
		} else {
			// Phase 6 step 7: classify the forward by the leader's zone so
			// latency metrics distinguish same-zone (fast) forwards from
			// cross-zone (ocean-latency) forwards. Leader zone is looked up
			// from the node registry; unknown/global zones are treated as
			// same-zone (no penalty).
			start := time.Now()
			if chunked {
				err = regionNode.ForwardChunks(ctx, route.Leader, payloads)
			} else {
				err = regionNode.ForwardWrite(ctx, route.Leader, payloads[0])
			}
			if err == nil && cs.recordGeoForward(route) {
				cs.crossZoneLatencyNs.Add(uint64(time.Since(start).Nanoseconds()))
			}
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if cs.logger.Enabled(log.LevelDebug) {
			cs.logger.Log(log.LevelDebug, "cluster store: write failed, refreshing routing and retrying",
				log.String("key", key),
				log.String("error", err.Error()),
				log.Int("attempt", attempt+1),
			)
		}
		cs.refreshRouting()
		if attempt < maxAttempts-1 && time.Until(deadline) > 0 {
			time.Sleep(backoff)
		}
	}
	if lastErr != nil {
		return fmt.Errorf("MOVED: write to region leader failed: %w", lastErr)
	}
	return fmt.Errorf("CLUSTERNOTREADY: region leader not elected for key %q", key)
}

// nodeID returns this node's Raft node ID.
func (cs *clusterStore) nodeID() uint64 {
	if cs.coord != nil {
		return cs.coord.host.NodeID()
	}
	return cs.node.NodeID()
}

// recordGeoForward classifies a forwarded write as cross-zone or same-zone
// and increments the corresponding counter. Returns true when the forward is
// cross-zone (the caller should record latency).
func (cs *clusterStore) recordGeoForward(route *cluster.RegionRoute) bool {
	if cs.geo == nil || cs.clientZone == "" || route == nil {
		cs.sameZoneForwards.Add(1)
		return false
	}
	leaderZone := cluster.ZoneOf(cs.geo.Zones(), route.Leader)
	if leaderZone != "" && leaderZone != cs.clientZone {
		cs.crossZoneForwards.Add(1)
		return true
	}
	cs.sameZoneForwards.Add(1)
	return false
}

// refreshRouting pulls the latest region metadata from etcd into the local
// table. Best-effort: a failure is ignored (the Watch will catch up).
func (cs *clusterStore) refreshRouting() {
	if cs.mgr == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = cs.mgr.Refresh(ctx)
}

func (cs *clusterStore) Set(key string, value []byte, ttl time.Duration) error {
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: SET",
			log.String("key", key),
			log.Int("value_len", len(value)),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	// Values at or below ChunkMax travel as a single atomic raft entry.
	// Larger values are split into a chunk chain (writeSeq 1); the FSM
	// reassembles on the last chunk. Exactly one chain per key is in flight
	// from a caller, so interleaving is only possible across clients and
	// self-heals via whole-chain retry.
	if len(value) > cluster.ChunkMax {
		chunks, err := encodeChunks(key, value, ttl)
		if err != nil {
			return err
		}
		if err = cs.routeWrite(key, chunks); err != nil {
			if cs.logger.Enabled(log.LevelError) {
				cs.logger.Log(log.LevelError, "cluster store: SET (chunked) failed",
					log.String("error", err.Error()),
					log.String("key", key),
				)
			}
			return err
		}
		if cs.logger.Enabled(log.LevelDebug) {
			cs.logger.Log(log.LevelDebug, "cluster store: SET (chunked) succeeded",
				log.String("key", key),
				log.Int("chunks", len(chunks)),
			)
		}
		return nil
	}
	data, err := cluster.EncodeSet(key, value, ttl)
	if err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: SET encode failed",
				log.String("error", err.Error()),
				log.Int("key_len", len(key)),
			)
		}
		return err
	}
	if err = cs.routeWrite(key, [][]byte{data}); err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: SET failed",
				log.String("error", err.Error()),
				log.String("key", key),
			)
		}
		return err
	}
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: SET succeeded",
			log.String("key", key),
		)
	}
	return nil
}

// encodeChunks splits a large value into a chunk chain of log-entry payloads.
func encodeChunks(key string, value []byte, ttl time.Duration) ([][]byte, error) {
	total := (len(value) + cluster.ChunkMax - 1) / cluster.ChunkMax
	chunks := make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		lo := i * cluster.ChunkMax
		hi := lo + cluster.ChunkMax
		if hi > len(value) {
			hi = len(value)
		}
		enc, err := cluster.EncodeChunkSet(key, ttl, 1, total, i, value[lo:hi])
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, enc)
	}
	return chunks, nil
}

// Delete routes a deletion through Raft (or forwards it to the region leader).
// It returns (true, nil) on successful replication; the boolean is NOT a report
// of whether the key previously existed — the forwarded/raft apply path does not
// surface that distinction. Error cases: encode failure, no covering region
// (CLUSTERDOWN), or propose/forward failure.
func (cs *clusterStore) Delete(key string) (bool, error) {
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: DEL",
			log.String("key", key),
		)
	}
	data, err := cluster.EncodeDel(key)
	if err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: DEL encode failed",
				log.String("error", err.Error()),
				log.Int("key_len", len(key)),
			)
		}
		return false, err
	}
	if err = cs.routeWrite(key, [][]byte{data}); err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: DEL failed",
				log.String("error", err.Error()),
				log.String("key", key),
			)
		}
		return false, err
	}
	return true, nil
}
