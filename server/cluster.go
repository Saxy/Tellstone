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
	"github.com/Saxy/Tellstone/internal/cluster/network"
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
	local      localReader
	node       *cluster.Node
	coord      *RegionCoordinator
	rt         *cluster.RoutingTable
	mgr        *cluster.RegionManager
	geo        *cluster.GeoManager // Phase 6: zone registry + policy for read routing (nil = not geo)
	clientZone string              // Phase 6: zone of this node (the read's client zone)
	// Phase 7 federation (ADR-011). fed resolves a key's home cluster from
	// the operator policy; gw forwards remote keys through this node's
	// dedicated gateway transport. Both are nil when this node is not
	// federated, keeping the cluster store's plain path untouched.
	fed       *cluster.FederationManager
	gw        *cluster.Gateway
	clusterID uint64
	logger    log.Logger
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

// SetFederation wires the Phase 7 federation sources into the cluster store
// after the node's gateway is online. From then on keys whose home cluster
// (per the operator federation policy) is not this one are forwarded through
// the gateway instead of reaching the local routing path.
func (cs *clusterStore) SetFederation(fed *cluster.FederationManager, gw *cluster.Gateway, clusterID uint64) {
	cs.fed = fed
	cs.gw = gw
	cs.clusterID = clusterID
}

// homeFor resolves a key's home cluster and whether it is remote. The
// default federation policy pins every key to the local cluster, so an
// unfederated node (fed nil) keeps every key local.
func (cs *clusterStore) homeFor(key string) (uint64, bool) {
	if cs.fed == nil {
		return cs.clusterID, false
	}
	home := cs.fed.Home([]byte(key))
	return home, home != cs.clusterID
}

// routeAnywhere routes a write payload (single entry or chunk chain) to the
// key's home cluster. Local keys take the existing raft routing path; keys
// owned by another cluster are forwarded to its gateway (Phase 7, ADR-011 D5:
// the write is ordered by the home cluster's own raft log). Failures are
// surfaced as CLUSTERDOWN-style errors per ADR-011 D6 — no buffering.
func (cs *clusterStore) routeAnywhere(key string, payloads [][]byte) error {
	home, remote := cs.homeFor(key)
	if !remote {
		return cs.routeWrite(key, payloads)
	}
	if cs.gw == nil {
		return fmt.Errorf("CLUSTERDOWN: key %q belongs to cluster %d but no gateway is configured", key, home)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := cs.gw.Call(ctx, home, network.OpXClusterWrite, cluster.EncodeXWrite(key, payloads)); err != nil {
		return fmt.Errorf("CLUSTERDOWN: cross-cluster write to cluster %d failed: %w", home, err)
	}
	return nil
}

// xGetErr reads a key from its home cluster through the gateway. A failed
// round-trip fails the read (ADR-011 D6): there is no stale fallback across
// clusters, so the error is surfaced to the caller — the command layer reports
// it as a storage error instead of a plain miss.
func (cs *clusterStore) xGetErr(key string, home uint64) ([]byte, bool, error) {
	if cs.gw == nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: cross-cluster read with no gateway configured",
				log.String("key", key),
				log.Uint64("home_cluster", home),
			)
		}
		return nil, false, fmt.Errorf("CLUSTERDOWN: key %q belongs to cluster %d but no gateway is configured", key, home)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	resp, err := cs.gw.Call(ctx, home, network.OpXClusterRead, cluster.EncodeXRead(key))
	if err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: cross-cluster read failed",
				log.String("key", key),
				log.Uint64("home_cluster", home),
				log.String("error", err.Error()),
			)
		}
		return nil, false, fmt.Errorf("CLUSTERDOWN: cross-cluster read of key %q in cluster %d failed: %w", key, home, err)
	}
	value, present, derr := cluster.DecodeXReadResp(resp)
	if derr != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: cross-cluster read reply malformed",
				log.String("key", key),
				log.String("error", derr.Error()),
			)
		}
		return nil, false, fmt.Errorf("CLUSTERDOWN: cross-cluster read of key %q in cluster %d returned a malformed reply: %w", key, home, derr)
	}
	return value, present, nil
}

// handleCrossClusterOp executes an inbound cross-cluster op forwarded through
// this node's gateway. It is wired as the gateway's executor in Run once the
// store exists, so the op runs against this cluster's proven local routing
// body. A forwarded read uses localRead directly (never full Get routing), so
// the home side serves it through the linearizable local path and cannot
// re-enter the gateway and loop.
func (cs *clusterStore) handleCrossClusterOp(op network.OpKind, payload []byte) ([]byte, error) {
	switch op {
	case network.OpXClusterWrite:
		key, chunks, err := cluster.DecodeXWrite(payload)
		if err != nil {
			return nil, fmt.Errorf("cluster gateway: malformed cross-cluster write: %w", err)
		}
		if err := cs.routeWrite(key, chunks); err != nil {
			return nil, err
		}
		return nil, nil
	case network.OpXClusterRead:
		key, err := cluster.DecodeXRead(payload)
		if err != nil {
			return nil, fmt.Errorf("cluster gateway: malformed cross-cluster read: %w", err)
		}
		value, present := cs.localRead(key)
		return cluster.EncodeXReadResp(value, present), nil
	default:
		return nil, fmt.Errorf("cluster gateway: unsupported cross-cluster op %d", op)
	}
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

// localRead serves a key through the linearizable local read path: wait
// (bounded) until this replica has applied at least the Raft commit index
// observed by ReadIndex, then serve from the local engine. On timeout/failure
// it falls back to the local value (best-effort, may be slightly stale) rather
// than failing the read. This is the body both reads and cross-cluster reads
// land on, so the home side of a forwarded read behaves exactly like a local
// get without re-entering federation routing.
func (cs *clusterStore) localRead(key string) ([]byte, bool) {
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

// GetErr is the error-surfacing read used by the command layer. A key whose
// home cluster is remote reads through the gateway, and a failed cross-cluster
// round-trip returns a CLUSTERDOWN error (ADR-011 D6) instead of an ordinary
// miss; a local key is served from localRead.
func (cs *clusterStore) GetErr(key string) ([]byte, bool, error) {
	// Phase 7: a key whose home cluster is remote reads through the gateway —
	// the home cluster serves it with the full linearizable read path.
	if home, remote := cs.homeFor(key); remote {
		return cs.xGetErr(key, home)
	}
	val, ok := cs.localRead(key)
	return val, ok, nil
}

// Get reads a key through the full cluster path, masking a failed
// cross-cluster read as a miss. Kept for callers that only care about
// presence; the command layer uses GetErr to surface federation failures.
func (cs *clusterStore) Get(key string) ([]byte, bool) {
	val, ok, _ := cs.GetErr(key)
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
		if err = cs.routeAnywhere(key, chunks); err != nil {
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
	if err = cs.routeAnywhere(key, [][]byte{data}); err != nil {
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
	if err = cs.routeAnywhere(key, [][]byte{data}); err != nil {
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
