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

// clusterStore wraps a RouterStore and routes writes through Raft when
// cluster mode is active. Phase 3 routing: a write is proposed directly when
// this node is the Raft leader, otherwise it is forwarded to the leader of
// the region that owns the key (read from the local routing table). Reads go
// to the local engine after a linearizable ReadIndex round-trip; if that
// round-trip cannot complete in time, the local value is served as a
// best-effort fallback and may be slightly stale (read-anywhere).
type clusterStore struct {
	local  *RouterStore
	node   *cluster.Node
	rt     *cluster.RoutingTable
	mgr    *cluster.RegionManager
	logger log.Logger
}

func newClusterStore(local *RouterStore, node *cluster.Node, rt *cluster.RoutingTable, mgr *cluster.RegionManager, logger log.Logger) *clusterStore {
	return &clusterStore{local: local, node: node, rt: rt, mgr: mgr, logger: logger}
}

func (cs *clusterStore) Get(key string) ([]byte, bool) {
	// Linearizable read: wait (bounded) until this replica has applied at least
	// the Raft commit index observed by ReadIndex, then serve from the local
	// engine. On timeout/failure we fall back to the local value (best-effort,
	// may be slightly stale) rather than failing the read.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cs.node.LinearizableRead(ctx); err != nil {
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

// routeWrite proposes directly when this node leads the Raft group, otherwise
// forwards the operation to the region leader resolved from the routing table.
// It is resilient to a stale or unreachable region leader (D4 churn): on a
// failed forward it refreshes the routing table and retries, and it waits
// briefly for the leader to be claimed before reporting CLUSTERNOTREADY.
func (cs *clusterStore) routeWrite(key string, data []byte) error {
	const (
		maxAttempts = 6
		backoff     = 200 * time.Millisecond
		// Overall cap for a single Set/Delete so a run of stale/unreachable
		// leaders cannot block the caller indefinitely.
		writeBudget = 8 * time.Second
	)
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
		// Bound each forward attempt by the remaining overall budget.
		budget := time.Until(deadline)
		if budget <= 0 {
			break
		}
		if budget > 5*time.Second {
			budget = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		err := cs.node.ForwardWrite(ctx, route.Leader, data)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if cs.logger.Enabled(log.LevelDebug) {
			cs.logger.Log(log.LevelDebug, "cluster store: forward failed, refreshing routing and retrying",
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
	if err = cs.routeWrite(key, data); err != nil {
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
	if err = cs.routeWrite(key, data); err != nil {
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
