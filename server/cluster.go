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
	"github.com/Saxy/Tellstone/internal/shard"
)

// shardDispatcher implements cluster.Dispatcher by routing committed Raft
// entries to the correct local shard via the same FNV-1a hash the normal
// request path uses. This ensures replicated data lands in the same shard
// on every node.
type shardDispatcher struct {
	shards []*shard.Shard
	logger log.Logger
}

func newShardDispatcher(shards []*shard.Shard, logger log.Logger) *shardDispatcher {
	return &shardDispatcher{shards: shards, logger: logger}
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
	shard := routeKey(d.shards, key)
	shardIdx := shard.ID
	resp := shard.Execute(opStr, key, value, ttl)
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

// routeKey picks the shard that owns a key using FNV-1a, matching the router.
func routeKey(shards []*shard.Shard, key string) *shard.Shard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	idx := h % uint32(len(shards))
	return shards[idx]
}

// clusterStore wraps a RouterStore and routes writes through Raft when
// cluster mode is active. Reads always go to the local engine. Writes on a
// non-leader return an error so the caller can redirect the client.
type clusterStore struct {
	local  *RouterStore
	node   *cluster.Node
	logger log.Logger
}

func newClusterStore(local *RouterStore, node *cluster.Node, logger log.Logger) *clusterStore {
	return &clusterStore{local: local, node: node, logger: logger}
}

func (cs *clusterStore) Get(key string) ([]byte, bool) {
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

func (cs *clusterStore) Set(key string, value []byte, ttl time.Duration) error {
	if !cs.node.IsLeader() {
		if cs.logger.Enabled(log.LevelDebug) {
			cs.logger.Log(log.LevelDebug, "cluster store: SET rejected, not leader",
				log.String("key", key),
			)
		}
		return fmt.Errorf("MOVED: not leader; redirect to leader")
	}
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: SET proposing via raft",
			log.String("key", key),
			log.Int("value_len", len(value)),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := cluster.EncodeSet(key, value, ttl)
	if err := cs.node.ProposeAndWait(ctx, data); err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: SET propose failed",
				log.String("error", err.Error()),
				log.String("key", key),
			)
		}
		return err
	}
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: SET propose succeeded",
			log.String("key", key),
		)
	}
	return nil
}

func (cs *clusterStore) Delete(key string) bool {
	if !cs.node.IsLeader() {
		if cs.logger.Enabled(log.LevelDebug) {
			cs.logger.Log(log.LevelDebug, "cluster store: DEL rejected, not leader",
				log.String("key", key),
			)
		}
		return false
	}
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: DEL proposing via raft",
			log.String("key", key),
		)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := cluster.EncodeDel(key)
	if err := cs.node.ProposeAndWait(ctx, data); err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: DEL propose failed",
				log.String("error", err.Error()),
				log.String("key", key),
			)
		}
		return false
	}
	return true
}
