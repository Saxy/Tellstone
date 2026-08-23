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

// Delete routes a deletion through Raft. It returns whether the key existed
// plus an error for the failure cases: not leader (MOVED redirect, same as
// Set), encode failure, or propose/timeout failure. The boolean is only
// meaningful when err is nil.
func (cs *clusterStore) Delete(key string) (bool, error) {
	if !cs.node.IsLeader() {
		if cs.logger.Enabled(log.LevelDebug) {
			cs.logger.Log(log.LevelDebug, "cluster store: DEL rejected, not leader",
				log.String("key", key),
			)
		}
		return false, fmt.Errorf("MOVED: not leader; redirect to leader")
	}
	if cs.logger.Enabled(log.LevelDebug) {
		cs.logger.Log(log.LevelDebug, "cluster store: DEL proposing via raft",
			log.String("key", key),
		)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
	if err := cs.node.ProposeAndWait(ctx, data); err != nil {
		if cs.logger.Enabled(log.LevelError) {
			cs.logger.Log(log.LevelError, "cluster store: DEL propose failed",
				log.String("error", err.Error()),
				log.String("key", key),
			)
		}
		return false, err
	}
	return true, nil
}
