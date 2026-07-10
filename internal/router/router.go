/*
Package router
Tellstone Request Router
File: router.go
Description: Routes incoming requests to the correct shard using FNV-1a hashing. Uses modulo against the shard count so any positive shard count works, not only powers of two.

Authors:

	Maximilian Hagen
*/
package router

import (
	"time"

	"github.com/Saxy/Tellstone/internal/shard"
)

type Router struct {
	shards    []*shard.Shard
	numShards uint32
}

func New(shards []*shard.Shard) *Router {
	return &Router{
		shards:    shards,
		numShards: uint32(len(shards)),
	}
}

func hashKey(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func (r *Router) Dispatch(op string, key string, value []byte, ttl time.Duration) shard.Response {
	sid := hashKey(key) % r.numShards
	return r.shards[sid].Execute(op, key, value, ttl)
}

func (r *Router) NumShards() int {
	return int(r.numShards)
}
