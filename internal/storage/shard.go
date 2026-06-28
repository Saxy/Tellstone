/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: shard.go
Description: Defines individual memory shard buckets and localized locking mechanisms.

Authors:

	Maximilian Hagen
*/
package storage

import (
	"sync"
)

type ShardCount uint32

// shardCount is the default number of shards to use.
// Computing in powers of two is extremely efficient.
// If we set the number of shards to a fixed power of two such as 2^8 = 256,
// we can replace the mathematically expensive modulo operator (%)
// with an extremely fast bitwise AND operation (&) when calculating the shard
/**
Why 256 Shards?
 - Zero-Alloc Sharding: 256 matches an uint8 boundary.
 - Shard calculation uses instant bit masking (uint8(hash & 0xff)) instead of heavy modulo arithmetic.
 - No Heap Escapes: Metrics can be tracked via a flat, primitive array ([256]int64) rather than dynamic slices or maps, ensuring $0\text{ B/op}$.
 - No Core Contention: On high-thread CPUs (like the 32-thread Ryzen 9 9950X),256 separate mutex-shards eliminate thread lock collisions.
 - Trade-off: Costs 2–5 MB of idle RAM at startup due to empty map initialization headers.
Why do the Metrics Split (Tellstone vs. Redis)?
 - Redis Flaw: Redis bundles data and socket buffer memory together. Network spikes can trigger accidental data eviction.
Tellstone Solution:
 - 1. Hot Path: Network loop only checks a lock-free atomic byte counter (0-alloc).Purely deterministic.
 - 2. Background: Heavy runtime.ReadMemStats sweeps are banned from the network layer and isolated in a 1-second background tick.
      If socket bloat risks an OOM, we drop connections but keep your database memory intact.
*/
const shardCount ShardCount = 256

// Shard represents a single memory space with its own mutex
type Shard struct {
	sync.RWMutex
	items map[string]Item
}

type shardMetrics struct {
	missCount     uint64
	hitCount      uint64
	totalCommands uint64
	_             [5]uint64 // Padding auf 64 Bytes gegen False Sharing
}
