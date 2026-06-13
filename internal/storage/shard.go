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
const shardCount ShardCount = 256

// Shard represents a single memory space with its own mutex
type Shard struct {
	sync.RWMutex
	items map[string]Item
}
