/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: engine.go
Description: defines the core functionality of the storage engine, including the Engine struct and its associated methods.

Authors:

	Maximilian Hagen
*/
package storage

import (
	"time"
)

// FNV-1a 32-bit hashing constants defined by the Fowler-Noll-Vo algorithm specification.
// These constants are mathematically optimized to ensure a uniform distribution of hash values
// with minimal collision rates, providing an O(1) allokation-free shard indexing pipeline.
const (
	// offset32 is the 32-bit FNV offset basis initialization value (2^24 + 403).
	// It serves as the non-zero starting point for the hash calculation to ensure
	// that strings with leading null bytes or similar prefixes yield highly distinct hashes.
	offset32 = 2166136261

	// prime32 is the 32-bit FNV prime multiplier.
	// This specific prime number (2^24 + 403) is chosen because its bit pattern
	// excels at mixing the bits of the hash during the multiplication step,
	// maximizing the avalanche effect across the 256 storage shards.
	prime32 = 16777619
)

// Engine represents a collection of shards
type Engine struct {
	shards      []*Shard
	chronometer *Chronometer
}

// NewEngine creates a new engine with the default number of shards to prevent thread contention.
func NewEngine(interval time.Duration, numSlots uint32) *Engine {
	// Validate parameters to avoid runtime panics later.
	if interval <= 0 {
		panic("chronometer interval must be > 0")
	}
	if numSlots == 0 {
		panic("chronometer numSlots must be > 0")
	}
	e := new(Engine)
	e.shards = make([]*Shard, shardCount)
	for i := ShardCount(0); i < shardCount; i++ {
		e.shards[i] = &Shard{items: make(map[string]Item)}
	}
	e.chronometer = NewChronometer(e.Delete, interval, numSlots)
	e.chronometer.Start()
	return e
}

// Close shuts down the engine and releases any background resources.
func (e *Engine) Close() {
	if e.chronometer != nil {
		e.chronometer.Stop()
	}
}

// getShardIndex returns the index of the shard for the given key.
func (e *Engine) getShardIndex(key string) uint32 {
	hash := uint32(offset32)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash & (uint32(shardCount) - 1)
}

// Set the value for the given key in the engine.
// Locking the shard before setting the value.
func (e *Engine) Set(key string, value []byte, ttl time.Duration) {
	shard := e.shards[e.getShardIndex(key)]
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	shard.Lock()
	shard.items[key] = Item{
		Value:      value,
		Expiration: exp,
	}
	shard.Unlock()
	if ttl > 0 && e.chronometer != nil {
		e.chronometer.Register(key, ttl)
	}
}

// Delete the shard from the storage engine
func (e *Engine) Delete(key string) {
	shard := e.shards[e.getShardIndex(key)]
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// Get shard from the storage engine by acquiring a read lock
func (e *Engine) Get(key string) ([]byte, bool) {
	shard := e.shards[e.getShardIndex(key)]
	shard.RLock()
	item, exist := shard.items[key]
	shard.RUnlock()
	if !exist {
		return nil, false
	}
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		e.Delete(key)
		return nil, false
	}
	return item.Value, true
}
