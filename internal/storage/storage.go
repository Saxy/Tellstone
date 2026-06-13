/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: storage.go

Description: implements the ultra-high-performance core in-memory engine
for Tellstone.

It provides a lock-free, linearly scaling key-value architecture designed
specifically to intercept high-frequency cloud-native application bursts.
By utilizing 256 ring-fenced, concurrently sharded memory buckets, the storage
engine eliminates global thread contention and achieves sub-millisecond execution
pipelines. Shard index resolution is optimized via deterministic FNV-1a hashing
and low-overhead bitwise mask processing rather than traditional modulo arithmetic.

Key features include:
  - Zero-Copy Protobuf-friendly raw binary storage (`[]byte`).
  - Thread-safe runtime access via fine-grained localized RWMutex boundaries.
  - O(1) active expiration compatibility with the Tellstone Timing Wheel.

"A contest of focus. Keep yours made of steel."

Authors:

	Maximilian Hagen
*/
package storage

import "time"

// Item represents a single binary value which lives inside the memory
type Item struct {
	Value      []byte
	Expiration time.Time
}
