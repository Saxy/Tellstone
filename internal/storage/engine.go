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
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

// FNV-1a 32-bit hashing constants defined by the Fowler-Noll-Vo algorithm specification.
// These constants are mathematically optimized to ensure a uniform distribution of hash values
// with minimal collision rates, providing an O(1) allokation-free shard indexing pipeline.
// plainBufPool provides reusable buffers for decrypted plaintext to avoid per‑call allocations.
var plainBufPool = sync.Pool{
	New: func() interface{} {
		// initial capacity 2048 bytes – will be grown if needed
		return make([]byte, 0, 2048)
	},
}

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
	shards       []*Shard
	chronometer  *Chronometer
	cryptoEngine *crypto.Engine
	logger       log.Logger
}

// NewEngine creates a new engine with the default number of shards to prevent thread contention.
func NewEngine(interval time.Duration, numSlots uint32, cryptoEngine *crypto.Engine, logger log.Logger) *Engine {
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
	e.chronometer = NewChronometer(e.Delete, interval, numSlots, logger)
	e.chronometer.Start()
	if cryptoEngine == nil {
		cryptoEngine, _ = crypto.NewEngine(nil, logger)
	}
	e.cryptoEngine = cryptoEngine
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	e.logger = logger
	if e.logger.Enabled(log.LevelInfo) {
		e.logger.Log(log.LevelInfo, "storage engine created")
	}
	return e
}

// Close shuts down the engine and releases any background resources.
func (e *Engine) Close() {
	if e.chronometer != nil {
		e.chronometer.Stop()
	}
	if e.logger.Enabled(log.LevelInfo) {
		e.logger.Log(log.LevelInfo, "storage engine and background chronometer stopped")
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
	var (
		exp        time.Time
		finalValue []byte
		err        error
	)
	idx := e.getShardIndex(key)
	shard := e.shards[idx]
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	if e.cryptoEngine.Enabled() {
		neededSize := 12 + len(value) + 16
		encryptedBuf := make([]byte, 0, neededSize)
		finalValue, err = e.cryptoEngine.EncryptInPlace(encryptedBuf, value)
		if err != nil {
			if e.logger.Enabled(log.LevelError) {
				e.logger.Log(log.LevelError, "in-place encryption failed during Set operation",
					log.String("key", key),
					log.Int("shard", int(idx)),
				)
			}
			return
		}
	} else {
		// No encryption: store the incoming slice directly to avoid allocation.
		// The caller must not modify the slice after Set (which is true for the benchmark).
		finalValue = value
	}
	shard.Lock()
	shard.items[key] = Item{
		Value:      finalValue,
		Expiration: exp,
	}
	shard.Unlock()
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "key written to engine state",
			log.String("key", key),
			log.Int("shard", int(idx)),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	if ttl > 0 && e.chronometer != nil {
		e.chronometer.Register(key, ttl)
	}
}

// Delete the shard from the storage engine
func (e *Engine) Delete(key string) {
	idx := e.getShardIndex(key)
	shard := e.shards[idx]
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "key deleted from engine state",
			log.String("key", key),
			log.Int("shard", int(idx)),
		)
	}
}

// Get shard from the storage engine by acquiring a read lock
func (e *Engine) Get(key string) ([]byte, bool) {
	var (
		plainValue []byte
		err        error
	)
	shard := e.shards[e.getShardIndex(key)]
	shard.RLock()
	item, exist := shard.items[key]
	shard.RUnlock()
	if !exist {
		return nil, false
	}
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		if e.logger.Enabled(log.LevelDebug) {
			e.logger.Log(log.LevelDebug, "lazy eviction triggered during Get", log.String("key", key))
		}
		e.Delete(key)
		return nil, false
	}
	if e.cryptoEngine.Enabled() {
		// Retrieve a reusable plaintext buffer from the sync.Pool.
		buf := plainBufPool.Get().([]byte)
		// Ensure the buffer has sufficient capacity for the decrypted data.
		if cap(buf) < len(item.Value) {
			buf = make([]byte, 0, len(item.Value))
		}
		buf = buf[:0]
		plainValue, err = e.cryptoEngine.DecryptInPlaceWithDst(buf, item.Value)
		if err != nil {
			if e.logger.Enabled(log.LevelError) {
				e.logger.Log(log.LevelError, "in-place decryption failed (integrity violation / corrupted memory)",
					log.String("key", key),
				)
			}
			return nil, false
		}
		// Return the plaintext slice directly. The slice references a pooled buffer, so no per‑call allocation occurs.
		return plainValue, true
	}
	return item.Value, true
}

// GetInto decrypts the value for the given key directly into the caller‑provided buffer.
// It returns the number of bytes written and a bool indicating whether the key existed.
// The caller must ensure the buffer has sufficient capacity for the plaintext.
func (e *Engine) GetInto(buf []byte, key string) (int, bool) {
	shard := e.shards[e.getShardIndex(key)]
	shard.RLock()
	item, exist := shard.items[key]
	shard.RUnlock()
	if !exist {
		return 0, false
	}
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		if e.logger.Enabled(log.LevelDebug) {
			e.logger.Log(log.LevelDebug, "lazy eviction triggered during GetInto", log.String("key", key))
		}
		e.Delete(key)
		return 0, false
	}
	if e.cryptoEngine.Enabled() {
		// Decrypt directly into the caller buffer.
		plain, err := e.cryptoEngine.DecryptInPlaceWithDst(buf[:0], item.Value)
		if err != nil {
			if e.logger.Enabled(log.LevelError) {
				e.logger.Log(log.LevelError, "in-place decryption failed inside GetInto target stream",
					log.String("key", key),
				)
			}
			return 0, false
		}
		return len(plain), true
	}
	// No encryption – copy the stored value into buf.
	if len(buf) < len(item.Value) {
		if e.logger.Enabled(log.LevelWarn) {
			e.logger.Log(log.LevelWarn, "insufficient buffer capacity provided by caller for GetInto",
				log.String("key", key),
				log.Int("available_cap", len(buf)),
				log.Int("required_len", len(item.Value)),
			)
		}
		return 0, false // insufficient capacity (caller responsibility)
	}
	copy(buf, item.Value)
	return len(item.Value), true
}
