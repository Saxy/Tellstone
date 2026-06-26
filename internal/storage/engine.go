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
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

var ErrEngineFull = errors.New("memory: limit reached")

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

// defaultMaxBytes defines the safety ceiling for memory consumption.
//
// Why 3 GB on 32-bit systems?
//  1. Architecture Limit: 32-bit registers can only address 4 GB of total RAM ($2^{32}$ bytes).
//  2. Kernel Split: Linux splits this space, reserving 1 GB for the kernel and leaving
//     a maximum of 3 GB for the user-space process.
//  3. OOM Protection: We need buffer room for runtime structures, GC overhead, and
//     Copy-on-Write page duplication during snapshots. Setting a 3 GB cap prevents
//     the OS from brutally killing the process with a SIGKILL.
//
// On 64-bit systems, this compiles to 0 (unlimited), letting the engine scale safely.
var defaultMaxBytes = func() uint64 {
	if bits.UintSize == 32 {
		// 3 GB safety cap for 32-bit
		return 3 * 1024 * 1024 * 1024
	}
	return 0
}()

// Engine represents a collection of shards
type Engine struct {
	shards       [shardCount]*Shard
	chronometer  TimelineWheel
	cryptoEngine *crypto.Engine
	logger       log.Logger

	maxBytes             uint64
	allocatedBytes       uint64
	missCount            uint64
	hitCount             uint64
	keyCount             uint64
	expiredCount         uint64
	cryptoEncryptedBytes uint64
	cryptoDecryptedBytes uint64
	totalCommands        uint64
}

// NewEngine creates a new engine with the default number of shards to prevent thread contention.
// NewEngine creates a new storage engine with the given chronometer interval,
// number of timing‑wheel slots, optional crypto engine and logger. It initialises all
// shards, starts the background chronometer and returns a ready‑to‑use *Engine.
func NewEngine(interval time.Duration, numSlots uint32, maxBytes uint64, logger log.Logger, cryptoEngine *crypto.Engine) *Engine {
	e := new(Engine)
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	for i := ShardCount(0); i < shardCount; i++ {
		e.shards[i] = &Shard{items: make(map[string]Item)}
	}
	if interval <= 0 || numSlots == 0 {
		e.chronometer = &NoOpChronometer{}
		if e.logger.Enabled(log.LevelInfo) {
			e.logger.Log(log.LevelInfo, "chronometer disabled; running without active eviction loop")
		}
	} else {
		c := NewChronometer(e.Delete, interval, numSlots, logger)
		c.Start()
		e.chronometer = c
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
// Close shuts down the engine and stops its background chronometer.
func (e *Engine) Close() {
	e.chronometer.Stop()
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
func (e *Engine) Set(key string, value []byte, ttl time.Duration) error {
	var (
		exp time.Time
		err error
	)
	neededSize := len(value)
	cryptoEnabled := e.cryptoEngine.Enabled()
	if cryptoEnabled {
		neededSize = 12 + len(value) + 16 // Nonce + Tag
	}
	if e.maxBytes > 0 {
		totalEntrySize := uint64(len(key) + neededSize)
		if atomic.LoadUint64(&e.allocatedBytes)+totalEntrySize > e.maxBytes {
			return ErrEngineFull
		}
	}
	idx := e.getShardIndex(key)
	shard := e.shards[idx]
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	if cryptoEnabled {
		encryptedBuf := make([]byte, 0, neededSize)
		value, err = e.cryptoEngine.EncryptInPlace(encryptedBuf, value)
		if err != nil {
			if e.logger.Enabled(log.LevelError) {
				e.logger.Log(log.LevelError, "in-place encryption failed during Set operation",
					log.String("key", key),
					log.Int("shard", int(idx)),
				)
			}
			return err
		}
	}
	shard.Lock()
	oldItem, isUpdate := shard.items[key]
	shard.items[key] = Item{
		Value:      value,
		Expiration: exp,
	}
	shard.Unlock()
	atomic.AddUint64(&e.totalCommands, 1)
	if isUpdate {
		oldSize := uint64(len(key) + len(oldItem.Value))
		newSize := uint64(len(key) + len(value))
		if newSize > oldSize {
			atomic.AddUint64(&e.allocatedBytes, newSize-oldSize)
		} else if oldSize > newSize {
			atomic.AddUint64(&e.allocatedBytes, ^(oldSize - newSize - 1))
		}
	} else {
		atomic.AddUint64(&e.allocatedBytes, uint64(len(key)+len(value)))
		atomic.AddUint64(&e.keyCount, 1)
	}
	if cryptoEnabled {
		atomic.AddUint64(&e.cryptoEncryptedBytes, uint64(len(value)))
	}
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "key written to engine state",
			log.String("key", key),
			log.Int("shard", int(idx)),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	if ttl > 0 {
		e.chronometer.Register(key, ttl)
	}
	return nil
}

// Delete the shard from the storage engine
func (e *Engine) Delete(key string) {
	idx := e.getShardIndex(key)
	shard := e.shards[idx]
	shard.Lock()
	item, exists := shard.items[key]
	if !exists {
		shard.Unlock()
		return
	}
	delete(shard.items, key)
	shard.Unlock()
	releasedBytes := uint64(len(key) + len(item.Value))
	// Go's sync/atomic package lacks a SubUint64 function.
	// To subtract atomic values, we use the two-complement bit-trick:
	// ^uint64(X - 1) mathematically equals -X.
	// Adding this inverted value causes a controlled CPU register overflow,
	// effectively performing a lightning-fast, lock-free subtraction.
	atomic.AddUint64(&e.allocatedBytes, ^(releasedBytes - 1))
	atomic.AddUint64(&e.totalCommands, 1)
	atomic.AddUint64(&e.keyCount, ^uint64(0))
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
		atomic.AddUint64(&e.missCount, 1)
		atomic.AddUint64(&e.totalCommands, 1)
		return nil, false
	}
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		if e.logger.Enabled(log.LevelDebug) {
			e.logger.Log(log.LevelDebug, "lazy eviction triggered during Get", log.String("key", key))
		}
		atomic.AddUint64(&e.expiredCount, 1)
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
		atomic.AddUint64(&e.hitCount, 1)
		atomic.AddUint64(&e.totalCommands, 1)
		atomic.AddUint64(&e.cryptoDecryptedBytes, uint64(len(plainValue)))
		return plainValue, true
	}
	atomic.AddUint64(&e.hitCount, 1)
	atomic.AddUint64(&e.totalCommands, 1)
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
		atomic.AddUint64(&e.missCount, 1)
		atomic.AddUint64(&e.totalCommands, 1)
		return 0, false
	}
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		if e.logger.Enabled(log.LevelDebug) {
			e.logger.Log(log.LevelDebug, "lazy eviction triggered during GetInto", log.String("key", key))
		}
		atomic.AddUint64(&e.expiredCount, 1)
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
		atomic.AddUint64(&e.hitCount, 1)
		atomic.AddUint64(&e.totalCommands, 1)
		atomic.AddUint64(&e.cryptoDecryptedBytes, uint64(len(plain)))
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
	atomic.AddUint64(&e.hitCount, 1)
	atomic.AddUint64(&e.totalCommands, 1)
	return len(item.Value), true
}

func (e *Engine) MaxBytes() uint64             { return atomic.LoadUint64(&e.maxBytes) }
func (e *Engine) AllocatedBytes() uint64       { return atomic.LoadUint64(&e.allocatedBytes) }
func (e *Engine) MissCount() uint64            { return atomic.LoadUint64(&e.missCount) }
func (e *Engine) HitCount() uint64             { return atomic.LoadUint64(&e.hitCount) }
func (e *Engine) KeyCount() uint64             { return atomic.LoadUint64(&e.keyCount) }
func (e *Engine) ExpiredCount() uint64         { return atomic.LoadUint64(&e.expiredCount) } // Passive Evictions
func (e *Engine) CryptoEncryptedBytes() uint64 { return atomic.LoadUint64(&e.cryptoEncryptedBytes) }
func (e *Engine) CryptoDecryptedBytes() uint64 { return atomic.LoadUint64(&e.cryptoDecryptedBytes) }
func (e *Engine) TotalCommands() uint64        { return atomic.LoadUint64(&e.totalCommands) }
func (e *Engine) Chronometer() TimelineWheel   { return e.chronometer }
