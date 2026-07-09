/*
Package storage
Tellstone Cloud-Native In-Memory Database
File: engine.go
Description: Single-map, lock-protected in-memory key-value store with optional TTL eviction, memory ceiling enforcement, and at-rest encryption. In shared-nothing mode each shard owns one Engine instance.

Authors:

	Maximilian Hagen
*/
package storage

import (
	"errors"
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

var ErrEngineFull = errors.New("memory: limit reached")

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
		return 3 * 1024 * 1024 * 1024
	}
	return 0
}()

// Engine is a single-map, lock-protected in-memory key-value store.
// In SN mode it is owned by exactly one goroutine so the lock is uncontended.
type Engine struct {
	mu                   sync.RWMutex
	items                map[string]Item
	chronometer          TimelineWheel
	cryptoEngine         *crypto.Engine
	logger               log.Logger
	maxBytes             uint64
	allocatedBytes       uint64
	keyCount             uint64
	expiredCount         uint64
	hitCount             uint64
	missCount            uint64
	totalCommands        uint64
	cryptoEncryptedBytes uint64
	cryptoDecryptedBytes uint64
}

func NewEngine(interval time.Duration, numSlots uint32, maxBytes uint64, logger log.Logger, cryptoEngine *crypto.Engine) *Engine {
	e := new(Engine)
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	e.logger = logger
	e.maxBytes = maxBytes
	if e.maxBytes == 0 {
		e.maxBytes = defaultMaxBytes
	}
	e.items = make(map[string]Item)
	if interval <= 0 || numSlots == 0 {
		e.chronometer = &NoOpChronometer{}
		if e.logger.Enabled(log.LevelInfo) {
			e.logger.Log(log.LevelInfo, "chronometer disabled; running without active eviction loop")
		}
	} else {
		c := NewChronometer(func(k string) { e.deleteIfExpired(k) }, interval, numSlots, logger)
		c.Start()
		e.chronometer = c
	}
	if cryptoEngine == nil {
		cryptoEngine, _ = crypto.NewEngine(nil, logger)
	}
	e.cryptoEngine = cryptoEngine
	if e.logger.Enabled(log.LevelInfo) {
		e.logger.Log(log.LevelInfo, "storage engine created")
	}
	return e
}

func (e *Engine) Close() {
	e.chronometer.Stop()
	if e.logger.Enabled(log.LevelInfo) {
		e.logger.Log(log.LevelInfo, "storage engine and background chronometer stopped")
	}
}

func (e *Engine) Set(key string, value []byte, ttl time.Duration) error {
	var exp time.Time
	var err error
	neededSize := len(value)
	cryptoEnabled := e.cryptoEngine.Enabled()
	if cryptoEnabled {
		neededSize = 12 + len(value) + 16
	}
	if e.maxBytes > 0 {
		totalEntrySize := uint64(len(key) + neededSize)
		if atomic.LoadUint64(&e.allocatedBytes)+totalEntrySize > e.maxBytes {
			return ErrEngineFull
		}
	}
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
				)
			}
			return err
		}
	} else {
		value = append([]byte(nil), value...)
	}
	storedKey := strings.Clone(key)
	e.mu.Lock()
	oldItem, isUpdate := e.items[storedKey]
	e.items[storedKey] = Item{
		Value:      value,
		Expiration: exp,
	}
	e.mu.Unlock()
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
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	if ttl > 0 {
		e.chronometer.Register(storedKey, ttl)
	}
	return nil
}

func (e *Engine) Delete(key string) {
	e.mu.Lock()
	item, exists := e.items[key]
	if !exists {
		e.mu.Unlock()
		return
	}
	delete(e.items, key)
	e.mu.Unlock()
	releasedBytes := uint64(len(key) + len(item.Value))
	atomic.AddUint64(&e.allocatedBytes, ^(releasedBytes - 1))
	atomic.AddUint64(&e.totalCommands, 1)
	atomic.AddUint64(&e.keyCount, ^uint64(0))
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "key deleted from engine state",
			log.String("key", key),
		)
	}
}

func (e *Engine) deleteIfExpired(key string) bool {
	e.mu.Lock()
	item, exists := e.items[key]
	if !exists || item.Expiration.IsZero() || !time.Now().After(item.Expiration) {
		e.mu.Unlock()
		return false
	}
	delete(e.items, key)
	e.mu.Unlock()
	releasedBytes := uint64(len(key) + len(item.Value))
	atomic.AddUint64(&e.allocatedBytes, ^(releasedBytes - 1))
	atomic.AddUint64(&e.totalCommands, 1)
	atomic.AddUint64(&e.keyCount, ^uint64(0))
	return true
}

func (e *Engine) Get(key string) ([]byte, bool) {
	e.mu.RLock()
	item, exist := e.items[key]
	e.mu.RUnlock()
	if !exist {
		atomic.AddUint64(&e.missCount, 1)
		atomic.AddUint64(&e.totalCommands, 1)
		return nil, false
	}
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		if e.logger.Enabled(log.LevelDebug) {
			e.logger.Log(log.LevelDebug, "lazy eviction triggered during Get", log.String("key", key))
		}
		if e.deleteIfExpired(key) {
			atomic.AddUint64(&e.expiredCount, 1)
		}
		return nil, false
	}
	if e.cryptoEngine.Enabled() {
		buf := make([]byte, 0, len(item.Value))
		plainValue, err := e.cryptoEngine.DecryptInPlaceWithDst(buf, item.Value)
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

func (e *Engine) GetInto(buf []byte, key string) (int, bool) {
	e.mu.RLock()
	item, exist := e.items[key]
	e.mu.RUnlock()
	if !exist {
		atomic.AddUint64(&e.missCount, 1)
		atomic.AddUint64(&e.totalCommands, 1)
		return 0, false
	}
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		if e.logger.Enabled(log.LevelDebug) {
			e.logger.Log(log.LevelDebug, "lazy eviction triggered during GetInto", log.String("key", key))
		}
		if e.deleteIfExpired(key) {
			atomic.AddUint64(&e.expiredCount, 1)
		}
		return 0, false
	}
	if e.cryptoEngine.Enabled() {
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
	if len(buf) < len(item.Value) {
		if e.logger.Enabled(log.LevelWarn) {
			e.logger.Log(log.LevelWarn, "insufficient buffer capacity provided by caller for GetInto",
				log.String("key", key),
				log.Int("available_cap", len(buf)),
				log.Int("required_len", len(item.Value)),
			)
		}
		return 0, false
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
func (e *Engine) ExpiredCount() uint64         { return atomic.LoadUint64(&e.expiredCount) }
func (e *Engine) CryptoEncryptedBytes() uint64 { return atomic.LoadUint64(&e.cryptoEncryptedBytes) }
func (e *Engine) CryptoDecryptedBytes() uint64 { return atomic.LoadUint64(&e.cryptoDecryptedBytes) }
func (e *Engine) TotalCommands() uint64        { return atomic.LoadUint64(&e.totalCommands) }
func (e *Engine) Chronometer() TimelineWheel   { return e.chronometer }
