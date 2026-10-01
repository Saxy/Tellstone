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
	"bytes"
	"errors"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

var (
	ErrEngineFull       = errors.New("memory: limit reached")
	ErrInvalidKeyLength = errors.New("storage: invalid key length for SetFromBuffer")
	// ErrKeyTooLong is returned when a key exceeds MaxKeyLen.
	ErrKeyTooLong = errors.New("storage: key exceeds the maximum length")
)

// MaxKeyLen is the longest key the front-coded B+Tree can represent.
//
// A node records each entry's suffix length in a uint16, so a key longer than
// 65535 bytes truncates on the way in. The stored key is then not the key that
// was written: the entry cannot be found again, so the write appears to succeed
// and the key is silently lost. Rejecting it is the only outcome that keeps the
// key and the entry in agreement.
const MaxKeyLen = 1<<16 - 1

// checkKeyLen reports whether a key is within MaxKeyLen.
func checkKeyLen(key string) error {
	if len(key) > MaxKeyLen {
		return ErrKeyTooLong
	}
	return nil
}

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
	mu             sync.RWMutex
	items          map[string]Item
	chronometer    TimelineWheel
	cryptoEngine   *crypto.Engine
	logger         log.Logger
	maxBytes       uint64
	allocatedBytes uint64
	keyCount       uint64
	// versionSeq hands out the Version stamped on every item entering the map.
	// It is a single engine-wide sequence rather than a per-key counter because
	// a version has to stay unique for the item's whole lifetime: a per-key
	// counter restarts at 1 when a key is deleted and re-created, which would
	// let a stale rollback token match an unrelated later write.
	versionSeq           uint64
	expiredCount         uint64
	hitCount             uint64
	missCount            uint64
	totalCommands        uint64
	cryptoEncryptedBytes uint64
	cryptoDecryptedBytes uint64

	// index is the ordered index over live keys, backing prefix range reads
	// (ADR-013 guardrail 1). It carries no lock of its own: every mutation
	// site below updates it while already holding mu, and ScanPrefix holds a
	// read lock for the duration of a walk because the tree restructures in
	// place on insert, split and compaction.
	index *btree
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
	e.index = newBTree()
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

// SetOutcome describes what a conditional write did to one key. It is returned
// by SetIfAbsent and SetIfPresent so a caller whose durability write fails
// afterwards can undo exactly its own change: Prev is the item that was
// replaced (meaningful when PrevOK), and Version identifies this write until
// some other writer stores a new value.
type SetOutcome struct {
	// Applied reports that the precondition held and the value was stored.
	Applied bool
	// PrevOK reports that a live entry existed beforehand. It is false both for
	// a key that was absent and for one whose entry had already expired, so a
	// rollback never resurrects an expired value.
	PrevOK bool
	// Prev is the entry the write replaced, including its expiration, so a
	// rollback restores the original TTL rather than dropping it.
	Prev Item
	// Version is the version this write stored.
	Version uint64
}

func (e *Engine) Set(key string, value []byte, ttl time.Duration) error {
	_, err := e.set(key, value, ttl, condAny)
	return err
}

// SetIfAbsent writes key only when it is currently absent, and reports what it
// did. The precondition and the write are evaluated under a single engine
// lock, so two concurrent callers racing on the same key cannot both observe
// "absent" and both create it.
func (e *Engine) SetIfAbsent(key string, value []byte, ttl time.Duration) (SetOutcome, error) {
	return e.set(key, value, ttl, condAbsent)
}

// SetIfPresent writes key only when it is currently present, and reports what
// it did. Like SetIfAbsent the check and the write share one critical section,
// so an absent key can never be "updated" by a racing writer.
func (e *Engine) SetIfPresent(key string, value []byte, ttl time.Duration) (SetOutcome, error) {
	return e.set(key, value, ttl, condPresent)
}

// cond is the precondition a conditional write evaluates inside the engine
// lock. An expired-but-present entry counts as absent, matching Get and Delete.
type cond uint8

const (
	condAny cond = iota
	condAbsent
	condPresent
)

// set is the shared body of Set and the conditional writes. It returns what the
// write did, including the entry it replaced and the version it stored, so a
// caller can compensate a failed durability write without discarding a
// concurrent writer's value.
func (e *Engine) set(key string, value []byte, ttl time.Duration, c cond) (SetOutcome, error) {
	if err := checkKeyLen(key); err != nil {
		return SetOutcome{}, err
	}
	var exp time.Time
	neededSize := len(value)
	cryptoEnabled := e.cryptoEngine.Enabled()
	if cryptoEnabled {
		neededSize = 12 + len(value) + 16
	}
	if e.maxBytes > 0 {
		totalEntrySize := uint64(len(key) + neededSize)
		if atomic.LoadUint64(&e.allocatedBytes)+totalEntrySize > e.maxBytes {
			if e.logger.Enabled(log.LevelWarn) {
				e.logger.Log(log.LevelWarn, "storage engine memory ceiling reached, write rejected",
					log.String("key", key),
					log.Uint64("allocated_bytes", atomic.LoadUint64(&e.allocatedBytes)),
					log.Uint64("max_bytes", e.maxBytes),
				)
			}
			return SetOutcome{}, ErrEngineFull
		}
	}
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	var storedKey string
	if cryptoEnabled {
		var err error
		encryptedBuf := make([]byte, 0, neededSize)
		value, err = e.cryptoEngine.EncryptInPlace(encryptedBuf, value)
		if err != nil {
			if e.logger.Enabled(log.LevelError) {
				e.logger.Log(log.LevelError, "in-place encryption failed during Set operation",
					log.String("key", key),
				)
			}
			return SetOutcome{}, err
		}
		storedKey = strings.Clone(key)
	} else {
		total := len(key) + len(value)
		buf := make([]byte, total)
		copy(buf, key)
		copy(buf[len(key):], value)
		storedKey = unsafe.String(unsafe.SliceData(buf), len(key))
		value = buf[len(key):]
	}
	e.mu.Lock()
	oldItem, isUpdate := e.items[storedKey]
	// An expired-but-still-resident entry counts as absent, so the precondition
	// is evaluated on the same liveness rule Get and Delete use.
	expired := isUpdate && !oldItem.Expiration.IsZero() && time.Now().After(oldItem.Expiration)
	present := isUpdate && !expired
	if c != condAny && present != (c == condPresent) {
		// Precondition unsatisfied: leave the map untouched and report the
		// write as not applied. Nothing was allocated, so nothing to account.
		e.mu.Unlock()
		atomic.AddUint64(&e.totalCommands, 1)
		return SetOutcome{}, nil
	}
	// Versions come from one engine-wide sequence, so every stored value is
	// uniquely attributable to the write that produced it.
	version := e.nextVersion()
	e.items[storedKey] = Item{
		Value:      value,
		Expiration: exp,
		Version:    version,
	}
	// The index keeps the stored value, which is the representation a reader
	// gets back: encrypted when crypto is on, so ScanPrefix decrypts the same
	// way Get does rather than handing out ciphertext.
	e.index.set(storedKey, value, exp)
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
	// An expired predecessor is not offered as a rollback target: putting it
	// back would resurrect a value the liveness rule had already retired.
	return SetOutcome{Applied: true, PrevOK: present, Prev: oldItem, Version: version}, nil
}

// nextVersion returns the next engine-wide item version. It must be called
// with e.mu held, which every writer and RestoreIf does.
func (e *Engine) nextVersion() uint64 {
	e.versionSeq++
	return e.versionSeq
}

// RestoreIf rolls back a conditional write whose durability record could not be
// written, putting back the entry the write replaced. It acts only while the
// key still holds the version that write stored, so a value another writer has
// stored in the meantime is never discarded. It reports whether the rollback
// happened; false means the key moved on and the write stands.
func (e *Engine) RestoreIf(key string, out SetOutcome) bool {
	if !out.Applied {
		return false
	}
	e.mu.Lock()
	item, exists := e.items[key]
	if !exists || item.Version != out.Version {
		e.mu.Unlock()
		return false
	}
	expired := !item.Expiration.IsZero() && time.Now().After(item.Expiration)
	if !out.PrevOK {
		// The write created the key, so undoing it removes the key again.
		delete(e.items, key)
		e.index.remove(key)
		e.mu.Unlock()
		e.releaseKey(key, item)
		return true
	}
	restored := out.Prev
	// A restore is itself a state change and takes a fresh version. Reusing the
	// replaced entry's version would let a token from an earlier write match
	// this state and roll back a write that never failed.
	restored.Version = e.nextVersion()
	e.items[key] = restored
	e.index.set(key, restored.Value, restored.Expiration)
	e.mu.Unlock()
	oldSize := uint64(len(key) + len(item.Value))
	newSize := uint64(len(key) + len(restored.Value))
	if newSize > oldSize {
		atomic.AddUint64(&e.allocatedBytes, newSize-oldSize)
	} else if oldSize > newSize {
		atomic.AddUint64(&e.allocatedBytes, ^(oldSize - newSize - 1))
	}
	// The chronometer still holds the rolled-back write's schedule, so a
	// restored TTL has to be re-armed for the time it has left.
	if !restored.Expiration.IsZero() && !expired {
		if remaining := time.Until(restored.Expiration); remaining > 0 {
			e.chronometer.Register(key, remaining)
		}
	}
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "conditional write rolled back after durability failure",
			log.String("key", key),
			log.Uint64("version", out.Version),
		)
	}
	return true
}

// SetFromBuffer stores a key-value pair from a pre-built buffer containing
// [keyBytes | valueBytes] contiguously. The engine takes ownership of buf —
// callers must not reuse it after this call. keyLen is the byte offset where
// the key ends and the value begins. This avoids the make+copy that Set
// performs, saving one allocation per call. Does not support encryption.
func (e *Engine) SetFromBuffer(buf []byte, keyLen int, ttl time.Duration) error {
	if keyLen < 0 || keyLen > len(buf) {
		return ErrInvalidKeyLength
	}
	if err := checkKeyLen(string(buf[:keyLen])); err != nil {
		return err
	}
	if e.cryptoEngine.Enabled() {
		return e.Set(string(buf[:keyLen]), buf[keyLen:], ttl)
	}
	if e.maxBytes > 0 {
		totalEntrySize := uint64(len(buf))
		if atomic.LoadUint64(&e.allocatedBytes)+totalEntrySize > e.maxBytes {
			return ErrEngineFull
		}
	}
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	storedKey := unsafe.String(unsafe.SliceData(buf), keyLen)
	value := buf[keyLen:]
	e.mu.Lock()
	oldItem, isUpdate := e.items[storedKey]
	e.items[storedKey] = Item{
		Value:      value,
		Expiration: exp,
		Version:    e.nextVersion(),
	}
	e.index.set(storedKey, value, exp)
	e.mu.Unlock()
	atomic.AddUint64(&e.totalCommands, 1)
	if isUpdate {
		oldSize := uint64(len(oldItem.Value) + keyLen)
		newSize := uint64(len(value) + keyLen)
		if newSize > oldSize {
			atomic.AddUint64(&e.allocatedBytes, newSize-oldSize)
		} else if oldSize > newSize {
			atomic.AddUint64(&e.allocatedBytes, ^(oldSize - newSize - 1))
		}
	} else {
		atomic.AddUint64(&e.allocatedBytes, uint64(len(buf)))
		atomic.AddUint64(&e.keyCount, 1)
	}
	if ttl > 0 {
		e.chronometer.Register(storedKey, ttl)
	}
	return nil
}

// SetRaw stores a key with a pre-encrypted value, bypassing encryption. This is
// used by snapshotRead when restoring an engine with crypto enabled — the
// snapshot already contains the encrypted values from ForEach, so re-encrypting
// would double-encrypt. The engine retains the value's backing bytes for the
// entry's lifetime; callers must not mutate or reuse the buffer after this call.
func (e *Engine) SetRaw(key string, value []byte, ttl time.Duration) error {
	if err := checkKeyLen(key); err != nil {
		return err
	}
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	if e.maxBytes > 0 {
		totalEntrySize := uint64(len(key) + len(value))
		if atomic.LoadUint64(&e.allocatedBytes)+totalEntrySize > e.maxBytes {
			return ErrEngineFull
		}
	}
	storedKey := strings.Clone(key)
	e.mu.Lock()
	oldItem, isUpdate := e.items[storedKey]
	e.items[storedKey] = Item{
		Value:      value,
		Expiration: exp,
		Version:    e.nextVersion(),
	}
	e.index.set(storedKey, value, exp)
	e.mu.Unlock()
	atomic.AddUint64(&e.totalCommands, 1)
	if isUpdate {
		oldSize := uint64(len(oldItem.Value) + len(storedKey))
		newSize := uint64(len(value) + len(storedKey))
		if newSize > oldSize {
			atomic.AddUint64(&e.allocatedBytes, newSize-oldSize)
		} else if oldSize > newSize {
			atomic.AddUint64(&e.allocatedBytes, ^(oldSize - newSize - 1))
		}
	} else {
		atomic.AddUint64(&e.allocatedBytes, uint64(len(key)+len(value)))
		atomic.AddUint64(&e.keyCount, 1)
	}
	atomic.AddUint64(&e.cryptoEncryptedBytes, uint64(len(value)))
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "key written to engine state (raw)",
			log.String("key", key),
			log.Int64("ttl_ms", ttl.Milliseconds()),
		)
	}
	if ttl > 0 {
		e.chronometer.Register(storedKey, ttl)
	}
	return nil
}

// Delete removes key and reports whether it was present. An expired key is
// evicted physically but counts as absent, mirroring Get's lazy eviction, so
// callers can derive "did the key exist" without a separate Get lookup.
func (e *Engine) Delete(key string) bool {
	e.mu.Lock()
	item, exists := e.items[key]
	if !exists {
		e.mu.Unlock()
		return false
	}
	expired := !item.Expiration.IsZero() && time.Now().After(item.Expiration)
	delete(e.items, key)
	e.index.remove(key)
	e.mu.Unlock()
	e.releaseKey(key, item)
	if expired {
		atomic.AddUint64(&e.expiredCount, 1)
		return false
	}
	if e.logger.Enabled(log.LevelDebug) {
		e.logger.Log(log.LevelDebug, "key deleted from engine state",
			log.String("key", key),
		)
	}
	return true
}

func (e *Engine) deleteIfExpired(key string) bool {
	e.mu.Lock()
	item, exists := e.items[key]
	if !exists || item.Expiration.IsZero() || !time.Now().After(item.Expiration) {
		e.mu.Unlock()
		return false
	}
	delete(e.items, key)
	e.index.remove(key)
	e.mu.Unlock()
	e.releaseKey(key, item)
	return true
}

// releaseKey adjusts the memory, command, and key counters after a successful
// in-memory deletion. It must be called with the item that was just removed.
func (e *Engine) releaseKey(key string, item Item) {
	releasedBytes := uint64(len(key) + len(item.Value))
	atomic.AddUint64(&e.allocatedBytes, ^(releasedBytes - 1))
	atomic.AddUint64(&e.totalCommands, 1)
	atomic.AddUint64(&e.keyCount, ^uint64(0))
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
func (e *Engine) CryptoEnabled() bool          { return e.cryptoEngine.Enabled() }
func (e *Engine) TotalCommands() uint64        { return atomic.LoadUint64(&e.totalCommands) }
func (e *Engine) Chronometer() TimelineWheel   { return e.chronometer }

// ForEach snapshots all live (non-expired) entries under a read lock, releases
// the lock, then calls fn for each captured entry. The callback may perform
// arbitrary work without holding the shard lock. Expired entries are skipped
// (not evicted) to keep the lock duration bounded.
func (e *Engine) ForEach(fn func(key string, value []byte, expiration time.Time)) {
	type entry struct {
		key string
		val []byte
		exp time.Time
	}
	now := time.Now()
	e.mu.RLock()
	snap := make([]entry, 0, len(e.items))
	for k, v := range e.items {
		if !v.Expiration.IsZero() && now.After(v.Expiration) {
			continue
		}
		// Copy value bytes under the lock so the snapshot survives mutations.
		vc := make([]byte, len(v.Value))
		copy(vc, v.Value)
		snap = append(snap, entry{key: k, val: vc, exp: v.Expiration})
	}
	e.mu.RUnlock()
	for i := range snap {
		fn(snap[i].key, snap[i].val, snap[i].exp)
	}
}

// Scan snapshots all live (non-expired) entries under a read lock, releases
// the lock, sorts keys lexicographically, then calls fn for every entry whose
// key satisfies start <= key < end. An empty start means negative infinity; an
// empty end means positive infinity. The callback may perform arbitrary work
// without holding the shard lock. Expired entries are skipped (not evicted).
func (e *Engine) Scan(start, end []byte, fn func(key string, value []byte)) {
	type entry struct {
		key string
		val []byte
	}
	now := time.Now()
	e.mu.RLock()
	snap := make([]entry, 0, len(e.items))
	for k, v := range e.items {
		if !v.Expiration.IsZero() && now.After(v.Expiration) {
			continue
		}
		kb := []byte(k)
		if len(start) > 0 && bytes.Compare(kb, start) < 0 {
			continue
		}
		if len(end) > 0 && bytes.Compare(kb, end) >= 0 {
			continue
		}
		vc := make([]byte, len(v.Value))
		copy(vc, v.Value)
		snap = append(snap, entry{key: k, val: vc})
	}
	e.mu.RUnlock()
	sort.Slice(snap, func(i, j int) bool {
		return snap[i].key < snap[j].key
	})
	for i := range snap {
		fn(snap[i].key, snap[i].val)
	}
}

// ScanPrefix calls fn for every live key beginning with prefix, in key order,
// stopping early if fn returns false, and reports how many keys were delivered.
//
// This is the range read ADR-013 guardrail 1 requires: a row is a set of column
// keys under one prefix, so reconstructing it is one walk of the ordered index
// rather than one lookup per column. The index duplicates each value into its
// leaf entry, and stored values are never mutated once they enter the engine, so
// the callback reads them without a copy and the whole scan allocates nothing on
// the engine side.
//
// Keys arrive as []byte rather than string, unlike Scan, because converting the
// index's decoded key to a string would allocate once per key and undo the point
// of the index. Callers that want a string copy it themselves.
//
// The callback runs under the engine's read lock, because the index restructures
// in place on insert, split and compaction and cannot be walked while that
// happens. The callback must not call back into a mutating engine method.
func (e *Engine) ScanPrefix(prefix string, fn func(key, value []byte) bool) int {
	now := time.Now()
	cryptoEnabled := e.cryptoEngine.Enabled()
	e.mu.RLock()
	defer e.mu.RUnlock()
	delivered := 0
	e.index.ScanPrefix(prefix, func(key, val []byte, exp time.Time) bool {
		if !exp.IsZero() && now.After(exp) {
			// Skipped rather than evicted, matching Scan and ForEach: eviction
			// takes the write lock, which a read-locked walk cannot take.
			return true
		}
		out := val
		if cryptoEnabled {
			buf := make([]byte, 0, len(val))
			plain, err := e.cryptoEngine.DecryptInPlaceWithDst(buf, val)
			if err != nil {
				if e.logger.Enabled(log.LevelError) {
					e.logger.Log(log.LevelError, "in-place decryption failed during ScanPrefix",
						log.String("key", string(key)),
					)
				}
				return true
			}
			out = plain
			atomic.AddUint64(&e.cryptoDecryptedBytes, uint64(len(plain)))
		}
		delivered++
		return fn(key, out)
	})
	atomic.AddUint64(&e.totalCommands, 1)
	return delivered
}
