/*
Tellstone Cloud-Native In-Memory Database
File: chronometer.go
Description: The Chronometer. A highly optimized, O(1) circular timeline
             for deterministic active TTL cache eviction.

"Time ticks forward, uniform and unforgiving."

Authors:
    Maximilian Hagen
*/

package storage

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

type TimelineWheel interface {
	Start()
	Stop()
	Register(key string, ttl time.Duration)
	ExpiredCount() uint64
	Overflows() uint64
}

// NoOpChronometer belegt 0 Byte RAM und optimiert die CPU-Branches weg.
type NoOpChronometer struct{}

func (n *NoOpChronometer) Start()                                 {}
func (n *NoOpChronometer) Stop()                                  {}
func (n *NoOpChronometer) Register(key string, ttl time.Duration) {}
func (n *NoOpChronometer) ExpiredCount() uint64                   { return 0 }
func (n *NoOpChronometer) Overflows() uint64                      { return 0 }

const (
	MaxSlots     = 1000
	SlotCapacity = 512
)

// slotLock pads a sync.Mutex out to a full cache line. Without the padding, 8 of the 1000
// per-slot mutexes below would share one cache line, and concurrent Register calls hitting
// neighboring slots would bounce that line between cores — quietly reintroducing the exact
// contention the per-slot striping is meant to remove.
type slotLock struct {
	sync.Mutex
	_ [56]byte
}

// Chronometer is a highly optimized, O(1) circular timeline for deterministic active TTL cache eviction.
// It organizes expiring keys into circular time slots (buckets) to avoid the high overhead
// of maintaining individual runtime timers per database key.
// Chronometer is a high‑performance timing wheel that groups keys by their
// expiration time. It provides O(1) registration and O(1) eviction per tick.
//
// Registration is append-only: re-registering a key (e.g. updating its TTL) leaves the
// old slot entry in place rather than relocating it, so a key may appear in more than one
// slot. This is safe because eviction is validated against the item's real Expiration via
// the engine's deleteIfExpired callback — a stale slot entry for a refreshed key simply
// no-ops when its slot fires.
//
// Locking is striped per slot (slotMu) rather than a single global mutex: Set() calls a
// chronometer of this size 1:1 for every TTL write, so a single mutex here would serialize all
// concurrent TTL writes across all 256 storage shards regardless of which slot they target.
// Per-slot locks mean two Register calls only contend if they land in the same future tick.
// curSlot is accessed via atomics for the same reason — advance() (the ticker goroutine) and
// Register (called from any number of caller goroutines) both need to read/advance it without
// a global lock.
//
// A sync.WaitGroup is used to ensure the background goroutine terminates cleanly when Stop() is called.
type Chronometer struct {
	interval  time.Duration
	slotMu    [MaxSlots]slotLock
	slots     [MaxSlots][SlotCapacity]string // each bucket holds keys scheduled to expire on that tick
	slotSizes [MaxSlots]int
	numSlots  uint32
	curSlot   atomic.Uint32
	ticker    *time.Ticker
	stop      chan struct{}
	stopOnce  sync.Once
	deletion  func(key string)
	wg        sync.WaitGroup // tracks the background ticker goroutine
	logger    log.Logger

	overflows    uint64
	expiredCount uint64
}

// NewChronometer instantiates a precision time‑tracking wheel bound to a specific storage Engine.
// The interval dictates the time resolution per tick, while numSlots defines the maximum future horizon.
func NewChronometer(deletion func(key string), interval time.Duration, numSlots uint32, logger log.Logger) *Chronometer {
	c := &Chronometer{
		deletion: deletion,
		interval: interval,
		numSlots: numSlots,
		stop:     make(chan struct{}),
	}
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	c.logger = logger
	return c
}

// advance turns the internal wheel by exactly one tick, isolates the expired bucket,
// and flushes the dead keys out of the sharded storage engine.
//
// The slot's keys are snapshotted and the slot is reset while holding only that slot's lock,
// but the (potentially shard-locking) deletions run AFTER releasing it. Holding a lock across
// the delete loop would block any concurrent Register() call targeting the same slot for the
// whole eviction wave, which was a major source of tail latency.
func (c *Chronometer) advance() {
	slot := c.curSlot.Load()

	c.slotMu[slot].Lock()
	size := c.slotSizes[slot]
	var batch []string
	if size > 0 {
		if c.logger.Enabled(log.LevelDebug) {
			c.logger.Log(log.LevelDebug, "active eviction wave triggered by chronometer tick",
				log.Int("slot_index", int(slot)),
				log.Int("evicted_keys_count", size),
			)
		}
		batch = make([]string, size)
		copy(batch, c.slots[slot][:size])
		c.slotSizes[slot] = 0
		atomic.AddUint64(&c.expiredCount, uint64(size))
	}
	c.slotMu[slot].Unlock()

	c.curSlot.Store((slot + 1) % c.numSlots)

	for _, key := range batch {
		c.deletion(key)
	}
}

// Start spawns the background orchestration loop, setting the Chronometer's internal gears into motion.
func (c *Chronometer) Start() {
	c.ticker = time.NewTicker(c.interval)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if c.logger.Enabled(log.LevelInfo) {
			c.logger.Log(log.LevelInfo, "chronometer timeline loop started")
		}
		for {
			select {
			case <-c.ticker.C:
				c.advance()
			case <-c.stop:
				c.ticker.Stop()
				return
			}
		}
	}()
}

// Stop safely freezes the Chronometer timeline, releases underlying system tickers, and waits for the background goroutine to exit.
func (c *Chronometer) Stop() {
	c.stopOnce.Do(func() {
		close(c.stop)
		if c.logger.Enabled(log.LevelInfo) {
			c.logger.Log(log.LevelInfo, "stopping chronometer background routine")
		}
	})
	c.wg.Wait()
}

// Register maps a key to a specific target slot in the future based on its given Time-To-Live (TTL).
// This mapping runs with O(1) efficiency without altering execution pipelines. Only the target
// slot's own lock is taken, so concurrent registrations for different future ticks never
// contend with each other.
func (c *Chronometer) Register(key string, ttl time.Duration) {
	maxSteps := uint64(c.numSlots) - 1
	steps := uint64(ttl / c.interval)
	if steps > maxSteps {
		steps = maxSteps
	}
	targetSlot := (c.curSlot.Load() + uint32(steps)) % c.numSlots

	c.slotMu[targetSlot].Lock()
	defer c.slotMu[targetSlot].Unlock()
	size := c.slotSizes[targetSlot]
	if size < SlotCapacity {
		c.slots[targetSlot][size] = key
		c.slotSizes[targetSlot] = size + 1
		if c.logger.Enabled(log.LevelDebug) {
			c.logger.Log(log.LevelDebug, "key registered in eviction timeline",
				log.String("key", key),
				log.Int("target_slot", int(targetSlot)),
				log.Int64("steps_ahead", int64(steps)),
			)
		}
	} else {
		atomic.AddUint64(&c.overflows, 1)
		if c.logger.Enabled(log.LevelWarn) {
			c.logger.Log(log.LevelWarn, "chronometer slot capacity exceeded! eviction will be delayed for key",
				log.String("key", key),
				log.Int("target_slot", int(targetSlot)),
				log.Int("slot_limit", SlotCapacity),
			)
		}
	}
}

func (c *Chronometer) ExpiredCount() uint64 { return atomic.LoadUint64(&c.expiredCount) }
func (c *Chronometer) Overflows() uint64    { return atomic.LoadUint64(&c.overflows) }
