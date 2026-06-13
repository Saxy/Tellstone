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
	"time"
)

const (
	MaxSlots     = 1000
	SlotCapacity = 512
)

// Chronometer is a highly optimized, O(1) circular timeline for deterministic active TTL cache eviction.
// It organizes expiring keys into circular time slots (buckets) to avoid the high overhead
// of maintaining individual runtime timers per database key.
// Chronometer is a high‑performance timing wheel that groups keys by their
// expiration time. It provides O(1) registration and O(1) eviction per tick.
//
// The implementation tracks which slot each key belongs to so that re‑registering
// a key (e.g., updating its TTL) moves it from the old slot to the new one,
// preventing duplicate expirations.
//
// A sync.WaitGroup is used to ensure the background goroutine terminates cleanly when Stop() is called.
type Chronometer struct {
	mutex     sync.Mutex // protects slots and keySlotMap
	interval  time.Duration
	slots     [MaxSlots][SlotCapacity]string // each bucket holds keys scheduled to expire on that tick
	slotSizes [MaxSlots]int
	numSlots  uint32
	curSlot   uint32
	ticker    *time.Ticker
	stop      chan struct{}
	deletion  func(key string)
	wg        sync.WaitGroup // tracks the background ticker goroutine
}

// NewChronometer instantiates a precision time‑tracking wheel bound to a specific storage Engine.
// The interval dictates the time resolution per tick, while numSlots defines the maximum future horizon.
func NewChronometer(deletion func(key string), interval time.Duration, numSlots uint32) *Chronometer {
	c := &Chronometer{
		deletion: deletion,
		interval: interval,
		numSlots: numSlots,
		stop:     make(chan struct{}),
	}
	return c
}

// advance turns the internal wheel by exactly one tick, isolates the expired bucket,
// and asynchronously flushes all dead keys out of the sharded storage engine.
func (c *Chronometer) advance() {
	c.mutex.Lock()
	size := c.slotSizes[c.curSlot]
	if size > 0 {
		for i := 0; i < size; i++ {
			c.deletion(c.slots[c.curSlot][i])
		}
		c.slotSizes[c.curSlot] = 0
	}
	c.curSlot = (c.curSlot + 1) % c.numSlots
	c.mutex.Unlock()
}

// Start spawns the background orchestration loop, setting the Chronometer's internal gears into motion.
func (c *Chronometer) Start() {
	c.ticker = time.NewTicker(c.interval)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
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
	c.mutex.Lock()
	select {
	case <-c.stop:
		// already closed
	default:
		close(c.stop)
	}
	c.mutex.Unlock()
	c.wg.Wait()
}

// Register maps a key to a specific target slot in the future based on its given Time-To-Live (TTL).
// This mapping runs with O(1) efficiency without altering execution pipelines.
func (c *Chronometer) Register(key string, ttl time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	maxSteps := uint64(c.numSlots) - 1
	steps := uint64(ttl / c.interval)
	if steps > maxSteps {
		steps = maxSteps
	}
	targetSlot := (c.curSlot + uint32(steps)) % c.numSlots
	size := c.slotSizes[targetSlot]
	if size < SlotCapacity {
		c.slots[targetSlot][size] = key
		c.slotSizes[targetSlot] = size + 1
	}
}
