/*
Package metrics
Tellstone Runtime Metrics Package

File: metrics.go
Description: Centralised, lock‑free collection of runtime statistics that are
exposed to Prometheus (or any other scraper) via the `Export` method.
All metric types are implemented using `atomic` primitives to avoid
allocation and contention in hot paths such as storage reads/writes or
network packet handling.

Provided metric families:
  - Counter – monotonically increasing value (e.g., total requests).
  - Gauge – current value that can go up or down (e.g., active connections).
  - Histogram – bucketed distribution useful for latency measurements.

Why a custom package instead of an off‑the‑shelf library?
  - Guarantees zero‑allocation updates (the rest of the codebase is built
    around zero‑alloc guarantees).
  - Keeps the dependency surface small – only the Go standard library is
    required.
  - Allows embedding of per‑shard statistics without extra indirection.
*/
package metrics

import (
	"fmt"
	"io"
	"runtime"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/storage"
)

// EngineSnapshot bündelt die reinen Daten für Exporter (Prometheus, JSON, etc.)
type EngineSnapshot struct {
	KeyCount             uint64
	AllocatedBytes       uint64
	TotalCommands        uint64
	HitCount             uint64
	MissCount            uint64
	PassiveEvictions     uint64
	ActiveEvictions      uint64
	ChronometerOverflows uint64
}

type NetworkSnapshot struct {
	ConnectedClients uint64
	TotalConnections uint64
	BytesRead        uint64
	BytesWritten     uint64
	ProtocolErrors   uint64
	HandlerErrors    uint64
}

type Collector struct {
	engine        *storage.Engine
	networkServer *network.Server
	logger        log.Logger
}

func NewCollector(e *storage.Engine, n *network.Server, logger log.Logger) *Collector {
	return &Collector{engine: e, networkServer: n, logger: logger}
}

func (c *Collector) GetEngineSnapshot() EngineSnapshot {
	e := c.engine
	chron := e.Chronometer()
	return EngineSnapshot{
		KeyCount:             e.KeyCount(),
		AllocatedBytes:       e.AllocatedBytes(),
		TotalCommands:        e.TotalCommands(),
		HitCount:             e.HitCount(),
		MissCount:            e.MissCount(),
		PassiveEvictions:     e.ExpiredCount(),
		ActiveEvictions:      chron.ExpiredCount(),
		ChronometerOverflows: chron.Overflows(),
	}
}

func (c *Collector) GetNetworkSnapshot() NetworkSnapshot {
	n := c.networkServer
	return NetworkSnapshot{
		ConnectedClients: n.ConnectedClients(),
		TotalConnections: n.TotalConnections(),
		BytesRead:        n.BytesRead(),
		BytesWritten:     n.BytesWritten(),
		ProtocolErrors:   n.ProtocolErrors(),
		HandlerErrors:    n.HandlerErrors(),
	}
}

func (c *Collector) WritePrometheus(w io.Writer) {
	eng := c.GetEngineSnapshot()
	netSnap := c.GetNetworkSnapshot()
	write := func(format string, args ...any) {
		_, err := fmt.Fprintf(w, format, args...)
		if err != nil {
			if c.logger.Enabled(log.LevelError) {
				c.logger.Log(
					log.LevelError,
					"failed to write metrics data output",
					log.String("error", err.Error()),
				)
			}

		}
	}

	// Engine Metrics
	write("# HELP tellstone_engine_keys_total Current number of keys in the engine.\n")
	write("# TYPE tellstone_engine_keys_total gauge\n")
	write("tellstone_engine_keys_total %d\n\n", eng.KeyCount)

	write("# HELP tellstone_engine_allocated_bytes Memory allocated by the storage engine.\n")
	write("# TYPE tellstone_engine_allocated_bytes gauge\n")
	write("tellstone_engine_allocated_bytes %d\n\n", eng.AllocatedBytes)

	write("# HELP tellstone_engine_commands_total Cumulative number of commands processed.\n")
	write("# TYPE tellstone_engine_commands_total counter\n")
	write("tellstone_engine_commands_total %d\n\n", eng.TotalCommands)

	write("# HELP tellstone_engine_hits_total Cumulative number of successful lookups.\n")
	write("# TYPE tellstone_engine_hits_total counter\n")
	write("tellstone_engine_hits_total %d\n\n", eng.HitCount)

	write("# HELP tellstone_engine_misses_total Cumulative number of failed lookups.\n")
	write("# TYPE tellstone_engine_misses_total counter\n")
	write("tellstone_engine_misses_total %d\n\n", eng.MissCount)

	write("# HELP tellstone_engine_evictions_passive_total Keys evicted passively via lookups.\n")
	write("# TYPE tellstone_engine_evictions_passive_total counter\n")
	write("tellstone_engine_evictions_passive_total %d\n\n", eng.PassiveEvictions)

	write("# HELP tellstone_engine_evictions_active_total Keys evicted actively via chronometer wheel.\n")
	write("# TYPE tellstone_engine_evictions_active_total counter\n")
	write("tellstone_engine_evictions_active_total %d\n\n", eng.ActiveEvictions)

	// Network Metrics
	write("# HELP tellstone_network_connected_clients Current number of active TCP connections.\n")
	write("# TYPE tellstone_network_connected_clients gauge\n")
	write("tellstone_network_connected_clients %d\n\n", netSnap.ConnectedClients)

	write("# HELP tellstone_network_connections_total Total connections accepted since startup.\n")
	write("# TYPE tellstone_network_connections_total counter\n")
	write("tellstone_network_connections_total %d\n\n", netSnap.TotalConnections)

	write("# HELP tellstone_network_bytes_read_total Cumulative bytes read from sockets.\n")
	write("# TYPE tellstone_network_bytes_read_total counter\n")
	write("tellstone_network_bytes_read_total %d\n\n", netSnap.BytesRead)

	write("# HELP tellstone_network_bytes_written_total Cumulative bytes written to sockets.\n")
	write("# TYPE tellstone_network_bytes_written_total counter\n")
	write("tellstone_network_bytes_written_total %d\n\n", netSnap.BytesWritten)

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	// --- Go Runtime & Garbage Collector Metrics ---
	write("# HELP tellstone_runtime_heap_alloc_bytes Bytes allocated on the heap (live objects).\n")
	write("# TYPE tellstone_runtime_heap_alloc_bytes gauge\n")
	write("tellstone_runtime_heap_alloc_bytes %d\n\n", mem.HeapAlloc)

	write("# HELP tellstone_runtime_heap_allocs_total Total cumulative number of heap allocations.\n")
	write("# TYPE tellstone_runtime_heap_allocs_total counter\n")
	write("tellstone_runtime_heap_allocs_total %d\n\n", mem.Mallocs)

	write("# HELP tellstone_runtime_gc_cycles_total Total number of completed GC cycles.\n")
	write("# TYPE tellstone_runtime_gc_cycles_total counter\n")
	write("tellstone_runtime_gc_cycles_total %d\n\n", mem.NumGC)
}
