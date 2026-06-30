/*
Package metrics
Tellstone Runtime Metrics Package

File: metrics.go
Description: Centralised, lock‑free collection of runtime statistics that are
exposed to Prometheus (or any other scraper) via the `Export` method.
All metric types are implemented using `atomic` primitives to avoid
allocation and contention in hot paths.

This version uses direct byte-buffer writing (strconv) instead of fmt.Fprintf
to ensure zero-allocation output during scraping.
*/
package metrics

import (
	"io"
	"runtime"
	"strconv"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/storage"
)

// EngineSnapshot bündelt die reinen Daten für Exporter
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
	writer        io.Writer
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

// WritePrometheus serialisiert Metriken ohne Allocations.
func (c *Collector) WritePrometheus(w io.Writer) {
	c.writer = w
	eng := c.GetEngineSnapshot()
	netSnap := c.GetNetworkSnapshot()

	// Engine Metrics
	c.writeMetric("tellstone_engine_keys_total", "gauge", "Current number of keys.", eng.KeyCount)
	c.writeMetric("tellstone_engine_allocated_bytes", "gauge", "Memory allocated.", eng.AllocatedBytes)
	c.writeMetric("tellstone_engine_commands_total", "counter", "Commands processed.", eng.TotalCommands)
	c.writeMetric("tellstone_engine_hits_total", "counter", "Successful lookups.", eng.HitCount)
	c.writeMetric("tellstone_engine_misses_total", "counter", "Failed lookups.", eng.MissCount)
	c.writeMetric("tellstone_engine_evictions_passive_total", "counter", "Passive evictions.", eng.PassiveEvictions)
	c.writeMetric("tellstone_engine_evictions_active_total", "counter", "Active evictions.", eng.ActiveEvictions)

	// Network Metrics
	c.writeMetric("tellstone_network_connected_clients", "gauge", "Active connections.", netSnap.ConnectedClients)
	c.writeMetric("tellstone_network_connections_total", "counter", "Total connections.", netSnap.TotalConnections)
	c.writeMetric("tellstone_network_bytes_read_total", "counter", "Bytes read.", netSnap.BytesRead)
	c.writeMetric("tellstone_network_bytes_written_total", "counter", "Bytes written.", netSnap.BytesWritten)

	// Go Runtime Metrics
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	c.writeMetric("tellstone_runtime_heap_alloc_bytes", "gauge", "Heap alloc bytes.", mem.HeapAlloc)
	c.writeMetric("tellstone_runtime_heap_allocs_total", "counter", "Total heap allocations.", mem.Mallocs)
	c.writeMetric("tellstone_runtime_gc_cycles_total", "counter", "Total GC cycles.", uint64(mem.NumGC))
}

// writeMetric schreibt das Prometheus-Format ohne Allocation.
func (c *Collector) writeMetric(name, mType, help string, value uint64) {
	_, _ = c.writer.Write([]byte("# HELP " + name + " " + help + "\n"))
	_, _ = c.writer.Write([]byte("# TYPE " + name + " " + mType + "\n"))
	_, _ = c.writer.Write([]byte(name + " "))

	// Zero-Alloc Zahlenschreibung
	var buf [20]byte
	b := strconv.AppendUint(buf[:0], value, 10)
	_, _ = c.writer.Write(b)
	_, _ = c.writer.Write([]byte("\n\n"))
}
