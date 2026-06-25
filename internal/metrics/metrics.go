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
}

func NewCollector(e *storage.Engine, n *network.Server) *Collector {
	return &Collector{engine: e, networkServer: n}
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
