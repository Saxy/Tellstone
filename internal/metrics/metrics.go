/*
Package metrics
Tellstone Cloud-Native In-Memory Database
File: metrics.go
Description: Prometheus-compatible metrics collection with per-shard collectors and an aggregate collector for the full engine view.

Authors:

	Maximilian Hagen
*/
package metrics

import (
	"io"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/shard"
	"github.com/Saxy/Tellstone/internal/storage"
)

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
	shard         *shard.Shard
	logger        log.Logger
	shardLabel    string
}

func NewCollector(e *storage.Engine, n *network.Server, logger log.Logger) *Collector {
	return &Collector{engine: e, networkServer: n, logger: logger}
}

func NewShardCollector(id uint32, sh *shard.Shard, e *storage.Engine, n *network.Server, logger log.Logger) *Collector {
	label := "shard_" + strconv.FormatUint(uint64(id), 10)
	return &Collector{
		engine:        e,
		networkServer: n,
		shard:         sh,
		logger:        logger,
		shardLabel:    label,
	}
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
	if c.shard != nil {
		return NetworkSnapshot{
			ConnectedClients: c.shard.ConnectedClients(),
			TotalConnections: c.shard.TotalConnections(),
			BytesRead:        c.shard.BytesRead(),
			BytesWritten:     c.shard.BytesWritten(),
		}
	}
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
	c.writer = w
	eng := c.GetEngineSnapshot()
	netSnap := c.GetNetworkSnapshot()
	prefix := ""
	if c.shardLabel != "" {
		prefix = c.shardLabel + "_"
	}
	c.writeMetric(prefix+"tellstone_engine_keys_total", "gauge", "Current number of keys.", eng.KeyCount)
	c.writeMetric(prefix+"tellstone_engine_allocated_bytes", "gauge", "Memory allocated.", eng.AllocatedBytes)
	c.writeMetric(prefix+"tellstone_engine_commands_total", "counter", "Commands processed.", eng.TotalCommands)
	c.writeMetric(prefix+"tellstone_engine_hits_total", "counter", "Successful lookups.", eng.HitCount)
	c.writeMetric(prefix+"tellstone_engine_misses_total", "counter", "Failed lookups.", eng.MissCount)
	c.writeMetric(prefix+"tellstone_engine_evictions_passive_total", "counter", "Passive evictions.", eng.PassiveEvictions)
	c.writeMetric(prefix+"tellstone_engine_evictions_active_total", "counter", "Active evictions.", eng.ActiveEvictions)
	c.writeMetric(prefix+"tellstone_network_connected_clients", "gauge", "Active connections.", netSnap.ConnectedClients)
	c.writeMetric(prefix+"tellstone_network_connections_total", "counter", "Total connections.", netSnap.TotalConnections)
	c.writeMetric(prefix+"tellstone_network_bytes_read_total", "counter", "Bytes read.", netSnap.BytesRead)
	c.writeMetric(prefix+"tellstone_network_bytes_written_total", "counter", "Bytes written.", netSnap.BytesWritten)
}

func (c *Collector) writeMetric(name, mType, help string, value uint64) {
	_, _ = c.writer.Write([]byte("# HELP " + name + " " + help + "\n"))
	_, _ = c.writer.Write([]byte("# TYPE " + name + " " + mType + "\n"))
	_, _ = c.writer.Write([]byte(name + " "))
	var buf [20]byte
	b := strconv.AppendUint(buf[:0], value, 10)
	_, _ = c.writer.Write(b)
	_, _ = c.writer.Write([]byte("\n\n"))
}

// TLSMetrics exposes process-level certificate rotation state without coupling
// the metrics package to the concrete filesystem watcher.
type TLSMetrics interface {
	ReloadTotal() uint64
	ReloadErrorsTotal() uint64
	CertificateExpirySeconds() int64
}

// RBACMetrics exposes authorization counters without coupling the metrics
// package to the concrete policy store — mirrors TLSMetrics above. The role
// counts map allocates at scrape time, never on the request path.
type RBACMetrics interface {
	AuthFailures() uint64
	DeniedCommands() uint64
	RoleCommandCounts() map[string]uint64
}

type AggregateCollector struct {
	shardCollectors []*Collector
	networkServer   *network.Server
	tlsMetrics      TLSMetrics
	rbacMetrics     RBACMetrics
}

func NewAggregateCollector(shardCollectors []*Collector, netSrv *network.Server, tlsMetrics TLSMetrics, rbacMetrics RBACMetrics) *AggregateCollector {
	return &AggregateCollector{
		shardCollectors: shardCollectors,
		networkServer:   netSrv,
		tlsMetrics:      tlsMetrics,
		rbacMetrics:     rbacMetrics,
	}
}

func (ac *AggregateCollector) WritePrometheus(w io.Writer) {
	for _, sc := range ac.shardCollectors {
		sc.WritePrometheus(w)
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var buf [20]byte
	writeRaw := func(name, mType, help string, value uint64) {
		_, _ = w.Write([]byte("# HELP " + name + " " + help + "\n"))
		_, _ = w.Write([]byte("# TYPE " + name + " " + mType + "\n"))
		_, _ = w.Write([]byte(name + " "))
		b := strconv.AppendUint(buf[:0], value, 10)
		_, _ = w.Write(b)
		_, _ = w.Write([]byte("\n\n"))
	}
	// writeLabeledSample writes a single labeled sample; the HELP and TYPE lines
	// are written once by the caller so per-role samples do not repeat metadata.
	writeLabeledSample := func(name, labelName, labelValue string, value uint64) {
		_, _ = w.Write([]byte(name + `{` + labelName + `="` + escapeLabelValue(labelValue) + `"} `))
		b := strconv.AppendUint(buf[:0], value, 10)
		_, _ = w.Write(b)
		_, _ = w.Write([]byte("\n\n"))
	}
	writeRaw("tellstone_runtime_heap_alloc_bytes", "gauge", "Heap alloc bytes.", mem.HeapAlloc)
	writeRaw("tellstone_runtime_heap_allocs_total", "counter", "Total heap allocations.", mem.Mallocs)
	writeRaw("tellstone_runtime_gc_cycles_total", "counter", "Total GC cycles.", uint64(mem.NumGC))
	if ac.tlsMetrics != nil {
		writeRaw("tellstone_tls_cert_reload_total", "counter", "Successful TLS certificate reloads.", ac.tlsMetrics.ReloadTotal())
		writeRaw("tellstone_tls_cert_reload_errors_total", "counter", "Failed TLS certificate reloads.", ac.tlsMetrics.ReloadErrorsTotal())
		expiry := ac.tlsMetrics.CertificateExpirySeconds()
		if expiry > 0 {
			writeRaw("tellstone_tls_cert_expiry_seconds", "gauge", "Active TLS leaf certificate NotAfter as Unix epoch seconds.", uint64(expiry))
		}
	}
	if ac.rbacMetrics != nil {
		writeRaw("tellstone_rbac_auth_failures_total", "counter", "Failed AUTH attempts.", ac.rbacMetrics.AuthFailures())
		writeRaw("tellstone_rbac_denied_commands_total", "counter", "Authorization-denied command attempts.", ac.rbacMetrics.DeniedCommands())
		counts := ac.rbacMetrics.RoleCommandCounts()
		names := make([]string, 0, len(counts))
		for name := range counts {
			names = append(names, name)
		}
		// Map iteration is unordered; sort so identical states render identically.
		sort.Strings(names)
		if len(names) > 0 {
			_, _ = w.Write([]byte("# HELP tellstone_rbac_commands_total Data commands executed per role.\n"))
			_, _ = w.Write([]byte("# TYPE tellstone_rbac_commands_total counter\n"))
		}
		for _, name := range names {
			writeLabeledSample("tellstone_rbac_commands_total", "role", name, counts[name])
		}
	}
}

// escapeLabelValue escapes the three characters the Prometheus exposition
// format requires in quoted label values: backslash, double quote, and newline.
// Role names are operator-controlled, so they must not break the text format.
func escapeLabelValue(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(v[i])
		}
	}
	return b.String()
}
