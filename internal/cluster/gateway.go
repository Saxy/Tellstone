/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: gateway.go
Description: Cross-cluster gateway (Phase 7, ADR-011). A federation-enabled
node runs a dedicated network.Transport + Pipeline on --gateway-addr, sized
for app-level cross-cluster ops only (no raft frames ever cross a gateway
connection). Peer addressing keys the transport connection map by *cluster ID*
rather than raft node ID, so each gateway keeps exactly one pipe per remote
cluster. Op payloads use the compact binary codecs below (no protobuf).

Inbound ops (OpXClusterWrite / OpXClusterRead) are dispatched to the hook set
by SetHandler — the cluster store's routing body on the home cluster — and
answered with OpXClusterResp. Responses route back through the caller's
cluster ID (pm.from), which is why --federation-clusters must be symmetric.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Saxy/Tellstone/internal/cluster/network"
	"github.com/Saxy/Tellstone/internal/log"
)

// gatewayOpGroupID is the pipeline group ID cross-cluster ops travel under on
// the gateway transport. The transport is dedicated: no raft groups exist on
// it, and protocol-level pings use gid 0 without consulting a handler, so the
// gateway's single handler owns this gid exclusively.
const gatewayOpGroupID = 0

// errGatewayNotStarted is returned when Call is used before Start (or after
// Stop), so callers fail fast instead of hanging on a context deadline.
var errGatewayNotStarted = errors.New("cluster gateway: not started")

// XClusterOpExecutor applies an inbound cross-cluster op on the home cluster.
// It mirrors network.PipeHandler so the gateway can hand ops straight through.
type XClusterOpExecutor func(op network.OpKind, payload []byte) ([]byte, error)

// GatewayConfig describes how a node enters the federation (ADR-011 D1/D2).
type GatewayConfig struct {
	// ClusterID is the ID of the cluster this node belongs to (--cluster-id).
	ClusterID uint64
	// Addr is the gateway listen address (--gateway-addr), e.g. ":9100".
	Addr string
	// Peers maps a remote cluster ID to its gateway address
	// (--federation-clusters "id@addr,...").
	Peers map[uint64]string
	// Logger receives gateway lifecycle and error logs.
	Logger log.Logger
}

// Gateway is the cross-cluster endpoint of one node. It owns a dedicated
// transport+pipeline; remote clusters dial it, and it dials the remote
// gateways listed in Peers (symmetric configuration per ADR-011 §6).
type Gateway struct {
	clusterID uint64
	addr      string
	peers     map[uint64]string
	logger    log.Logger

	transport *network.Transport
	pipeline  *network.Pipeline

	executorMu sync.RWMutex
	executor   XClusterOpExecutor

	started  atomic.Bool
	stopOnce sync.Once
}

// NewGateway validates the configuration and builds a gateway. ClusterID must
// be non-zero (the federation namespace) and the listen address non-empty.
func NewGateway(cfg GatewayConfig) (*Gateway, error) {
	if cfg.ClusterID == 0 {
		return nil, errors.New("cluster gateway: cluster id must be non-zero")
	}
	if cfg.Addr == "" {
		return nil, errors.New("cluster gateway: listen address is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	peers := make(map[uint64]string, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		if id == cfg.ClusterID {
			return nil, fmt.Errorf("cluster gateway: cannot federate with the local cluster id %d", cfg.ClusterID)
		}
		if addr == "" {
			continue
		}
		peers[id] = addr
	}
	return &Gateway{
		clusterID: cfg.ClusterID,
		addr:      cfg.Addr,
		peers:     peers,
		logger:    logger,
	}, nil
}

// Start creates the dedicated gateway transport, registers the remote
// clusters, accepts inbound connections, and launches the pipeline. The op
// executor may be nil during the brief window before the server wires it;
// inbound ops fail with an explicit error until SetHandler is called.
func (g *Gateway) Start() error {
	if !g.started.CompareAndSwap(false, true) {
		return nil
	}
	t := network.NewTransport(g.addr, g.clusterID, nil, g.logger)
	for id, addr := range g.peers {
		t.RegisterPeer(id, addr)
	}
	if err := t.Listen(); err != nil {
		g.started.Store(false)
		return err
	}
	g.transport = t
	g.pipeline = t.Pipeline(g.clusterID, g.logger)
	g.pipeline.RegisterHandler(gatewayOpGroupID, g.handleOp)
	if g.logger.Enabled(log.LevelInfo) {
		g.logger.Log(log.LevelInfo, "cluster gateway: started",
			log.String("addr", t.Addr()),
			log.Uint64("cluster_id", g.clusterID),
			log.Uint("peers", uint32(len(g.peers))),
		)
	}
	return nil
}

// Stop tears down the gateway transport (listener, pipeline, connections).
func (g *Gateway) Stop() {
	g.stopOnce.Do(func() {
		if g.transport != nil {
			g.transport.Stop()
		}
	})
}

// Addr returns the resolved gateway listen address.
func (g *Gateway) Addr() string {
	if g.transport != nil {
		return g.transport.Addr()
	}
	return g.addr
}

// AddPeer dynamically registers (or replaces) the gateway address of a remote
// cluster. Safe before or after Start; a self-reference is ignored. Used for
// in-process tests and future dynamic federation.
func (g *Gateway) AddPeer(clusterID uint64, addr string) {
	if clusterID == g.clusterID {
		return
	}
	if t := g.transport; t != nil {
		t.RegisterPeer(clusterID, addr)
	}
}

// ClusterID returns this gateway's cluster ID.
func (g *Gateway) ClusterID() uint64 { return g.clusterID }

// SetHandler wires the op executor — the receiving cluster's own store
// routing body. Idempotent; safe to call before or after Start (inbound ops
// error out until an executor is present).
func (g *Gateway) SetHandler(h XClusterOpExecutor) {
	g.executorMu.Lock()
	g.executor = h
	g.executorMu.Unlock()
}

// Call forwards a cross-cluster op to the remote cluster's gateway and blocks
// until its OpXClusterResp arrives, ctx is cancelled, or the link drops.
// clusterID is the remote cluster ID (ADR-011 D1). The returned payload is
// the framed response data: empty on a successful write, or the read result
// body for OpXClusterRead.
func (g *Gateway) Call(ctx context.Context, clusterID uint64, op network.OpKind, payload []byte) ([]byte, error) {
	p := g.pipeline
	if p == nil {
		return nil, errGatewayNotStarted
	}
	// The remote cluster must be a registered peer; failing fast here beats a
	// confusing keepalive drop.
	return p.Call(ctx, clusterID, gatewayOpGroupID, op, payload)
}

// handleOp dispatches an inbound cross-cluster op to the configured executor.
func (g *Gateway) handleOp(op network.OpKind, payload []byte) ([]byte, error) {
	g.executorMu.RLock()
	h := g.executor
	g.executorMu.RUnlock()
	if h == nil {
		return nil, errors.New("cluster gateway: no cross-cluster op executor configured")
	}
	return h(op, payload)
}

// MaxKeyLen bounds the key size accepted from a remote cluster so a malicious
// or buggy peer cannot cause oversized allocations before validation.
const MaxKeyLen = 4 << 20 // 4 MiB

// EncodeXWrite serializes a forwarded write: [2B key_len][key][4B
// chunk_count][per chunk: 4B len][data]. It reuses the intra-cluster chunk
// encoding layout so the far side can hand the chain straight to routeWrite.
func EncodeXWrite(key string, chunks [][]byte) []byte {
	buf := make([]byte, 0, 2+len(key)+4)
	buf = appendUint16(buf, uint16(len(key)))
	buf = append(buf, key...)
	buf = appendUint32(buf, uint32(len(chunks)))
	for _, c := range chunks {
		buf = appendUint32(buf, uint32(len(c)))
		buf = append(buf, c...)
	}
	return buf
}

// DecodeXWrite parses a forwarded write request. The returned chunks share
// the input buffer's backing array where possible; callers that retain them
// across reads must copy.
func DecodeXWrite(payload []byte) (string, [][]byte, error) {
	key, rest, err := readXKey(payload)
	if err != nil {
		return "", nil, err
	}
	if len(rest) < 4 {
		return "", nil, errors.New("cluster gateway: truncated write chunk count")
	}
	count := int(binary.BigEndian.Uint32(rest[0:4]))
	rest = rest[4:]
	if count < 0 || count > 65536 {
		return "", nil, fmt.Errorf("cluster gateway: implausible write chunk count %d", count)
	}
	chunks := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		if len(rest) < 4 {
			return "", nil, fmt.Errorf("cluster gateway: truncated chunk %d length", i)
		}
		l := int(binary.BigEndian.Uint32(rest[0:4]))
		rest = rest[4:]
		if l < 0 || len(rest) < l {
			return "", nil, fmt.Errorf("cluster gateway: truncated chunk %d body", i)
		}
		chunks = append(chunks, rest[:l])
		rest = rest[l:]
	}
	_ = rest
	return key, chunks, nil
}

// EncodeXRead serializes a forwarded read: [2B key_len][key].
func EncodeXRead(key string) []byte {
	buf := make([]byte, 0, 2+len(key))
	buf = appendUint16(buf, uint16(len(key)))
	return append(buf, key...)
}

// DecodeXRead parses a forwarded read request.
func DecodeXRead(payload []byte) (string, error) {
	key, _, err := readXKey(payload)
	return key, err
}

// EncodeXReadResp serializes a read result: [1B present][value]. present=1
// distinguishes a stored empty value from a missing key.
func EncodeXReadResp(value []byte, present bool) []byte {
	buf := make([]byte, 1, 1+len(value))
	if present {
		buf[0] = 1
	}
	return append(buf, value...)
}

// DecodeXReadResp parses a read result into its value and presence flag.
func DecodeXReadResp(payload []byte) ([]byte, bool, error) {
	if len(payload) < 1 {
		return nil, false, errors.New("cluster gateway: truncated read response")
	}
	return payload[1:], payload[0] == 1, nil
}

// readXKey parses the shared [2B key_len][key] header, bounding the key size
// to MaxKeyLen.
func readXKey(payload []byte) (string, []byte, error) {
	if len(payload) < 2 {
		return "", nil, errors.New("cluster gateway: truncated key length")
	}
	klen := int(binary.BigEndian.Uint16(payload[0:2]))
	rest := payload[2:]
	if klen > MaxKeyLen {
		return "", nil, fmt.Errorf("cluster gateway: key length %d exceeds limit %d", klen, MaxKeyLen)
	}
	if len(rest) < klen {
		return "", nil, errors.New("cluster gateway: truncated key body")
	}
	return string(rest[:klen]), rest[klen:], nil
}
