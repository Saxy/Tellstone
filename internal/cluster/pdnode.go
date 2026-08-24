/*
Package cluster
Tellstone Placement Driver + TSO (Phase 2)
File: pdnode.go
Description: Ties the embedded member (pd.go), the watermark granter
(tso.go) and the local pre-allocated pool into one lifecycle per server
process, and runs the refill loop that top-ups the pool before it runs
dry. Role drives the topology (ADR-010 §5): hybrid/pd host an embedded
member, data dials an external one — both end up with the same pool that
the write path will consume once MVCC lands in a later phase.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// tsoRefillPoll bounds how often the manager checks the pool budget. The
// watermark CAS is cheap and, critically, allocation itself never waits on
// this loop — only a genuinely empty pool does, so a 200ms cadence keeps
// refills invisible under normal load.
const tsoRefillPoll = 200 * time.Millisecond

// StartPDNodeConfig carries everything StartPDNode needs, already resolved
// from application config. Keeping it config-agnostic lets the server build
// it and keeps this package free of config imports.
type StartPDNodeConfig struct {
	Role           string // hybrid | pd | data
	NodeID         uint64
	DataAddr       string // base address used for endpoint derivation (hybrid/pd)
	PDAddr         string // external PD endpoint (data role only)
	Members        []Peer // effective PD membership (hybrid/pd)
	PDDir          string
	ClientOverride string // optional local etcd client endpoint
	PeerOverride   string // optional local etcd peer endpoint
	TSO            TSOPoolConfig
}

// PDNode bundles the PD/TSO stack for one process. Pool is the public
// surface the rest of the system consumes.
type PDNode struct {
	role    string
	pd      *PD
	cli     *clientv3.Client
	granter *EtcdGranter
	pool    *TSOPool
	mgr     *TSOManager
}

// Pool returns the local timestamp pool.
func (p *PDNode) Pool() *TSOPool { return p.pool }

// Role reports the topology this node runs.
func (p *PDNode) Role() string { return p.role }

// Stop tears the stack down: refill loop first, then the embedded member
// (if any), then the client. Order is not strict because the pool stands
// alone once adopted, but stopping the loop before the member avoids grant
// churn during shutdown.
func (p *PDNode) Stop() {
	if p.mgr != nil {
		p.mgr.Stop()
	}
	if p.pd != nil {
		p.pd.Stop()
	}
	if p.cli != nil {
		p.cli.Close()
	}
}

// StartPDNode assembles the PD/TSO stack for the given role. A non-data
// node boots an embedded etcd member from the resolved member list; a data
// node dials the external PD at PDAddr instead. Both prime the pool from
// etcd and start the refill loop.
func StartPDNode(cfg StartPDNodeConfig) (*PDNode, error) {
	if cfg.Role != "hybrid" && cfg.Role != "pd" && cfg.Role != "data" {
		return nil, fmt.Errorf("cluster: unknown role %q", cfg.Role)
	}

	node := &PDNode{role: cfg.Role}

	if cfg.Role == "data" {
		if cfg.PDAddr == "" {
			return nil, fmt.Errorf("cluster: data role requires --pd-addr")
		}
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{cfg.PDAddr},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("cluster: dialing external PD: %w", err)
		}
		node.cli = cli
		node.granter = NewEtcdGranter(cli)
		return node.withPool(cfg.TSO), nil
	}

	// hybrid / pd: derive member endpoints and boot the embedded member.
	// Endpoints are built deterministically from the data address (ADR-010
	// §6: client = data+10000, peer = data+20000) with an explicit scheme,
	// so we never depend on a map lookup that may return a stale value.
	clientURL, peerURL, err := pdEndpointURLs(cfg.DataAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: PD endpoints from %q: %w", cfg.DataAddr, err)
	}
	if cfg.ClientOverride != "" {
		clientURL = withScheme(cfg.ClientOverride)
	}
	if cfg.PeerOverride != "" {
		peerURL = withScheme(cfg.PeerOverride)
	}
	peerMap := make(map[uint64]string, len(cfg.Members))
	for _, m := range cfg.Members {
		_, pu, e := pdEndpointURLs(m.Addr)
		if e != nil {
			return nil, fmt.Errorf("cluster: PD peer endpoint from %q: %w", m.Addr, e)
		}
		peerMap[m.ID] = pu
	}

	pd, err := StartPD(PDConfig{
		NodeID:          cfg.NodeID,
		DataDir:         cfg.PDDir,
		ClientListenURL: clientURL,
		PeerListenURL:   peerURL,
		AllPeerURLs:     peerMap,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: starting embedded PD: %w", err)
	}
	node.pd = pd

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pd.ClientURL()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		pd.Stop()
		return nil, fmt.Errorf("cluster: dialing embedded PD: %w", err)
	}
	node.cli = cli
	node.granter = NewEtcdGranter(cli)
	return node.withPool(cfg.TSO), nil
}

func (p *PDNode) withPool(tsoCfg TSOPoolConfig) *PDNode {
	p.pool = NewTSOPool(tsoCfg)
	p.mgr = NewTSOManager(p.pool, p.granter)
	p.mgr.Run()
	return p
}

// TSOManager runs the background refill loop: primes the pool once, then
// tops it up whenever the budget crosses the configured refill threshold.
type TSOManager struct {
	pool    *TSOPool
	granter *EtcdGranter
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewTSOManager creates a manager bound to a pool and its granter.
func NewTSOManager(pool *TSOPool, granter *EtcdGranter) *TSOManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &TSOManager{pool: pool, granter: granter, ctx: ctx, cancel: cancel}
}

// Run starts the refill goroutine.
func (m *TSOManager) Run() {
	m.done = make(chan struct{})
	go m.loop()
}

// Stop signals the loop and waits for it to exit.
func (m *TSOManager) Stop() {
	m.cancel()
	<-m.done
}

func (m *TSOManager) loop() {
	defer close(m.done)
	m.refill() // prime immediately so the pool is never born empty
	ticker := time.NewTicker(tsoRefillPoll)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if m.pool.NeedsRefill() {
				m.refill()
			}
		}
	}
}

// refill grants the next batch and publishes it. A failed grant is left
// for the next tick; the pool only ever grows, never stalls the hot path.
func (m *TSOManager) refill() {
	n := m.pool.NextBatchSize()
	start, end, err := m.granter.GrantRange(m.ctx, n)
	if err != nil {
		return
	}
	_ = m.pool.Adopt(start, end)
}

// pdEndpointURLs derives the embedded etcd client and peer URLs from a
// node's data address (ADR-010 §6): client = data+10000, peer = data+20000,
// both with an explicit http:// scheme for etcd's URL parser.
func pdEndpointURLs(dataAddr string) (client, peer string, err error) {
	host, portStr, err := net.SplitHostPort(dataAddr)
	if err != nil {
		return "", "", err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", "", err
	}
	// The peer endpoint is base+20000; reject bases that would overflow the
	// valid TCP port range so we fail fast instead of letting etcd try to
	// bind an invalid port.
	if port+20000 > 65535 {
		return "", "", fmt.Errorf("cluster: data address port %d leaves no room for derived PD ports (need +20000 ≤ 65535)", port)
	}
	client = fmt.Sprintf("http://%s:%d", host, port+10000)
	peer = fmt.Sprintf("http://%s:%d", host, port+20000)
	return client, peer, nil
}

// withScheme ensures an endpoint carries an http:// scheme.
func withScheme(u string) string {
	if strings.Contains(u, "://") {
		return u
	}
	return "http://" + u
}
