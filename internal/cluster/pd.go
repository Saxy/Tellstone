/*
Package cluster
Tellstone Placement Driver (Phase 2)
File: pd.go
Description: Embedded etcd bootstrap for the Tellstone placement driver
(ADR-001, ADR-010). Every hybrid or pd node hosts one etcd member; the
etcd group elects the PD leader that grants TSO timestamp ranges. This
file owns member startup, endpoint derivation, and graceful shutdown —
timestamp allocation itself lives in tso.go.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// InitialClusterToken namespaces the embedded etcd group so a stray
// foreign etcd member can never join a Tellstone PD (and vice versa).
const initialClusterToken = "tellstone-pd"

// pdReadyTimeout bounds how long StartPD waits for the member to catch up
// with the group. A cold 3-member election typically lands well under 5s;
// the margin covers slow CI machines spinning up all members in-process.
const pdReadyTimeout = 30 * time.Second

// pdStopTimeout bounds graceful shutdown; see Stop.
const pdStopTimeout = 10 * time.Second

// ErrPDNotReady is returned when an embedded member fails to report
// readiness within pdReadyTimeout.
var ErrPDNotReady = errors.New("cluster: embedded PD member did not become ready in time")

// PDEndpoints holds the deterministic etcd endpoints of every PD member,
// keyed by node ID. Peer URLs carry inter-member Raft traffic; client URLs
// serve control-plane requests (pool refills, elections).
//
// Remote endpoints are always derived from the member's data address
// (ADR-010 §6): data port +10000 for clients, +20000 for peers. The
// per-node override flags only affect the local member's own endpoints —
// mixed topologies where some nodes override and others do not are a
// phase-3 concern (dedicated PD address advertisement).
type PDEndpoints struct {
	PeerURLs   map[uint64]string
	ClientURLs map[uint64]string
}

// DerivePDEndpoints computes the etcd endpoint tables for the given PD
// member list. Addresses must parse as host:port (the ID@addr form has
// already been parsed into peers).
func DerivePDEndpoints(members []Peer) (*PDEndpoints, error) {
	ep := &PDEndpoints{
		PeerURLs:   make(map[uint64]string, len(members)),
		ClientURLs: make(map[uint64]string, len(members)),
	}
	for _, m := range members {
		host, port, err := hostPortPair(m.Addr)
		if err != nil {
			return nil, fmt.Errorf("cluster: PD member %d address %q: %w", m.ID, m.Addr, err)
		}
		// The peer endpoint is base+20000; reject bases that would overflow
		// the valid TCP port range so we fail fast instead of binding :0.
		if port > 45535 {
			return nil, fmt.Errorf("cluster: PD member %d address %q: base port %d leaves no room for derived PD ports (need +20000 ≤ 65535)", m.ID, m.Addr, port)
		}
		ep.ClientURLs[m.ID] = (&url.URL{
			Scheme: "http",
			Host:   joinHostPort(host, port+10000),
		}).String()
		ep.PeerURLs[m.ID] = (&url.URL{
			Scheme: "http",
			Host:   joinHostPort(host, port+20000),
		}).String()
	}
	return ep, nil
}

// hostPortPair splits and range-checks a host:port address.
func hostPortPair(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("port %q out of range", portStr)
	}
	return host, port, nil
}

// joinHostPort reassembles a host:port string without IPv6 quoting traps.
func joinHostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// PDConfig describes one embedded etcd member.
type PDConfig struct {
	NodeID             uint64
	DataDir            string
	ClientListenURL    string
	ClientAdvertiseURL string
	PeerListenURL      string
	PeerAdvertiseURL   string
	AllPeerURLs        map[uint64]string
}

// PD owns one embedded etcd member and its lifecycle.
type PD struct {
	emb       *embed.Etcd
	clientURL string
}

// StartPD launches the embedded etcd member described by cfg and blocks
// until it reports readiness (leader contact or election win).
func StartPD(cfg PDConfig) (*PD, error) {
	if len(cfg.AllPeerURLs) == 0 {
		return nil, errors.New("cluster: PD bootstrap requires at least one member")
	}
	if _, ok := cfg.AllPeerURLs[cfg.NodeID]; !ok {
		return nil, fmt.Errorf("cluster: member %d missing from PD bootstrap list", cfg.NodeID)
	}
	clientListen, err := parseURL(cfg.ClientListenURL)
	if err != nil {
		return nil, fmt.Errorf("cluster: PD client listen URL: %w", err)
	}
	clientAdv := clientListen
	if cfg.ClientAdvertiseURL != "" {
		if clientAdv, err = parseURL(cfg.ClientAdvertiseURL); err != nil {
			return nil, fmt.Errorf("cluster: PD client advertise URL: %w", err)
		}
	}
	peerListen, err := parseURL(cfg.PeerListenURL)
	if err != nil {
		return nil, fmt.Errorf("cluster: PD peer listen URL: %w", err)
	}
	peerAdv := peerListen
	if cfg.PeerAdvertiseURL != "" {
		if peerAdv, err = parseURL(cfg.PeerAdvertiseURL); err != nil {
			return nil, fmt.Errorf("cluster: PD peer advertise URL: %w", err)
		}
	}

	ec := embed.NewConfig()
	ec.Name = memberName(cfg.NodeID)
	ec.Dir = cfg.DataDir
	ec.ListenClientUrls = []url.URL{clientListen}
	ec.AdvertiseClientUrls = []url.URL{clientAdv}
	ec.ListenPeerUrls = []url.URL{peerListen}
	ec.AdvertisePeerUrls = []url.URL{peerAdv}
	ec.InitialCluster = initialClusterString(ec.Name, peerAdv.String(), cfg.AllPeerURLs)
	ec.InitialClusterToken = initialClusterToken
	ec.ClusterState = "new"
	ec.LogLevel = "error"
	ec.LogOutputs = []string{"stderr"}

	emb, err := embed.StartEtcd(ec)
	if err != nil {
		return nil, fmt.Errorf("cluster: starting embedded PD member %d: %w", cfg.NodeID, err)
	}

	// One startup budget covers both the etcd ready signal and the leader
	// wait, so a slow member fails at pdReadyTimeout rather than 2x that.
	startDeadline := time.Now().Add(pdReadyTimeout)
	select {
	case <-emb.Server.ReadyNotify():
		if err = waitForPDLeader(clientAdv.String(), startDeadline); err != nil {
			stopEmbedded(emb)
			return nil, fmt.Errorf("cluster: member %d: %w", cfg.NodeID, err)
		}
		return &PD{emb: emb, clientURL: clientAdv.String()}, nil
	case err := <-emb.Err():
		stopEmbedded(emb)
		return nil, fmt.Errorf("cluster: embedded PD member %d failed: %w", cfg.NodeID, err)
	case <-time.After(time.Until(startDeadline)):
		stopEmbedded(emb)
		return nil, fmt.Errorf("cluster: member %d: %w", cfg.NodeID, ErrPDNotReady)
	}
}

// waitForPDLeader polls the member's client endpoint until etcd reports a
// cluster leader, or pdReadyTimeout elapses. A single node elects itself;
// a multi-node group waits for quorum.
func waitForPDLeader(endpoint string, deadline time.Time) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("connecting to PD member: %w", err)
	}
	defer cli.Close()

	for {
		// Honor an already-expired (or fully consumed) startup deadline.
		if time.Now().After(deadline) {
			return ErrPDNotReady
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		st, serr := cli.Status(ctx, endpoint)
		cancel()
		if serr == nil && st.Leader != 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// stopEmbedded tears an embed.Etcd instance down with the bounded Stop
// semantics (see PD.Stop).
func stopEmbedded(emb *embed.Etcd) {
	done := make(chan struct{})
	go func() {
		emb.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(pdStopTimeout):
	}
}

// ClientURL returns the advertised client endpoint other components dial.
func (p *PD) ClientURL() string { return p.clientURL }

// Stop shuts the member down gracefully, flushing its WAL. Safe to call
// more than once. Teardown is bounded: an etcd member cut off from its
// peers can wedge in Close waiting for connections that will never drain,
// and callers must not hang behind it.
func (p *PD) Stop() {
	if p == nil || p.emb == nil {
		return
	}
	emb := p.emb
	p.emb = nil
	done := make(chan struct{})
	go func() {
		emb.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(pdStopTimeout):
		// Leave the member to finish dying in the background; process exit
		// reclaims whatever is left.
	}
}

// memberName renders the stable etcd name for a node ID.
func memberName(id uint64) string { return fmt.Sprintf("tellstone-%d", id) }

// initialClusterString renders etcd's --initial-cluster value:
// "name1=peerURL1,name2=peerURL2,…". The local entry uses its advertised
// URL so every member computes an identical string for the group.
func initialClusterString(localName, localPeerURL string, members map[uint64]string) string {
	parts := make([]string, 0, len(members))
	for id := range members {
		name := memberName(id)
		u := members[id]
		if name == localName {
			u = localPeerURL
		}
		parts = append(parts, name+"="+u)
	}
	return strings.Join(parts, ",")
}

// parseURL validates a single endpoint string.
func parseURL(raw string) (url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return url.URL{}, err
	}
	if u.Host == "" || u.Scheme == "" {
		return url.URL{}, fmt.Errorf("%q is not an absolute endpoint", raw)
	}
	return *u, nil
}
