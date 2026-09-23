/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: config.go
Description: Cluster configuration types and helpers for multi-node Tellstone
with Raft consensus per region.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"fmt"
	"hash/fnv"
	"net"
	"strconv"
	"strings"
)

// Config holds cluster-mode configuration derived from CLI flags.
type Config struct {
	// Enabled is true when --cluster-mode is active.
	Enabled bool
	// NodeID is the unique identifier for this node in the cluster.
	NodeID uint64
	// PeerAddr is the address this node listens on for Raft transport.
	PeerAddr string
	// Peers is the list of peer addresses for initial cluster bootstrap.
	Peers []Peer
}

// Peer represents a cluster peer with its node ID and transport address.
type Peer struct {
	ID   uint64
	Addr string
}

// ParsePeers parses a comma-separated list of "id@host:port" or "host:port"
// strings into Peer values. When no explicit ID is given, the address is
// hashed (FNV-1a) to derive a deterministic node ID.
func ParsePeers(raw string) ([]Peer, error) {
	if raw == "" {
		return nil, nil
	}
	var peers []Peer
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, addr, ok := strings.Cut(s, "@")
		if !ok {
			// No explicit ID — derive from address.
			addr = s
			id = fmt.Sprintf("%d", hashAddr(addr))
		}
		nid, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cluster: invalid peer ID %q: %w", id, err)
		}
		if addr == "" {
			return nil, fmt.Errorf("cluster: peer address is empty for ID %d", nid)
		}
		peers = append(peers, Peer{ID: nid, Addr: addr})
	}
	return peers, nil
}

// hashAddr returns a deterministic uint64 hash of an address string.
func hashAddr(addr string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(addr))
	return h.Sum64()
}

// ParseFederationClusters parses --federation-clusters: a comma-separated
// list of "clusterid@host:port" entries into a map keyed by remote cluster
// ID (ADR-011 D1/D2). Unlike ParsePeers, an explicit cluster ID is required
// — the federation namespace is operator-defined, and deriving IDs from
// addresses would silently misroute keys to the wrong cluster. Duplicate IDs
// and the zero ID are rejected. Returns nil for an empty string.
func ParseFederationClusters(raw string) (map[uint64]string, error) {
	if raw == "" {
		return nil, nil
	}
	out := make(map[uint64]string)
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(s, "@")
		if !ok {
			return nil, fmt.Errorf("cluster: federation entry %q must be clusterid@addr (id required)", s)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cluster: invalid federation cluster ID %q: %w", idStr, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("cluster: federation cluster ID must be non-zero (entry %q)", s)
		}
		if addr == "" {
			return nil, fmt.Errorf("cluster: federation address is empty for cluster %d (entry %q)", id, s)
		}
		// The address is dialed as a gateway endpoint, so it must be a usable
		// host:port: a bare host (missing port) and an out-of-range port would
		// expire the dial and surface as an opaque CLUSTERDOWN downstream.
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil || host == "" {
			return nil, fmt.Errorf("cluster: federation address %q for cluster %d must be host:port (entry %q)", addr, id, s)
		}
		if port, perr := strconv.Atoi(portStr); perr != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("cluster: federation address %q for cluster %d has invalid port %q (entry %q)", addr, id, portStr, s)
		}
		if _, dup := out[id]; dup {
			return nil, fmt.Errorf("cluster: duplicate federation cluster id %d", id)
		}
		out[id] = addr
	}
	return out, nil
}

// LocalPeer returns the peer entry matching nodeID, or nil if not found.
func (c *Config) LocalPeer() *Peer {
	for i := range c.Peers {
		if c.Peers[i].ID == c.NodeID {
			return &c.Peers[i]
		}
	}
	return nil
}

// PeerByID returns the peer with the given ID, or nil if not found.
func (c *Config) PeerByID(id uint64) *Peer {
	for i := range c.Peers {
		if c.Peers[i].ID == id {
			return &c.Peers[i]
		}
	}
	return nil
}

// PeerAddrs returns all peer addresses (excluding the local node).
func (c *Config) PeerAddrs() []string {
	var addrs []string
	for _, p := range c.Peers {
		if p.ID != c.NodeID {
			addrs = append(addrs, p.Addr)
		}
	}
	return addrs
}
