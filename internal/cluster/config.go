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
