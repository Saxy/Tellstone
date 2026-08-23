/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: config_test.go
Description: Unit tests for cluster configuration parsing: peer list
parsing, address-hash ID derivation, and bootstrap membership helpers.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"testing"
)

func TestParsePeersEmpty(t *testing.T) {
	peers, err := ParsePeers("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected 0 peers, got %d", len(peers))
	}
}

func TestParsePeersWithExplicitIDs(t *testing.T) {
	peers, err := ParsePeers("1@127.0.0.1:9989,2@127.0.0.1:9990,3@127.0.0.1:9991")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(peers))
	}
	if peers[0].ID != 1 || peers[0].Addr != "127.0.0.1:9989" {
		t.Fatalf("peer 0: got ID=%d addr=%q, want ID=1 addr=127.0.0.1:9989", peers[0].ID, peers[0].Addr)
	}
	if peers[1].ID != 2 || peers[1].Addr != "127.0.0.1:9990" {
		t.Fatalf("peer 1: got ID=%d addr=%q, want ID=2 addr=127.0.0.1:9990", peers[1].ID, peers[1].Addr)
	}
	if peers[2].ID != 3 || peers[2].Addr != "127.0.0.1:9991" {
		t.Fatalf("peer 2: got ID=%d addr=%q, want ID=3 addr=127.0.0.1:9991", peers[2].ID, peers[2].Addr)
	}
}

func TestParsePeersWithoutIDs(t *testing.T) {
	peers, err := ParsePeers("127.0.0.1:9989,127.0.0.1:9990")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}
	// IDs should be derived from hash.
	if peers[0].ID == 0 || peers[1].ID == 0 {
		t.Fatalf("expected non-zero derived IDs, got %d and %d", peers[0].ID, peers[1].ID)
	}
	if peers[0].ID == peers[1].ID {
		t.Fatalf("expected different IDs for different addresses, both got %d", peers[0].ID)
	}
}

func TestParsePeersInvalidID(t *testing.T) {
	_, err := ParsePeers("abc@127.0.0.1:9989")
	if err == nil {
		t.Fatal("expected error for invalid peer ID")
	}
}

func TestParsePeersEmptyAddress(t *testing.T) {
	_, err := ParsePeers("1@")
	if err == nil {
		t.Fatal("expected error for empty address")
	}
}

func TestHashAddrDeterministic(t *testing.T) {
	a := hashAddr("127.0.0.1:9989")
	b := hashAddr("127.0.0.1:9989")
	if a != b {
		t.Fatalf("hashAddr is not deterministic: %d != %d", a, b)
	}
}

func TestHashAddrDifferent(t *testing.T) {
	a := hashAddr("127.0.0.1:9989")
	b := hashAddr("127.0.0.1:9990")
	if a == b {
		t.Fatalf("hashAddr returned same value for different addresses: %d", a)
	}
}

func TestConfigLocalPeer(t *testing.T) {
	cfg := &Config{
		NodeID: 2,
		Peers: []Peer{
			{ID: 1, Addr: "127.0.0.1:9989"},
			{ID: 2, Addr: "127.0.0.1:9990"},
			{ID: 3, Addr: "127.0.0.1:9991"},
		},
	}
	p := cfg.LocalPeer()
	if p == nil {
		t.Fatal("LocalPeer returned nil")
	}
	if p.ID != 2 || p.Addr != "127.0.0.1:9990" {
		t.Fatalf("LocalPeer: got ID=%d addr=%q, want ID=2 addr=127.0.0.1:9990", p.ID, p.Addr)
	}
}

func TestConfigPeerByID(t *testing.T) {
	cfg := &Config{
		Peers: []Peer{
			{ID: 1, Addr: "127.0.0.1:9989"},
			{ID: 3, Addr: "127.0.0.1:9991"},
		},
	}
	p := cfg.PeerByID(3)
	if p == nil {
		t.Fatal("PeerByID returned nil for ID 3")
	}
	if p.Addr != "127.0.0.1:9991" {
		t.Fatalf("PeerByID: got addr=%q, want 127.0.0.1:9991", p.Addr)
	}
	if cfg.PeerByID(99) != nil {
		t.Fatal("PeerByID should return nil for non-existent ID")
	}
}

func TestConfigPeerAddrs(t *testing.T) {
	cfg := &Config{
		NodeID: 2,
		Peers: []Peer{
			{ID: 1, Addr: "127.0.0.1:9989"},
			{ID: 2, Addr: "127.0.0.1:9990"},
			{ID: 3, Addr: "127.0.0.1:9991"},
		},
	}
	addrs := cfg.PeerAddrs()
	if len(addrs) != 2 {
		t.Fatalf("expected 2 peer addresses (excluding local), got %d", len(addrs))
	}
	if addrs[0] != "127.0.0.1:9989" || addrs[1] != "127.0.0.1:9991" {
		t.Fatalf("unexpected peer addresses: %v", addrs)
	}
}
