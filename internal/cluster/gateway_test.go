/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: gateway_test.go
Description: Tests for the Phase 7 cross-cluster gateway: op payload codecs,
configuration validation, and a round-trip over two in-process gateway
transports proving a forwarded write/read is executed on the remote side and
answered over the cluster-ID-addressed pipe.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/cluster/network"
	"github.com/Saxy/Tellstone/internal/log"
)

func TestEncodeDecodeXWrite(t *testing.T) {
	chunks := [][]byte{
		[]byte("chunk-one"),
		[]byte("chunk-two-with-more-bytes"),
		nil,
	}
	enc := EncodeXWrite("user:eu:alice", chunks)
	key, got, err := DecodeXWrite(enc)
	if err != nil {
		t.Fatalf("DecodeXWrite: %v", err)
	}
	if key != "user:eu:alice" {
		t.Errorf("key = %q, want user:eu:alice", key)
	}
	if len(got) != len(chunks) {
		t.Fatalf("decoded %d chunks, want %d", len(got), len(chunks))
	}
	for i := range chunks {
		if !bytes.Equal(got[i], chunks[i]) {
			t.Errorf("chunk %d = %q, want %q", i, got[i], chunks[i])
		}
	}
	// Re-encode the decoded form must be byte-identical (canonical).
	if !bytes.Equal(EncodeXWrite(key, got), enc) {
		t.Errorf("re-encode changed the wire form")
	}
}

func TestDecodeXWriteTruncated(t *testing.T) {
	enc := EncodeXWrite("key", [][]byte{[]byte("data")})
	for _, cut := range []int{0, 1, 2, 4, 8, len(enc) - 1} {
		key, chunks, err := DecodeXWrite(enc[:cut])
		if err == nil {
			t.Errorf("DecodeXWrite(%d bytes) unexpectedly ok (key=%q chunks=%d)", cut, key, len(chunks))
		}
	}
	// Reject an implausibly big chunk count without a big allocation.
	bad := appendUint16(nil, 3)
	bad = append(bad, "key"...)
	bad = appendUint32(bad, 0xffffffff) // absurd chunk count
	bad = appendUint32(bad, 1)
	bad = append(bad, 'x')
	if _, _, err := DecodeXWrite(bad); err == nil {
		t.Errorf("DecodeXWrite with absurd chunk count accepted")
	}
}

func TestEncodeDecodeXRead(t *testing.T) {
	enc := EncodeXRead("user:na:bob")
	key, err := DecodeXRead(enc)
	if err != nil {
		t.Fatalf("DecodeXRead: %v", err)
	}
	if key != "user:na:bob" {
		t.Errorf("key = %q, want user:na:bob", key)
	}
	if _, err := DecodeXRead(nil); err == nil {
		t.Errorf("DecodeXRead(nil) accepted")
	}
}

func TestEncodeDecodeXReadResp(t *testing.T) {
	if v, present, err := DecodeXReadResp(EncodeXReadResp([]byte("v+"), true)); err != nil || !present || string(v) != "v+" {
		t.Errorf("present round-trip = %q present=%v err=%v", v, present, err)
	}
	// present=0 with a value is a stored empty value, not a miss.
	if v, present, err := DecodeXReadResp(EncodeXReadResp(nil, true)); err != nil || !present || len(v) != 0 {
		t.Errorf("empty-value round-trip = %q present=%v err=%v", v, present, err)
	}
	if v, present, err := DecodeXReadResp(EncodeXReadResp(nil, false)); err != nil || present || len(v) != 0 {
		t.Errorf("missing round-trip = %q present=%v err=%v", v, present, err)
	}
	if _, _, err := DecodeXReadResp(nil); err == nil {
		t.Errorf("DecodeXReadResp(nil) accepted")
	}
}

func TestGatewayConfigValidation(t *testing.T) {
	if _, err := NewGateway(GatewayConfig{ClusterID: 0, Addr: ":0"}); err == nil {
		t.Errorf("NewGateway with cluster id 0 accepted")
	}
	if _, err := NewGateway(GatewayConfig{ClusterID: 1, Addr: ""}); err == nil {
		t.Errorf("NewGateway with empty addr accepted")
	}
	if _, err := NewGateway(GatewayConfig{ClusterID: 1, Addr: ":0", Peers: map[uint64]string{1: "x:1"}}); err == nil {
		t.Errorf("NewGateway with self-referencing peer accepted")
	}
	g, err := NewGateway(GatewayConfig{ClusterID: 1, Addr: ":0", Peers: map[uint64]string{2: "127.0.0.1:1", 3: ""}})
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	if len(g.peers) != 1 || g.peers[2] != "127.0.0.1:1" {
		t.Errorf("empty-addr peer not filtered, peers=%v", g.peers)
	}
}

func TestParserFederationClusters(t *testing.T) {
	m, err := ParseFederationClusters("2@10.0.0.1:9100, 3@10.0.0.2:9100")
	if err != nil {
		t.Fatalf("ParseFederationClusters: %v", err)
	}
	if len(m) != 2 || m[2] != "10.0.0.1:9100" || m[3] != "10.0.0.2:9100" {
		t.Errorf("parsed map = %v", m)
	}
	if m, err := ParseFederationClusters(""); err != nil || m != nil {
		t.Errorf("empty input: map=%v err=%v", m, err)
	}
	for _, bad := range []string{
		"host:9100",        // no explicit cluster id
		"0@host:9100",      // zero id
		"2@host:9100,2@x",  // duplicate id
		"ab@host:9100",     // non-numeric id
		"2@",               // empty addr
		"2@host",           // addr missing port
		"2@host:notaport",  // non-numeric port
		"2@host:0",         // out-of-range port
		"2@host:70000",     // out-of-range port
		"2@:9100",          // empty host
		"2@host:9100,@x:1", // second entry explicit-id violation
	} {
		if _, err := ParseFederationClusters(bad); err == nil {
			t.Errorf("ParseFederationClusters(%q) accepted", bad)
		}
	}
}

// TestGatewayCrossClusterRoundTrip boots two gateways (clusters 1 and 2) and
// proves an OpXClusterRead and an OpXClusterWrite travel over the
// cluster-ID-addressed pipe, execute on the far side, and return.
func TestGatewayCrossClusterRoundTrip(t *testing.T) {
	logger := log.NewNoOpLogger()
	gwA, err := NewGateway(GatewayConfig{ClusterID: 1, Addr: "127.0.0.1:0", Logger: logger})
	if err != nil {
		t.Fatalf("NewGateway(A): %v", err)
	}
	if err := gwA.Start(); err != nil {
		t.Fatalf("gwA.Start: %v", err)
	}
	defer gwA.Stop()

	gwB, err := NewGateway(GatewayConfig{
		ClusterID: 2,
		Addr:      "127.0.0.1:0",
		Peers:     map[uint64]string{1: gwA.Addr()},
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("NewGateway(B): %v", err)
	}
	if err := gwB.Start(); err != nil {
		t.Fatalf("gwB.Start: %v", err)
	}
	defer gwB.Stop()
	gwA.AddPeer(2, gwB.Addr())

	// B is "cluster 2": its executor emulates the cluster store.
	gwB.SetHandler(func(op network.OpKind, payload []byte) ([]byte, error) {
		switch op {
		case network.OpXClusterRead:
			key, err := DecodeXRead(payload)
			if err != nil {
				return nil, err
			}
			switch key {
			case "user:na:bob":
				return EncodeXReadResp([]byte("bob-value"), true), nil
			case "missing":
				return EncodeXReadResp(nil, false), nil
			default:
				return nil, nil
			}
		case network.OpXClusterWrite:
			key, chunks, err := DecodeXWrite(payload)
			if err != nil {
				return nil, err
			}
			if key != "user:eu:alice" || len(chunks) != 1 || string(chunks[0]) != "set-value" {
				t.Errorf("far side write = key %q chunks %v", key, chunks)
			}
			return nil, nil
		default:
			return nil, nil
		}
	})
	// Give B's keepalive a moment to dial A (so the response route is warm),
	// then call either direction.
	time.Sleep(200 * time.Millisecond)

	// A reads through B's gateway (cluster 2).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := gwA.Call(ctx, 2, network.OpXClusterRead, EncodeXRead("user:na:bob"))
	if err != nil {
		t.Fatalf("cross-cluster read: %v", err)
	}
	if v, present, derr := DecodeXReadResp(resp); derr != nil || !present || string(v) != "bob-value" {
		t.Errorf("read result = %q present=%v err=%v, want bob-value present", v, present, derr)
	}

	// A missing key must come back as present=false.
	resp, err = gwA.Call(ctx, 2, network.OpXClusterRead, EncodeXRead("missing"))
	if err != nil {
		t.Fatalf("cross-cluster read missing: %v", err)
	}
	if _, present, _ := DecodeXReadResp(resp); present {
		t.Errorf("missing key read back as present")
	}

	// A writes through B's gateway.
	writePayload := EncodeXWrite("user:eu:alice", [][]byte{[]byte("set-value")})
	if _, err := gwA.Call(ctx, 2, network.OpXClusterWrite, writePayload); err != nil {
		t.Fatalf("cross-cluster write: %v", err)
	}

	// The receiving side is not configured (no executor) until SetHandler is
	// called; a fresh gateway must surface that instead of hanging.
	gwUntouched, uerr := NewGateway(GatewayConfig{ClusterID: 3, Addr: "127.0.0.1:0", Logger: logger})
	if uerr != nil {
		t.Fatalf("NewGateway(untouched): %v", uerr)
	}
	if err := gwUntouched.Start(); err != nil {
		t.Fatalf("gwUntouched.Start: %v", err)
	}
	defer gwUntouched.Stop()
	if _, err := gwUntouched.Call(context.Background(), 99, network.OpXClusterRead, nil); err == nil {
		t.Errorf("Call to unregistered peer succeeded")
	} else if !strings.Contains(err.Error(), "unknown peer") {
		t.Errorf("unregistered peer error = %v", err)
	}
}
