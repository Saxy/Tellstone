/*
Package cluster
Tellstone Placement Driver (Phase 2)
File: pd_test.go
Description: Tests for the embedded etcd bootstrap: endpoint derivation,
member validation, single-member round-trips, and three-member cluster
formation with cross-member visibility.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// freePort grabs an ephemeral port and releases it for the next listener.
// The bind-close-reuse window is small enough for in-process tests.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// joinHostPortURL renders an http endpoint for a host and port.
func joinHostPortURL(host string, port int) string {
	return "http://" + net.JoinHostPort(host, strconv.Itoa(port))
}

func TestDerivePDEndpoints(t *testing.T) {
	ep, err := DerivePDEndpoints([]Peer{
		{ID: 1, Addr: "10.0.0.1:7000"},
		{ID: 2, Addr: "[::1]:7100"},
	})
	if err != nil {
		t.Fatalf("DerivePDEndpoints: %v", err)
	}
	if got := ep.ClientURLs[1]; got != "http://10.0.0.1:17000" {
		t.Fatalf("client URL node 1: got %q, want http://10.0.0.1:17000", got)
	}
	if got := ep.PeerURLs[1]; got != "http://10.0.0.1:27000" {
		t.Fatalf("peer URL node 1: got %q, want http://10.0.0.1:27000", got)
	}
	if got := ep.ClientURLs[2]; got != "http://[::1]:17100" {
		t.Fatalf("client URL node 2 (IPv6): got %q", got)
	}

	if _, err := DerivePDEndpoints([]Peer{{ID: 3, Addr: "no-port"}}); err == nil {
		t.Fatal("expected error for malformed member address")
	}
}

func TestStartPDValidation(t *testing.T) {
	if _, err := StartPD(PDConfig{NodeID: 1}); err == nil {
		t.Fatal("expected error for empty bootstrap list")
	}
	if _, err := StartPD(PDConfig{
		NodeID:          9,
		ClientListenURL: "http://127.0.0.1:12379",
		PeerListenURL:   "http://127.0.0.1:12380",
		AllPeerURLs:     map[uint64]string{1: "http://127.0.0.1:12380"},
	}); err == nil {
		t.Fatal("expected error when local member is missing from the list")
	}
}

// TestStartSingleMemberRoundTrip proves the embedded member serves client
// traffic end to end: start, put through a clientv3 dial, read it back.
func TestStartSingleMemberRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cport, pport := freePort(t), freePort(t)

	pd, err := StartPD(PDConfig{
		NodeID:          1,
		DataDir:         dir,
		ClientListenURL: joinHostPortURL("127.0.0.1", cport),
		PeerListenURL:   joinHostPortURL("127.0.0.1", pport),
		AllPeerURLs: map[uint64]string{
			1: joinHostPortURL("127.0.0.1", pport),
		},
	})
	if err != nil {
		t.Fatalf("StartPD: %v", err)
	}
	defer pd.Stop()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pd.ClientURL()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, "tso/test", "42"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, err := cli.Get(ctx, "tso/test")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "42" {
		t.Fatalf("round-trip: got %v, want [42]", resp.Kvs)
	}
}

// TestStartThreeMemberCluster forms a full group in-process and verifies
// that a write through member 1 is visible via member 3 — consensus plus
// replication across the embedded group.
func TestStartThreeMemberCluster(t *testing.T) {
	type member struct {
		id                 uint64
		cport, pport       int
		clientURL, peerURL string
	}
	members := make([]member, 3)
	peerURLs := make(map[uint64]string, 3)
	for i := range members {
		m := &members[i]
		m.id = uint64(i + 1)
		m.cport, m.pport = freePort(t), freePort(t)
		m.clientURL = joinHostPortURL("127.0.0.1", m.cport)
		m.peerURL = joinHostPortURL("127.0.0.1", m.pport)
		peerURLs[m.id] = m.peerURL
	}

	// All members boot concurrently: an etcd group only reports readiness
	// once quorum is live, so sequential starts would deadlock member 1
	// waiting for peers that have not been launched yet.
	pds := make([]*PD, len(members))
	errs := make([]error, len(members))
	var wg sync.WaitGroup
	for i := range members {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := members[i]
			pd, err := StartPD(PDConfig{
				NodeID:          m.id,
				DataDir:         t.TempDir(),
				ClientListenURL: m.clientURL,
				PeerListenURL:   m.peerURL,
				AllPeerURLs:     peerURLs,
			})
			pds[i], errs[i] = pd, err
		}(i)
	}
	wg.Wait()
	defer func() {
		for _, p := range pds {
			p.Stop()
		}
	}()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("StartPD member %d: %v", members[i].id, err)
		}
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pds[0].ClientURL()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3.New (writer): %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, "cluster/proof", "replicated"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	reader, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pds[2].ClientURL()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3.New (reader): %v", err)
	}
	defer reader.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, gerr := reader.Get(rctx, "cluster/proof")
		rcancel()
		if gerr == nil && len(resp.Kvs) == 1 && string(resp.Kvs[0].Value) == "replicated" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("write not visible on member 3 within deadline: err=%v kvs=%v", gerr, resp.Kvs)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
