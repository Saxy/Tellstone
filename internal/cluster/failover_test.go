/*
Package cluster
Tellstone Placement Driver + TSO (Phase 2)
File: failover_test.go
Description: End-to-end acceptance for the decentralized TSO grant. A
three-member PD cluster feeds three independent pools. Killing the whole
etcd quorum (the only thing that can stop a CAS) drains the in-memory
pools; Alloc then blocks instead of handing out duplicates, and a
restarted quorum resumes granting. This is the stronger guarantee the
decentralized design gives versus a single-leader allocator.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// TestTSODrainAndRecover brings up a 3-member PD cluster, proves three
// pools can all allocate, then stops the entire quorum so no range can be
// granted. The pools drain to empty and Alloc blocks (proving a write path
// would stall safely), and restarting the quorum resumes allocation.
func TestTSODrainAndRecover(t *testing.T) {
	// 1. Three embedded members forming one etcd cluster.
	members := make([]*PD, 3)
	memberCfg := make([]PDConfig, 3)
	peers := map[uint64]string{}
	clientURLs := []string{}
	for i := 0; i < 3; i++ {
		c, p := freePort(t), freePort(t)
		cu := joinHostPortURL("127.0.0.1", c)
		pu := joinHostPortURL("127.0.0.1", p)
		peers[uint64(i+1)] = pu
		clientURLs = append(clientURLs, cu)
		memberCfg[i] = PDConfig{
			NodeID:          uint64(i + 1),
			DataDir:         t.TempDir(),
			ClientListenURL: cu,
			PeerListenURL:   pu,
		}
	}
	// Start all members concurrently: a 3-node cluster needs a quorum
	// before any single member can elect a leader, so a sequential StartPD
	// (which waits for readiness) would deadlock on member 1.
	var wg sync.WaitGroup
	startErr := make([]error, 3)
	for i := 0; i < 3; i++ {
		cfg := memberCfg[i]
		cfg.AllPeerURLs = peers
		wg.Add(1)
		go func(i int, cfg PDConfig) {
			defer wg.Done()
			pd, err := StartPD(cfg)
			if err != nil {
				startErr[i] = err
				return
			}
			members[i] = pd
		}(i, cfg)
	}
	wg.Wait()
	for i := range startErr {
		if startErr[i] != nil {
			t.Fatalf("start member %d: %v", i+1, startErr[i])
		}
	}
	defer func() {
		for _, m := range members {
			m.Stop()
		}
	}()

	// 2. One granter + pool + refill manager per node, sharing the cluster.
	pools := make([]*TSOPool, 3)
	mgrs := make([]*TSOManager, 3)
	for i := 0; i < 3; i++ {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   clientURLs,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("client %d: %v", i+1, err)
		}
		t.Cleanup(func() { cli.Close() })
		g := NewEtcdGranter(cli)
		pool := NewTSOPool(TSOPoolConfig{
			MinBatch:           500,
			Headroom:           30 * time.Second,
			RefillThresholdPct: 20,
		})
		pools[i] = pool
		mgr := NewTSOManager(pool, g)
		mgrs[i] = mgr
		mgr.Run()
		t.Cleanup(mgr.Stop)
	}

	// 3. All three pools allocate independently right after startup.
	for i := 0; i < 3; i++ {
		waitForAlloc(t, pools[i])
	}

	// 4. Stop the entire quorum — the only failure that halts a CAS.
	for _, m := range members {
		m.Stop()
	}

	// 5. Drain node 0's pool. Whatever ranges were granted while the
	//    quorum was up are finite; with etcd down no refill can land.
	pool := pools[0]
	drained := 0
	for i := 0; i < 5_000_000; i++ {
		if _, err := pool.TryAlloc(); err != nil {
			break
		}
		drained++
	}
	if drained == 0 {
		t.Fatal("expected node 0 to have been granted at least one range")
	}

	// 6. With the pool empty and etcd down, Alloc must block, not invent a
	//    timestamp. A short deadline proves the stall.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if _, err := pool.Alloc(ctx); err == nil {
		t.Fatal("Alloc should block (error) when pool is empty and etcd down")
	}

	// 7. Restart the quorum concurrently (same data dirs preserve the
	//    watermark). A sequential restart would, again, stall on quorum.
	var rwg sync.WaitGroup
	restartErr := make([]error, 3)
	for i := 0; i < 3; i++ {
		cfg := memberCfg[i]
		cfg.AllPeerURLs = peers
		rwg.Add(1)
		go func(i int, cfg PDConfig) {
			defer rwg.Done()
			pd, err := StartPD(cfg)
			if err != nil {
				restartErr[i] = err
				return
			}
			members[i] = pd
		}(i, cfg)
	}
	rwg.Wait()
	for i := range restartErr {
		if restartErr[i] != nil {
			t.Fatalf("restart member %d: %v", i+1, restartErr[i])
		}
	}

	// 8. The surviving manager resumes granting within a few ticks.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := pool.TryAlloc(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("node 0 did not resume allocating after quorum restart")
}
