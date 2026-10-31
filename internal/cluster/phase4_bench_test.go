/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: phase4_bench_test.go
Description: Phase 4 (Raft batching + leader lease reads) baseline harness.
Boots a real multi-node Raft cluster on localhost with the production timing
(10 election ticks / 1 heartbeat tick / 100ms tick) and measures the write
path, the linearizable read path, the local apply floor, replication traffic
and idle heartbeat chatter.

The benchmarks are the repeatable throughput/allocation numbers. The report
test prints latency percentiles, messages per write and idle traffic; it is
gated behind PHASE4_REPORT=1 so the normal test suite does not pay for it:

	PHASE4_REPORT=1 go test -run TestPhase4BaselineReport -v ./internal/cluster/

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// benchDispatcher is the benchmark state machine: it records the operation
// count and keeps the latest value per key so local reads can be measured
// without the test dispatcher's op log (which would allocate per write and
// pollute the numbers).
type benchDispatcher struct {
	ops   atomic.Int64
	mu    sync.RWMutex
	store map[string][]byte
}

func newBenchDispatcher() *benchDispatcher {
	return &benchDispatcher{store: make(map[string][]byte)}
}

func (d *benchDispatcher) Dispatch(key string, op byte, value []byte, ttl time.Duration) error {
	switch op {
	case OpSet, OpSetNX, OpSetXX:
		d.mu.Lock()
		d.store[key] = value
		d.mu.Unlock()
	case OpDel:
		d.mu.Lock()
		delete(d.store, key)
		d.mu.Unlock()
	}
	d.ops.Add(1)
	return nil
}

func (d *benchDispatcher) get(key string) ([]byte, bool) {
	d.mu.RLock()
	v, ok := d.store[key]
	d.mu.RUnlock()
	return v, ok
}

// benchCluster is a leader-ready cluster with one state machine per node.
type benchCluster struct {
	nodes       []*Node
	dispatchers []*benchDispatcher
	leader      *Node
	leaderIdx   int
}

// startBenchCluster boots an N-node Raft cluster using the same timing as
// server.initCluster and waits for a leader. The cluster is stopped when the
// test or benchmark finishes.
func startBenchCluster(tb testing.TB, nodeCount int) *benchCluster {
	tb.Helper()

	peers := make([]Peer, nodeCount)
	for i := range peers {
		peers[i] = Peer{ID: uint64(i + 1)}
	}

	nodes := make([]*Node, nodeCount)
	dispatchers := make([]*benchDispatcher, nodeCount)
	safeReads := os.Getenv("PHASE4_SAFE_READS") != ""
	// Match the production default (--raft-heartbeat-tick 2). Override with
	// PHASE4_HEARTBEAT_TICK to reproduce the pre-D5 1-tick heartbeat.
	heartbeatTick := 2
	if v := os.Getenv("PHASE4_HEARTBEAT_TICK"); v != "" {
		if n, convErr := strconv.Atoi(v); convErr == nil && n > 0 {
			heartbeatTick = n
		}
	}
	for i := range nodes {
		d := newBenchDispatcher()
		dispatchers[i] = d
		n, err := NewNode(NodeConfig{
			NodeID:        uint64(i + 1),
			PeerAddr:      "127.0.0.1:0",
			Peers:         peers,
			ElectionTick:  10,
			HeartbeatTick: heartbeatTick,
			TickInterval:  100 * time.Millisecond,
			Dispatcher:    d,
			Logger:        log.NewNoOpLogger(),
			SafeReads:     safeReads,
		})
		if err != nil {
			tb.Fatalf("NewNode(%d): %v", i+1, err)
		}
		nodes[i] = n
	}

	for _, n := range nodes {
		if err := n.Start(); err != nil {
			tb.Fatalf("Node.Start(%d): %v", n.cfg.NodeID, err)
		}
	}
	tb.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	// Register the OS-assigned ports now that every listener is up.
	for _, n := range nodes {
		for _, other := range nodes {
			if n != other {
				n.transport.RegisterPeer(other.cfg.NodeID, other.transport.Addr())
			}
		}
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if n.IsLeader() {
				return &benchCluster{
					nodes:       nodes,
					dispatchers: dispatchers,
					leader:      n,
					leaderIdx:   i,
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatal("timeout waiting for leader election")
	return nil
}

// statsDelta sums the transport counters across every node. A write on a
// three-node cluster therefore counts both the leader's sends and the
// followers' replies.
type statsDelta struct {
	msgs   int64
	frames int64
	bytes  int64
}

func snapshotStats(nodes []*Node) statsDelta {
	var s statsDelta
	for _, n := range nodes {
		st := n.transport.Stats()
		s.msgs += st.MessagesSent.Load()
		s.frames += st.FramesSent.Load()
		s.bytes += st.BytesSent.Load()
	}
	return s
}

func (s statsDelta) sub(before statsDelta) statsDelta {
	return statsDelta{
		msgs:   s.msgs - before.msgs,
		frames: s.frames - before.frames,
		bytes:  s.bytes - before.bytes,
	}
}

func benchSetData(size int) ([]byte, error) {
	return EncodeSet("phase4-key", make([]byte, size), 0)
}

// ---------------------------------------------------------------------------
// Benchmarks (repeatable throughput / allocation numbers)
// ---------------------------------------------------------------------------

// BenchmarkPhase4ProposeSequential measures one client writing one entry at a
// time: the full propose -> replicate -> commit -> apply -> signal round trip.
func BenchmarkPhase4ProposeSequential(b *testing.B) {
	c := startBenchCluster(b, 3)
	data, err := benchSetData(64)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Minute)
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.leader.ProposeAndWait(ctx, data); err != nil {
			b.Fatalf("ProposeAndWait: %v", err)
		}
	}
}

// BenchmarkPhase4ProposeParallel measures many concurrent clients proposing to
// the leader, which is the case etcd raft's own Ready batching optimizes for.
func BenchmarkPhase4ProposeParallel(b *testing.B) {
	c := startBenchCluster(b, 3)
	data, err := benchSetData(64)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Minute)
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.leader.ProposeAndWait(ctx, data); err != nil {
				b.Errorf("ProposeAndWait: %v", err)
				return
			}
		}
	})
}

// BenchmarkPhase4LinearizableRead measures the current read path: ReadIndex
// (a quorum round trip in safe mode) followed by the applied-index wait.
func BenchmarkPhase4LinearizableRead(b *testing.B) {
	c := startBenchCluster(b, 3)
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Minute)
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.leader.LinearizableRead(ctx); err != nil {
			b.Fatalf("LinearizableRead: %v", err)
		}
	}
}

// BenchmarkPhase4LinearizableReadParallel measures concurrent linearizable
// reads. It shows whether ReadIndex requests are coalesced.
func BenchmarkPhase4LinearizableReadParallel(b *testing.B) {
	c := startBenchCluster(b, 3)
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Minute)
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.leader.LinearizableRead(ctx); err != nil {
				b.Errorf("LinearizableRead: %v", err)
				return
			}
		}
	})
}

// BenchmarkPhase4LocalRead is the floor: a read straight from the applied
// state machine, with no consensus involvement.
func BenchmarkPhase4LocalRead(b *testing.B) {
	c := startBenchCluster(b, 3)
	data, err := benchSetData(64)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Minute)
	defer cancel()
	if err := c.leader.ProposeAndWait(ctx, data); err != nil {
		b.Fatalf("seed write: %v", err)
	}
	d := c.dispatchers[c.leaderIdx]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := d.get("phase4-key"); !ok {
			b.Fatal("seed key missing")
		}
	}
}

// BenchmarkPhase4FSMApply isolates the state-machine apply cost (decode +
// dispatch) from consensus, to size the "batched apply" opportunity.
func BenchmarkPhase4FSMApply(b *testing.B) {
	d := newBenchDispatcher()
	f := NewFSM(d, log.NewNoOpLogger())
	data, err := benchSetData(64)
	if err != nil {
		b.Fatal(err)
	}
	entryType := pb.EntryNormal
	entryTerm := uint64(1)
	entryIndex := uint64(1)
	entry := &pb.Entry{Index: &entryIndex, Term: &entryTerm, Type: &entryType, Data: data}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entryIndex = uint64(i + 1)
		if err := f.Apply(entry); err != nil {
			b.Fatalf("Apply: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// Report (latency percentiles, traffic per write, idle chatter)
// ---------------------------------------------------------------------------

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func latencyStats(samples []time.Duration) (mean, p50, p95, p99 time.Duration) {
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	return total / time.Duration(len(sorted)),
		percentile(sorted, 0.50),
		percentile(sorted, 0.95),
		percentile(sorted, 0.99)
}

func fmtDur(d time.Duration) string {
	if d < time.Microsecond {
		return d.String()
	}
	return d.Round(time.Microsecond).String()
}

// TestPhase4BaselineReport prints the Phase 4 baseline: sequential write and
// read percentiles, concurrent throughput, messages per write, apply cost and
// idle heartbeat traffic. Gated so the normal suite stays fast.
func TestPhase4BaselineReport(t *testing.T) {
	if os.Getenv("PHASE4_REPORT") == "" {
		t.Skip("set PHASE4_REPORT=1 to run the Phase 4 baseline report")
	}

	c := startBenchCluster(t, 3)
	data, err := benchSetData(64)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Warm up connections and elect a stable leader before measuring.
	for i := 0; i < 50; i++ {
		if err := c.leader.ProposeAndWait(ctx, data); err != nil {
			t.Fatalf("warmup write: %v", err)
		}
	}

	// --- Sequential write latency + replication traffic per write. ---
	const writeSamples = 500
	before := snapshotStats(c.nodes)
	writes := make([]time.Duration, 0, writeSamples)
	for i := 0; i < writeSamples; i++ {
		start := time.Now()
		if err := c.leader.ProposeAndWait(ctx, data); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		writes = append(writes, time.Since(start))
	}
	after := snapshotStats(c.nodes)
	delta := after.sub(before)

	mean, p50, p95, p99 := latencyStats(writes)
	t.Logf("sequential write (%d samples): mean=%s p50=%s p95=%s p99=%s",
		writeSamples, fmtDur(mean), fmtDur(p50), fmtDur(p95), fmtDur(p99))
	t.Logf("replication traffic per write: msgs=%.2f frames=%.2f bytes=%.0f",
		float64(delta.msgs)/writeSamples,
		float64(delta.frames)/writeSamples,
		float64(delta.bytes)/writeSamples)

	// --- Concurrent write throughput (32 clients by default, 1.5s). ---
	writers := 32
	if v := os.Getenv("PHASE4_WRITERS"); v != "" {
		if n, convErr := strconv.Atoi(v); convErr == nil && n > 0 {
			writers = n
		}
	}
	const writeWindow = 1500 * time.Millisecond
	var writeOps atomic.Int64
	var writeErrs atomic.Int64
	var wg sync.WaitGroup
	stopWriters := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopWriters:
					return
				default:
				}
				if err := c.leader.ProposeAndWait(ctx, data); err != nil {
					writeErrs.Add(1)
					return
				}
				writeOps.Add(1)
			}
		}()
	}
	before = snapshotStats(c.nodes)
	beforeBatch := c.leader.BatchingStats()
	time.Sleep(writeWindow)
	close(stopWriters)
	wg.Wait()
	after = snapshotStats(c.nodes)
	delta = after.sub(before)
	afterBatch := c.leader.BatchingStats()
	nOps := writeOps.Load()
	t.Logf("concurrent write (%d clients, %s): %.0f ops/s, %d ops total, errors=%d",
		writers, writeWindow, float64(nOps)/writeWindow.Seconds(), nOps, writeErrs.Load())
	t.Logf("concurrent replication traffic per write: msgs=%.2f frames=%.2f",
		float64(delta.msgs)/float64(nOps), float64(delta.frames)/float64(nOps))
	dReady := float64(afterBatch.ReadyBatches - beforeBatch.ReadyBatches)
	dEntries := float64(afterBatch.CommittedEntries - beforeBatch.CommittedEntries)
	dAppendMsgs := float64(afterBatch.AppendMsgs - beforeBatch.AppendMsgs)
	dAppendEntries := float64(afterBatch.AppendEntriesEntries - beforeBatch.AppendEntriesEntries)
	ratio := func(num, den float64) float64 {
		if den == 0 {
			return 0
		}
		return num / den
	}
	t.Logf("leader batching: entries/ready=%.2f entries/appendEntries=%.2f appends/write=%.2f",
		ratio(dEntries, dReady), ratio(dAppendEntries, dAppendMsgs), ratio(dAppendMsgs, float64(nOps)))

	// --- Read latency: ReadIndex + applied wait vs local state machine. ---
	const readSamples = 500
	reads := make([]time.Duration, 0, readSamples)
	for i := 0; i < readSamples; i++ {
		start := time.Now()
		if err := c.leader.LinearizableRead(ctx); err != nil {
			t.Fatalf("linearizable read %d: %v", i, err)
		}
		reads = append(reads, time.Since(start))
	}
	rMean, rP50, rP95, rP99 := latencyStats(reads)
	t.Logf("linearizable read (%d samples): mean=%s p50=%s p95=%s p99=%s",
		readSamples, fmtDur(rMean), fmtDur(rP50), fmtDur(rP95), fmtDur(rP99))

	d := c.dispatchers[c.leaderIdx]
	localReads := make([]time.Duration, 0, readSamples)
	for i := 0; i < readSamples; i++ {
		start := time.Now()
		if _, ok := d.get("phase4-key"); !ok {
			t.Fatal("seed key missing")
		}
		localReads = append(localReads, time.Since(start))
	}
	lMean, lP50, lP95, lP99 := latencyStats(localReads)
	t.Logf("local state-machine read (%d samples): mean=%s p50=%s p95=%s p99=%s",
		readSamples, fmtDur(lMean), fmtDur(lP50), fmtDur(lP95), fmtDur(lP99))

	// --- Concurrent read throughput (32 clients, 1.5s). ---
	var readOps atomic.Int64
	var rwg sync.WaitGroup
	var readErrs atomic.Int64
	stopReads := make(chan struct{})
	for i := 0; i < writers; i++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stopReads:
					return
				default:
				}
				if err := c.leader.LinearizableRead(ctx); err != nil {
					readErrs.Add(1)
					return
				}
				readOps.Add(1)
			}
		}()
	}
	before = snapshotStats(c.nodes)
	time.Sleep(writeWindow)
	close(stopReads)
	rwg.Wait()
	after = snapshotStats(c.nodes)
	delta = after.sub(before)
	rOps := readOps.Load()
	t.Logf("concurrent linearizable read (%d clients, %s): %.0f ops/s, %d ops, errors=%d",
		writers, writeWindow, float64(rOps)/writeWindow.Seconds(), rOps, readErrs.Load())
	t.Logf("concurrent read traffic per op: msgs=%.2f frames=%.2f",
		float64(delta.msgs)/float64(rOps), float64(delta.frames)/float64(rOps))

	// --- Apply floor: FSM decode + dispatch, no consensus. ---
	f := NewFSM(newBenchDispatcher(), log.NewNoOpLogger())
	entryType := pb.EntryNormal
	entryTerm := uint64(1)
	entryIndex := uint64(1)
	entry := &pb.Entry{Index: &entryIndex, Term: &entryTerm, Type: &entryType, Data: data}
	const applySamples = 2000
	applies := make([]time.Duration, 0, applySamples)
	for i := 0; i < applySamples; i++ {
		entryIndex = uint64(i + 1)
		start := time.Now()
		if err := f.Apply(entry); err != nil {
			t.Fatalf("apply: %v", err)
		}
		applies = append(applies, time.Since(start))
	}
	aMean, aP50, aP95, aP99 := latencyStats(applies)
	t.Logf("fsm apply (%d samples): mean=%s p50=%s p95=%s p99=%s",
		applySamples, fmtDur(aMean), fmtDur(aP50), fmtDur(aP95), fmtDur(aP99))

	// --- Idle heartbeat chatter (no client traffic, 2s). ---
	const idleWindow = 2 * time.Second
	before = snapshotStats(c.nodes)
	time.Sleep(idleWindow)
	after = snapshotStats(c.nodes)
	delta = after.sub(before)
	t.Logf("idle cluster traffic (%s): %.1f msgs/s, %.1f frames/s",
		idleWindow, float64(delta.msgs)/idleWindow.Seconds(), float64(delta.frames)/idleWindow.Seconds())
}

// TestReadOnlyOptionMapping pins the SafeReads knob to the raft read-only
// modes: lease reads are the default because they remove the per-read quorum
// round trip, and --raft-safe-reads must restore ReadOnlySafe exactly.
func TestReadOnlyOptionMapping(t *testing.T) {
	if got := readOnlyOption(false); got != raft.ReadOnlyLeaseBased {
		t.Fatalf("default read-only option = %v, want ReadOnlyLeaseBased", got)
	}
	if got := readOnlyOption(true); got != raft.ReadOnlySafe {
		t.Fatalf("safe read-only option = %v, want ReadOnlySafe", got)
	}
}

// TestPhase4LeaseReadStopsOnQuorumLoss covers the safety bound of lease
// reads: a leader that loses contact with a quorum may serve from its lease
// until it expires, but CheckQuorum must make it step down within one
// election timeout, after which it must stop answering linearizable reads.
func TestPhase4LeaseReadStopsOnQuorumLoss(t *testing.T) {
	c := startBenchCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.leader.LinearizableRead(ctx); err != nil {
		t.Fatalf("baseline linearizable read: %v", err)
	}

	// Isolate the leader: no raft traffic can reach or leave it, so the
	// quorum check has to expire its lease.
	c.leader.Transport().Stop()

	deadline := time.Now().Add(5 * time.Second)
	for c.leader.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("former leader still reports leadership after losing quorum")
		}
		time.Sleep(20 * time.Millisecond)
	}

	rctx, rcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer rcancel()
	if err := c.leader.LinearizableRead(rctx); err == nil {
		t.Fatal("linearizable read succeeded on an isolated former leader")
	}
}
