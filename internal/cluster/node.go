/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: node.go
Description: Raft node lifecycle management. Wraps etcd/raft v3 Node, the TCP
transport, the in-memory storage, and the FSM into a single start/stop unit.
The main loop reads from Node.Ready(), persists state, sends messages to peers,
applies committed entries to the FSM, and ticks at a configurable interval.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/cluster/network"
	"github.com/Saxy/Tellstone/internal/log"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// ErrNotLeader is returned by ProposeAndWait when the node is not the
// current Raft leader. Callers should redirect the client to the leader.
var ErrNotLeader = errors.New("cluster: not leader")

const (
	// logCompactionThreshold compacts the local raft log once this many
	// entries have been applied since the previous compaction. The storage
	// is in-memory, so without periodic compaction the entry slice grows
	// without bound on a long-running leader.
	logCompactionThreshold = 8192
	// logRetention keeps this many applied entries behind the compaction
	// point so lagging followers can still catch up from us instead of
	// needing a full snapshot resend.
	logRetention = 4096
)

// NodeConfig holds the parameters for creating a new Raft node.
type NodeConfig struct {
	NodeID        uint64
	GroupID       uint64 // Raft group (region) ID for transport demux; 0 = legacy single-group
	PeerAddr      string
	Peers         []Peer
	ElectionTick  int
	HeartbeatTick int
	TickInterval  time.Duration
	Dispatcher    Dispatcher
	Logger        log.Logger
	// Transport, when non-nil, reuses an existing transport instead of
	// creating a new listener. This is how a process hosts multiple Raft
	// groups (regions) over one TCP endpoint: each group node registers its
	// GroupID with the shared transport. SharedTransport must be true when
	// Transport is set so the node neither listens nor stops the shared
	// transport.
	Transport       *network.Transport
	SharedTransport bool
}

// Node wraps a raft.Node with its transport, storage, and FSM.
type Node struct {
	cfg       NodeConfig
	raftNode  raft.Node
	storage   *Storage
	fsm       *FSM
	transport *network.Transport
	stopCh    chan struct{}
	stopped   chan struct{}
	wg        sync.WaitGroup
	// proposals tracks pending synchronous write proposals. Each proposal
	// is tagged with a unique ID; the readyLoop signals completion when
	// the corresponding committed entry is applied.
	proposals *proposalTracker
	// chunkMu serializes ProposeChunked so one chunk chain's entries are
	// proposed contiguously in the raft log. Without this, concurrent calls
	// (two followers forwarding chains on different connections, or two local
	// large SETs) interleave raftNode.Propose per chunk; the assembler's
	// writeSeq supersede logic then resets both chains and neither assembles,
	// yet the final chunk still reports a nil apply error — silent data loss.
	chunkMu sync.Mutex
	// quiesced, when true, rejects all new proposals (ProposeAndWait,
	// ForwardWrite) so in-flight proposals can drain during a region split.
	quiesced atomic.Bool
	// stopOnce ensures Stop is idempotent — safe to call multiple times.
	stopOnce sync.Once
	// appliedIndex is the highest Raft log index applied to the FSM. Updated
	// atomically by processReady after each committed entry is applied.
	appliedIndex uint64
	// lastCompacted is the log index through which storage has been
	// compacted. Only touched by the readyLoop goroutine, so no lock needed.
	lastCompacted uint64
	// electionTick mirrors cfg.ElectionTick, guarded atomically so a
	// recalculated election bias (HostRegion, Phase 6) can be applied to an
	// already-stored node without racing readers.
	electionTick atomic.Int64
	// readIndexChans maps a ReadIndex correlation id to the waiter that wants
	// the committed index to wait for.
	readIndexID    atomic.Uint64
	readIndexChans sync.Map // correlation id (uint64) -> chan uint64
	// peerAddrs resolves a peer node ID to its configured address (used by
	// routing/forwarding to target the region leader).
	peerAddrs map[uint64]string
	// ownsTransport is true when this node created its own transport (the
	// standalone case). Nodes sharing another node's transport (config
	// Transport set) neither listen nor Stop it.
	ownsTransport bool
}

// NewNode creates a new Raft node but does not start it. Call Start() to
// begin the consensus loop.
func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}

	store := NewStorage()
	fsm := NewFSM(cfg.Dispatcher, cfg.Logger)

	addrs := make(map[uint64]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		addrs[p.ID] = p.Addr
	}

	n := &Node{
		cfg:       cfg,
		storage:   store,
		fsm:       fsm,
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
		proposals: newProposalTracker(),
		peerAddrs: addrs,
	}
	n.electionTick.Store(int64(cfg.ElectionTick))

	// Build the peer list for StartNode. On the initial bootstrap every node
	// is a voter.
	peers := make([]raft.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, raft.Peer{ID: p.ID})
	}

	raftCfg := raft.Config{
		ID:              cfg.NodeID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         store,
		MaxSizePerMsg:   1024 * 1024, // 1 MiB per append message
		MaxInflightMsgs: 256,
		PreVote:         true,
		CheckQuorum:     true,
	}
	n.raftNode = raft.StartNode(&raftCfg, peers)

	// Build the TCP transport. The handler feeds received messages into the
	// raft node via Step(). The node's raft group (region) ID is registered
	// so inbound frames demux to this node's group — group 0 is the legacy
	// single-group mapping and behaves exactly as before.
	//
	// A node may instead reuse an existing transport (RegionCoordinator
	// spawning per-region group nodes): the shared transport already listens
	// and is owned by the bootstrap node.
	if cfg.Transport != nil {
		n.transport = cfg.Transport
		n.ownsTransport = false
		if cfg.SharedTransport {
			// Multi-group node: register our group handler with the shared
			// transport so inbound frames demux to this region.
			cfg.Transport.RegisterGroup(cfg.GroupID, n.handleMessage)
		}
	} else {
		n.transport = network.NewTransport(cfg.PeerAddr, cfg.NodeID, n.handleMessage, cfg.Logger)
		n.ownsTransport = true
		n.transport.RegisterGroup(cfg.GroupID, n.handleMessage)
	}

	// Register all peers so the transport can dial them. Nodes sharing the
	// bootstrap node's transport skip this: the owning node already maps peer
	// IDs to their (real) addresses, and re-registering from config could
	// clobber addresses with placeholders in dev/test.
	if n.ownsTransport {
		for _, p := range cfg.Peers {
			if p.ID != cfg.NodeID {
				n.transport.RegisterPeer(p.ID, p.Addr)
			}
		}
	}

	// Bind this group's forwarded-write handler on the pipeline. On a shared
	// transport each group node registers its own group ID; the pipeline
	// demultiplexes inbound opts to the node owning that group.
	n.transport.Pipeline(cfg.NodeID, cfg.Logger).RegisterHandler(cfg.GroupID, n.handleForwardOp)

	return n, nil
}

// Start begins the transport listener and the raft processing loop. It is
// non-blocking: the loop runs in background goroutines. Nodes sharing another
// node's transport skip the Listen call (the owning node already listens).
func (n *Node) Start() error {
	if n.ownsTransport {
		if err := n.transport.Listen(); err != nil {
			return err
		}
	}
	n.wg.Add(2)
	go n.tickLoop()
	go n.readyLoop()
	return nil
}

// Stop shuts down the raft node, transport, and processing loops. It waits
// for all goroutines to exit. Safe to call multiple times. A shared-transport
// node stops only its own raft lifecycle; the owning node's transport is left
// running for the remaining region groups.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.raftNode.Stop()
		if n.ownsTransport {
			n.transport.Stop()
		}
		n.wg.Wait()
		close(n.stopped)
	})
}

// Transport returns the node's TCP transport. The returned value satisfies
// the metrics.ClusterMetrics interface structurally (no import required).
func (n *Node) Transport() *network.Transport { return n.transport }

// FSM returns the node's finite state machine. Used by the server to attach
// region-size tracking after the node starts.
func (n *Node) FSM() *FSM { return n.fsm }

// NodeID returns this node's Raft node ID.
func (n *Node) NodeID() uint64 { return n.cfg.NodeID }

// SetElectionTick applies a recalculated election timeout (in ticks) to the
// stored node, updating its recorded bias. The raft library bakes the election
// clock in when the node starts, so the change is reflected in the node's
// configuration and status — and in nodes (re)built after the update — without
// the unsafe teardown of a live group. Guarded so callers can update the bias
// from a policy re-pin without racing readers.
func (n *Node) SetElectionTick(ticks int) {
	if ticks <= 0 {
		return
	}
	n.cfg.ElectionTick = ticks
	n.electionTick.Store(int64(ticks))
}

// ElectionTick returns the node's configured election timeout in ticks.
func (n *Node) ElectionTick() int { return int(n.electionTick.Load()) }

// GroupID returns the Raft group (region) ID this node's transport messages
// are tagged with.
func (n *Node) GroupID() uint64 { return n.cfg.GroupID }

// ErrQuiesced is returned by ProposeAndWait when the node is quiesced
// (briefly paused during a region split).
var ErrQuiesced = errors.New("cluster: node quiesced (region split in progress)")

// Quiesce sets the quiesced flag and drains all in-flight proposals so
// the region split can proceed with no concurrent writes. It returns true
// when all proposals have drained within the timeout.
func (n *Node) Quiesce(timeout time.Duration) bool {
	n.quiesced.Store(true)
	return n.proposals.drain(timeout)
}

// Resume clears the quiesced flag so new proposals are accepted again.
func (n *Node) Resume() {
	n.quiesced.Store(false)
}

// IsQuiesced reports whether the node is currently quiesced.
func (n *Node) IsQuiesced() bool {
	return n.quiesced.Load()
}

// ProposeAndWait submits data to the Raft log and blocks until the entry is
// committed and applied locally. It returns nil on success, an error on
// FSM apply failure, timeout, ErrNotLeader if this node is not the current
// leader, or ErrQuiesced if the node is quiesced during a split.
func (n *Node) ProposeAndWait(ctx context.Context, data []byte) error {
	if n.quiesced.Load() {
		return ErrQuiesced
	}
	if !n.IsLeader() {
		return ErrNotLeader
	}
	id, done := n.proposals.add()
	tagged := tagProposal(tagID(id), data)
	if err := n.raftNode.Propose(ctx, tagged); err != nil {
		n.proposals.remove(id)
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		n.proposals.remove(id)
		return ctx.Err()
	case <-n.stopCh:
		n.proposals.remove(id)
		return errors.New("cluster node stopped")
	}
}

// ProposeChunked submits a chunk chain (one raft entry per chunk) and blocks
// until the final chunk has been committed and applied. ProposeChunked calls
// are serialized so a chain's entries are always contiguous in the log — no
// other chunk chain can interleave — which keeps the FSM's reassembly
// deterministic. The FSM reassembles via the write sequence and a retry of
// the whole chain with the same sequence is safe. Returns the last chunk's
// apply error.
func (n *Node) ProposeChunked(ctx context.Context, chunks [][]byte) error {
	if n.quiesced.Load() {
		return ErrQuiesced
	}
	if !n.IsLeader() {
		return ErrNotLeader
	}
	if len(chunks) == 0 {
		return nil
	}
	// Hold the lock across the entire chain so every chunk reaches the raft
	// log contiguously; quiesced checks, proposal tracking, and error cleanup
	// below are unchanged and run inside the locked section.
	n.chunkMu.Lock()
	defer n.chunkMu.Unlock()
	// Register a waiter and propose every chunk immediately. The caller only
	// waits for the last chunk, which is when the value becomes complete.
	ids := make([]uint64, 0, len(chunks))
	dones := make([]chan error, 0, len(chunks))
	for _, c := range chunks {
		if n.quiesced.Load() {
			return ErrQuiesced
		}
		id, done := n.proposals.add()
		ids = append(ids, id)
		dones = append(dones, done)
		if err := n.raftNode.Propose(ctx, tagProposal(tagID(id), c)); err != nil {
			for j := 0; j < len(ids); j++ {
				n.proposals.remove(ids[j])
			}
			return err
		}
	}
	select {
	case err := <-dones[len(dones)-1]:
		return err
	case <-ctx.Done():
		for _, id := range ids {
			n.proposals.remove(id)
		}
		return ctx.Err()
	case <-n.stopCh:
		for _, id := range ids {
			n.proposals.remove(id)
		}
		return errors.New("cluster node stopped")
	}
}

// Propose encodes a log entry payload and sends it to the raft node without
// waiting for commit. Use ProposeAndWait for synchronous write operations.
func (n *Node) Propose(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return n.raftNode.Propose(ctx, data)
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raftNode.Status().RaftState == raft.StateLeader
}

// LeaderID returns the node ID of the current Raft leader (0 if none yet).
func (n *Node) LeaderID() uint64 {
	return n.raftNode.Status().Lead
}

// AppliedIndex returns the highest Raft log index applied to the local FSM.
func (n *Node) AppliedIndex() uint64 {
	return atomic.LoadUint64(&n.appliedIndex)
}

// PeerAddr resolves a peer node ID to its configured address.
func (n *Node) PeerAddr(id uint64) string {
	return n.peerAddrs[id]
}

// ReadIndex performs a Raft read-only query: it returns the committed index
// the local state machine must have applied before serving a linearizable
// read. See LinearizableRead for the wait.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	id := n.readIndexID.Add(1)
	ch := make(chan uint64, 1)
	n.readIndexChans.Store(id, ch)
	defer n.readIndexChans.Delete(id)
	if err := n.raftNode.ReadIndex(ctx, uint64ToBytes(id)); err != nil {
		return 0, err
	}
	select {
	case idx := <-ch:
		return idx, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, errors.New("cluster node stopped")
	}
}

// LinearizableRead blocks until the local FSM has applied at least the
// committed index returned by ReadIndex, guaranteeing a read observes all
// previously committed writes. This is how followers serve reads locally
// (D2: Read Anywhere, no forwarding).
func (n *Node) LinearizableRead(ctx context.Context) error {
	idx, err := n.ReadIndex(ctx)
	if err != nil {
		return err
	}
	for {
		if atomic.LoadUint64(&n.appliedIndex) >= idx {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return errors.New("cluster node stopped")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// ForwardWrite sends an already-encoded FSM operation to the region leader
// (identified by leaderID) over the pipeline and waits for the apply result.
// Used by clusterStore when this node is not the leader (D3). The response
// error is the leader's apply error (or ErrNotLeader when the routing table is
// stale and the target can no longer propose).
func (n *Node) ForwardWrite(ctx context.Context, leaderID uint64, opData []byte) error {
	// Routing may resolve to this node (e.g. it just became leader and the
	// table is briefly stale), or the caller may already be the leader. In
	// that case propose directly instead of round-tripping to ourself.
	if leaderID == n.cfg.NodeID {
		return n.ProposeAndWait(ctx, opData)
	}
	_, err := n.pipeline().Call(ctx, leaderID, n.cfg.GroupID, network.OpForwardWrite, opData)
	return err
}

// ForwardChunks sends a chunk chain (OpForwardChunks) to the region leader over
// the pipeline and waits for the apply result of the chain. Used by clusterStore
// for large values proposed on the leader. The chain is serialized into the
// request payload; the leader reassembles and proposes it via ProposeChunked.
func (n *Node) ForwardChunks(ctx context.Context, leaderID uint64, chunks [][]byte) error {
	if leaderID == n.cfg.NodeID {
		return n.ProposeChunked(ctx, chunks)
	}
	_, err := n.pipeline().Call(ctx, leaderID, n.cfg.GroupID, network.OpForwardChunks, encodeChunks(chunks))
	return err
}

// pipeline returns the process pipeline for this node's transport, creating it
// on first use. All nodes sharing a transport share the returned pipeline and
// demultiplex forwarded operations by group ID.
func (n *Node) pipeline() *network.Pipeline {
	return n.transport.Pipeline(n.cfg.NodeID, n.cfg.Logger)
}

// handleMessage is called by the transport for every inbound message. Raft
// messages are delivered to the raft node via Step(). Forwarded writes arrive
// as pipeline ops (handled on the pipeline worker pool), never as raft
// messages, so there is nothing to intercept here.
func (n *Node) handleMessage(msg *pb.Message) {
	_ = n.raftNode.Step(context.Background(), msg)
}

// handleForwardOp applies a forwarded write on the leader (the only node that
// may propose) and returns the apply result, which the pipeline frames as the
// response. A single forwarded op may carry a chunk chain in its payload; the
// leader proposes every chunk in order via ProposeChunked. If this node is no
// longer leader (stale routing), ProposeAndWait returns ErrNotLeader and the
// caller retries via the routing table.
func (n *Node) handleForwardOp(op network.OpKind, payload []byte) ([]byte, error) {
	if n.quiesced.Load() {
		// Quiesced: reply with an error so the follower retries later. The
		// response is framed as an error by the pipeline.
		return nil, ErrQuiesced
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	switch op {
	case network.OpForwardChunks:
		chain, err := decodeChunks(payload)
		if err != nil {
			return nil, err
		}
		return nil, n.ProposeChunked(ctx, chain)
	default:
		return nil, n.ProposeAndWait(ctx, payload)
	}
}

// encodeChunks serializes a chunk chain into a single payload for the pipeline.
// Layout: [4B count][per chunk: 4B length][data].
func encodeChunks(chunks [][]byte) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(len(chunks)))
	for _, c := range chunks {
		out = binary.BigEndian.AppendUint32(out, uint32(len(c)))
		out = append(out, c...)
	}
	return out
}

// decodeChunks parses a chunk chain payload produced by encodeChunks.
func decodeChunks(payload []byte) ([][]byte, error) {
	if len(payload) < 4 {
		return nil, errors.New("cluster node: malformed chunk chain payload")
	}
	count := int(binary.BigEndian.Uint32(payload))
	if count < 0 || count > 64*1024 {
		return nil, errors.New("cluster node: implausible chunk count")
	}
	chain := make([][]byte, 0, count)
	off := 4
	for i := 0; i < count; i++ {
		if len(payload)-off < 4 {
			return nil, errors.New("cluster node: malformed chunk chain payload")
		}
		l := int(binary.BigEndian.Uint32(payload[off:]))
		off += 4
		if l < 0 || len(payload)-off < l {
			return nil, errors.New("cluster node: malformed chunk chain payload")
		}
		c := make([]byte, l)
		copy(c, payload[off:off+l])
		chain = append(chain, c)
		off += l
	}
	return chain, nil
}

// uint64ToBytes / bytesToUint64 encode a correlation id for the wire.
func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// tickLoop advances the raft node's logical clock at the configured interval.
func (n *Node) tickLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.raftNode.Tick()
		}
	}
}

// readyLoop reads from the raft Node's Ready channel and processes each
// batch: persists state, sends messages, and applies committed entries.
func (n *Node) readyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case rd := <-n.raftNode.Ready():
			n.processReady(rd)
			n.raftNode.Advance()
		}
	}
}

// processReady handles a single Ready batch from the raft node.
func (n *Node) processReady(rd raft.Ready) {
	// 1. Persist hard state and log entries to stable storage.
	if !raft.IsEmptyHardState(rd.HardState) {
		n.storage.SetHardState(rd.HardState)
	}
	if len(rd.Entries) > 0 {
		n.storage.Append(rd.Entries)
	}
	if rd.Snapshot != nil && !raft.IsEmptySnap(rd.Snapshot) {
		n.storage.SetSnapshot(rd.Snapshot)
	}

	// 2. Send messages to peers via the transport (batched — messages to
	//    the same peer are coalesced into a single TCP frame).
	if len(rd.Messages) > 0 {
		// Group by destination peer.
		groups := make(map[uint64][]*pb.Message)
		for _, msg := range rd.Messages {
			groups[msg.GetTo()] = append(groups[msg.GetTo()], msg)
		}
		for _, batch := range groups {
			if err := n.transport.SendBatchTo(n.cfg.GroupID, batch); err != nil && n.cfg.Logger.Enabled(log.LevelWarn) {
				n.cfg.Logger.Log(log.LevelWarn, "cluster: send batch failed",
					log.String("error", err.Error()),
					log.Int("count", len(batch)),
					log.Uint64("to", batch[0].GetTo()),
				)
			}
		}
	}

	// 3. Apply committed entries to the FSM. Tagged entries (synchronous
	// proposals) have their 8-byte ID prefix stripped before being passed
	// to the FSM, then the waiting goroutine is signaled.
	if len(rd.CommittedEntries) > 0 && n.cfg.Logger.Enabled(log.LevelDebug) {
		n.cfg.Logger.Log(log.LevelDebug, "cluster: applying committed entries",
			log.Int("count", len(rd.CommittedEntries)),
			log.Uint64("first_index", rd.CommittedEntries[0].GetIndex()),
			log.Uint64("last_index", rd.CommittedEntries[len(rd.CommittedEntries)-1].GetIndex()),
		)
	}
	for _, e := range rd.CommittedEntries {
		data := e.GetData()
		if id, ok := extractProposalID(data); ok {
			// Strip the proposal tag before applying.
			stripped := &pb.Entry{
				Index: e.Index,
				Term:  e.Term,
				Type:  e.Type,
				Data:  data[proposalIDSize:],
			}
			applyErr := n.fsm.Apply(stripped)
			if applyErr != nil && n.cfg.Logger.Enabled(log.LevelError) {
				n.cfg.Logger.Log(log.LevelError, "cluster: fsm apply failed",
					log.String("error", applyErr.Error()),
					log.Uint64("index", e.GetIndex()),
				)
			}
			n.proposals.complete(id, applyErr)
			if applyErr == nil && n.cfg.Logger.Enabled(log.LevelDebug) {
				n.cfg.Logger.Log(log.LevelDebug, "cluster: proposal applied successfully",
					log.Uint64("proposal_id", id),
					log.Uint64("index", e.GetIndex()),
				)
			}
		} else if len(data) < opHeaderSize {
			// Raft internal entries (no-ops on leader election, ConfChanges)
			// have empty or short payloads that carry no FSM operation.
			// Skip them — the FSM has nothing to do.
		} else if err := n.fsm.Apply(e); err != nil && n.cfg.Logger.Enabled(log.LevelError) {
			n.cfg.Logger.Log(log.LevelError, "cluster: fsm apply failed (untagged entry)",
				log.String("error", err.Error()),
				log.Uint64("index", e.GetIndex()),
				log.Int("data_len", len(data)),
			)
		}
		// Track the highest applied index for the appliedIndex counter.
		idx := e.GetIndex()
		for {
			cur := atomic.LoadUint64(&n.appliedIndex)
			if idx <= cur {
				break
			}
			if atomic.CompareAndSwapUint64(&n.appliedIndex, cur, idx) {
				break
			}
		}
	}

	// 3b. Resolve any pending ReadIndex queries. The leader (or a follower
	// after a ReadIndex round-trip) has now caught up to the returned commit
	// index, so waiters can proceed with a linearizable read.
	for _, rs := range rd.ReadStates {
		if v, ok := n.readIndexChans.Load(bytesToUint64(rs.RequestCtx)); ok {
			ch := v.(chan uint64)
			select {
			case ch <- rs.Index:
			default:
			}
		}
	}

	// 4. Periodically compact the log through an entry safely behind the
	// applied index. Compaction only ever advances to applied-retention, so
	// entries still needed by lagging followers remain available; raft never
	// reads its own storage past the applied point.
	applied := atomic.LoadUint64(&n.appliedIndex)
	if applied-n.lastCompacted >= logCompactionThreshold {
		compactTo := applied - logRetention
		if first, err := n.storage.FirstIndex(); err == nil && compactTo >= first {
			n.storage.Compact(compactTo)
			n.lastCompacted = compactTo
			if n.cfg.Logger.Enabled(log.LevelDebug) {
				n.cfg.Logger.Log(log.LevelDebug, "cluster: log compacted",
					log.Uint64("through_index", compactTo),
				)
			}
		}
	}
}

// Done returns a channel that is closed when the node has fully stopped.
func (n *Node) Done() <-chan struct{} {
	return n.stopped
}

// Status returns a snapshot of the raft node's status.
func (n *Node) Status() raft.Status {
	return n.raftNode.Status()
}
