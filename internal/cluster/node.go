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

// Application-level message types carried over the existing network.Transport
// (D3: no protobuf/gRPC for Phase 3). They use out-of-range raftpb.MessageType
// codes and are intercepted in handleMessage before being passed to
// raftNode.Step, so raft never sees them.
const (
	msgForwardWrite pb.MessageType = 100
	msgForwardResp  pb.MessageType = 101
)

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
	PeerAddr      string
	Peers         []Peer
	ElectionTick  int
	HeartbeatTick int
	TickInterval  time.Duration
	Dispatcher    Dispatcher
	Logger        log.Logger
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
	// stopOnce ensures Stop is idempotent — safe to call multiple times.
	stopOnce sync.Once
	// appliedIndex is the highest Raft log index applied to the FSM. Updated
	// atomically by processReady after each committed entry is applied.
	appliedIndex uint64
	// lastCompacted is the log index through which storage has been
	// compacted. Only touched by the readyLoop goroutine, so no lock needed.
	lastCompacted uint64
	// readIndexChans maps a ReadIndex correlation id to the waiter that wants
	// the committed index to wait for.
	readIndexID    atomic.Uint64
	readIndexChans sync.Map // correlation id (uint64) -> chan uint64
	// forwardChans maps a forwarded-write correlation id to its result waiter.
	forwardID    atomic.Uint64
	forwardChans sync.Map // correlation id (uint64) -> chan error
	// peerAddrs resolves a peer node ID to its configured address (used by
	// routing/forwarding to target the region leader).
	peerAddrs map[uint64]string
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
	// raft node via Step().
	n.transport = network.NewTransport(cfg.PeerAddr, cfg.NodeID, n.handleMessage, cfg.Logger)

	// Register all peers so the transport can dial them.
	for _, p := range cfg.Peers {
		if p.ID != cfg.NodeID {
			n.transport.RegisterPeer(p.ID, p.Addr)
		}
	}

	return n, nil
}

// Start begins the transport listener and the raft processing loop. It is
// non-blocking: the loop runs in background goroutines.
func (n *Node) Start() error {
	if err := n.transport.Listen(); err != nil {
		return err
	}
	n.wg.Add(2)
	go n.tickLoop()
	go n.readyLoop()
	return nil
}

// Stop shuts down the raft node, transport, and processing loops. It waits
// for all goroutines to exit. Safe to call multiple times.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.raftNode.Stop()
		n.transport.Stop()
		n.wg.Wait()
		close(n.stopped)
	})
}

// Transport returns the node's TCP transport. The returned value satisfies
// the metrics.ClusterMetrics interface structurally (no import required).
func (n *Node) Transport() *network.Transport { return n.transport }

// ProposeAndWait submits data to the Raft log and blocks until the entry is
// committed and applied locally. It returns nil on success, an error on
// FSM apply failure, timeout, or ErrNotLeader if this node is not the
// current leader. The data is tagged with a proposal ID so the readyLoop
// can signal completion.
func (n *Node) ProposeAndWait(ctx context.Context, data []byte) error {
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
// (identified by leaderID) over the existing transport and waits for the
// apply result. Used by clusterStore when this node is not the leader (D3).
func (n *Node) ForwardWrite(ctx context.Context, leaderID uint64, opData []byte) error {
	// Routing may resolve to this node (e.g. it just became leader and the
	// table is briefly stale), or the caller may already be the leader. In
	// that case propose directly instead of round-tripping to ourself.
	if leaderID == n.cfg.NodeID {
		return n.ProposeAndWait(ctx, opData)
	}
	id := n.forwardID.Add(1)
	ch := make(chan error, 1)
	n.forwardChans.Store(id, ch)
	defer n.forwardChans.Delete(id)
	msg := &pb.Message{
		Type:    msgForwardType(),
		From:    &n.cfg.NodeID,
		To:      &leaderID,
		Context: uint64ToBytes(id),
		Entries: []*pb.Entry{{Data: opData}},
	}
	if err := n.transport.Send(msg); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return errors.New("cluster node stopped")
	}
}

// handleMessage is called by the transport for every inbound message.
// Application-level forward messages are handled locally; everything else is
// delivered to the raft node via Step().
func (n *Node) handleMessage(msg *pb.Message) {
	switch msg.GetType() {
	case msgForwardWrite:
		n.handleForwardWrite(msg)
		return
	case msgForwardResp:
		if len(msg.Context) == 8 {
			if v, ok := n.forwardChans.Load(bytesToUint64(msg.Context)); ok {
				ch := v.(chan error)
				var rerr error
				if len(msg.Entries) > 0 && len(msg.Entries[0].Data) > 0 {
					rerr = errors.New(string(msg.Entries[0].Data))
				}
				select {
				case ch <- rerr:
				default:
				}
			}
		}
		return
	}
	_ = n.raftNode.Step(context.Background(), msg)
}

// handleForwardWrite applies a forwarded write on the leader (the only node
// that may propose) and replies to the requester. If this node is no longer
// leader (stale routing), it replies with ErrNotLeader so the caller retries.
func (n *Node) handleForwardWrite(msg *pb.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var opData []byte
	if len(msg.Entries) > 0 {
		opData = msg.Entries[0].Data
	}
	applyErr := n.ProposeAndWait(ctx, opData)
	var respData []byte
	if applyErr != nil {
		respData = []byte(applyErr.Error())
	}
	resp := &pb.Message{
		Type:    msgForwardRespType(),
		From:    &n.cfg.NodeID,
		To:      msg.From,
		Context: msg.Context,
		Entries: []*pb.Entry{{Data: respData}},
	}
	_ = n.transport.Send(resp)
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

// msgForwardType / msgForwardRespType return pointers to the application-level
// message types for use in raftpb.Message struct literals (which take pointer
// fields).
func msgForwardType() *pb.MessageType {
	t := msgForwardWrite
	return &t
}

func msgForwardRespType() *pb.MessageType {
	t := msgForwardResp
	return &t
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
			if err := n.transport.SendBatch(batch); err != nil && n.cfg.Logger.Enabled(log.LevelWarn) {
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
