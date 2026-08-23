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

	n := &Node{
		cfg:       cfg,
		storage:   store,
		fsm:       fsm,
		stopCh:    make(chan struct{}),
		stopped:   make(chan struct{}),
		proposals: newProposalTracker(),
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

// handleMessage is called by the transport when a raft message arrives from a
// peer. It delivers the message to the raft node via Step().
func (n *Node) handleMessage(msg *pb.Message) {
	_ = n.raftNode.Step(context.Background(), msg)
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
}

// Done returns a channel that is closed when the node has fully stopped.
func (n *Node) Done() <-chan struct{} {
	return n.stopped
}

// Status returns a snapshot of the raft node's status.
func (n *Node) Status() raft.Status {
	return n.raftNode.Status()
}
