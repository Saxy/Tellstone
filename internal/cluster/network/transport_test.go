package network

import (
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestTransportListenAndStop(t *testing.T) {
	logger := log.NewNoOpLogger()
	var received atomic.Int32
	handler := func(msg *pb.Message) {
		received.Add(1)
	}

	tr := NewTransport("127.0.0.1:0", 1, handler, logger)
	if err := tr.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Stop()

	if tr.Addr() == "" {
		t.Fatal("expected non-empty listen address")
	}
}

func TestTransportSendReceive(t *testing.T) {
	logger := log.NewNoOpLogger()
	var mu sync.Mutex
	var received []*pb.Message

	handler := func(msg *pb.Message) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	}

	// Node 1 listens.
	tr1 := NewTransport("127.0.0.1:0", 1, handler, logger)
	if err := tr1.Listen(); err != nil {
		t.Fatalf("tr1 Listen: %v", err)
	}
	defer tr1.Stop()

	// Node 2 listens.
	tr2 := NewTransport("127.0.0.1:0", 2, handler, logger)
	if err := tr2.Listen(); err != nil {
		t.Fatalf("tr2 Listen: %v", err)
	}
	defer tr2.Stop()

	// Register peer addresses.
	tr1.RegisterPeer(2, tr2.Addr())
	tr2.RegisterPeer(1, tr1.Addr())

	// Send a message from node 1 to node 2.
	to2 := uint64(2)
	msg := &pb.Message{
		To:   &to2,
		Type: func() *pb.MessageType { v := pb.MessageType_MsgApp; return &v }(),
	}

	if err := tr1.Send(msg); err != nil {
		t.Fatalf("Send from tr1: %v", err)
	}

	// Give time for the batch sender to flush and read loop to deliver.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 message received, got %d", count)
	}
}

func TestTransportRegisterPeer(t *testing.T) {
	logger := log.NewNoOpLogger()
	tr := NewTransport("127.0.0.1:0", 1, func(msg *pb.Message) {}, logger)

	tr.RegisterPeer(42, "10.0.0.1:9989")
	addr := tr.lookupPeerAddr(42)
	if addr != "10.0.0.1:9989" {
		t.Fatalf("lookupPeerAddr(42): got %q, want %q", addr, "10.0.0.1:9989")
	}
	if tr.lookupPeerAddr(99) != "" {
		t.Fatal("expected empty address for unknown peer")
	}
}

func TestTransportConnectionCount(t *testing.T) {
	logger := log.NewNoOpLogger()
	tr := NewTransport("127.0.0.1:0", 1, func(msg *pb.Message) {}, logger)
	if err := tr.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Stop()

	if tr.ConnectionCount() != 0 {
		t.Fatalf("expected 0 connections, got %d", tr.ConnectionCount())
	}
}

func TestTransportStats(t *testing.T) {
	logger := log.NewNoOpLogger()
	var mu sync.Mutex
	var received []*pb.Message

	handler := func(msg *pb.Message) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	}

	tr1 := NewTransport("127.0.0.1:0", 1, handler, logger)
	if err := tr1.Listen(); err != nil {
		t.Fatalf("tr1 Listen: %v", err)
	}
	defer tr1.Stop()

	tr2 := NewTransport("127.0.0.1:0", 2, handler, logger)
	if err := tr2.Listen(); err != nil {
		t.Fatalf("tr2 Listen: %v", err)
	}
	defer tr2.Stop()

	tr1.RegisterPeer(2, tr2.Addr())
	tr2.RegisterPeer(1, tr1.Addr())

	// Send 5 messages from node 1 to node 2.
	for i := 0; i < 5; i++ {
		to2 := uint64(2)
		msg := &pb.Message{
			To:   &to2,
			Type: func() *pb.MessageType { v := pb.MessageType_MsgApp; return &v }(),
		}
		if err := tr1.Send(msg); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()
	if count != 5 {
		t.Fatalf("expected 5 messages received, got %d", count)
	}

	// Check sender stats (tr1).
	senderStats := tr1.Stats()
	if senderStats.MessagesSent.Load() != 5 {
		t.Fatalf("MessagesSent = %d, want 5", senderStats.MessagesSent.Load())
	}
	if senderStats.FramesSent.Load() < 1 {
		t.Fatalf("FramesSent = %d, want >= 1", senderStats.FramesSent.Load())
	}
	if senderStats.BytesSent.Load() <= 0 {
		t.Fatalf("BytesSent = %d, want > 0", senderStats.BytesSent.Load())
	}

	// Check receiver stats (tr2).
	recvStats := tr2.Stats()
	if recvStats.MessagesRecv.Load() != 5 {
		t.Fatalf("MessagesRecv = %d, want 5", recvStats.MessagesRecv.Load())
	}
	if recvStats.FramesRecv.Load() < 1 {
		t.Fatalf("FramesRecv = %d, want >= 1", recvStats.FramesRecv.Load())
	}

	t.Logf("sender: %d msgs in %d frames (%d bytes)", senderStats.MessagesSent.Load(), senderStats.FramesSent.Load(), senderStats.BytesSent.Load())
	t.Logf("receiver: %d msgs in %d frames", recvStats.MessagesRecv.Load(), recvStats.FramesRecv.Load())
}

func TestTransportStatsBatchingRatio(t *testing.T) {
	// Simulate a realistic Raft heartbeat burst: 10 messages to 3 peers,
	// all flushed together via the batch timer. Measures how many TCP
	// frames the batching layer actually writes.
	logger := log.NewNoOpLogger()
	var mu sync.Mutex
	var received []*pb.Message

	handler := func(msg *pb.Message) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	}

	// 3 receivers.
	transports := make([]*Transport, 3)
	for i := range transports {
		tr := NewTransport("127.0.0.1:0", uint64(i+2), handler, logger)
		if err := tr.Listen(); err != nil {
			t.Fatalf("tr%d Listen: %v", i, err)
		}
		transports[i] = tr
	}
	defer func() {
		for _, tr := range transports {
			tr.Stop()
		}
	}()

	// Sender (node 1).
	sender := NewTransport("127.0.0.1:0", 1, handler, logger)
	if err := sender.Listen(); err != nil {
		t.Fatalf("sender Listen: %v", err)
	}
	defer sender.Stop()

	// Register peers.
	for i, tr := range transports {
		sender.RegisterPeer(uint64(i+2), tr.Addr())
	}

	// Burst: 10 messages to each of 3 peers (30 total).
	peerIDs := []uint64{2, 3, 4}
	for _, peerID := range peerIDs {
		for j := 0; j < 10; j++ {
			msg := &pb.Message{
				To:   &peerID,
				Type: func() *pb.MessageType { v := pb.MessageType_MsgApp; return &v }(),
				Term: uint64Ptr(1),
			}
			if err := sender.Send(msg); err != nil {
				t.Fatalf("Send to %d: %v", peerID, err)
			}
		}
	}

	// Wait for batch flush (100μs timer + margin).
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	s := sender.Stats()

	t.Logf("=== BATCHING RATIO ===")
	t.Logf("messages_sent:     %d", s.MessagesSent.Load())
	t.Logf("frames_sent:       %d", s.FramesSent.Load())
	t.Logf("bytes_sent:        %d", s.BytesSent.Load())
	t.Logf("messages_received: %d", count)
	t.Logf("frames_received:   %d", s.FramesRecv.Load())

	if s.MessagesSent.Load() != 30 {
		t.Fatalf("MessagesSent = %d, want 30", s.MessagesSent.Load())
	}
	if count != 30 {
		t.Fatalf("received %d messages, want 30", count)
	}
	// Key assertion: 30 messages should be in far fewer than 30 frames.
	// With 100μs batching, bursts to the same peer are coalesced.
	if s.FramesSent.Load() >= 30 {
		t.Fatalf("FramesSent = %d — batching not working (expected << 30)", s.FramesSent.Load())
	}
	ratio := float64(s.MessagesSent.Load()) / float64(s.FramesSent.Load())
	t.Logf("batching ratio:    %.1f messages/frame", ratio)
}

func TestTransportMalformedFrameKillsConnection(t *testing.T) {
	logger := log.NewNoOpLogger()
	var received atomic.Int32
	handler := func(msg *pb.Message) {
		received.Add(1)
	}

	tr := NewTransport("127.0.0.1:0", 1, handler, logger)
	if err := tr.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer tr.Stop()

	// Manually dial the transport's listener and send a valid-length frame
	// containing garbage bytes. The readLoop will read the full payload,
	// fail to decode it, and kill the connection.
	conn, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Frame: 4-byte length prefix (10 bytes of payload) + garbage payload.
	// The payload claims 3 messages (count=3) followed by bytes that don't
	// form valid messages.
	payload := []byte{3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	// Wait for the readLoop to process the frame and kill the connection.
	time.Sleep(200 * time.Millisecond)

	// Verify the handler was never called (garbage can't decode).
	if received.Load() != 0 {
		t.Fatalf("expected 0 messages from garbage frame, got %d", received.Load())
	}

	// Verify the server closed the connection: a read should return EOF or error.
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	conn.Close()
	if n != 0 || err == nil {
		t.Fatalf("expected read to fail on killed connection, n=%d err=%v", n, err)
	}
}
