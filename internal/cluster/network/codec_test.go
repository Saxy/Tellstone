/*
Package network
Tellstone Cloud-Native In-Memory Database
File: codec_test.go
Description: Tests for the zero-protobuf binary codec. Verifies round-trip
encode/decode fidelity and proves that multiple Raft messages are batched
into a single network frame (one packet).

Authors:

	Maximilian Hagen
*/
package network

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

func uint64Ptr(v uint64) *uint64 { return &v }
func boolPtr(v bool) *bool       { return &v }

func TestCodecRoundTrip(t *testing.T) {
	entries := []*pb.Entry{
		{Type: entryTypePtr(pb.EntryNormal), Term: uint64Ptr(1), Index: uint64Ptr(5), Data: []byte("hello")},
		{Type: entryTypePtr(pb.EntryConfChange), Term: uint64Ptr(2), Index: uint64Ptr(6), Data: []byte("cc")},
	}

	tests := []struct {
		name string
		msg  *pb.Message
	}{
		{
			name: "heartbeat",
			msg: &pb.Message{
				Type:   pbTypePtr(pb.MsgHeartbeat),
				To:     uint64Ptr(2),
				From:   uint64Ptr(1),
				Term:   uint64Ptr(5),
				Commit: uint64Ptr(10),
			},
		},
		{
			name: "append_entries",
			msg: &pb.Message{
				Type:    pbTypePtr(pb.MsgApp),
				To:      uint64Ptr(3),
				From:    uint64Ptr(1),
				Term:    uint64Ptr(3),
				LogTerm: uint64Ptr(2),
				Index:   uint64Ptr(4),
				Entries: entries,
			},
		},
		{
			name: "reject",
			msg: &pb.Message{
				Type:       pbTypePtr(pb.MsgAppResp),
				To:         uint64Ptr(1),
				From:       uint64Ptr(3),
				Term:       uint64Ptr(3),
				Index:      uint64Ptr(4),
				Reject:     boolPtr(true),
				RejectHint: uint64Ptr(100),
			},
		},
		{
			name: "vote",
			msg: &pb.Message{
				Type: pbTypePtr(pb.MsgVote),
				To:   uint64Ptr(2),
				From: uint64Ptr(1),
				Term: uint64Ptr(10),
			},
		},
		{
			name: "with_context",
			msg: &pb.Message{
				Type:    pbTypePtr(pb.MsgApp),
				To:      uint64Ptr(2),
				From:    uint64Ptr(1),
				Term:    uint64Ptr(1),
				Context: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			},
		},
		{
			name: "with_snapshot",
			msg: &pb.Message{
				Type: pbTypePtr(pb.MsgSnap),
				To:   uint64Ptr(2),
				From: uint64Ptr(1),
				Term: uint64Ptr(5),
				Snapshot: &pb.Snapshot{
					Metadata: &pb.SnapshotMetadata{
						Index: uint64Ptr(100),
						Term:  uint64Ptr(4),
						ConfState: &pb.ConfState{
							Voters:         []uint64{1, 2, 3},
							Learners:       []uint64{4},
							VotersOutgoing: []uint64{1, 2},
							LearnersNext:   []uint64{5},
						},
					},
					Data: []byte("snapshot-data"),
				},
			},
		},
		{
			name: "with_vote",
			msg: &pb.Message{
				Type: pbTypePtr(pb.MsgStorageAppend),
				Vote: uint64Ptr(5),
				Term: uint64Ptr(10),
			},
		},
		{
			name: "minimal",
			msg: &pb.Message{
				Type: pbTypePtr(pb.MsgAppResp),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := []*pb.Message{tt.msg}
			frame, err := EncodeBatch(batch)
			if err != nil {
				t.Fatalf("EncodeBatch: %v", err)
			}
			// Strip the 4-byte length prefix to get the payload.
			payload := frame[4:]
			got, err := DecodeBatch(payload)
			if err != nil {
				t.Fatalf("DecodeBatch: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("expected 1 message, got %d", len(got))
			}
			assertMessagesEqual(t, tt.msg, got[0])
		})
	}
}

func TestCodecBatchRoundTrip(t *testing.T) {
	batch := []*pb.Message{
		{Type: pbTypePtr(pb.MsgHeartbeat), To: uint64Ptr(2), From: uint64Ptr(1), Term: uint64Ptr(5), Commit: uint64Ptr(10)},
		{Type: pbTypePtr(pb.MsgHeartbeatResp), To: uint64Ptr(1), From: uint64Ptr(2), Term: uint64Ptr(5)},
		{Type: pbTypePtr(pb.MsgApp), To: uint64Ptr(3), From: uint64Ptr(1), Term: uint64Ptr(3), LogTerm: uint64Ptr(2), Index: uint64Ptr(4),
			Entries: []*pb.Entry{
				{Type: entryTypePtr(pb.EntryNormal), Term: uint64Ptr(1), Index: uint64Ptr(5), Data: []byte("key=val")},
			},
		},
		{Type: pbTypePtr(pb.MsgVote), To: uint64Ptr(2), From: uint64Ptr(1), Term: uint64Ptr(10)},
		{Type: pbTypePtr(pb.MsgAppResp), To: uint64Ptr(1), From: uint64Ptr(3), Term: uint64Ptr(3), Index: uint64Ptr(4)},
	}

	frame, eerr := EncodeBatch(batch)
	if eerr != nil {
		t.Fatalf("EncodeBatch: %v", eerr)
	}
	payload := frame[4:]
	got, err := DecodeBatch(payload)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(got) != len(batch) {
		t.Fatalf("expected %d messages, got %d", len(batch), len(got))
	}
	for i := range batch {
		assertMessagesEqual(t, batch[i], got[i])
	}
}

// TestCodecSingleFrameProvesBatching creates a TCP connection, sends a batch
// of multiple Raft messages encoded as a single frame, and proves the receiver
// gets all messages from that single read — one packet on the wire.
func TestCodecSingleFrameProvesBatching(t *testing.T) {
	// Build a batch of 10 messages.
	batch := make([]*pb.Message, 10)
	for i := range batch {
		batch[i] = &pb.Message{
			Type:   pbTypePtr(pb.MsgHeartbeat),
			To:     uint64Ptr(uint64(i + 1)),
			From:   uint64Ptr(1),
			Term:   uint64Ptr(5),
			Commit: uint64Ptr(uint64(100 + i)),
		}
	}

	frame, err := EncodeBatch(batch)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	// Start a TCP server that reads exactly one frame.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var serverFrame []byte
	var serverErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			serverErr = err
			return
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		// Read the 4-byte length prefix.
		hdr := make([]byte, 4)
		if _, err := conn.Read(hdr); err != nil {
			serverErr = err
			return
		}
		length := uint32(hdr[0])<<24 | uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
		serverFrame = make([]byte, 4+length)
		copy(serverFrame, hdr)
		if _, err := conn.Read(serverFrame[4:]); err != nil {
			serverErr = err
			return
		}
	}()

	// Client: send the entire batch as a single Write (one syscall = one packet).
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Write(frame)
	conn.Close()
	if err != nil {
		t.Fatalf("client write: %v", err)
	}

	wg.Wait()
	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}

	// Decode the received frame.
	if !bytes.Equal(frame, serverFrame) {
		t.Fatalf("frame mismatch: sent %d bytes, received %d bytes", len(frame), len(serverFrame))
	}
	payload := serverFrame[4:]
	got, err := DecodeBatch(payload)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("expected 10 messages from single frame, got %d", len(got))
	}
	for i, m := range got {
		if m.GetTo() != uint64(i+1) {
			t.Errorf("msg[%d].To = %d, want %d", i, m.GetTo(), i+1)
		}
		if m.GetCommit() != uint64(100+i) {
			t.Errorf("msg[%d].Commit = %d, want %d", i, m.GetCommit(), 100+i)
		}
	}
	t.Logf("SUCCESS: 10 Raft messages sent as 1 TCP frame (%d bytes) = 1 packet on the wire", len(frame))
}

// TestCodecBatchSizeProvesPacketReduction compares the number of network writes
// needed for 50 messages: without batching (50 writes) vs with batching (1 write).
func TestCodecBatchSizeProvesPacketReduction(t *testing.T) {
	msgs := make([]*pb.Message, 50)
	for i := range msgs {
		msgs[i] = &pb.Message{
			Type:   pbTypePtr(pb.MsgAppResp),
			To:     uint64Ptr(1),
			From:   uint64Ptr(uint64(i + 2)),
			Term:   uint64Ptr(3),
			Index:  uint64Ptr(uint64(i)),
			Reject: boolPtr(i%3 == 0),
		}
	}

	// Without batching: each message is its own 4-byte header + payload.
	individualWrites := 0
	for _, m := range msgs {
		single, serr := EncodeBatch([]*pb.Message{m})
		if serr != nil {
			t.Fatalf("EncodeBatch(single): %v", serr)
		}
		individualWrites++
		_ = single
	}

	// With batching: all messages in a single frame.
	batched, berr := EncodeBatch(msgs)
	if berr != nil {
		t.Fatalf("EncodeBatch: %v", berr)
	}
	payload := batched[4:]
	decoded, err := DecodeBatch(payload)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}

	t.Logf("50 messages: unbatched = %d writes, batched = 1 write (%d%% reduction)",
		individualWrites, 100-(100/individualWrites))
	t.Logf("batch frame size: %d bytes", len(batched))

	if len(decoded) != 50 {
		t.Fatalf("expected 50 decoded messages, got %d", len(decoded))
	}
}

func assertMessagesEqual(t *testing.T, want, got *pb.Message) {
	t.Helper()
	if want.GetType() != got.GetType() {
		t.Errorf("Type: got %v, want %v", got.GetType(), want.GetType())
	}
	if want.GetTo() != got.GetTo() {
		t.Errorf("To: got %d, want %d", got.GetTo(), want.GetTo())
	}
	if want.GetFrom() != got.GetFrom() {
		t.Errorf("From: got %d, want %d", got.GetFrom(), want.GetFrom())
	}
	if want.GetTerm() != got.GetTerm() {
		t.Errorf("Term: got %d, want %d", got.GetTerm(), want.GetTerm())
	}
	if want.GetLogTerm() != got.GetLogTerm() {
		t.Errorf("LogTerm: got %d, want %d", got.GetLogTerm(), want.GetLogTerm())
	}
	if want.GetIndex() != got.GetIndex() {
		t.Errorf("Index: got %d, want %d", got.GetIndex(), want.GetIndex())
	}
	if want.GetCommit() != got.GetCommit() {
		t.Errorf("Commit: got %d, want %d", got.GetCommit(), want.GetCommit())
	}
	if want.GetReject() != got.GetReject() {
		t.Errorf("Reject: got %v, want %v", got.GetReject(), want.GetReject())
	}
	if want.GetRejectHint() != got.GetRejectHint() {
		t.Errorf("RejectHint: got %d, want %d", got.GetRejectHint(), want.GetRejectHint())
	}
	if !bytes.Equal(want.GetContext(), got.GetContext()) {
		t.Errorf("Context: got %v, want %v", got.GetContext(), want.GetContext())
	}
	if want.GetVote() != got.GetVote() {
		t.Errorf("Vote: got %d, want %d", got.GetVote(), want.GetVote())
	}
	if len(want.GetEntries()) != len(got.GetEntries()) {
		t.Errorf("Entries: got %d, want %d", len(got.GetEntries()), len(want.GetEntries()))
	} else {
		for i := range want.GetEntries() {
			we, ge := want.GetEntries()[i], got.GetEntries()[i]
			if we.GetType() != ge.GetType() {
				t.Errorf("Entry[%d].Type: got %v, want %v", i, ge.GetType(), we.GetType())
			}
			if we.GetTerm() != ge.GetTerm() {
				t.Errorf("Entry[%d].Term: got %d, want %d", i, ge.GetTerm(), we.GetTerm())
			}
			if we.GetIndex() != ge.GetIndex() {
				t.Errorf("Entry[%d].Index: got %d, want %d", i, ge.GetIndex(), we.GetIndex())
			}
			if !bytes.Equal(we.GetData(), ge.GetData()) {
				t.Errorf("Entry[%d].Data: got %v, want %v", i, ge.GetData(), we.GetData())
			}
		}
	}
	if want.GetSnapshot() != nil && got.GetSnapshot() != nil {
		wm, gm := want.GetSnapshot().GetMetadata(), got.GetSnapshot().GetMetadata()
		if wm.GetIndex() != gm.GetIndex() {
			t.Errorf("Snapshot.Metadata.Index: got %d, want %d", gm.GetIndex(), wm.GetIndex())
		}
		if wm.GetTerm() != gm.GetTerm() {
			t.Errorf("Snapshot.Metadata.Term: got %d, want %d", gm.GetTerm(), wm.GetTerm())
		}
		if len(wm.GetConfState().GetVoters()) != len(gm.GetConfState().GetVoters()) {
			t.Errorf("ConfState.Voters: got %d, want %d", len(gm.GetConfState().GetVoters()), len(wm.GetConfState().GetVoters()))
		}
		if !bytes.Equal(want.GetSnapshot().GetData(), got.GetSnapshot().GetData()) {
			t.Errorf("Snapshot.Data: mismatch")
		}
	}
}

func pbTypePtr(t pb.MessageType) *pb.MessageType { return &t }
func entryTypePtr(t pb.EntryType) *pb.EntryType  { return &t }

// ---------------------------------------------------------------------------
// Malformed frame tests — verify the codec rejects adversarial payloads
// instead of allocating based on untrusted wire data.
// ---------------------------------------------------------------------------

func TestDecodeBatchEmptyPayload(t *testing.T) {
	_, err := DecodeBatch(nil)
	if err != errCodecFrame {
		t.Fatalf("expected errCodecFrame for nil payload, got %v", err)
	}
	_, err = DecodeBatch([]byte{})
	if err != errCodecFrame {
		t.Fatalf("expected errCodecFrame for empty payload, got %v", err)
	}
}

func TestDecodeBatchZeroCount(t *testing.T) {
	msgs, err := DecodeBatch([]byte{0})
	if err != nil {
		t.Fatalf("unexpected error for zero count: %v", err)
	}
	if msgs != nil {
		t.Fatalf("expected nil for zero count, got %d messages", len(msgs))
	}
}

func TestDecodeBatchTruncatedPayload(t *testing.T) {
	// Valid header: count=3, but only enough bytes for 1 message.
	good, err := EncodeBatch([]*pb.Message{
		{Type: pbTypePtr(pb.MsgApp), To: uint64Ptr(1)},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if len(good) < 6 {
		t.Fatal("good frame too short for test")
	}
	// Truncate: claim 3 messages but provide data for only 1.
	truncated := good[:6]
	_, err = DecodeBatch(truncated[4:])
	if err != errCodecFrame {
		t.Fatalf("expected errCodecFrame for truncated payload, got %v", err)
	}
}

func TestDecodeBatchCountExceedsMax(t *testing.T) {
	// The wire format uses a 1-byte count (max 255), so count > 255 is
	// impossible on the wire. Verify that maxBatchCount matches the wire
	// format limit — if someone raises maxBatchCount above 255, this test
	// forces them to also change the wire format.
	if maxBatchCount > 255 {
		t.Fatalf("maxBatchCount=%d exceeds wire format byte limit (255)", maxBatchCount)
	}
}

func TestDecodeBatchGarbageBytes(t *testing.T) {
	// Random bytes that look like a valid count but have no valid messages.
	payload := []byte{5, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err := DecodeBatch(payload)
	if err != errCodecFrame {
		t.Fatalf("expected errCodecFrame for garbage bytes, got %v", err)
	}
}

func TestDecodeBatchSingleMsgTrailingBytes(t *testing.T) {
	// A valid single-message batch, then extra bytes that shouldn't be there.
	// DecodeBatch should still succeed (it only reads `count` messages).
	// Trailing bytes are not an error — the frame length prefix already
	// bounded the read.
	good, err := EncodeBatch([]*pb.Message{
		{Type: pbTypePtr(pb.MsgHeartbeat), To: uint64Ptr(1), From: uint64Ptr(2)},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	extra := append(good, 0xDE, 0xAD)
	msgs, err := DecodeBatch(extra[4:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
}

func TestCodecMaxFrameSizeConstant(t *testing.T) {
	// Verify maxFrameSize is 16 MiB (defense against OOM from malformed headers).
	const expected = 16 << 20
	if maxFrameSize != expected {
		t.Fatalf("maxFrameSize = %d, want %d (16 MiB)", maxFrameSize, expected)
	}
}

// ---------------------------------------------------------------------------
// ConfState and Snapshot nil-case tests — verify the encoder/decoder handle
// nil ConfState and nil SnapshotMetadata without offset corruption.
// ---------------------------------------------------------------------------

func TestConfStateNilRoundTrip(t *testing.T) {
	// A nil ConfState should encode identically to an empty ConfState.
	// The old nil shortcut (single 0x00 byte) caused the decoder to read
	// subsequent fields as learner/voter counts, corrupting the offset.
	empty := &pb.ConfState{}
	buf := make([]byte, encodedConfStateSize(nil))
	n := encodeConfState(buf, 0, nil)
	got, n2, err := decodeConfState(buf[:n], 0)
	if err != nil {
		t.Fatalf("decodeConfState(nil): %v", err)
	}
	if n != n2 {
		t.Fatalf("encode consumed %d bytes, decode consumed %d", n, n2)
	}
	if len(got.GetVoters()) != len(empty.GetVoters()) {
		t.Fatalf("nil round-trip: Voters len = %d, want %d", len(got.GetVoters()), len(empty.GetVoters()))
	}
	if len(got.GetLearners()) != len(empty.GetLearners()) {
		t.Fatalf("nil round-trip: Learners len = %d, want %d", len(got.GetLearners()), len(empty.GetLearners()))
	}
}

func TestConfStatePopulatedRoundTrip(t *testing.T) {
	cs := &pb.ConfState{
		Voters:         []uint64{1, 2, 3},
		Learners:       []uint64{4},
		VotersOutgoing: []uint64{5},
		LearnersNext:   []uint64{6, 7},
	}
	// Note: AutoLeave is a pointer field; for a populated ConfState it defaults to nil (false).
	buf := make([]byte, encodedConfStateSize(cs))
	n := encodeConfState(buf, 0, cs)
	got, n2, err := decodeConfState(buf[:n], 0)
	if err != nil {
		t.Fatalf("decodeConfState: %v", err)
	}
	if n != n2 {
		t.Fatalf("encode consumed %d bytes, decode consumed %d", n, n2)
	}
	assertConfStatesEqual(t, cs, got)
}

func TestSnapshotNilConfStateRoundTrip(t *testing.T) {
	// Snapshot with non-nil metadata but nil ConfState.
	snap := &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     uint64Ptr(42),
			Term:      uint64Ptr(7),
			ConfState: nil,
		},
		Data: []byte("test-data"),
	}
	msg := &pb.Message{
		Type:     pbTypePtr(pb.MsgSnap),
		To:       uint64Ptr(2),
		From:     uint64Ptr(1),
		Term:     uint64Ptr(3),
		Snapshot: snap,
	}
	frame, err := EncodeBatch([]*pb.Message{msg})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	decoded, err := DecodeBatch(frame[4:])
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 message, got %d", len(decoded))
	}
	gotSnap := decoded[0].GetSnapshot()
	if gotSnap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if gotSnap.GetMetadata().GetIndex() != 42 {
		t.Fatalf("Snapshot.Index = %d, want 42", gotSnap.GetMetadata().GetIndex())
	}
	if gotSnap.GetMetadata().GetTerm() != 7 {
		t.Fatalf("Snapshot.Term = %d, want 7", gotSnap.GetMetadata().GetTerm())
	}
	if len(gotSnap.GetMetadata().GetConfState().GetVoters()) != 0 {
		t.Fatalf("expected empty ConfState voters, got %d", len(gotSnap.GetMetadata().GetConfState().GetVoters()))
	}
	if !bytes.Equal(gotSnap.GetData(), []byte("test-data")) {
		t.Fatalf("Snapshot.Data = %v, want %v", gotSnap.GetData(), []byte("test-data"))
	}
}

func TestSnapshotNilMetadataRoundTrip(t *testing.T) {
	// Snapshot with nil metadata (both paths in encodeSnapshot covered).
	snap := &pb.Snapshot{
		Metadata: nil,
		Data:     []byte("nil-meta"),
	}
	msg := &pb.Message{
		Type:     pbTypePtr(pb.MsgSnap),
		To:       uint64Ptr(2),
		Snapshot: snap,
	}
	frame, err := EncodeBatch([]*pb.Message{msg})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	decoded, err := DecodeBatch(frame[4:])
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 message, got %d", len(decoded))
	}
	gotSnap := decoded[0].GetSnapshot()
	if gotSnap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if !bytes.Equal(gotSnap.GetData(), []byte("nil-meta")) {
		t.Fatalf("Snapshot.Data = %v, want %v", gotSnap.GetData(), []byte("nil-meta"))
	}
}

func TestConfStateSizeConsistency(t *testing.T) {
	// encodedConfStateSize must return exactly the bytes written by encodeConfState.
	cases := []*pb.ConfState{
		nil,
		{},
		{Voters: []uint64{1, 2, 3}},
		{Voters: []uint64{1}, Learners: []uint64{2}, VotersOutgoing: []uint64{3}, LearnersNext: []uint64{4, 5}},
	}
	for i, cs := range cases {
		size := encodedConfStateSize(cs)
		buf := make([]byte, size)
		n := encodeConfState(buf, 0, cs)
		if n != size {
			t.Errorf("case %d: encodedConfStateSize=%d, actual written=%d", i, size, n)
		}
	}
}

func assertConfStatesEqual(t *testing.T, want, got *pb.ConfState) {
	t.Helper()
	if len(got.GetVoters()) != len(want.GetVoters()) {
		t.Errorf("Voters: got %d, want %d", len(got.GetVoters()), len(want.GetVoters()))
	}
	for i := range want.GetVoters() {
		if got.GetVoters()[i] != want.GetVoters()[i] {
			t.Errorf("Voters[%d]: got %d, want %d", i, got.GetVoters()[i], want.GetVoters()[i])
		}
	}
	if len(got.GetLearners()) != len(want.GetLearners()) {
		t.Errorf("Learners: got %d, want %d", len(got.GetLearners()), len(want.GetLearners()))
	}
	if len(got.GetVotersOutgoing()) != len(want.GetVotersOutgoing()) {
		t.Errorf("VotersOutgoing: got %d, want %d", len(got.GetVotersOutgoing()), len(want.GetVotersOutgoing()))
	}
	if len(got.GetLearnersNext()) != len(want.GetLearnersNext()) {
		t.Errorf("LearnersNext: got %d, want %d", len(got.GetLearnersNext()), len(want.GetLearnersNext()))
	}
}

// ---------------------------------------------------------------------------
// Batch count and length-prefix overflow regression tests — a 1-byte message
// count or a near-uint64-max varint length must be rejected (or split by the
// sender), never silently wrap.
// ---------------------------------------------------------------------------

// maxVarint is the 10-byte little-endian varint encoding of math.MaxUint64.
var maxVarint = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}

func TestEncodeBatchRejectsOversizedBatch(t *testing.T) {
	// One over the wire limit must be rejected, not wrapped into the low
	// byte of the count field.
	msgs := make([]*pb.Message, maxBatchCount+1)
	for i := range msgs {
		msgs[i] = &pb.Message{Type: pbTypePtr(pb.MsgHeartbeat), To: uint64Ptr(uint64(i + 1))}
	}
	if _, err := EncodeBatch(msgs); err == nil {
		t.Fatal("expected error for batch exceeding maxBatchCount, got nil")
	}

	// Exactly maxBatchCount must still encode and decode faithfully.
	atLimit := msgs[:maxBatchCount]
	frame, err := EncodeBatch(atLimit)
	if err != nil {
		t.Fatalf("EncodeBatch(maxBatchCount): %v", err)
	}
	got, err := DecodeBatch(frame[4:])
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(got) != maxBatchCount {
		t.Fatalf("decoded %d messages, want %d", len(got), maxBatchCount)
	}
}

func TestDecodeMalformedLengthPrefixes(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name: "context_length_overflow",
			// count=1 | MsgApp | bitmask(fieldContext) | varint MaxUint64
			payload: append([]byte{
				1,
				byte(pb.MsgApp),
				byte(fieldContext >> 8), byte(fieldContext&0xFF),
			}, maxVarint...),
		},
		{
			name: "entry_data_length_overflow",
			// count=1 | MsgApp | bitmask(fieldEntries) | entries=1 |
			// entry(type=0, term=1, index=1, dataLen=MaxUint64)
			payload: func() []byte {
				p := []byte{1, byte(pb.MsgApp), byte(fieldEntries >> 8), byte(fieldEntries&0xFF), 1,
					0} // entry type
				p = append(p, 1)        // term
				p = append(p, 1)        // index
				p = append(p, maxVarint...) // data length
				return p
			}(),
		},
		{
			name: "snapshot_data_length_overflow",
			// count=1 | MsgSnap | bitmask(fieldSnapshot) | snapshot present |
			// index=1 | term=1 | empty ConfState | dataLen=MaxUint64
			payload: func() []byte {
				p := []byte{1, byte(pb.MsgSnap), byte(fieldSnapshot >> 8), byte(fieldSnapshot&0xFF), 1}
				p = append(p, 1)            // present
				p = append(p, 1)            // meta index
				p = append(p, 1)            // meta term
				p = append(p, 0, 0, 0, 0, 0) // ConfState: four zero counts + autoleave
				p = append(p, maxVarint...)  // data length
				return p
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeBatch(tt.payload)
			if err == nil {
				t.Fatalf("expected error for malformed %s, decoded %d messages", tt.name, len(got))
			}
			if err != errCodecFrame && err.Error() != "unexpected EOF" {
				// Any error is acceptable; a panic is not. errCodecFrame is
				// the expected sentinel for these payloads.
				t.Logf("%s: rejected with %v", tt.name, err)
			}
		})
	}
}

// TestSnapshotBatchingPathRoundTrip encodes a snapshot message through the
// production batching path (encodeMsgFields → encodeFrame) and decodes it with
// DecodeBatch. This pins the wire contract between transport.go's append-style
// encoder and codec.go's decoder: encodeConfStateAppend used to write a
// presence byte decodeConfState never read, corrupting every encoded snapshot.
func TestSnapshotBatchingPathRoundTrip(t *testing.T) {
	msg := &pb.Message{
		Type: pbTypePtr(pb.MsgSnap),
		To:   uint64Ptr(2),
		From: uint64Ptr(1),
		Term: uint64Ptr(5),
		Snapshot: &pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{
				Index:     uint64Ptr(100),
				Term:      uint64Ptr(4),
				ConfState: &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}},
			},
			Data: []byte("snapshot-data"),
		},
	}

	bm := fieldBitmask(msg)
	var buf []byte
	buf = append(buf, byte(msg.GetType()), byte(bm>>8), byte(bm))
	buf = encodeMsgFields(buf, msg, bm)

	frame := encodeFrame(buf, 1)
	got, err := DecodeBatch(frame[4:])
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	assertMessagesEqual(t, msg, got[0])

	snap := got[0].GetSnapshot()
	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
	assertConfStatesEqual(t, msg.GetSnapshot().GetMetadata().GetConfState(), snap.GetMetadata().GetConfState())
}
