/*
Package network
Tellstone Cloud-Native In-Memory Database
File: codec.go
Description: Zero-protobuf binary codec for Raft messages. Encodes and decodes
raftpb.Message to/from a compact wire format without touching the protobuf
runtime. This keeps the hot path free of protobuf overhead (reflection,
descriptor lookups, allocation through protoiface).

Wire format:

    Frame:  [4B big-endian payload_length][payload]
    Payload:[1B msg_count][msg_1]...[msg_N]

Each message:

    [1B raft_msg_type][2B present_fields_bitmask][fields in bit order]

Field bitmask bits (only present fields are encoded, in bit order):

    0  To          uint64 varint
    1  From        uint64 varint
    2  Term        uint64 varint
    3  LogTerm     uint64 varint
    4  Index       uint64 varint
    5  Commit      uint64 varint
    6  Reject      1-byte bool
    7  RejectHint  uint64 varint
    8  Context     varint length + bytes
    9  Entries     varint count + entries
    10 Snapshot    inline snapshot encoding
    11 Vote        uint64 varint
    12 Responses   varint count + messages (recursive)

Varint encoding (little-endian, 7 bits per byte, MSB = continuation):

    [0xxxxxxx]                     value < 128
    [1xxxxxxx][0xxxxxxx]           value < 16384
    [1xxxxxxx][1xxxxxxx]...[0xxxxxxx]  up to 10 bytes (uint64 max)

Authors:

	Maximilian Hagen
*/
package network

import (
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
)

var errCodecFrame = errors.New("cluster network: malformed codec frame")

// maxBatchCount is the maximum number of messages allowed in a single batch
// frame. The wire format uses a 1-byte count (max 255), but we enforce a
// lower practical limit. Even at 255 messages, each message's decode is
// bounds-checked per-byte, so the real constraint is allocation pressure from
// the msgs slice — 255 * 24 bytes (pointer) ≈6 KiB is negligible.
const maxBatchCount = 255

// Field bitmask positions for the compact message encoding.
const (
	fieldTo uint16 = 1 << iota
	fieldFrom
	fieldTerm
	fieldLogTerm
	fieldIndex
	fieldCommit
	fieldReject
	fieldRejectHint
	fieldContext
	fieldEntries
	fieldSnapshot
	fieldVote
	fieldResponses
)

// EncodeBatch writes a batch of Raft messages into a length-prefixed frame.
// It returns the complete frame bytes including the 4-byte header. Batches
// larger than maxBatchCount are rejected: the wire format stores the message
// count in one byte, so an oversized input would silently wrap and decode as
// a truncated (or empty) batch on the receiving side. The batching sender
// flushes before reaching this limit; EncodeBatch is the backstop.
func EncodeBatch(msgs []*pb.Message) ([]byte, error) {
	if len(msgs) > maxBatchCount {
		return nil, fmt.Errorf("cluster network: batch of %d messages exceeds maxBatchCount %d", len(msgs), maxBatchCount)
	}
	totalPayload := 1 // 1 byte for message count
	for _, m := range msgs {
		totalPayload += encodedMsgSize(m)
	}

	frame := make([]byte, 4+totalPayload)
	frame[0] = byte(totalPayload >> 24)
	frame[1] = byte(totalPayload >> 16)
	frame[2] = byte(totalPayload >> 8)
	frame[3] = byte(totalPayload)
	frame[4] = byte(len(msgs))

	offset := 5
	for _, m := range msgs {
		offset = encodeMsg(frame, offset, m)
	}
	return frame[:offset], nil
}

// DecodeBatch parses a batch payload (after the 4-byte length prefix has been
// consumed) and returns the decoded messages. It validates that the declared
// message count does not exceed maxBatchCount and that the decode offset never
// exceeds the payload length, preventing both allocation abuse and out-of-bounds
// reads on malformed data.
func DecodeBatch(payload []byte) ([]*pb.Message, error) {
	if len(payload) < 1 {
		return nil, errCodecFrame
	}
	count := int(payload[0])
	if count == 0 {
		return nil, nil
	}
	if count > maxBatchCount {
		return nil, errCodecFrame
	}
	msgs := make([]*pb.Message, count)
	offset := 1
	for i := 0; i < count; i++ {
		if offset >= len(payload) {
			return nil, errCodecFrame
		}
		m, n, err := decodeMsg(payload[offset:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			// decodeMsg returned zero bytes consumed — would loop forever.
			return nil, errCodecFrame
		}
		msgs[i] = m
		offset += n
	}
	return msgs, nil
}

// ---------------------------------------------------------------------------
// Message encoding
// ---------------------------------------------------------------------------

func encodedMsgSize(m *pb.Message) int {
	size := 3 // 1 type + 2 bitmask
	bm := fieldBitmask(m)
	if bm&fieldTo != 0 {
		size += varintSize(m.GetTo())
	}
	if bm&fieldFrom != 0 {
		size += varintSize(m.GetFrom())
	}
	if bm&fieldTerm != 0 {
		size += varintSize(m.GetTerm())
	}
	if bm&fieldLogTerm != 0 {
		size += varintSize(m.GetLogTerm())
	}
	if bm&fieldIndex != 0 {
		size += varintSize(m.GetIndex())
	}
	if bm&fieldCommit != 0 {
		size += varintSize(m.GetCommit())
	}
	if bm&fieldReject != 0 {
		size++
	}
	if bm&fieldRejectHint != 0 {
		size += varintSize(m.GetRejectHint())
	}
	if bm&fieldContext != 0 {
		c := m.GetContext()
		size += varintSize(uint64(len(c))) + len(c)
	}
	if bm&fieldEntries != 0 {
		entries := m.GetEntries()
		size += varintSize(uint64(len(entries)))
		for _, e := range entries {
			size += encodedEntrySize(e)
		}
	}
	if bm&fieldSnapshot != 0 {
		size += encodedSnapshotSize(m.GetSnapshot())
	}
	if bm&fieldVote != 0 {
		size += varintSize(m.GetVote())
	}
	if bm&fieldResponses != 0 {
		responses := m.GetResponses()
		size += varintSize(uint64(len(responses)))
		for _, r := range responses {
			size += encodedMsgSize(r)
		}
	}
	return size
}

func encodeMsg(buf []byte, off int, m *pb.Message) int {
	buf[off] = byte(m.GetType())
	off++

	bm := fieldBitmask(m)
	buf[off] = byte(bm >> 8)
	buf[off+1] = byte(bm)
	off += 2

	if bm&fieldTo != 0 {
		off = putVarint(buf, off, m.GetTo())
	}
	if bm&fieldFrom != 0 {
		off = putVarint(buf, off, m.GetFrom())
	}
	if bm&fieldTerm != 0 {
		off = putVarint(buf, off, m.GetTerm())
	}
	if bm&fieldLogTerm != 0 {
		off = putVarint(buf, off, m.GetLogTerm())
	}
	if bm&fieldIndex != 0 {
		off = putVarint(buf, off, m.GetIndex())
	}
	if bm&fieldCommit != 0 {
		off = putVarint(buf, off, m.GetCommit())
	}
	if bm&fieldReject != 0 {
		if m.GetReject() {
			buf[off] = 1
		}
		off++
	}
	if bm&fieldRejectHint != 0 {
		off = putVarint(buf, off, m.GetRejectHint())
	}
	if bm&fieldContext != 0 {
		c := m.GetContext()
		off = putVarint(buf, off, uint64(len(c)))
		off += copy(buf[off:], c)
	}
	if bm&fieldEntries != 0 {
		entries := m.GetEntries()
		off = putVarint(buf, off, uint64(len(entries)))
		for _, e := range entries {
			off = encodeEntry(buf, off, e)
		}
	}
	if bm&fieldSnapshot != 0 {
		off = encodeSnapshot(buf, off, m.GetSnapshot())
	}
	if bm&fieldVote != 0 {
		off = putVarint(buf, off, m.GetVote())
	}
	if bm&fieldResponses != 0 {
		responses := m.GetResponses()
		off = putVarint(buf, off, uint64(len(responses)))
		for _, r := range responses {
			off = encodeMsg(buf, off, r)
		}
	}
	return off
}

func fieldBitmask(m *pb.Message) uint16 {
	var bm uint16
	if m.GetTo() != 0 {
		bm |= fieldTo
	}
	if m.GetFrom() != 0 {
		bm |= fieldFrom
	}
	if m.GetTerm() != 0 {
		bm |= fieldTerm
	}
	if m.GetLogTerm() != 0 {
		bm |= fieldLogTerm
	}
	if m.GetIndex() != 0 {
		bm |= fieldIndex
	}
	if m.GetCommit() != 0 {
		bm |= fieldCommit
	}
	if m.GetReject() {
		bm |= fieldReject
	}
	if m.GetRejectHint() != 0 {
		bm |= fieldRejectHint
	}
	if len(m.GetContext()) > 0 {
		bm |= fieldContext
	}
	if len(m.GetEntries()) > 0 {
		bm |= fieldEntries
	}
	if m.GetSnapshot() != nil {
		bm |= fieldSnapshot
	}
	if m.GetVote() != 0 {
		bm |= fieldVote
	}
	if len(m.GetResponses()) > 0 {
		bm |= fieldResponses
	}
	return bm
}

// ---------------------------------------------------------------------------
// Message decoding
// ---------------------------------------------------------------------------

func decodeMsg(data []byte) (*pb.Message, int, error) {
	if len(data) < 3 {
		return nil, 0, errCodecFrame
	}
	m := &pb.Message{}
	typ := pb.MessageType(data[0])
	m.Type = &typ
	bm := uint16(data[1])<<8 | uint16(data[2])
	off := 3
	var err error

	if bm&fieldTo != 0 {
		m.To, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldFrom != 0 {
		m.From, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldTerm != 0 {
		m.Term, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldLogTerm != 0 {
		m.LogTerm, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldIndex != 0 {
		m.Index, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldCommit != 0 {
		m.Commit, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldReject != 0 {
		if off >= len(data) {
			return nil, 0, errCodecFrame
		}
		v := data[off] != 0
		m.Reject = &v
		off++
	}
	if bm&fieldRejectHint != 0 {
		m.RejectHint, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldContext != 0 {
		m.Context, off, err = getBytes(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldEntries != 0 {
		var count uint64
		count, off, err = getVarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		// Each entry is at least 3 bytes (type + term + index varints).
		// Reject if count exceeds what the remaining payload could hold.
		if count > uint64(len(data)-off) {
			return nil, 0, errCodecFrame
		}
		m.Entries = make([]*pb.Entry, count)
		for i := uint64(0); i < count; i++ {
			e, n, eerr := decodeEntry(data[off:])
			if eerr != nil {
				return nil, 0, eerr
			}
			m.Entries[i] = e
			off += n
		}
	}
	if bm&fieldSnapshot != 0 {
		snap, n, serr := decodeSnapshot(data[off:])
		if serr != nil {
			return nil, 0, serr
		}
		m.Snapshot = snap
		off += n
	}
	if bm&fieldVote != 0 {
		m.Vote, off, err = varintptr(data, off)
		if err != nil {
			return nil, 0, err
		}
	}
	if bm&fieldResponses != 0 {
		var count uint64
		count, off, err = getVarint(data, off)
		if err != nil {
			return nil, 0, err
		}
		if count > uint64(len(data)-off) {
			return nil, 0, errCodecFrame
		}
		m.Responses = make([]*pb.Message, count)
		for i := uint64(0); i < count; i++ {
			r, n, rerr := decodeMsg(data[off:])
			if rerr != nil {
				return nil, 0, rerr
			}
			m.Responses[i] = r
			off += n
		}
	}
	return m, off, nil
}

// ---------------------------------------------------------------------------
// Entry encoding
// ---------------------------------------------------------------------------

func encodedEntrySize(e *pb.Entry) int {
	size := 1 + varintSize(uint64(e.GetTerm())) + varintSize(uint64(e.GetIndex()))
	d := e.GetData()
	size += varintSize(uint64(len(d))) + len(d)
	return size
}

func encodeEntry(buf []byte, off int, e *pb.Entry) int {
	buf[off] = byte(e.GetType())
	off++
	off = putVarint(buf, off, uint64(e.GetTerm()))
	off = putVarint(buf, off, uint64(e.GetIndex()))
	d := e.GetData()
	off = putVarint(buf, off, uint64(len(d)))
	off += copy(buf[off:], d)
	return off
}

func decodeEntry(data []byte) (*pb.Entry, int, error) {
	if len(data) < 1 {
		return nil, 0, errCodecFrame
	}
	e := &pb.Entry{}
	typ := pb.EntryType(data[0])
	e.Type = &typ
	off := 1
	var err error
	e.Term, off, err = varintptr(data, off)
	if err != nil {
		return nil, 0, err
	}
	e.Index, off, err = varintptr(data, off)
	if err != nil {
		return nil, 0, err
	}
	e.Data, off, err = getBytes(data, off)
	if err != nil {
		return nil, 0, err
	}
	return e, off, nil
}

// ---------------------------------------------------------------------------
// Snapshot encoding
// ---------------------------------------------------------------------------

func encodedSnapshotSize(s *pb.Snapshot) int {
	if s == nil {
		return 1
	}
	size := 1 // presence byte
	meta := s.GetMetadata()
	if meta != nil {
		size += varintSize(meta.GetIndex())
		size += varintSize(meta.GetTerm())
		size += encodedConfStateSize(meta.GetConfState())
	} else {
		// Nil metadata: encoder writes index=0, term=0, empty ConfState.
		size += varintSize(0) + varintSize(0) + encodedConfStateSize(nil)
	}
	d := s.GetData()
	size += varintSize(uint64(len(d))) + len(d)
	return size
}

func encodeSnapshot(buf []byte, off int, s *pb.Snapshot) int {
	if s == nil {
		buf[off] = 0
		return off + 1
	}
	buf[off] = 1
	off++
	meta := s.GetMetadata()
	if meta != nil {
		off = putVarint(buf, off, meta.GetIndex())
		off = putVarint(buf, off, meta.GetTerm())
		off = encodeConfState(buf, off, meta.GetConfState())
	} else {
		off = putVarint(buf, off, 0)
		off = putVarint(buf, off, 0)
		off = encodeConfState(buf, off, nil)
	}
	d := s.GetData()
	off = putVarint(buf, off, uint64(len(d)))
	off += copy(buf[off:], d)
	return off
}

func decodeSnapshot(data []byte) (*pb.Snapshot, int, error) {
	if len(data) < 1 {
		return nil, 0, errCodecFrame
	}
	if data[0] == 0 {
		return nil, 1, nil
	}
	off := 1
	s := &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{},
	}
	var err error
	s.Metadata.Index, off, err = varintptr(data, off)
	if err != nil {
		return nil, 0, err
	}
	s.Metadata.Term, off, err = varintptr(data, off)
	if err != nil {
		return nil, 0, err
	}
	s.Metadata.ConfState, off, err = decodeConfState(data, off)
	if err != nil {
		return nil, 0, err
	}
	s.Data, off, err = getBytes(data, off)
	if err != nil {
		return nil, 0, err
	}
	return s, off, nil
}

// ---------------------------------------------------------------------------
// ConfState encoding
// ---------------------------------------------------------------------------

func encodedConfStateSize(cs *pb.ConfState) int {
	// Nil ConfState is encoded as an empty ConfState (0 voters, 0 learners,
	// etc.) to keep the wire format symmetric with the decoder, which always
	// reads the full field set.
	if cs == nil {
		cs = &pb.ConfState{}
	}
	size := varintSize(uint64(len(cs.GetVoters())))
	for _, v := range cs.GetVoters() {
		size += varintSize(v)
	}
	size += varintSize(uint64(len(cs.GetLearners())))
	for _, l := range cs.GetLearners() {
		size += varintSize(l)
	}
	size += varintSize(uint64(len(cs.GetVotersOutgoing())))
	for _, v := range cs.GetVotersOutgoing() {
		size += varintSize(v)
	}
	size += varintSize(uint64(len(cs.GetLearnersNext())))
	for _, l := range cs.GetLearnersNext() {
		size += varintSize(l)
	}
	size++ // AutoLeave bool
	return size
}

func encodeConfState(buf []byte, off int, cs *pb.ConfState) int {
	// Nil ConfState is encoded identically to an empty ConfState so that
	// decodeConfState (which always reads voters/learners/autoleave) stays
	// in sync. The old nil shortcut wrote a single 0x00 byte, causing the
	// decoder to read subsequent fields as learner/voter counts.
	if cs == nil {
		cs = &pb.ConfState{}
	}
	off = putVarint(buf, off, uint64(len(cs.GetVoters())))
	for _, v := range cs.GetVoters() {
		off = putVarint(buf, off, v)
	}
	off = putVarint(buf, off, uint64(len(cs.GetLearners())))
	for _, l := range cs.GetLearners() {
		off = putVarint(buf, off, l)
	}
	off = putVarint(buf, off, uint64(len(cs.GetVotersOutgoing())))
	for _, v := range cs.GetVotersOutgoing() {
		off = putVarint(buf, off, v)
	}
	off = putVarint(buf, off, uint64(len(cs.GetLearnersNext())))
	for _, l := range cs.GetLearnersNext() {
		off = putVarint(buf, off, l)
	}
	if cs.GetAutoLeave() {
		buf[off] = 1
	} else {
		buf[off] = 0
	}
	off++
	return off
}

func decodeConfState(data []byte, off int) (*pb.ConfState, int, error) {
	var err error
	cs := &pb.ConfState{}

	var count uint64
	count, off, err = getVarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	// Each voter ID is at least 1 byte (varint). If count exceeds the
	// remaining payload, the frame is corrupt — reject immediately to
	// prevent a huge allocation followed by an OOM panic.
	if count > uint64(len(data)-off) {
		return nil, 0, errCodecFrame
	}
	if count > 0 {
		cs.Voters = make([]uint64, count)
		for i := uint64(0); i < count; i++ {
			cs.Voters[i], off, err = getVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
		}
	}

	count, off, err = getVarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	if count > uint64(len(data)-off) {
		return nil, 0, errCodecFrame
	}
	if count > 0 {
		cs.Learners = make([]uint64, count)
		for i := uint64(0); i < count; i++ {
			cs.Learners[i], off, err = getVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
		}
	}

	count, off, err = getVarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	if count > uint64(len(data)-off) {
		return nil, 0, errCodecFrame
	}
	if count > 0 {
		cs.VotersOutgoing = make([]uint64, count)
		for i := uint64(0); i < count; i++ {
			cs.VotersOutgoing[i], off, err = getVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
		}
	}

	count, off, err = getVarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	if count > uint64(len(data)-off) {
		return nil, 0, errCodecFrame
	}
	if count > 0 {
		cs.LearnersNext = make([]uint64, count)
		for i := uint64(0); i < count; i++ {
			cs.LearnersNext[i], off, err = getVarint(data, off)
			if err != nil {
				return nil, 0, err
			}
		}
	}

	if off >= len(data) {
		return nil, 0, errCodecFrame
	}
	v := data[off] != 0
	cs.AutoLeave = &v
	off++

	return cs, off, nil
}

// ---------------------------------------------------------------------------
// Varint encoding (protobuf-style: 7 bits per byte, MSB = continuation)
// ---------------------------------------------------------------------------

func putVarint(buf []byte, off int, v uint64) int {
	for v >= 0x80 {
		buf[off] = byte(v) | 0x80
		v >>= 7
		off++
	}
	buf[off] = byte(v)
	return off + 1
}

func getVarint(data []byte, off int) (uint64, int, error) {
	var v uint64
	var shift uint
	for {
		if off >= len(data) {
			return 0, 0, errCodecFrame
		}
		b := data[off]
		v |= uint64(b&0x7f) << shift
		off++
		if b < 0x80 {
			break
		}
		shift += 7
		if shift > 63 {
			return 0, 0, errCodecFrame
		}
	}
	return v, off, nil
}

// varintptr is a convenience that calls getVarint and returns a pointer to the
// result, suitable for assigning directly to raftpb pointer fields.
func varintptr(data []byte, off int) (*uint64, int, error) {
	v, n, err := getVarint(data, off)
	if err != nil {
		return nil, n, err
	}
	return &v, n, nil
}

func getBytes(data []byte, off int) ([]byte, int, error) {
	length, newOff, err := getVarint(data, off)
	if err != nil {
		return nil, 0, err
	}
	// Compare in uint64 space before converting to int: a malformed
	// near-uint64-max length would wrap to a negative int and slip past a
	// bounds check performed after the conversion.
	if length > uint64(len(data)-newOff) {
		return nil, 0, errCodecFrame
	}
	l := int(length)
	result := make([]byte, l)
	copy(result, data[newOff:newOff+l])
	return result, newOff + l, nil
}

func varintSize(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
