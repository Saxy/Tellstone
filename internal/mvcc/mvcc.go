/*
Package mvcc
Tellstone Multi-Version Concurrency Control
File: mvcc.go
Description: The on-disk encoding of a versioned value.

A versioned value is an append-only list of (commit timestamp, value or
tombstone) frames held inside a single store value. A reader resolves the list
at a snapshot timestamp and takes the newest frame at or before it, so reads are
stable against writes that commit later. New frames are appended, never
overwritten, so an older snapshot can still resolve the value it saw.

The envelope is deliberately self-describing and lives at the value layer, not
the key layer, so the key layout is unchanged and every range operation keeps
working: a scan still sees one key per live cell, it just resolves the key's
bytes instead of reading them as the value.

A value without the magic prefix is a value written before versioning existed. It
is read as a single frame at timestamp 0, visible to every snapshot, so old data
stays readable and a key is upgraded into the versioned form only when it is next
written.

The hot paths (Append and Resolve) are deliberately allocation-lean. Append is
append-only: it never decodes and re-encodes the whole list, because a node's
commit timestamps are monotonic, so a newly appended frame is already the newest
and the list stays ordered. Resolve and Latest scan linearly for the frame they
want, so they never need the list sorted either. decode returns sub-slices into
its input rather than copying every frame value; the caller's store value outlives
the resolve, and the store hands back a fresh buffer on every Get, so the
sub-slices stay valid.
*/
package mvcc

import (
	"encoding/binary"
)

// magic prefixes a versioned value. The NUL first byte keeps it out of the space
// of a plain text value, so a raw value is never mistaken for an envelope.
var magic = []byte{0x00, 'T', 'M', 'V'}

// frameHeaderLen is the fixed per-frame header: 8-byte timestamp, 1 byte of
// flags (tombstone bit), and a 4-byte value length.
const frameHeaderLen = 13

// frame is one committed version of a key.
type frame struct {
	ts    uint64
	tomb  bool
	value []byte
}

// isVersioned reports whether raw carries the version envelope, without walking
// the frame list.
func isVersioned(raw []byte) bool {
	return len(raw) >= len(magic) && string(raw[:len(magic)]) == string(magic)
}

// decode parses a stored value into its frame list. A value without the magic is
// a legacy raw value, returned as one frame at timestamp 0; the bool reports
// whether the value was versioned. A value with the magic that does not parse is
// treated as raw rather than dropped, so a corrupt envelope is still readable.
//
// The frame values are sub-slices of raw, not copies: a call that only needs the
// timestamps (Latest) pays nothing for the values, and one that needs the value
// (Resolve) reads it straight out of the store's buffer, which the store handed
// over as a fresh allocation.
func decode(raw []byte) ([]frame, bool) {
	if !isVersioned(raw) {
		if len(raw) == 0 {
			return nil, false
		}
		return []frame{{ts: 0, value: raw}}, false
	}
	body := raw[len(magic):]
	if len(body) < 4 {
		return []frame{{ts: 0, value: raw}}, false
	}
	n := binary.BigEndian.Uint32(body[:4])
	body = body[4:]
	frames := make([]frame, 0, n)
	for i := uint32(0); i < n; i++ {
		if len(body) < frameHeaderLen {
			return []frame{{ts: 0, value: raw}}, false
		}
		f := frame{
			ts:   binary.BigEndian.Uint64(body[:8]),
			tomb: body[8]&1 != 0,
		}
		l := binary.BigEndian.Uint32(body[9:13])
		body = body[frameHeaderLen:]
		if uint64(len(body)) < uint64(l) {
			return []frame{{ts: 0, value: raw}}, false
		}
		if !f.tomb {
			// Sub-slice, not a copy: the value aliases raw, which the store
			// owns and keeps alive for the caller.
			f.value = body[:l:l]
		}
		body = body[l:]
		frames = append(frames, f)
	}
	return frames, true
}

// encode renders a frame list. Frames are written in the order given, which the
// append path keeps ascending by timestamp; Resolve and Latest scan, so nothing
// depends on the order being sorted and the sort step is gone from the hot path.
func encode(frames []frame) []byte {
	buf := make([]byte, 0, len(magic)+4+len(frames)*frameHeaderLen)
	buf = append(buf, magic...)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(frames)))
	buf = append(buf, n[:]...)
	for _, f := range frames {
		buf = appendFrame(buf, f.ts, f.tomb, f.value)
	}
	return buf
}

// appendFrame writes one frame header and value to buf and returns the extended
// buffer. It is shared by encode and Append so the two can never disagree on the
// on-disk layout.
func appendFrame(buf []byte, ts uint64, tomb bool, value []byte) []byte {
	var hdr [frameHeaderLen]byte
	binary.BigEndian.PutUint64(hdr[:8], ts)
	if tomb {
		hdr[8] = 1
	}
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(value)))
	buf = append(buf, hdr[:]...)
	buf = append(buf, value...)
	return buf
}

// Versioned reports whether a stored value carries the version envelope. It is
// how a caller with no need to resolve a value can tell a version list from a
// raw value.
func Versioned(raw []byte) bool {
	return isVersioned(raw)
}

// Resolve returns the value a key had at snapshot, and whether the key was
// present then. A tombstone at or before the snapshot reads as absent. A legacy
// value is visible at every snapshot.
func Resolve(raw []byte, snapshot uint64) ([]byte, bool) {
	frames, versioned := decode(raw)
	if !versioned {
		if len(frames) == 0 {
			return nil, false
		}
		return frames[0].value, true
	}
	var best *frame
	for i := range frames {
		if frames[i].ts <= snapshot && (best == nil || frames[i].ts >= best.ts) {
			best = &frames[i]
		}
	}
	if best == nil || best.tomb {
		return nil, false
	}
	return best.value, true
}

// Latest returns the newest frame timestamp of a stored value, or 0 when the
// value is legacy or absent. A writer that sees a newer committed frame than its
// own start timestamp has lost the race.
func Latest(raw []byte) uint64 {
	frames, versioned := decode(raw)
	if !versioned {
		return 0
	}
	var max uint64
	for _, f := range frames {
		if f.ts > max {
			max = f.ts
		}
	}
	return max
}

// Append adds one frame to a stored value under a commit timestamp. A legacy
// value is folded in as its timestamp-0 frame first, so upgrading a key to
// versioned storage does not lose the value already there.
//
// This is the write hot path and it is append-only: it never decodes and
// re-encodes the existing frames. The new frame is appended in place behind the
// existing ones, which is correct because a node's commit timestamps are
// monotonic and Resolve/Latest scan rather than assume order.
//
// horizon, when non-zero, is the oldest open transaction's start timestamp:
// frames strictly older than it can no longer be read by any live snapshot, so
// Append drops them to keep the version list bounded instead of letting it grow
// without end. A horizon of zero means nothing can be dropped.
func Append(raw []byte, ts uint64, value []byte, tomb bool, horizon uint64) []byte {
	raw = appendFrameTo(raw, ts, value, tomb)
	if horizon > 0 {
		raw = compact(raw, horizon)
	}
	return raw
}

// appendFrameTo appends one frame to an already-versioned value's bytes, bumping
// the frame count. raw must carry the version envelope.
func appendFrameTo(raw []byte, ts uint64, value []byte, tomb bool) []byte {
	if !isVersioned(raw) {
		// Legacy value: build a versioned envelope with the legacy value as the
		// timestamp-0 frame (when non-empty) and the new frame appended.
		frameCount := uint32(1)
		if len(raw) > 0 {
			frameCount = 2
		}
		buf := make([]byte, 0, len(magic)+4+(len(raw)+frameHeaderLen)+frameHeaderLen+len(value))
		buf = append(buf, magic...)
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], frameCount)
		buf = append(buf, n[:]...)
		if len(raw) > 0 {
			buf = appendFrame(buf, 0, false, raw)
		}
		return appendFrame(buf, ts, tomb, value)
	}
	// Versioned: bump the count and append the new frame behind the existing
	// bytes, keeping the already-encoded frames untouched.
	n := binary.BigEndian.Uint32(raw[len(magic) : len(magic)+4])
	buf := make([]byte, 0, len(raw)+frameHeaderLen+len(value))
	buf = append(buf, raw[:len(magic)]...)
	var nb [4]byte
	binary.BigEndian.PutUint32(nb[:], n+1)
	buf = append(buf, nb[:]...)
	buf = append(buf, raw[len(magic)+4:]...)
	return appendFrame(buf, ts, tomb, value)
}

// compact drops every frame strictly older than horizon, keeping the newest
// frame at or before it as the floor a snapshot exactly at horizon still
// resolves. The value must be versioned. The scan reads only the fixed-size
// frame headers (no value copy), so it is cheap even on a long list. Frames are
// assumed ascending by timestamp (append-only with a monotonic oracle), so the
// floor is simply the last frame whose timestamp is at or before the horizon.
func compact(raw []byte, horizon uint64) []byte {
	if !isVersioned(raw) {
		return raw
	}
	body := raw[len(magic):]
	if len(body) < 4 {
		return raw
	}
	n := int(binary.BigEndian.Uint32(body[:4]))
	body = body[4:]
	// Walk the frames, remembering the offset of the floor (the newest frame
	// with ts <= horizon) and counting the frames kept after it.
	floorOff := -1
	keptAfter := 0
	off := len(magic) + 4
	sawPast := false
	for i := 0; i < n; i++ {
		if len(body) < frameHeaderLen {
			return raw
		}
		ts := binary.BigEndian.Uint64(body[:8])
		l := int(binary.BigEndian.Uint32(body[9:13]))
		body = body[frameHeaderLen:]
		if uint64(len(body)) < uint64(l) {
			return raw
		}
		switch {
		case ts <= horizon:
			floorOff = off
		case !sawPast:
			// First frame past the horizon; everything from here is kept.
			keptAfter = n - i
			sawPast = true
		}
		off += frameHeaderLen + l
		body = body[l:]
	}
	if floorOff < 0 {
		// No frame at or before the horizon: nothing can be dropped.
		return raw
	}
	// Keep the floor frame plus every frame after it. The floor's offset points
	// at its header; everything from there to the end of raw is the kept suffix.
	buf := make([]byte, 0, len(magic)+4+(len(raw)-floorOff))
	buf = append(buf, magic...)
	var nb [4]byte
	binary.BigEndian.PutUint32(nb[:], uint32(1+keptAfter))
	buf = append(buf, nb[:]...)
	buf = append(buf, raw[floorOff:]...)
	return buf
}
