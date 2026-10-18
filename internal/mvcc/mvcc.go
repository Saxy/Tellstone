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
*/
package mvcc

import (
	"encoding/binary"
	"sort"
)

// magic prefixes a versioned value. The NUL first byte keeps it out of the space
// of a plain text value, so a raw value is never mistaken for an envelope.
var magic = []byte{0x00, 'T', 'M', 'V'}

// frame is one committed version of a key.
type frame struct {
	ts    uint64
	tomb  bool
	value []byte
}

// decode parses a stored value into its frame list. A value without the magic is
// a legacy raw value, returned as one frame at timestamp 0; the bool reports
// whether the value was versioned. A value with the magic that does not parse is
// treated as raw rather than dropped, so a corrupt envelope is still readable.
func decode(raw []byte) ([]frame, bool) {
	if len(raw) < len(magic) || string(raw[:len(magic)]) != string(magic) {
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
		if len(body) < 13 {
			return []frame{{ts: 0, value: raw}}, false
		}
		f := frame{
			ts:   binary.BigEndian.Uint64(body[:8]),
			tomb: body[8]&1 != 0,
		}
		l := binary.BigEndian.Uint32(body[9:13])
		body = body[13:]
		if uint64(len(body)) < uint64(l) {
			return []frame{{ts: 0, value: raw}}, false
		}
		if !f.tomb {
			f.value = append([]byte(nil), body[:l]...)
		}
		body = body[l:]
		frames = append(frames, f)
	}
	return frames, true
}

// encode renders a frame list, sorted ascending by timestamp.
func encode(frames []frame) []byte {
	sort.SliceStable(frames, func(i, j int) bool { return frames[i].ts < frames[j].ts })
	buf := make([]byte, 0, len(magic)+4+len(frames)*17)
	buf = append(buf, magic...)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(frames)))
	buf = append(buf, n[:]...)
	for _, f := range frames {
		var hdr [13]byte
		binary.BigEndian.PutUint64(hdr[:8], f.ts)
		if f.tomb {
			hdr[8] = 1
		}
		binary.BigEndian.PutUint32(hdr[9:13], uint32(len(f.value)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, f.value...)
	}
	return buf
}

// Versioned reports whether a stored value carries the version envelope. It is
// how a caller with no need to resolve a value can tell a version list from a
// raw value.
func Versioned(raw []byte) bool {
	_, v := decode(raw)
	return v
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
func Append(raw []byte, ts uint64, value []byte, tomb bool) []byte {
	frames, versioned := decode(raw)
	if !versioned && len(raw) > 0 {
		frames = []frame{{ts: 0, value: raw}}
	}
	frames = append(frames, frame{ts: ts, tomb: tomb, value: value})
	return encode(frames)
}
