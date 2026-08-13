/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: replay.go
Description: Startup reader for the persisted audit log. Walks the audit files a
previous process left in the configured directory and recovers the recent
auth-failure and acl-deny records, so the ACL LOG buffer — which lives in memory
and would otherwise start empty — carries its history across a restart. Reading
happens once during startup, before any listener accepts connections; nothing
here runs while the server is serving.

Authors:

	Mohamad Radi
*/
package audit

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

// auditFileGlob matches the names fileName generates. Audit files carry no
// index or manifest, so discovery is a directory glob. Built from the writer's
// own suffix: a reader that spelled the name out again would stop finding
// anything the moment the writer's changed, and would do it silently, since a
// glob that matches nothing is indistinguishable from a directory with no
// history in it.
const auditFileGlob = "*" + auditFileSuffix

// ReplayEntry is one recovered security event, shaped for the ACL LOG buffer.
// The audit package does not import rbac: the caller translates these into
// rbac.AuthLogEntry values, keeping the two packages independent.
type ReplayEntry struct {
	Timestamp  time.Time
	Username   string
	RemoteAddr string
	Reason     string
}

// record is the subset of an audit JSON line replay cares about. Fields absent
// from a given event type decode as empty strings.
type record struct {
	Time       string `json:"time"`
	Event      string `json:"event"`
	User       string `json:"user"`
	RemoteAddr string `json:"remote_addr"`
	Reason     string `json:"reason"`
	Command    string `json:"command"`
	Key        string `json:"key"`
}

// ReplayAuthLog recovers up to maxEntries of the most recent auth-failure and
// acl-deny records this engine's directory already holds, returned oldest first
// — the order the ACL LOG buffer expects.
//
// Everything replay needs is the engine's own state, so a caller cannot pair a
// directory with the wrong key: the destination and the DEK that seals it are
// both taken from the file writer. An engine that is disabled or writing to
// stdout has no file writer and replays nothing, since neither has recoverable
// history.
//
// Records this run has written are included, which is why callers replay before
// serving traffic; at that point the current file is still empty and costs one
// read.
func (e *LogEngine) ReplayAuthLog(maxEntries int) []ReplayEntry {
	f, ok := e.writer.(*file)
	if !ok {
		return nil
	}
	return replayAuthLog(f.dir, f.ce, maxEntries, f.logger)
}

// replayAuthLog walks the audit files in dir. engine is the one that sealed
// them, or nil when they are plaintext.
//
// Every failure mode here is non-fatal and returns whatever was recovered so
// far: a missing directory, an unreadable file, a corrupt record. History is a
// convenience, never a reason to refuse to boot.
func replayAuthLog(dir string, engine *crypto.Engine, maxEntries int, logger log.Logger) []ReplayEntry {
	if maxEntries <= 0 {
		return nil
	}
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	// Glob reports no error for a directory that does not exist, so a first run
	// with no prior audit history needs no special case.
	paths, err := filepath.Glob(filepath.Join(dir, auditFileGlob))
	if err != nil {
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "audit: replay could not list audit files",
				log.String("dir", dir),
				log.String("error", err.Error()),
			)
		}
		return nil
	}
	// File names lead with a creation timestamp in nanoseconds, so lexical order
	// is creation order. Walking it backwards visits the newest file first and
	// stops as soon as maxEntries is met, leaving older files unopened.
	sort.Strings(paths)
	// Left nil rather than preallocated so "nothing recovered" is a nil slice,
	// matching what the ACL LOG buffer returns when it is empty.
	var out []ReplayEntry
	for i := len(paths) - 1; i >= 0 && len(out) < maxEntries; i-- {
		entries := readFile(paths[i], engine, logger)
		for j := len(entries) - 1; j >= 0 && len(out) < maxEntries; j-- {
			out = append(out, entries[j])
		}
	}
	// The walk collected newest first; the buffer wants oldest first.
	for l, r := 0, len(out)-1; l < r; l, r = l+1, r-1 {
		out[l], out[r] = out[r], out[l]
	}
	return out
}

// readFile decodes one audit file into its replayable entries, oldest first. An
// unreadable file yields nothing: one damaged file must not cost the history
// held in its siblings.
func readFile(path string, engine *crypto.Engine, logger log.Logger) []ReplayEntry {
	// Audit files are bounded by rotateAfterBytes, and this runs once at startup
	// off any serving path, so reading a file whole is simpler than streaming it
	// and costs one short-lived allocation per file visited.
	data, err := os.ReadFile(path)
	if err != nil {
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "audit: replay could not read audit file",
				log.String("filename", path),
				log.String("error", err.Error()),
			)
		}
		return nil
	}
	// The same condition newFile applies when deciding to seal records, so the
	// reader and the writer cannot disagree about the format.
	if engine != nil && engine.Enabled() {
		return decodeEncrypted(data, engine, path, logger)
	}
	return decodePlaintext(data)
}

// decodePlaintext walks newline-delimited JSON, the format the encoder writes
// when encryption is off. Records are delimited independently of their content,
// so a malformed line costs only itself — including the partial trailing line a
// process killed mid-write leaves behind.
func decodePlaintext(data []byte) []ReplayEntry {
	var out []ReplayEntry
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if entry, ok := decodeRecord(line); ok {
			out = append(out, entry)
		}
	}
	return out
}

// decodeEncrypted walks the [4-byte big-endian length][sealed blob] framing the
// file writer emits when encryption is on.
//
// A record that fails to decrypt is skipped rather than fatal: its length prefix
// still located the next record, so the stream stays aligned. Broken framing is
// different — the position of the next record becomes unknowable, so decoding
// stops and keeps what came before. That is exactly the shape of a process
// killed between writing a prefix and writing its blob.
func decodeEncrypted(data []byte, engine *crypto.Engine, path string, logger log.Logger) []ReplayEntry {
	var out []ReplayEntry
	for len(data) >= 4 {
		// Kept unsigned, the width the prefix is written in, and compared as
		// uint64 so the check means the same thing on every platform. Converting
		// to int first would turn a prefix of 0x80000000 or more negative where
		// int is 32 bits, slipping past the bounds check and panicking on the
		// slice below. The length comes from disk, so a crash or a tampered file
		// can put any value here.
		blobLen := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if blobLen == 0 || uint64(blobLen) > uint64(len(data)) {
			if logger.Enabled(log.LevelWarn) {
				logger.Log(log.LevelWarn, "audit: replay stopped at a truncated record",
					log.String("filename", path),
					log.Uint("record_bytes", blobLen),
					log.Int("remaining_bytes", len(data)),
				)
			}
			return out
		}
		plain, err := engine.DecryptInPlace(data[:blobLen])
		data = data[blobLen:]
		if err != nil {
			if logger.Enabled(log.LevelWarn) {
				logger.Log(log.LevelWarn, "audit: replay skipped an undecryptable record",
					log.String("filename", path),
					log.String("error", err.Error()),
				)
			}
			continue
		}
		if entry, ok := decodeRecord(plain); ok {
			out = append(out, entry)
		}
	}
	return out
}

// decodeRecord turns one JSON audit line into a ReplayEntry. ok is false for
// malformed JSON, an unparseable timestamp, and every event type ACL LOG does
// not display — the audit trail holds far more than the two kinds replayed here.
func decodeRecord(data []byte) (ReplayEntry, bool) {
	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return ReplayEntry{}, false
	}
	entry := ReplayEntry{Username: r.User, RemoteAddr: r.RemoteAddr}
	switch EventType(r.Event) {
	case EventAuthFailure:
		entry.Reason = r.Reason
	case EventACLDeny:
		// Rebuilt with the format rbac.Store.LogDenied writes live, so a
		// replayed denial and a fresh one are indistinguishable in ACL LOG.
		entry.Reason = "NOPERM command=" + r.Command + " key=" + r.Key
	default:
		return ReplayEntry{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, r.Time)
	if err != nil {
		return ReplayEntry{}, false
	}
	entry.Timestamp = ts
	return entry, true
}
