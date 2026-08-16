package audit

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

// writeAuthEvents records one auth failure and one denial through a real engine
// and closes it, leaving a finished audit file for replay to read back.
func writeAuthEvents(t *testing.T, dir string, engine *crypto.Engine) {
	t.Helper()
	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, engine)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	e.Record(EventAuthFailure, "authentication failed",
		log.String("user", "alice"),
		log.String("remote_addr", "10.0.0.1:5000"),
		log.String("reason", "invalid password"),
		log.String("protocol", "resp"),
	)
	e.Record(EventACLDeny, "command denied by rbac policy",
		log.String("user", "bob"),
		log.String("command", "set"),
		log.String("key", "forbidden"),
		log.String("remote_addr", "10.0.0.2:5001"),
		log.String("protocol", "resp"),
	)
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}
}

// TestReplayAuthLogPlaintext verifies both replayable event types round-trip
// through an unencrypted audit file with their fields intact.
func TestReplayAuthLogPlaintext(t *testing.T) {
	dir := t.TempDir()
	writeAuthEvents(t, dir, nil)

	entries := replayAuthLog(dir, nil, 100, log.NewNoOpLogger())
	if len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Username != "alice" || entries[0].Reason != "invalid password" {
		t.Fatalf("entry 0 = %+v, want alice/invalid password", entries[0])
	}
	if entries[0].RemoteAddr != "10.0.0.1:5000" {
		t.Fatalf("entry 0 remote addr = %q", entries[0].RemoteAddr)
	}
	if entries[0].Timestamp.IsZero() {
		t.Fatal("entry 0 has zero timestamp")
	}
	// The denial's reason is rebuilt in the format LogDenied writes live.
	if want := "NOPERM command=set key=forbidden"; entries[1].Reason != want {
		t.Fatalf("entry 1 Reason = %q, want %q", entries[1].Reason, want)
	}
	if entries[1].Username != "bob" {
		t.Fatalf("entry 1 username = %q, want bob", entries[1].Username)
	}
}

// TestReplayAuthLogEncrypted verifies the length-prefixed sealed format decodes
// back to the same entries as the plaintext one.
func TestReplayAuthLogEncrypted(t *testing.T) {
	dir := t.TempDir()
	ce, err := crypto.NewEngine(bytes.Repeat([]byte{0x42}, 32), log.NewNoOpLogger())
	if err != nil {
		t.Fatal("NewEngine:", err)
	}
	writeAuthEvents(t, dir, ce)

	entries := replayAuthLog(dir, ce, 100, log.NewNoOpLogger())
	if len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Reason != "invalid password" {
		t.Fatalf("entry 0 Reason = %q", entries[0].Reason)
	}
	if entries[1].Reason != "NOPERM command=set key=forbidden" {
		t.Fatalf("entry 1 Reason = %q", entries[1].Reason)
	}
}

// TestReplayAuthLogIgnoresOtherEvents verifies the audit trail's other event
// types never reach ACL LOG.
func TestReplayAuthLogIgnoresOtherEvents(t *testing.T) {
	dir := t.TempDir()
	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	e.Record(EventConnect, "client connected", log.String("remote_addr", "10.0.0.1:1"))
	e.Record(EventAuthSuccess, "client authenticated", log.String("user", "alice"))
	e.Record(EventCommand, "command dispatched", log.String("command", "get"))
	e.Record(EventDisconnect, "client disconnected", log.String("remote_addr", "10.0.0.1:1"))
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	if entries := replayAuthLog(dir, nil, 100, log.NewNoOpLogger()); len(entries) != 0 {
		t.Fatalf("ReplayAuthLog = %+v, want no entries", entries)
	}
}

// TestReplayAuthLogCapsAtMaxEntries verifies the cap keeps the newest records
// and still returns them oldest first.
func TestReplayAuthLogCapsAtMaxEntries(t *testing.T) {
	dir := t.TempDir()
	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	for i := 0; i < 10; i++ {
		e.Record(EventAuthFailure, "authentication failed",
			log.String("user", "u"+string(rune('0'+i))),
			log.String("reason", "invalid password"),
		)
	}
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	entries := replayAuthLog(dir, nil, 3, log.NewNoOpLogger())
	if len(entries) != 3 {
		t.Fatalf("ReplayAuthLog len = %d, want 3", len(entries))
	}
	// Writes 7, 8 and 9 are the newest three, oldest first.
	for i, want := range []string{"u7", "u8", "u9"} {
		if entries[i].Username != want {
			t.Fatalf("entry %d username = %q, want %q", i, entries[i].Username, want)
		}
	}
}

// TestReplayAuthLogAcrossRotatedFiles verifies chronological order is preserved
// across a rotation boundary, since each file is a separate read.
func TestReplayAuthLogAcrossRotatedFiles(t *testing.T) {
	dir := t.TempDir()
	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	f, ok := e.writer.(*file)
	if !ok {
		t.Fatalf("engine writer is %T, want *file", e.writer)
	}
	// Rotate after every record so each lands in its own file.
	f.maxSize = 1
	for i := 0; i < 4; i++ {
		e.Record(EventAuthFailure, "authentication failed",
			log.String("user", "u"+string(rune('0'+i))),
			log.String("reason", "invalid password"),
		)
	}
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}
	if paths := auditFilePaths(t, dir); len(paths) < 2 {
		t.Fatalf("expected rotation to produce several files, got %d", len(paths))
	}

	entries := replayAuthLog(dir, nil, 100, log.NewNoOpLogger())
	if len(entries) != 4 {
		t.Fatalf("ReplayAuthLog len = %d, want 4", len(entries))
	}
	for i, want := range []string{"u0", "u1", "u2", "u3"} {
		if entries[i].Username != want {
			t.Fatalf("entry %d username = %q, want %q", i, entries[i].Username, want)
		}
	}

	// The cap must also work across the boundary, keeping the newest records.
	capped := replayAuthLog(dir, nil, 2, log.NewNoOpLogger())
	if len(capped) != 2 {
		t.Fatalf("capped len = %d, want 2", len(capped))
	}
	if capped[0].Username != "u2" || capped[1].Username != "u3" {
		t.Fatalf("capped = %+v, want u2 then u3", capped)
	}
}

// TestReplayAuthLogSkipsCorruptLine verifies a damaged record costs only itself:
// valid records on both sides of it are still recovered.
func TestReplayAuthLogSkipsCorruptLine(t *testing.T) {
	dir := t.TempDir()
	writeAuthEvents(t, dir, nil)

	path := singleAuditFile(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("ReadFile:", err)
	}
	// Splice a malformed line between the two valid records. The pieces are
	// copied into a buffer of the test's own rather than appended onto one of
	// them: SplitN returns subslices of data, and appending to one in place
	// would overwrite the record that follows it.
	lines := bytes.SplitN(data, []byte{'\n'}, 2)
	var corrupted []byte
	corrupted = append(corrupted, lines[0]...)
	corrupted = append(corrupted, "\n{not json at all\n"...)
	corrupted = append(corrupted, lines[1]...)
	if err = os.WriteFile(path, corrupted, 0o600); err != nil {
		t.Fatal("WriteFile:", err)
	}

	// Three lines in, two records out: the middle one is the skipped record.
	if got := bytes.Count(corrupted, []byte{'\n'}); got != 3 {
		t.Fatalf("corrupted file has %d lines, want 3", got)
	}
	entries := replayAuthLog(dir, nil, 100, log.NewNoOpLogger())
	if len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2 (corrupt line skipped)", len(entries))
	}
	if entries[0].Username != "alice" || entries[1].Username != "bob" {
		t.Fatalf("entries = %+v, want alice then bob", entries)
	}
}

// TestReplayAuthLogTruncatedPlaintextTail simulates a process killed mid-write:
// the partial trailing line is dropped, the completed records survive.
func TestReplayAuthLogTruncatedPlaintextTail(t *testing.T) {
	dir := t.TempDir()
	writeAuthEvents(t, dir, nil)

	path := singleAuditFile(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("ReadFile:", err)
	}
	partial := append(data, []byte(`{"time":"2026-08-12T10:00:00Z","event":"auth_fail`)...)
	if err = os.WriteFile(path, partial, 0o600); err != nil {
		t.Fatal("WriteFile:", err)
	}

	entries := replayAuthLog(dir, nil, 100, log.NewNoOpLogger())
	if len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2", len(entries))
	}
}

// TestReplayAuthLogTruncatedEncryptedTail covers both broken-framing shapes: a
// length prefix cut short, and a prefix promising more bytes than remain.
func TestReplayAuthLogTruncatedEncryptedTail(t *testing.T) {
	ce, err := crypto.NewEngine(bytes.Repeat([]byte{0x42}, 32), log.NewNoOpLogger())
	if err != nil {
		t.Fatal("NewEngine:", err)
	}

	for name, tail := range map[string][]byte{
		"partial length prefix": {0x00, 0x00},
		"length beyond file":    {0x00, 0x00, 0xFF, 0xFF, 0x01, 0x02},
	} {
		dir := t.TempDir()
		writeAuthEvents(t, dir, ce)
		path := singleAuditFile(t, dir)
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal("ReadFile:", readErr)
		}
		if writeErr := os.WriteFile(path, append(data, tail...), 0o600); writeErr != nil {
			t.Fatal("WriteFile:", writeErr)
		}
		entries := replayAuthLog(dir, ce, 100, log.NewNoOpLogger())
		if len(entries) != 2 {
			t.Fatalf("%s: ReplayAuthLog len = %d, want 2", name, len(entries))
		}
	}
}

// TestReplayAuthLogUndecryptableRecord verifies a corrupt blob is skipped rather
// than ending the file: its length prefix still located the next record.
func TestReplayAuthLogUndecryptableRecord(t *testing.T) {
	dir := t.TempDir()
	ce, err := crypto.NewEngine(bytes.Repeat([]byte{0x42}, 32), log.NewNoOpLogger())
	if err != nil {
		t.Fatal("NewEngine:", err)
	}
	writeAuthEvents(t, dir, ce)

	path := singleAuditFile(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("ReadFile:", err)
	}
	// Flip a byte inside the first sealed blob, past the 22-byte file header
	// and past its 4-byte length prefix, so authentication fails for that
	// record alone.
	data[auditFileHeaderLen+4] ^= 0xFF
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal("WriteFile:", err)
	}

	entries := replayAuthLog(dir, ce, 100, log.NewNoOpLogger())
	if len(entries) != 1 {
		t.Fatalf("ReplayAuthLog len = %d, want 1 (first record undecryptable)", len(entries))
	}
	if entries[0].Username != "bob" {
		t.Fatalf("surviving entry = %+v, want bob", entries[0])
	}
}

// TestReplayAuthLogMissingDirectory covers a first run with no prior history:
// the audit directory does not exist yet.
func TestReplayAuthLogMissingDirectory(t *testing.T) {
	if entries := replayAuthLog(t.TempDir()+"/never-created", nil, 100, log.NewNoOpLogger()); entries != nil {
		t.Fatalf("ReplayAuthLog = %+v, want nil", entries)
	}
}

// TestReplayAuthLogEmptyDirectory covers audit being enabled for the first time
// against an existing but empty directory.
func TestReplayAuthLogEmptyDirectory(t *testing.T) {
	if entries := replayAuthLog(t.TempDir(), nil, 100, log.NewNoOpLogger()); entries != nil {
		t.Fatalf("ReplayAuthLog = %+v, want nil", entries)
	}
}

// TestReplayAuthLogZeroMaxEntries verifies a non-positive cap reads nothing at
// all rather than everything.
func TestReplayAuthLogZeroMaxEntries(t *testing.T) {
	dir := t.TempDir()
	writeAuthEvents(t, dir, nil)
	if entries := replayAuthLog(dir, nil, 0, log.NewNoOpLogger()); entries != nil {
		t.Fatalf("ReplayAuthLog = %+v, want nil", entries)
	}
}

// TestReplayAuthLogNilLogger verifies the logger is optional, matching newFile.
func TestReplayAuthLogNilLogger(t *testing.T) {
	dir := t.TempDir()
	writeAuthEvents(t, dir, nil)
	if entries := replayAuthLog(dir, nil, 100, nil); len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2", len(entries))
	}
}

// TestReplayAuthLogFormatMismatch documents that the header is the authority:
// a headed file is decoded according to its keyMode, not the engine the caller
// provides. A plaintext file with a KeyModeSimple header is decoded as
// plaintext even when an encrypted engine is supplied (the header wins). An
// encrypted file whose header fingerprint does not match the engine is skipped
// (fail-closed). Headerless legacy files still fall back to engine-based
// inference.
func TestReplayAuthLogFormatMismatch(t *testing.T) {
	ce, err := crypto.NewEngine(bytes.Repeat([]byte{0x42}, 32), log.NewNoOpLogger())
	if err != nil {
		t.Fatal("NewEngine:", err)
	}

	// Plaintext file with KeyModeSimple header: decoded as plaintext even when
	// an encrypted engine is supplied. The header is the authority.
	plainDir := t.TempDir()
	writeAuthEvents(t, plainDir, nil)
	if entries := replayAuthLog(plainDir, ce, 100, log.NewNoOpLogger()); len(entries) == 0 {
		t.Fatal("plaintext file with KeyModeSimple header should be decoded as plaintext")
	}

	// Encrypted file with no engine: fail-closed, zero entries recovered.
	encDir := t.TempDir()
	writeAuthEvents(t, encDir, ce)
	if entries := replayAuthLog(encDir, nil, 100, log.NewNoOpLogger()); len(entries) != 0 {
		t.Fatalf("encrypted file with no engine should yield no entries, got %+v", entries)
	}

	// Encrypted file replayed through a different key: fingerprint mismatch,
	// fail-closed, zero entries recovered. This exercises the
	// KeyFingerprint() != fingerprint branch.
	wrongKey := bytes.Repeat([]byte{0x99}, 32)
	wrongCE, err := crypto.NewEngine(wrongKey, log.NewNoOpLogger())
	if err != nil {
		t.Fatal("NewEngine:", err)
	}
	if entries := replayAuthLog(encDir, wrongCE, 100, log.NewNoOpLogger()); len(entries) != 0 {
		t.Fatalf("encrypted file with wrong key fingerprint should yield no entries, got %+v", entries)
	}
}

// TestReplayAuthLogTimestampPreserved verifies the recovered timestamp is the
// one the record carried, not the time of the replay.
func TestReplayAuthLogTimestampPreserved(t *testing.T) {
	dir := t.TempDir()
	before := time.Now()
	writeAuthEvents(t, dir, nil)
	after := time.Now()

	entries := replayAuthLog(dir, nil, 100, log.NewNoOpLogger())
	if len(entries) == 0 {
		t.Fatal("no entries replayed")
	}
	ts := entries[0].Timestamp
	if ts.Before(before.Add(-time.Second)) || ts.After(after.Add(time.Second)) {
		t.Fatalf("timestamp %v outside the write window %v..%v", ts, before, after)
	}
}

// TestLogEngineReplayAuthLogFromFile verifies the method sources the directory
// and the key from the engine's own writer rather than from its caller.
func TestLogEngineReplayAuthLogFromFile(t *testing.T) {
	dir := t.TempDir()
	writeAuthEvents(t, dir, nil)

	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	t.Cleanup(func() { _ = e.Close() })

	entries := e.ReplayAuthLog(100)
	if len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Username != "alice" || entries[1].Username != "bob" {
		t.Fatalf("entries = %+v, want alice then bob", entries)
	}
}

// TestLogEngineReplayAuthLogEnvelope is the case the method exists for: in
// envelope mode the records are sealed with the audit log's own DEK, not the
// shared engine, and a restart with the same KEK must recover them.
func TestLogEngineReplayAuthLogEnvelope(t *testing.T) {
	dir := t.TempDir()
	kek := bytes.Repeat([]byte{0x2a}, 32)

	first, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), true, kek, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	first.Record(EventAuthFailure, "authentication failed",
		log.String("user", "alice"),
		log.String("remote_addr", "10.0.0.1:5000"),
		log.String("reason", "invalid password"),
	)
	first.Record(EventACLDeny, "command denied by rbac policy",
		log.String("user", "bob"),
		log.String("command", "set"),
		log.String("key", "forbidden"),
		log.String("remote_addr", "10.0.0.2:5001"),
	)
	if err = first.Close(); err != nil {
		t.Fatal("Close:", err)
	}

	// The restart reloads the stored DEK, so replay decrypts what the previous
	// run sealed.
	second, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), true, kek, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	entries := second.ReplayAuthLog(100)
	if len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Reason != "invalid password" {
		t.Fatalf("entry 0 Reason = %q", entries[0].Reason)
	}
	if entries[1].Reason != "NOPERM command=set key=forbidden" {
		t.Fatalf("entry 1 Reason = %q", entries[1].Reason)
	}
}

// TestLogEngineReplayAuthLogNoFileWriter verifies the destinations that hold no
// recoverable history replay nothing instead of guessing at a directory.
func TestLogEngineReplayAuthLogNoFileWriter(t *testing.T) {
	disabled, err := NewLogEngine(false, nil, t.TempDir(), log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	if entries := disabled.ReplayAuthLog(100); entries != nil {
		t.Fatalf("disabled engine replayed %+v, want nil", entries)
	}

	stdout, err := NewLogEngine(true, ParseEventTypes("all"), "stdout", log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	if entries := stdout.ReplayAuthLog(100); entries != nil {
		t.Fatalf("stdout engine replayed %+v, want nil", entries)
	}
}

// TestReplayAuthLogHugeFrameLength feeds the length prefixes that a corrupt or
// tampered file can carry. 0x80000000 is the one that matters: it converts to a
// negative int where int is 32 bits, which would pass a bounds check written
// against int and panic on the slice.
func TestReplayAuthLogHugeFrameLength(t *testing.T) {
	ce, err := crypto.NewEngine(bytes.Repeat([]byte{0x42}, 32), log.NewNoOpLogger())
	if err != nil {
		t.Fatal("NewEngine:", err)
	}

	for name, prefix := range map[string][]byte{
		"high bit set": {0x80, 0x00, 0x00, 0x00},
		"max uint32":   {0xFF, 0xFF, 0xFF, 0xFF},
	} {
		dir := t.TempDir()
		writeAuthEvents(t, dir, ce)
		path := singleAuditFile(t, dir)
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal("ReadFile:", readErr)
		}
		// Append the bogus prefix plus a little payload, so the only thing
		// stopping the decoder is the length check itself.
		tail := append(append([]byte(nil), prefix...), 0x01, 0x02, 0x03, 0x04)
		if writeErr := os.WriteFile(path, append(data, tail...), 0o600); writeErr != nil {
			t.Fatal("WriteFile:", writeErr)
		}
		entries := replayAuthLog(dir, ce, 100, log.NewNoOpLogger())
		if len(entries) != 2 {
			t.Fatalf("%s: replayAuthLog len = %d, want 2", name, len(entries))
		}
	}
}
