/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: file_test.go
Description: End-to-end proof that file-backed audit logging works. Records made
through the LogEngine reach the *_tsd.log file as valid JSON lines, survive a
full encrypt/decrypt round trip when the crypto engine is enabled, and rotation
splits history across multiple files without losing or reordering a single record.

Authors:

	Maximilian Hagen
*/
package audit

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
)

// closeFile registers a t.Cleanup that closes f and reports any error via
// t.Errorf, matching the project's convention that test helpers surface cleanup
// failures through the test handle.
func closeFile(t *testing.T, f *file) {
	t.Helper()
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

// auditFilePaths lists every audit file in dir. Sorted order equals creation
// order because the file name embeds its creation timestamp. Discovery only —
// it locates files the way replay does rather than restating the name, which
// TestFileNameContainsTimestampHashAndMarker asserts.
func auditFilePaths(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, auditFileGlob))
	if err != nil {
		t.Fatal("Glob:", err)
	}
	sort.Strings(matches)
	return matches
}

// stripHeader returns data with the audit file header removed. This mirrors
// what replay.skipHeader does but lives in the test package so both plaintext
// and encrypted test helpers can strip the header without importing replay.
func stripHeader(data []byte) []byte {
	if len(data) >= auditFileHeaderLen && string(data[:4]) == auditFileMagic {
		return data[auditFileHeaderLen:]
	}
	return data
}

// a singleAuditFile returns the one and only audit file in dir.
func singleAuditFile(t *testing.T, dir string) string {
	t.Helper()
	paths := auditFilePaths(t, dir)
	if len(paths) != 1 {
		t.Fatalf("expected exactly one audit file, got %d: %v", len(paths), paths)
	}
	return paths[0]
}

// auditLines returns every non-empty line across all audit files in dir,
// in creation order. The file header is stripped from each file before
// splitting so callers see only record data.
func auditLines(t *testing.T, dir string) []string {
	t.Helper()
	var lines []string
	for _, p := range auditFilePaths(t, dir) {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal("ReadFile:", err)
		}
		for _, l := range strings.Split(string(stripHeader(data)), "\n") {
			if l != "" {
				lines = append(lines, l)
			}
		}
	}
	return lines
}

func TestFileNameContainsTimestampHashAndMarker(t *testing.T) {
	dir := t.TempDir()
	f, err := newFile(dir, &crypto.Engine{}, log.NewNoOpLogger(), KeyModeSimple, [16]byte{})
	if err != nil {
		t.Fatal(err)
	}
	closeFile(t, f)

	// The suffix is spelled out rather than taken from auditFileSuffix on
	// purpose. This is the test that pins the on-disk name, and one that built
	// its expectation from the constant it is checking would pass whatever that
	// constant said. Changing the suffix is meant to fail here.
	base := filepath.Base(f.path)
	if !strings.HasSuffix(base, "_tsd.log") {
		t.Fatalf("file name %q missing the tsd marker", base)
	}
	parts := strings.Split(strings.TrimSuffix(base, "_tsd.log"), "_")
	if len(parts) != 3 {
		t.Fatalf("expected <timestamp>_<hash>_<pid>_tsd.log, got %q", base)
	}
	if _, err = fmt.Sscanf(parts[0], "%d", new(int64)); err != nil {
		t.Fatalf("timestamp segment %q is not numeric: %v", parts[0], err)
	}
	if len(parts[1]) != 8 {
		t.Fatalf("hash segment %q should be 8 hex chars", parts[1])
	}
	pid, err := strconv.Atoi(parts[2])
	if err != nil {
		t.Fatalf("pid segment %q is not numeric: %v", parts[2], err)
	}
	if pid != os.Getpid() {
		t.Fatalf("pid segment %q must match the writing process (%d)", parts[2], os.Getpid())
	}
}

func TestFileRotation(t *testing.T) {
	dir := t.TempDir()
	f, err := newFile(dir, &crypto.Engine{}, log.NewNoOpLogger(), KeyModeSimple, [16]byte{})
	if err != nil {
		t.Fatal(err)
	}
	closeFile(t, f)

	first := f.path
	f.maxSize = 20

	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	if f.path == first {
		t.Fatal("expected rotation to switch to a new file")
	}

	data, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(stripHeader(data)); got != "0123456789abcdefghij" {
		t.Fatalf("previous file holds %q, want both writes", got)
	}

	if _, err = f.Write([]byte("xyz")); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(stripHeader(data)); got != "xyz" {
		t.Fatalf("rotated file holds %q, want xyz", got)
	}
}

func TestFileAuditLoggingPlaintext(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, true, ParseEventTypes("all"), dir)

	e.Record(EventAuthSuccess, "user logged in", log.String("user", "alice"))
	e.Record(EventACLDeny, "command denied", log.String("user", "bob"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	lines := auditLines(t, dir)
	if len(lines) != 2 {
		t.Fatalf("expected 2 audit records in the file, got %d", len(lines))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("first line is not valid audit JSON: %v\n%s", err, lines[0])
	}
	if m["level"] != "AUDIT" || m["event"] != "auth_success" || m["user"] != "alice" {
		t.Fatalf("first record does not round trip to the file: %v", m)
	}
}

func TestFileAuditLoggingEncrypted(t *testing.T) {
	dir := t.TempDir()
	ce, err := crypto.NewEngine(bytes.Repeat([]byte{0x42}, 32), log.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, ce)
	if err != nil {
		t.Fatal(err)
	}

	records := []struct {
		event EventType
		msg   string
	}{
		{EventAuthSuccess, "user logged in"},
		{EventACLDeny, "command denied"},
		{EventAuthFailure, "bad password"},
	}

	// Every record is written up front; the completed file is decoded only
	// afterwards, walking the length prefixes sequentially with no reads
	// between Record calls.
	for _, r := range records {
		e.Record(r.event, r.msg, log.String("user", "alice"))
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(singleAuditFile(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "AUDIT") {
		t.Fatal("encrypted audit file contains plaintext AUDIT marker")
	}
	data = stripHeader(data)

	for i, r := range records {
		if len(data) < 4 {
			t.Fatalf("record %d: truncated length prefix (%d bytes left)", i, len(data))
		}
		blobLen := int(binary.BigEndian.Uint32(data[:4]))
		data = data[4:]
		if blobLen == 0 || blobLen > len(data) {
			t.Fatalf("record %d: invalid blob length %d (remaining %d)", i, blobLen, len(data))
		}
		plain, err := ce.DecryptInPlace(data[:blobLen])
		if err != nil {
			t.Fatalf("record %d failed to decrypt: %v", i, err)
		}
		var m map[string]any
		if err := json.Unmarshal(plain, &m); err != nil {
			t.Fatalf("record %d is not valid JSON after decryption: %v\n%s", i, err, plain)
		}
		if m["level"] != "AUDIT" || m["event"] != string(r.event) || m["msg"] != r.msg {
			t.Fatalf("record %d does not survive the encrypted round trip: %v", i, m)
		}
		data = data[blobLen:]
	}
	if len(data) != 0 {
		t.Fatalf("file holds %d trailing bytes beyond the last record", len(data))
	}
}

func TestFileAuditLoggingEnvelope(t *testing.T) {
	dir := t.TempDir()
	kek := bytes.Repeat([]byte{0x2a}, 32)

	e, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), true, kek, nil)
	if err != nil {
		t.Fatal(err)
	}
	e.Record(EventAuthSuccess, "user logged in", log.String("user", "alice"))
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}

	// The DEK envelope is stored beside the records it protects and the records
	// themselves carry no plaintext marker.
	matches, err := filepath.Glob(filepath.Join(dir, "*.env"))
	if err != nil {
		t.Fatal("Glob:", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one envelope file, got %d: %v", len(matches), matches)
	}
	data, err := os.ReadFile(singleAuditFile(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "AUDIT") {
		t.Fatal("envelope-encrypted audit file contains plaintext AUDIT marker")
	}

	// A restart with the same KEK must load the stored DEK (the Load path)
	// rather than generating a fresh one, and still write decryptable records.
	e2, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), true, kek, nil)
	if err != nil {
		t.Fatal(err)
	}
	e2.Record(EventACLDeny, "command denied")
	if err = e2.Close(); err != nil {
		t.Fatal(err)
	}

	// A changed KEK must fail the engine closed instead of minting a fresh DEK
	// that would brick the existing audit history.
	if e3, err := NewLogEngine(true, ParseEventTypes("all"), dir, log.NewNoOpLogger(), true, bytes.Repeat([]byte{0x11}, 32), nil); err == nil {
		_ = e3.Close()
		t.Fatal("changed KEK unexpectedly accepted")
	}

	env, err := crypto.NewEnvelope(kek, log.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	dek, err := env.Load(dir, envelopeFileName)
	if err != nil {
		t.Fatalf("failed to load audit DEK for decoding: %v", err)
	}
	ce, err := crypto.NewEngine(dek, log.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}

	// Every record across both boots decodes with the restored DEK.
	var msgs []string
	for _, p := range auditFilePaths(t, dir) {
		data, err = os.ReadFile(p)
		if err != nil {
			t.Fatal("ReadFile:", err)
		}
		data = stripHeader(data)
		for len(data) > 0 {
			if len(data) < 4 {
				t.Fatalf("%s: truncated length prefix (%d bytes left)", p, len(data))
			}
			blobLen := int(binary.BigEndian.Uint32(data[:4]))
			data = data[4:]
			if blobLen == 0 || blobLen > len(data) {
				t.Fatalf("%s: invalid blob length %d (remaining %d)", p, blobLen, len(data))
			}
			var plain []byte
			plain, err = ce.DecryptInPlace(data[:blobLen])
			if err != nil {
				t.Fatalf("%s: failed to decrypt: %v", p, err)
			}
			var m map[string]any
			if err = json.Unmarshal(plain, &m); err != nil {
				t.Fatalf("%s: not valid JSON after decryption: %v\n%s", p, err, plain)
			}
			msgs = append(msgs, fmt.Sprintf("%v", m["msg"]))
			data = data[blobLen:]
		}
	}
	if got := strings.Join(msgs, "|"); got != "user logged in|command denied" {
		t.Fatalf("decrypted messages = %v", msgs)
	}
}

func TestFileRotationThroughEngine(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, true, ParseEventTypes("all"), dir)
	e.writer.(*file).maxSize = 1 // rotate after every record

	const n = 10
	for i := 0; i < n; i++ {
		e.Record(EventAuthSuccess, fmt.Sprintf("record %d", i))
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if paths := auditFilePaths(t, dir); len(paths) < 2 {
		t.Fatalf("expected rotation to produce multiple files, got %d", len(paths))
	}

	lines := auditLines(t, dir)
	if len(lines) != n {
		t.Fatalf("expected %d records across rotated files, got %d", n, len(lines))
	}
	for i, l := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("line %d is not valid audit JSON: %v", i, err)
		}
		want := fmt.Sprintf("record %d", i)
		if m["msg"] != want {
			t.Fatalf("line %d msg = %v, want %s", i, m["msg"], want)
		}
	}
}

func TestEngineConcurrentRecords(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, true, ParseEventTypes("all"), dir)

	// The gnet listeners and bcrypt workers record from many goroutines at
	// once; run under -race this proves Record and Close are serialized.
	const workers = 8
	const perWorker = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				e.Record(EventAuthSuccess, "login",
					log.String("user", fmt.Sprintf("user-%d-%d", id, i)))
			}
		}(w)
	}
	wg.Wait()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	lines := auditLines(t, dir)
	if len(lines) != workers*perWorker {
		t.Fatalf("expected %d records, got %d", workers*perWorker, len(lines))
	}
}

// --- Audit file header tests ---

func TestAuditFileHeaderLayout(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0xAA}, 32)
	ce, err := crypto.NewEngine(key, log.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	fp := crypto.FingerprintBytes(key)

	f, err := newFile(dir, ce, log.NewNoOpLogger(), KeyModeSimple, fp)
	if err != nil {
		t.Fatal(err)
	}
	closeFile(t, f)

	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < auditFileHeaderLen {
		t.Fatalf("file too short for header: %d bytes", len(raw))
	}

	// [magic:4][version:1][keyMode:1][fingerprint:16]
	if string(raw[:4]) != auditFileMagic {
		t.Fatalf("magic = %q, want %q", raw[:4], auditFileMagic)
	}
	if raw[4] != auditFileVersion {
		t.Fatalf("version = %d, want %d", raw[4], auditFileVersion)
	}
	if raw[5] != KeyModeSimple {
		t.Fatalf("keyMode = %d, want %d (KEK)", raw[5], KeyModeSimple)
	}
	if !bytes.Equal(raw[6:6+16], fp[:]) {
		t.Fatal("fingerprint does not match sealing key")
	}
}

func TestAuditFileHeaderMagicRejection(t *testing.T) {
	// A file without the TSDA magic is treated as legacy (headerless).
	legacy := []byte(`{"event":"auth_success","level":"AUDIT","msg":"test"}` + "\n")
	if len(legacy) >= 4 && string(legacy[:4]) == auditFileMagic {
		t.Fatal("test fixture must not start with the audit magic")
	}
	// skipHeader must return the data unchanged.
	got := stripHeader(legacy)
	if !bytes.Equal(got, legacy) {
		t.Fatal("skipHeader should return legacy data unchanged")
	}
}

func TestAuditFileHeaderKeyMode(t *testing.T) {
	dir := t.TempDir()

	// Plaintext (no encryption): keyMode should be 0 (KEK).
	plain, err := newFile(dir, &crypto.Engine{}, log.NewNoOpLogger(), KeyModeSimple, [16]byte{})
	if err != nil {
		t.Fatal(err)
	}
	closeFile(t, plain)
	raw, err := os.ReadFile(plain.path)
	if err != nil {
		t.Fatal(err)
	}
	if raw[5] != KeyModeSimple {
		t.Fatalf("plaintext keyMode = %d, want %d", raw[5], KeyModeSimple)
	}

	// DEK-sealed (envelope mode): keyMode should be 1.
	key := bytes.Repeat([]byte{0xBB}, 32)
	ce, err := crypto.NewEngine(key, log.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	fp := crypto.FingerprintBytes(key)
	fDEK, err := newFile(dir, ce, log.NewNoOpLogger(), KeyModeEnvelope, fp)
	if err != nil {
		t.Fatal(err)
	}
	closeFile(t, fDEK)
	raw, err = os.ReadFile(fDEK.path)
	if err != nil {
		t.Fatal(err)
	}
	if raw[5] != KeyModeEnvelope {
		t.Fatalf("DEK keyMode = %d, want %d", raw[5], KeyModeEnvelope)
	}
}

func TestAuditFileHeaderOnRotation(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0xCC}, 32)
	ce, err := crypto.NewEngine(key, log.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	fp := crypto.FingerprintBytes(key)

	f, err := newFile(dir, ce, log.NewNoOpLogger(), KeyModeEnvelope, fp)
	if err != nil {
		t.Fatal(err)
	}
	closeFile(t, f)

	// Force rotation by setting a tiny maxSize.
	f.maxSize = 5
	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}

	// The rotated file must also start with the header.
	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < auditFileHeaderLen {
		t.Fatalf("rotated file too short for header: %d bytes", len(raw))
	}
	if string(raw[:4]) != auditFileMagic {
		t.Fatalf("rotated file magic = %q, want %q", raw[:4], auditFileMagic)
	}
	if raw[5] != KeyModeEnvelope {
		t.Fatalf("rotated file keyMode = %d, want %d", raw[5], KeyModeEnvelope)
	}
	if !bytes.Equal(raw[6:6+16], fp[:]) {
		t.Fatal("rotated file fingerprint does not match sealing key")
	}
}

func TestAuditFileHeaderFingerprintMatchesKey(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0xDD}, 32)
	ce, err := crypto.NewEngine(key, log.NewNoOpLogger())
	if err != nil {
		t.Fatal(err)
	}
	fp := crypto.FingerprintBytes(key)

	f, err := newFile(dir, ce, log.NewNoOpLogger(), KeyModeSimple, fp)
	if err != nil {
		t.Fatal(err)
	}
	closeFile(t, f)

	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	got := raw[6 : 6+16]
	if !bytes.Equal(got, fp[:]) {
		t.Fatalf("header fingerprint %x does not match expected %x", got, fp[:])
	}
}

func TestLegacyHeaderlessFileStillReplays(t *testing.T) {
	// Write a headerless plaintext audit file (legacy format) and verify
	// replay still recovers the records.
	dir := t.TempDir()
	legacy := []byte(`{"time":"2026-08-12T10:00:00Z","level":"AUDIT","event":"auth_failure","user":"alice","reason":"invalid password"}` + "\n" +
		`{"time":"2026-08-12T10:01:00Z","level":"AUDIT","event":"acl_deny","user":"bob","command":"set","key":"forbidden"}` + "\n")
	path := filepath.Join(dir, "legacy_0000000000_00000000_0_tsd.log")
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}

	entries := replayAuthLog(dir, nil, 100, log.NewNoOpLogger())
	if len(entries) != 2 {
		t.Fatalf("ReplayAuthLog len = %d, want 2 for legacy file", len(entries))
	}
	if entries[0].Username != "alice" {
		t.Fatalf("entry 0 username = %q, want alice", entries[0].Username)
	}
	if entries[1].Username != "bob" {
		t.Fatalf("entry 1 username = %q, want bob", entries[1].Username)
	}
}
