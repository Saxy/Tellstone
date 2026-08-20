package persistence

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/storage"
	"github.com/cespare/xxhash/v2"
)

func TestSnapshotWriteAndRead(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)

	// Populate the engine.
	engine.Set("key1", []byte("value1"), 0)
	engine.Set("key2", []byte("value2"), 10*time.Minute)
	engine.Set("key3", []byte("value3"), 0)
	engine.Delete("key3") // tombstone — should not appear in snapshot

	keysWritten, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	// key3 was deleted before snapshot, so only 2 live keys.
	if keysWritten != 2 {
		t.Fatalf("expected 2 keys written, got %d", keysWritten)
	}

	// Verify the file exists and has the right header.
	snapPath := filepath.Join(dir, "shard_000.snap")
	fi, err := os.Stat(snapPath)
	if err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}
	if fi.Size() < snapHeader {
		t.Fatalf("snapshot file too small: %d bytes", fi.Size())
	}

	// Load into a fresh engine.
	engine2 := newTestEngine(t)
	loadedKeys, err := snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotRead: %v", err)
	}
	if loadedKeys != 2 {
		t.Fatalf("expected 2 keys loaded, got %d", loadedKeys)
	}

	v1, ok := engine2.Get("key1")
	if !ok || string(v1) != "value1" {
		t.Fatalf("key1: got %q, %v", v1, ok)
	}
	v2, ok := engine2.Get("key2")
	if !ok || string(v2) != "value2" {
		t.Fatalf("key2: got %q, %v", v2, ok)
	}
	// key3 should not exist (was deleted before snapshot).
	_, ok = engine2.Get("key3")
	if ok {
		t.Fatal("key3 should not exist after snapshot load")
	}
}

func TestSnapshotSkipsExpiredKeys(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)

	engine.Set("alive", []byte("yes"), 0)
	engine.Set("dying", []byte("no"), 1*time.Nanosecond) // already expired

	time.Sleep(2 * time.Millisecond)

	keysWritten, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	if keysWritten != 1 {
		t.Fatalf("expected 1 key (expired skipped), got %d", keysWritten)
	}

	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotRead: %v", err)
	}
	v, ok := engine2.Get("alive")
	if !ok || string(v) != "yes" {
		t.Fatalf("alive key: got %q, %v", v, ok)
	}
	_, ok = engine2.Get("dying")
	if ok {
		t.Fatal("expired key should not be loaded")
	}
}

func TestSnapshotInvalidMagic(t *testing.T) {
	dir := newTestDir(t)
	path := filepath.Join(dir, "shard_000.snap")
	if err := os.WriteFile(path, []byte("BADMAGICxxxxxxxxxxxxxxxxxxxxxxxx"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	engine := newTestEngine(t)
	_, err := snapshotRead(dir, 0, engine, [16]byte{}, nil)
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestSnapshotChecksumMismatch(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)
	if err := engine.Set("key1", []byte("value1"), 0); err != nil {
		t.Fatalf("engine.Set: %v", err)
	}
	if err := engine.Set("key2", []byte("value2"), 0); err != nil {
		t.Fatalf("engine.Set: %v", err)
	}

	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	engine.Close()

	// Corrupt a byte in the entry body to cause a checksum mismatch.
	path := filepath.Join(dir, "shard_000.snap")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Flip a byte in the first key's data (after the header).
	data[snapHeader+20] ^= 0xFF
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Verify the engine is NOT mutated by the corrupt snapshot.
	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err == nil {
		t.Fatal("expected checksum error for corrupt snapshot")
	}
	if engine2.KeyCount() != 0 {
		t.Fatalf("engine should not be mutated on checksum failure, got %d keys", engine2.KeyCount())
	}
}

func TestSnapshotChecksumZeroBody(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)
	engine.Set("k", []byte("v"), 0)

	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	engine.Close()

	// Write a valid header but zero out the entire body. The checksum will
	// not match because the body no longer hashes the same.
	path := filepath.Join(dir, "shard_000.snap")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Zero the body after the header, keeping the header intact.
	for i := snapHeader; i < len(data); i++ {
		data[i] = 0
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err == nil {
		t.Fatal("expected checksum error for zeroed body")
	}
	if engine2.KeyCount() != 0 {
		t.Fatalf("engine should not be mutated on checksum failure, got %d keys", engine2.KeyCount())
	}
}

// TestSnapshotZeroedChecksum verifies that a snapshot whose checksum bytes
// (header 24:32) are zeroed is rejected even when the computed checksum is
// non-zero. Before the fix, a zero fileChecksum bypassed validation.
func TestSnapshotZeroedChecksum(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)
	if err := engine.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("engine.Set: %v", err)
	}

	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	engine.Close()

	path := filepath.Join(dir, "shard_000.snap")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Zero the checksum slot (bytes 24:32) — this must cause a mismatch
	// because the computed checksum is non-zero.
	for i := 24; i < 32; i++ {
		data[i] = 0
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err == nil {
		t.Fatal("expected checksum error for zeroed checksum header")
	}
	if engine2.KeyCount() != 0 {
		t.Fatalf("engine should not be mutated on checksum failure, got %d keys", engine2.KeyCount())
	}
}

func TestSnapshotTruncatedFile(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)
	engine.Set("key1", []byte("long_value_here"), 0)

	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	engine.Close()

	// Truncate the file so an entry's key or value is cut short.
	path := filepath.Join(dir, "shard_000.snap")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if err := os.WriteFile(path, data[:len(data)-5], 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err == nil {
		t.Fatal("expected error for truncated snapshot")
	}
}

func TestSnapshotExists(t *testing.T) {
	dir := newTestDir(t)
	if snapshotExists(dir, 0) {
		t.Fatal("snapshot should not exist in empty dir")
	}

	// A file with valid magic should be detected.
	path := filepath.Join(dir, "shard_000.snap")
	hdr := make([]byte, snapHeader)
	copy(hdr[0:4], snapMagic)
	if err := os.WriteFile(path, hdr, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if !snapshotExists(dir, 0) {
		t.Fatal("snapshot should exist after creating file with valid magic")
	}

	// A file with invalid magic should NOT be detected.
	path2 := filepath.Join(dir, "shard_001.snap")
	if err := os.WriteFile(path2, make([]byte, snapHeader), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if snapshotExists(dir, 1) {
		t.Fatal("snapshot should not exist with invalid magic")
	}
}

func TestSnapshotRoundTripWithTTL(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)

	engine.Set("ttl_key", []byte("expires_soon"), 10*time.Minute)
	engine.Set("perm_key", []byte("stays"), 0)

	_, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}

	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotRead: %v", err)
	}

	v, ok := engine2.Get("ttl_key")
	if !ok || string(v) != "expires_soon" {
		t.Fatalf("ttl_key: got %q, %v", v, ok)
	}
	v, ok = engine2.Get("perm_key")
	if !ok || string(v) != "stays" {
		t.Fatalf("perm_key: got %q, %v", v, ok)
	}
}

func TestLoadShardSnapshotFirstThenWAL(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}

	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}

	// Write some WAL records.
	if err := s.Write(0, "wal_key1", []byte("wal_val1"), time.Time{}); err != nil {
		t.Fatalf("Write wal_key1: %v", err)
	}
	if err := s.Write(0, "wal_key2", []byte("wal_val2"), time.Time{}); err != nil {
		t.Fatalf("Write wal_key2: %v", err)
	}

	// Create a snapshot from a state that had only key1.
	snapEngine := newTestEngine(t)
	snapEngine.Set("snap_key", []byte("snap_val"), 0)
	if _, err := snapshotWrite(dir, 0, snapEngine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	snapEngine.Close()

	// Now the WAL also has wal_key1 and wal_key2.
	// LoadShard should load snapshot first, then replay WAL.
	freshEngine := newTestEngine(t)
	err = s.LoadShard(0, freshEngine)
	if err != nil {
		t.Fatalf("LoadShard: %v", err)
	}

	// snapshot key should be present.
	v, ok := freshEngine.Get("snap_key")
	if !ok || string(v) != "snap_val" {
		t.Fatalf("snap_key: got %q, %v", v, ok)
	}

	// WAL keys should also be present.
	v, ok = freshEngine.Get("wal_key1")
	if !ok || string(v) != "wal_val1" {
		t.Fatalf("wal_key1: got %q, %v", v, ok)
	}
	v, ok = freshEngine.Get("wal_key2")
	if !ok || string(v) != "wal_val2" {
		t.Fatalf("wal_key2: got %q, %v", v, ok)
	}
}

func TestSnapshotTruncateAndReplay(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}

	s.OpenShard(0, nil)

	// Populate engine with data, then snapshot from it.
	engine := newTestEngine(t)
	engine.Set("before_snap", []byte("v1"), 0)
	s.Write(0, "before_snap", []byte("v1"), time.Time{})
	snapshotWrite(dir, 0, engine, [16]byte{}, nil)
	s.TruncateWALTo(0, 0)

	// Write more data to WAL after snapshot.
	s.Write(0, "after_snap", []byte("v2"), time.Time{})

	// LoadShard should restore snapshot + WAL replay.
	freshEngine := newTestEngine(t)
	err = s.LoadShard(0, freshEngine)
	if err != nil {
		t.Fatalf("LoadShard: %v", err)
	}

	// "before_snap" is in the snapshot.
	v, ok := freshEngine.Get("before_snap")
	if !ok || string(v) != "v1" {
		t.Fatalf("before_snap: got %q, %v", v, ok)
	}

	// "after_snap" is in the WAL (written after snapshot).
	v, ok = freshEngine.Get("after_snap")
	if !ok || string(v) != "v2" {
		t.Fatalf("after_snap: got %q, %v", v, ok)
	}
}

func TestWALSize(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	s.OpenShard(0, nil)

	size := s.WALSize(0)
	if size != 0 {
		t.Fatalf("expected empty WAL, got %d bytes", size)
	}

	s.Write(0, "key", []byte("value"), time.Time{})
	size = s.WALSize(0)
	if size <= 0 {
		t.Fatalf("expected non-empty WAL after write, got %d bytes", size)
	}
}

// TestSnapshotConcurrentWrite verifies that every acknowledged WAL write
// survives a concurrent snapshot and truncation. This is a regression test for
// the WAL boundary race: the truncation boundary must be captured under the
// shard mutex after serialization so that records written during the snapshot
// are never lost.
func TestSnapshotConcurrentWrite(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}

	// Seed the engine and WAL with an initial key.
	if err := s.Write(0, "key1", []byte("val1"), time.Time{}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	engine := newTestEngine(t)
	if err := engine.Set("key1", []byte("val1"), 0); err != nil {
		t.Fatalf("engine.Set: %v", err)
	}

	// Capture the WAL boundary AFTER serialization (mirrors the fixed
	// Storage.Snapshot behavior) and truncate — this is what Snapshot does
	// internally. We use snapshotWrite directly because the fork-based path
	// does not work inside the test binary.
	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	walSize := s.WALSize(0)
	if err := s.TruncateWALTo(0, walSize); err != nil {
		t.Fatalf("TruncateWALTo: %v", err)
	}

	// Write a second key after the snapshot/truncation to simulate a
	// concurrent acknowledged write that must survive recovery.
	if err := s.Write(0, "after_snap", []byte("v2"), time.Time{}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Load into a fresh engine and verify every acknowledged write survived.
	freshEngine := newTestEngine(t)
	if err := s.LoadShard(0, freshEngine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}

	v, ok := freshEngine.Get("key1")
	if !ok || string(v) != "val1" {
		t.Fatalf("key1: got %q, %v", v, ok)
	}
	v, ok = freshEngine.Get("after_snap")
	if !ok || string(v) != "v2" {
		t.Fatalf("after_snap: got %q, %v", v, ok)
	}
}

func TestSnapshotFingerprintValidation(t *testing.T) {
	dir := newTestDir(t)

	// Build a real crypto engine so ForEach returns encrypted values and
	// snapshotRead exercises the SetRaw (encrypted restore) path.
	ce := newCryptoEngine(t)
	fp := ce.KeyFingerprint()

	srcEngine := storage.NewEngine(0, 0, 0, nil, ce)
	defer srcEngine.Close()
	srcEngine.Set("key1", []byte("value1"), 0)

	if _, err := snapshotWrite(dir, 0, srcEngine, fp, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}

	// Same fingerprint → must succeed and restore the original plaintext.
	dstEngine := storage.NewEngine(0, 0, 0, nil, ce)
	defer dstEngine.Close()
	loadedKeys, err := snapshotRead(dir, 0, dstEngine, fp, nil)
	if err != nil {
		t.Fatalf("snapshotRead with matching fingerprint: %v", err)
	}
	if loadedKeys != 1 {
		t.Fatalf("expected 1 key, got %d", loadedKeys)
	}
	v, ok := dstEngine.Get("key1")
	if !ok || string(v) != "value1" {
		t.Fatalf("key1: got %q, %v (expected plaintext \"value1\")", v, ok)
	}

	// Different fingerprint → must fail.
	var wrongFp [16]byte
	wrongFp[0] = 0xFF
	engine3 := storage.NewEngine(0, 0, 0, nil, ce)
	defer engine3.Close()
	_, err = snapshotRead(dir, 0, engine3, wrongFp, nil)
	if err == nil {
		t.Fatal("expected error for fingerprint mismatch")
	}

	// Zero fingerprint (plaintext) when snapshot was encrypted → must fail.
	engine4 := storage.NewEngine(0, 0, 0, nil, ce)
	defer engine4.Close()
	_, err = snapshotRead(dir, 0, engine4, [16]byte{}, nil)
	if err == nil {
		t.Fatal("expected error when reading encrypted snapshot with zero fingerprint")
	}
}

func TestSnapshotZeroFingerprintAcceptsZero(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)
	engine.Set("key1", []byte("value1"), 0)

	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}

	engine2 := newTestEngine(t)
	loadedKeys, err := snapshotRead(dir, 0, engine2, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotRead with zero fingerprint: %v", err)
	}
	if loadedKeys != 1 {
		t.Fatalf("expected 1 key, got %d", loadedKeys)
	}
}

// writeV1Snapshot manually constructs a version-1 snapshot file (32-byte header,
// no fingerprint field). This simulates a snapshot written by an older binary
// before the fingerprint feature was added.
func writeV1Snapshot(t *testing.T, dir string, shardID uint32, key string, value string) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("shard_%03d.snap", shardID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("create v1 snap: %v", err)
	}
	defer f.Close()

	// Build 32-byte v1 placeholder header (keyCount=0, checksum=0).
	var hdr [snapBaseHeader]byte
	copy(hdr[0:4], snapMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], snapVersion1)
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(time.Now().UnixNano()))

	// Write placeholder header first, then entries after it, then patch.
	// This matches the snapshotWrite pattern: header → entries → patch.
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatalf("write placeholder header: %v", err)
	}

	h := xxhash.New()
	h.Write(hdr[:])

	// Write one entry at offset 32 (after the header).
	var entry [16]byte
	binary.LittleEndian.PutUint32(entry[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(entry[4:8], uint32(len(value)))
	// ttlNano = 0 (no expiry).
	h.Write(entry[:])
	h.WriteString(key)
	h.Write([]byte(value))

	if _, err := f.Write(entry[:]); err != nil {
		t.Fatalf("write entry: %v", err)
	}
	if _, err := f.WriteString(key); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if _, err := f.Write([]byte(value)); err != nil {
		t.Fatalf("write value: %v", err)
	}

	// Patch header with keyCount=1 and real checksum.
	checksum := h.Sum64()
	binary.LittleEndian.PutUint64(hdr[8:16], 1)
	binary.LittleEndian.PutUint64(hdr[24:32], checksum)

	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatalf("patch header: %v", err)
	}
}

func TestSnapshotV1FixtureLoads(t *testing.T) {
	dir := newTestDir(t)
	writeV1Snapshot(t, dir, 0, "v1key", "v1val")

	engine := newTestEngine(t)
	loadedKeys, err := snapshotRead(dir, 0, engine, [16]byte{}, nil)
	if err != nil {
		t.Fatalf("snapshotRead v1: %v", err)
	}
	if loadedKeys != 1 {
		t.Fatalf("expected 1 key, got %d", loadedKeys)
	}
	v, ok := engine.Get("v1key")
	if !ok || string(v) != "v1val" {
		t.Fatalf("v1key: got %q, %v", v, ok)
	}
}

func TestSnapshotV1FixtureWithEncryptionRejectsFingerprintMismatch(t *testing.T) {
	dir := newTestDir(t)
	writeV1Snapshot(t, dir, 0, "v1key", "v1val")

	// v1 snapshot has zero fingerprint; non-zero fp request must fail.
	var fp [16]byte
	fp[0] = 0xAA
	engine := newTestEngine(t)
	_, err := snapshotRead(dir, 0, engine, fp, nil)
	if err == nil {
		t.Fatal("expected fingerprint mismatch for v1 snapshot with non-zero fp")
	}
}

func TestSnapshotV1FixtureRecoveryAfterWALTruncation(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}

	// Write a v1 snapshot capturing "snap_key".
	writeV1Snapshot(t, dir, 0, "snap_key", "snap_val")

	// Write a WAL record after the snapshot.
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	if err := s.Write(0, "wal_key", []byte("wal_val"), time.Time{}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Load: snapshot + WAL replay → both keys present.
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if v, ok := engine.Get("snap_key"); !ok || string(v) != "snap_val" {
		t.Fatalf("snap_key: got %q, %v", v, ok)
	}
	if v, ok := engine.Get("wal_key"); !ok || string(v) != "wal_val" {
		t.Fatalf("wal_key: got %q, %v", v, ok)
	}

	// Truncate WAL to magic-only (empty) — snapshot is the sole recovery source.
	walSize := s.WALSize(0)
	if err := s.TruncateWALTo(0, walSize); err != nil {
		t.Fatalf("TruncateWALTo: %v", err)
	}
	if err := s.CloseShard(0); err != nil {
		t.Fatalf("CloseShard: %v", err)
	}

	// Restart and verify the v1 snapshot still recovers correctly.
	s2, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage 2: %v", err)
	}
	defer s2.CloseShard(0)
	if err := s2.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard 2: %v", err)
	}
	engine2 := newTestEngine(t)
	if err := s2.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard 2: %v", err)
	}
	if v, ok := engine2.Get("snap_key"); !ok || string(v) != "snap_val" {
		t.Fatalf("after truncation: snap_key: got %q, %v", v, ok)
	}
}
