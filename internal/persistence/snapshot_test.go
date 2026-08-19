package persistence

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotWriteAndRead(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)

	// Populate the engine.
	engine.Set("key1", []byte("value1"), 0)
	engine.Set("key2", []byte("value2"), 10*time.Minute)
	engine.Set("key3", []byte("value3"), 0)
	engine.Delete("key3") // tombstone — should not appear in snapshot

	keysWritten, err := snapshotWrite(dir, 0, engine, nil)
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
	loadedKeys, err := snapshotRead(dir, 0, engine2, nil)
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

	keysWritten, err := snapshotWrite(dir, 0, engine, nil)
	if err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}
	if keysWritten != 1 {
		t.Fatalf("expected 1 key (expired skipped), got %d", keysWritten)
	}

	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, nil)
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
	os.WriteFile(path, []byte("BADMAGICxxxxxxxxxxxxxxxxxxxxxxxx"), 0600)

	engine := newTestEngine(t)
	_, err := snapshotRead(dir, 0, engine, nil)
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestSnapshotExists(t *testing.T) {
	dir := newTestDir(t)
	if snapshotExists(dir, 0) {
		t.Fatal("snapshot should not exist in empty dir")
	}

	// Create a minimal snap file.
	path := filepath.Join(dir, "shard_000.snap")
	os.WriteFile(path, make([]byte, snapHeader), 0600)
	if !snapshotExists(dir, 0) {
		t.Fatal("snapshot should exist after creating file")
	}
}

func TestSnapshotRoundTripWithTTL(t *testing.T) {
	dir := newTestDir(t)
	engine := newTestEngine(t)

	engine.Set("ttl_key", []byte("expires_soon"), 10*time.Minute)
	engine.Set("perm_key", []byte("stays"), 0)

	_, err := snapshotWrite(dir, 0, engine, nil)
	if err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}

	engine2 := newTestEngine(t)
	_, err = snapshotRead(dir, 0, engine2, nil)
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

	s.OpenShard(0)

	// Write some WAL records.
	s.Write(0, "wal_key1", []byte("wal_val1"), time.Time{})
	s.Write(0, "wal_key2", []byte("wal_val2"), time.Time{})

	// Create a snapshot from a state that had only key1.
	snapEngine := newTestEngine(t)
	snapEngine.Set("snap_key", []byte("snap_val"), 0)
	snapshotWrite(dir, 0, snapEngine, nil)
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

	s.OpenShard(0)

	// Populate engine with data, then snapshot from it.
	engine := newTestEngine(t)
	engine.Set("before_snap", []byte("v1"), 0)
	s.Write(0, "before_snap", []byte("v1"), time.Time{})
	snapshotWrite(dir, 0, engine, nil)
	s.TruncateWAL(0)

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
	s.OpenShard(0)

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
