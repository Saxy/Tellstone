package persistence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/storage"
)

func newTestEngine(t *testing.T) *storage.Engine {
	t.Helper()
	engine := storage.NewEngine(0, 0, 0, nil, nil)
	t.Cleanup(func() { engine.Close() })
	return engine
}

func newTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

// --- NewStorage ---

func TestNewStorageDisabled(t *testing.T) {
	s, err := NewStorage(false, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Enabled() {
		t.Fatal("expected disabled storage")
	}
	if s.dir != "" {
		t.Fatalf("expected empty dir, got %q", s.dir)
	}
}

func TestNewStorageEnabledCreatesDir(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !s.Enabled() {
		t.Fatal("expected enabled storage")
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("data dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected data dir to be a directory")
	}
}

func TestNewStorageEnabledDefaultDir(t *testing.T) {
	s, err := NewStorage(true, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !s.Enabled() {
		t.Fatal("expected enabled storage")
	}
	if s.dir == "" {
		t.Fatal("expected default dir to be set")
	}
}

func TestNewStorageNilLogger(t *testing.T) {
	s, err := NewStorage(false, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.logger == nil {
		t.Fatal("expected nil logger to be replaced with NoOpLogger")
	}
}

func TestNewStorageMkdirFail(t *testing.T) {
	s, err := NewStorage(true, nil, "/nonexistent/deeply/nested/path/that/should/not/exist")
	if err == nil {
		t.Fatal("expected error when MkdirAll fails with explicit enable")
	}
	if s != nil {
		t.Fatal("expected nil storage on MkdirAll failure")
	}
}

func TestStorageEnabled(t *testing.T) {
	s, err := NewStorage(false, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Enabled() {
		t.Fatal("expected Enabled() == false")
	}
	s2, err := NewStorage(true, nil, newTestDir(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !s2.Enabled() {
		t.Fatal("expected Enabled() == true")
	}
}

// --- OpenShard ---

func TestOpenShardCreatesFile(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard(0): %v", err)
	}
	path := filepath.Join(dir, "shard_000.db")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("shard file not created: %v", err)
	}
}

func TestOpenShardMultipleFiles(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 4; i++ {
		if err := s.OpenShard(i, nil); err != nil {
			t.Fatalf("OpenShard(%d): %v", i, err)
		}
	}
	for i := uint32(0); i < 4; i++ {
		path := filepath.Join(dir, fmt.Sprintf("shard_%03d.db", i))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("shard file %d not created: %v", i, err)
		}
	}
}

func TestOpenShardInvalidPath(t *testing.T) {
	s, err := NewStorage(true, nil, newTestDir(t))
	if err != nil {
		t.Fatal(err)
	}
	s.dir = "/nonexistent/path"
	if err := s.OpenShard(0, nil); err == nil {
		t.Fatal("expected error when opening shard with invalid dir")
	}
}

// --- Write ---

func TestWriteDisabledNoError(t *testing.T) {
	s, err := NewStorage(false, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(0, "key", []byte("value"), time.Time{}); err != nil {
		t.Fatalf("Write on disabled storage should not error: %v", err)
	}
}

func TestWriteToUnopenedShard(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Write(99, "key", []byte("value"), time.Time{})
	if err == nil {
		t.Fatal("expected error writing to unopened shard")
	}
}

func TestWriteRecordBinaryFormat(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}

	key := "testkey"
	val := []byte("testval")
	ttl := time.Unix(0, 1234567890)
	if err := s.Write(0, key, val, ttl); err != nil {
		t.Fatal(err)
	}

	// Read back the raw file and verify binary format
	path := filepath.Join(dir, fmt.Sprintf("shard_%03d.db", 0))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read shard file: %v", err)
	}

	if len(data) != 16+len(key)+len(val) {
		t.Fatalf("record size mismatch: got %d, want %d", len(data), 16+len(key)+len(val))
	}

	keyLen := binary.LittleEndian.Uint32(data[0:4])
	valLen := binary.LittleEndian.Uint32(data[4:8])
	ttlNano := int64(binary.LittleEndian.Uint64(data[8:16]))

	if keyLen != uint32(len(key)) {
		t.Fatalf("keyLen mismatch: got %d, want %d", keyLen, len(key))
	}
	if valLen != uint32(len(val)) {
		t.Fatalf("valLen mismatch: got %d, want %d", valLen, len(val))
	}
	if ttlNano != 1234567890 {
		t.Fatalf("ttlNano mismatch: got %d, want 1234567890", ttlNano)
	}
	if string(data[16:16+len(key)]) != key {
		t.Fatalf("key mismatch: got %q", string(data[16:16+len(key)]))
	}
	if string(data[16+len(key):]) != string(val) {
		t.Fatalf("value mismatch: got %q", string(data[16+len(key):]))
	}
}

func TestWriteZeroTTL(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(0, "k", []byte("v"), time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteMultipleRecords(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}
	type record struct {
		key string
		val []byte
	}
	var records []record
	for i := 0; i < 100; i++ {
		key := "key_" + string(rune('0'+i/10)) + string(rune('0'+i%10))
		val := []byte("value_" + key)
		records = append(records, record{key: key, val: val})
		if err := s.Write(0, key, val, time.Time{}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	path := filepath.Join(dir, fmt.Sprintf("shard_%03d.db", 0))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var expectedSize int
	for _, r := range records {
		expectedSize += 16 + len(r.key) + len(r.val)
	}
	if len(data) != expectedSize {
		t.Fatalf("total file size mismatch: got %d, want %d", len(data), expectedSize)
	}
}

// --- LoadShard ---

func TestLoadShardEmptyFile(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard on empty file: %v", err)
	}
	if engine.KeyCount() != 0 {
		t.Fatalf("expected 0 keys, got %d", engine.KeyCount())
	}
}

func TestLoadShardRoundTrip(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}

	type record struct {
		key   string
		value []byte
		ttl   time.Time
	}
	records := []record{
		{"user:1", []byte("Alice"), time.Time{}},
		{"config:timeout", []byte("30s"), time.Time{}},
		{"session:abc", []byte{0x00, 0xFF, 0xDE, 0xAD}, time.Time{}},
	}

	for _, r := range records {
		if err := s.Write(0, r.key, r.value, r.ttl); err != nil {
			t.Fatal(err)
		}
	}

	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatal(err)
	}

	if engine.KeyCount() != uint64(len(records)) {
		t.Fatalf("expected %d keys, got %d", len(records), engine.KeyCount())
	}

	for _, r := range records {
		val, ok := engine.Get(r.key)
		if !ok {
			t.Fatalf("key %q not found after LoadShard", r.key)
		}
		if string(val) != string(r.value) {
			t.Fatalf("value mismatch for key %q: got %q, want %q", r.key, val, r.value)
		}
	}
}

func TestLoadShardSkipsExpiredKeys(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}

	pastTTL := time.Now().Add(-1 * time.Hour)
	futureTTL := time.Now().Add(1 * time.Hour)

	if err := s.Write(0, "expired_key", []byte("dead"), pastTTL); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(0, "live_key", []byte("alive"), futureTTL); err != nil {
		t.Fatal(err)
	}

	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatal(err)
	}

	if engine.KeyCount() != 1 {
		t.Fatalf("expected 1 key (expired skipped), got %d", engine.KeyCount())
	}
	val, ok := engine.Get("live_key")
	if !ok {
		t.Fatal("live_key not found")
	}
	if string(val) != "alive" {
		t.Fatalf("value mismatch: got %q", val)
	}
}

func TestLoadShardUnopenedShard(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	engine := newTestEngine(t)
	err = s.LoadShard(99, engine)
	if err == nil {
		t.Fatal("expected error loading unopened shard")
	}
}

func TestLoadShardTwiceAppended(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}

	if err := s.Write(0, "k1", []byte("v1"), time.Time{}); err != nil {
		t.Fatal(err)
	}

	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatal(err)
	}
	if engine.KeyCount() != 1 {
		t.Fatalf("expected 1 key, got %d", engine.KeyCount())
	}

	// Write another record
	if err := s.Write(0, "k2", []byte("v2"), time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Seek back to start and reload into fresh engine
	engine2 := newTestEngine(t)
	if err := s.LoadShard(0, engine2); err != nil {
		t.Fatal(err)
	}
	if engine2.KeyCount() != 2 {
		t.Fatalf("expected 2 keys after reload, got %d", engine2.KeyCount())
	}
}

func TestLoadShardTTLRefresh(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}

	futureTTL := time.Now().Add(10 * time.Second)
	if err := s.Write(0, "ttl_key", []byte("ttl_val"), futureTTL); err != nil {
		t.Fatal(err)
	}

	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatal(err)
	}

	val, ok := engine.Get("ttl_key")
	if !ok {
		t.Fatal("ttl_key not found after load")
	}
	if string(val) != "ttl_val" {
		t.Fatalf("value mismatch: got %q", val)
	}
}

// --- Pass-through behavior ---

func TestDisabledWriteAndLoadAreNoOps(t *testing.T) {
	s, err := NewStorage(false, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	// Write should not panic or error
	if err := s.Write(0, "key", []byte("val"), time.Time{}); err != nil {
		t.Fatalf("Write on disabled: %v", err)
	}
	// Enabled should return false
	if s.Enabled() {
		t.Fatal("expected Enabled() == false")
	}
}

// --- Edge cases ---

func TestWriteEmptyKeyAndValue(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(0, "", []byte{}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatal(err)
	}
	_, ok := engine.Get("")
	if !ok {
		t.Fatal("empty key not found after load")
	}
}

func TestWriteLargeValue(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}
	largeVal := make([]byte, 1024*1024) // 1 MiB
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}
	if err := s.Write(0, "big", largeVal, time.Time{}); err != nil {
		t.Fatal(err)
	}

	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatal(err)
	}
	val, ok := engine.Get("big")
	if !ok {
		t.Fatal("large value key not found")
	}
	if len(val) != len(largeVal) {
		t.Fatalf("large value length mismatch: got %d, want %d", len(val), len(largeVal))
	}
}

func TestOpenShardReopen(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(0, "k1", []byte("v1"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Reopening appends to existing file
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(0, "k2", []byte("v2"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatal(err)
	}
	if engine.KeyCount() != 2 {
		t.Fatalf("expected 2 keys after reopen+write, got %d", engine.KeyCount())
	}
}

// --- Concurrency ---

func TestWriteConcurrent(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				key := "key_" + string(rune('0'+n)) + "_" + string(rune('0'+j/10)) + string(rune('0'+j%10))
				if err := s.Write(0, key, []byte("val"), time.Time{}); err != nil {
					t.Errorf("Write: %v", err)
				}
			}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	if err := s.CloseShard(0); err != nil {
		t.Fatalf("close shard: %v", err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("reopen shard: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard after concurrent writes: %v", err)
	}
	expected := 10 * 100
	if engine.KeyCount() != uint64(expected) {
		t.Fatalf("expected %d keys after concurrent writes, got %d", expected, engine.KeyCount())
	}
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			key := "key_" + string(rune('0'+i)) + "_" + string(rune('0'+j/10)) + string(rune('0'+j%10))
			val, ok := engine.Get(key)
			if !ok {
				t.Errorf("key %q not found after reload", key)
				continue
			}
			if string(val) != "val" {
				t.Errorf("key %q value = %q, want %q", key, val, "val")
			}
		}
	}
}

// --- getDefaultDir ---

func TestGetDefaultDir(t *testing.T) {
	dir := getDefaultDir()
	if dir == "" {
		t.Fatal("getDefaultDir returned empty string")
	}
}

// --- Encrypted WAL tests ---

func newCryptoEngine(t *testing.T) *crypto.Engine {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	eng, err := crypto.NewEngine(key, nil)
	if err != nil {
		t.Fatalf("crypto.NewEngine: %v", err)
	}
	return eng
}

// TestEncryptedWALRoundTrip writes records to an encrypted WAL, closes the shard,
// reopens it, and replays to verify all records survive.
func TestEncryptedWALRoundTrip(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	ce := newCryptoEngine(t)
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}

	// Write records.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		if err := s.Write(0, key, []byte(val), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := s.CloseShard(0); err != nil {
		t.Fatalf("CloseShard: %v", err)
	}

	// Verify no plaintext keys or values on disk.
	data, err := os.ReadFile(filepath.Join(dir, "shard_000.db"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val := []byte(fmt.Sprintf("val_%d", i))
		if bytes.Contains(data, key) {
			t.Errorf("plaintext key %q found on disk", key)
		}
		if bytes.Contains(data, val) {
			t.Errorf("plaintext value %q found on disk", val)
		}
	}

	// Reopen and replay.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("reopen OpenShard: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if engine.KeyCount() != 50 {
		t.Fatalf("expected 50 keys, got %d", engine.KeyCount())
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key_%d", i)
		expected := fmt.Sprintf("val_%d", i)
		val, ok := engine.Get(key)
		if !ok {
			t.Errorf("key %q not found", key)
			continue
		}
		if string(val) != expected {
			t.Errorf("key %q = %q, want %q", key, val, expected)
		}
	}
}

// TestEncryptedWALHeaderWritten verifies that OpenShard with a crypto engine
// writes the WAL magic header to a new file.
func TestEncryptedWALHeaderWritten(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	ce := newCryptoEngine(t)
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	s.CloseShard(0)

	// Read first 4 bytes from the WAL file.
	data, err := os.ReadFile(filepath.Join(dir, "shard_000.db"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) < walMagicLen {
		t.Fatalf("file too short: %d bytes", len(data))
	}
	if string(data[:walMagicLen]) != walMagic {
		t.Fatalf("WAL header = %q, want %q", string(data[:walMagicLen]), walMagic)
	}
}

// TestEncryptedWALRejectsMismatchedCrypto verifies that opening an encrypted
// WAL without a crypto engine fails. Opening a plaintext WAL with a crypto
// engine now succeeds — it migrates the WAL to encrypted format.
func TestEncryptedWALRejectsMismatchedCrypto(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	ce := newCryptoEngine(t)

	// Write header + record.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	if err := s.Write(0, "k", []byte("v"), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	s.CloseShard(0)

	// Reopen without crypto — should fail.
	if err := s.OpenShard(0, nil); err == nil {
		t.Fatal("expected error opening encrypted WAL without crypto")
	}
	s.CloseShard(0)

	// Write a plaintext WAL.
	if err := s.OpenShard(1, nil); err != nil {
		t.Fatalf("OpenShard plaintext: %v", err)
	}
	if err := s.Write(1, "k", []byte("v"), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	s.CloseShard(1)

	// Reopen plaintext WAL with crypto — should migrate, not fail.
	if err := s.OpenShard(1, ce); err != nil {
		t.Fatalf("OpenShard plaintext+crypto should migrate: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(1, engine); err != nil {
		t.Fatalf("LoadShard after migration: %v", err)
	}
	val, ok := engine.Get("k")
	if !ok || string(val) != "v" {
		t.Fatalf("key 'k' = %q, want 'v'", val)
	}
	s.CloseShard(1)

	// Reopen — should now be an encrypted WAL (magic header present).
	if err := s.OpenShard(1, ce); err != nil {
		t.Fatalf("OpenShard migrated WAL: %v", err)
	}
	if err := s.LoadShard(1, engine); err != nil {
		t.Fatalf("LoadShard migrated WAL: %v", err)
	}
	val, ok = engine.Get("k")
	if !ok || string(val) != "v" {
		t.Fatalf("key 'k' after reopen = %q, want 'v'", val)
	}
}

// TestPlaintextToEncryptedMigration verifies the full upgrade path:
// write plaintext WAL (v1.2.0), then open with crypto enabled, replay,
// migrate, write new encrypted records, close, reopen, verify all data.
func TestPlaintextToEncryptedMigration(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}

	// Phase 1: write plaintext WAL (simulates v1.2.0).
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard plaintext: %v", err)
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("legacy_%02d", i)
		val := fmt.Sprintf("plain-%02d", i)
		if err := s.Write(0, key, []byte(val), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write %s: %v", key, err)
		}
	}
	s.CloseShard(0)

	// Phase 2: open with crypto — triggers migration.
	ce := newCryptoEngine(t)
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard with crypto: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	// All 20 plaintext records should be in memory.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("legacy_%02d", i)
		want := fmt.Sprintf("plain-%02d", i)
		got, ok := engine.Get(key)
		if !ok || string(got) != want {
			t.Fatalf("after migration key %q = %q, want %q", key, got, want)
		}
	}
	// New writes should go encrypted.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("fresh_%02d", i)
		val := fmt.Sprintf("enc-%02d", i)
		if err := s.Write(0, key, []byte(val), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write %s: %v", key, err)
		}
	}
	s.CloseShard(0)

	// Phase 3: reopen as encrypted WAL — all data survives.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard encrypted: %v", err)
	}
	engine2 := newTestEngine(t)
	if err := s.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("legacy_%02d", i)
		want := fmt.Sprintf("plain-%02d", i)
		got, ok := engine2.Get(key)
		if !ok || string(got) != want {
			t.Fatalf("final legacy key %q = %q, want %q", key, got, want)
		}
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("fresh_%02d", i)
		want := fmt.Sprintf("enc-%02d", i)
		got, ok := engine2.Get(key)
		if !ok || string(got) != want {
			t.Fatalf("final fresh key %q = %q, want %q", key, got, want)
		}
	}
	// WAL file should start with encrypted magic header.
	h := s.getShard(0)
	if h == nil {
		t.Fatal("shard 0 not found")
	}
	magic := make([]byte, 4)
	h.mu.Lock()
	if _, err := h.file.ReadAt(magic, 0); err != nil {
		h.mu.Unlock()
		t.Fatalf("ReadAt: %v", err)
	}
	h.mu.Unlock()
	if string(magic) != walMagic {
		t.Fatalf("WAL magic = %q, want %q", magic, walMagic)
	}
}

// TestDevPlaintextToEncryptedUpgrade verifies the upgrade path where the same
// dev binary first runs without encryption (plaintext WAL), then restarts
// with encryption enabled — the WAL migrates transparently.
func TestDevPlaintextToEncryptedUpgrade(t *testing.T) {
	dir := newTestDir(t)
	ce := newCryptoEngine(t)

	// Session 1: no crypto — write plaintext WAL.
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("v%02d", i)
		if err := s.Write(0, key, []byte(val), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := s.CloseShard(0); err != nil {
		t.Fatalf("CloseShard: %v", err)
	}

	// Session 2: crypto enabled — should migrate and recover all keys.
	s2, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	if err := s2.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard with crypto: %v", err)
	}
	engine := newTestEngine(t)
	if err := s2.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		want := fmt.Sprintf("v%02d", i)
		got, ok := engine.Get(key)
		if !ok || string(got) != want {
			t.Fatalf("after migration key %q = %q, want %q", key, got, want)
		}
	}
	// Verify nonce sidecar was written during migration.
	sidecarPath := nonceSidecarPath(dir, 0)
	if _, err := os.Stat(sidecarPath); os.IsNotExist(err) {
		t.Fatal("nonce sidecar not created during migration")
	}

	// Session 3: reopen as encrypted — data still survives.
	if err := s2.CloseShard(0); err != nil {
		t.Fatalf("CloseShard: %v", err)
	}
	s3, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	if err := s3.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	engine2 := newTestEngine(t)
	if err := s3.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		want := fmt.Sprintf("v%02d", i)
		got, ok := engine2.Get(key)
		if !ok || string(got) != want {
			t.Fatalf("final key %q = %q, want %q", key, got, want)
		}
	}
}

// TestEncryptedWALNonceCounterRecovery verifies that after closing and reopening
// an encrypted shard, the nonce counter is recovered from the WAL so nonces
// never repeat.
func TestEncryptedWALNonceCounterRecovery(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	ce := newCryptoEngine(t)

	// First session: write 10 records.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Write(0, fmt.Sprintf("k%d", i), []byte("v"), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	s.CloseShard(0)

	// Second session: reopen, write more records.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("reopen OpenShard: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}

	// Check the nonce counter was recovered. After 10 writes, the counter
	// tracks the max nonce (9) and recovery sets it to max+1 = 10.
	h := s.getShard(0)
	if h == nil {
		t.Fatal("shard not found")
	}
	ctr := h.nonceCtr.Load()
	if ctr != 10 {
		t.Fatalf("nonce counter = %d, want 10", ctr)
	}

	// Write more records — these must not reuse nonces.
	for i := 0; i < 10; i++ {
		if err := s.Write(0, fmt.Sprintf("k2_%d", i), []byte("v"), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write second session: %v", err)
		}
	}
	s.CloseShard(0)

	// Verify all records survive.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("final OpenShard: %v", err)
	}
	engine2 := newTestEngine(t)
	if err := s.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if engine2.KeyCount() != 20 {
		t.Fatalf("expected 20 keys, got %d", engine2.KeyCount())
	}
}

// TestEncryptedWALCorruptRecordStopsReplay verifies that a corrupted encrypted
// record is detected and replay stops cleanly (remaining records are dropped).
func TestEncryptedWALCorruptRecordStopsReplay(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	ce := newCryptoEngine(t)

	// Write 5 records.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := s.Write(0, fmt.Sprintf("k%d", i), []byte("v"), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	s.CloseShard(0)

	// Corrupt the file: flip a byte in the 12-byte nonce of the first encrypted
	// record (after the 4-byte WAL magic header and 4-byte recLen).
	path := filepath.Join(dir, "shard_000.db")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	// Flip a byte within the nonce (offset walMagicLen+4+2 = 10, which is
	// nonce byte 2). This invalidates the nonce and causes AEAD decryption
	// to fail for the first record, stopping replay immediately.
	if len(data) > walMagicLen+10 {
		data[walMagicLen+10] ^= 0xFF
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Reopen — should not crash, and no records load since the first
	// record's nonce is corrupted and AEAD verification fails.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if engine.KeyCount() != 0 {
		t.Fatalf("expected 0 keys (corruption stops replay), got %d", engine.KeyCount())
	}
}

// TestEncryptedWALPlaintextReplayUnchanged verifies that the plaintext WAL path
// (walVer=0) still works correctly with the refactored replay code.
func TestEncryptedWALPlaintextReplayUnchanged(t *testing.T) {
	dir := newTestDir(t)
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	// Use nil crypto engine (plaintext WAL).
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	for i := 0; i < 30; i++ {
		if err := s.Write(0, fmt.Sprintf("k%d", i), []byte("v"), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	s.CloseShard(0)

	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if engine.KeyCount() != 30 {
		t.Fatalf("expected 30 keys, got %d", engine.KeyCount())
	}
}

// TestEncryptedNonceCounterSurvivesTruncateWithoutClose verifies that the nonce
// counter does not regress when the WAL is truncated to walMagicLen (effectively
// empty) without calling CloseShard. The sidecar must be persisted before
// truncation in truncateWALLocked so a crash after truncation still has the
// correct counter on restart.
func TestEncryptedNonceCounterSurvivesTruncateWithoutClose(t *testing.T) {
	dir := newTestDir(t)
	ce := newCryptoEngine(t)

	// Phase 1: open shard, write 10 records, close (persist sidecar).
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Write(0, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)), time.Time{}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := s.CloseShard(0); err != nil {
		t.Fatalf("CloseShard: %v", err)
	}

	// Phase 2: reopen, write 5 more records (counter is now 15), then
	// truncate to walMagicLen (empty WAL) WITHOUT CloseShard.
	if err := s.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard phase 2: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard phase 2: %v", err)
	}
	for i := 10; i < 15; i++ {
		if err := s.Write(0, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)), time.Time{}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	h := s.getShard(0)
	if h == nil {
		t.Fatal("shard not found")
	}
	ctrBeforeTruncate := h.nonceCtr.Load()
	if ctrBeforeTruncate != 15 {
		t.Fatalf("counter before truncate = %d, want 15", ctrBeforeTruncate)
	}

	// Truncate WAL to magic-only (empty). Do NOT call CloseShard — simulates
	// a crash after truncation but before graceful shutdown.
	if err := s.TruncateWALTo(0, walMagicLen); err != nil {
		t.Fatalf("TruncateWALTo: %v", err)
	}
	s2, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage phase 3: %v", err)
	}
	defer s2.CloseShard(0)
	if err := s2.OpenShard(0, ce); err != nil {
		t.Fatalf("OpenShard phase 3: %v", err)
	}
	engine2 := newTestEngine(t)
	if err := s2.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard phase 3: %v", err)
	}

	h2 := s2.getShard(0)
	if h2 == nil {
		t.Fatal("shard not found phase 3")
	}
	ctrAfterRestart := h2.nonceCtr.Load()
	if ctrAfterRestart != 15 {
		t.Fatalf("counter after restart = %d, want 15 (no regression from truncation)", ctrAfterRestart)
	}
}

// writeRawPlaintextWAL writes raw v1.2.0-format WAL records directly to disk.
// This simulates a data directory left behind by a Tellstone v1.2.0 node
// (plaintext WAL, no magic header, no snapshots, no encryption).
func writeRawPlaintextWAL(t *testing.T, path string, records [][]byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("create WAL file: %v", err)
	}
	for _, rec := range records {
		if _, err := f.Write(rec); err != nil {
			t.Fatalf("write record: %v", err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync WAL: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
}

// buildPlaintextRecord builds a single plaintext WAL record in the v1.2.0 binary
// format: [keyLen:4B LE][valLen:4B LE][ttlNano:8B LE][key][value].
func buildPlaintextRecord(key string, value []byte, ttl time.Time) []byte {
	var header [16]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	if !ttl.IsZero() {
		binary.LittleEndian.PutUint64(header[8:16], uint64(ttl.UnixNano()))
	}
	rec := make([]byte, 0, 16+len(key)+len(value))
	rec = append(rec, header[:]...)
	rec = append(rec, key...)
	rec = append(rec, value...)
	return rec
}

// TestV120PlaintextWALBackwardCompat verifies that v1.3.0 can open, replay, and
// read data from a plaintext WAL created by v1.2.0 (no magic header, no snapshot
// files, no encryption). The WAL bytes are hand-crafted to match the v1.2.0
// on-disk format exactly.
func TestV120PlaintextWALBackwardCompat(t *testing.T) {
	dir := newTestDir(t)

	// Simulate a v1.2.0 data directory: 20 records written directly to disk
	// in plaintext WAL format. No .snap files, no .nonce files, no magic header.
	var records [][]byte
	for i := 0; i < 20; i++ {
		records = append(records, buildPlaintextRecord(
			fmt.Sprintf("legacy_%d", i),
			[]byte(fmt.Sprintf("val_%d", i)),
			time.Time{},
		))
	}
	walPath := filepath.Join(dir, "shard_000.db")
	writeRawPlaintextWAL(t, walPath, records)

	// v1.3.0 opens the shard with nil crypto (plaintext mode).
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}

	// LoadShard should skip snapshot (none exists) and replay the WAL.
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}

	if engine.KeyCount() != 20 {
		t.Fatalf("expected 20 keys after v1.2.0 WAL replay, got %d", engine.KeyCount())
	}

	// Spot-check a few records.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("legacy_%d", i)
		want := fmt.Sprintf("val_%d", i)
		v, ok := engine.Get(key)
		if !ok {
			t.Fatalf("key %q not found after v1.2.0 WAL replay", key)
		}
		if string(v) != want {
			t.Fatalf("key %q: got %q, want %q", key, v, want)
		}
	}
}

// TestV120PlaintextWALSnapshotMigration verifies the full upgrade path from
// v1.2.0 to v1.3.0:
//
//  1. Hand-craft a v1.2.0 plaintext WAL (no magic, no snapshot).
//  2. Open with v1.3.0, load, verify all records.
//  3. Take a snapshot with v1.3.0 (produces a .snap file).
//  4. Truncate the WAL.
//  5. Close and reopen — only the snapshot + truncated WAL should exist.
//  6. Load again, verify all original records survived.
func TestV120PlaintextWALSnapshotMigration(t *testing.T) {
	dir := newTestDir(t)

	// Step 1: Create a v1.2.0 plaintext WAL with 30 records.
	var records [][]byte
	for i := 0; i < 30; i++ {
		records = append(records, buildPlaintextRecord(
			fmt.Sprintf("mig_%d", i),
			[]byte(fmt.Sprintf("data_%d", i)),
			time.Time{},
		))
	}
	walPath := filepath.Join(dir, "shard_000.db")
	writeRawPlaintextWAL(t, walPath, records)

	// Step 2: Open with v1.3.0, load, verify.
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if engine.KeyCount() != 30 {
		t.Fatalf("expected 30 keys, got %d", engine.KeyCount())
	}

	// Step 3: Snapshot — serializes the engine to a .snap file, then truncates WAL.
	// Use snapshotWrite directly (Snapshot() uses ForkExec which needs a built binary).
	if _, err := snapshotWrite(dir, 0, engine, [16]byte{}, nil); err != nil {
		t.Fatalf("snapshotWrite: %v", err)
	}

	// Truncate WAL to snapshot boundary (same as what Snapshot() does after writing).
	if err := s.TruncateWALTo(0, 0); err != nil {
		t.Fatalf("TruncateWALTo: %v", err)
	}

	// Verify snapshot file exists.
	snapPath := filepath.Join(dir, "shard_000.snap")
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}

	// Step 4: Close the shard.
	if err := s.CloseShard(0); err != nil {
		t.Fatalf("CloseShard: %v", err)
	}

	// Step 5: Reopen — should load from snapshot + truncated WAL.
	s2, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage (reopen): %v", err)
	}
	defer s2.CloseShard(0)
	if err := s2.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard (reopen): %v", err)
	}
	engine2 := newTestEngine(t)
	if err := s2.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard (reopen): %v", err)
	}

	// Step 6: Verify all 30 records survived the migration.
	if engine2.KeyCount() != 30 {
		t.Fatalf("expected 30 keys after migration, got %d", engine2.KeyCount())
	}
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("mig_%d", i)
		want := fmt.Sprintf("data_%d", i)
		v, ok := engine2.Get(key)
		if !ok {
			t.Fatalf("key %q not found after migration", key)
		}
		if string(v) != want {
			t.Fatalf("key %q: got %q, want %q", key, v, want)
		}
	}
}

// TestV120PlaintextWALWriteAfterMigration verifies that new writes after
// upgrading from a v1.2.0 plaintext WAL work correctly alongside the legacy
// data.
func TestV120PlaintextWALWriteAfterMigration(t *testing.T) {
	dir := newTestDir(t)

	// Create a v1.2.0 plaintext WAL with 10 records.
	var records [][]byte
	for i := 0; i < 10; i++ {
		records = append(records, buildPlaintextRecord(
			fmt.Sprintf("old_%d", i),
			[]byte(fmt.Sprintf("v%d", i)),
			time.Time{},
		))
	}
	walPath := filepath.Join(dir, "shard_000.db")
	writeRawPlaintextWAL(t, walPath, records)

	// Open with v1.3.0, load legacy data, then write new data.
	s, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	defer s.CloseShard(0)

	if err := s.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	engine := newTestEngine(t)
	if err := s.LoadShard(0, engine); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if engine.KeyCount() != 10 {
		t.Fatalf("expected 10 legacy keys, got %d", engine.KeyCount())
	}

	// Write new records through v1.3.0.
	for i := 0; i < 10; i++ {
		if err := s.Write(0, fmt.Sprintf("new_%d", i), []byte(fmt.Sprintf("nv%d", i)), time.Time{}); err != nil {
			t.Fatalf("Write new_%d: %v", i, err)
		}
	}

	// Close and reopen — both legacy and new records should survive.
	s.CloseShard(0)
	s2, err := NewStorage(true, nil, dir)
	if err != nil {
		t.Fatalf("NewStorage (reopen): %v", err)
	}
	defer s2.CloseShard(0)
	if err := s2.OpenShard(0, nil); err != nil {
		t.Fatalf("OpenShard (reopen): %v", err)
	}
	engine2 := newTestEngine(t)
	if err := s2.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard (reopen): %v", err)
	}

	if engine2.KeyCount() != 20 {
		t.Fatalf("expected 20 keys (10 legacy + 10 new), got %d", engine2.KeyCount())
	}

	// Verify legacy records.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("old_%d", i)
		want := fmt.Sprintf("v%d", i)
		v, ok := engine2.Get(key)
		if !ok {
			t.Fatalf("legacy key %q not found", key)
		}
		if string(v) != want {
			t.Fatalf("legacy key %q: got %q, want %q", key, v, want)
		}
	}

	// Verify new records.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("new_%d", i)
		want := fmt.Sprintf("nv%d", i)
		v, ok := engine2.Get(key)
		if !ok {
			t.Fatalf("new key %q not found", key)
		}
		if string(v) != want {
			t.Fatalf("new key %q: got %q, want %q", key, v, want)
		}
	}
}
