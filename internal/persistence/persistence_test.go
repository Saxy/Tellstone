package persistence

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	if err := s.OpenShard(0); err != nil {
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
		if err := s.OpenShard(i); err != nil {
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
	if err := s.OpenShard(0); err == nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(0, "k1", []byte("v1"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Reopening appends to existing file
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
	if err := s.OpenShard(0); err != nil {
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
