package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/storage"
)

const (
	osWindows = "windows"
	osDarwin  = "darwin"
)

var tombstoneTTL int64 = math.MinInt64

func getDefaultDir() string {
	var baseDir string
	switch runtime.GOOS {
	case osWindows:
		baseDir = os.Getenv("APPDATA")
	case osDarwin:
		baseDir = filepath.Join(os.Getenv("HOME"), "Library", "Application Support")
	default:
		baseDir = filepath.Join(os.Getenv("HOME"), ".local", "share")
	}
	return filepath.Join(baseDir, "tellstone", "data")
}

// shardHandle holds the WAL file and its per-shard mutex for a single shard.
type shardHandle struct {
	file *os.File
	mu   sync.Mutex
}

// Storage provides a per-shard, append-only write-ahead log (WAL) for crash recovery.
// Each shard owns an independent file, eliminating cross-shard coordination during writes.
// When disabled, Write and Delete are no-ops and no files are opened.
type Storage struct {
	dir     string
	enabled bool
	logger  log.Logger
	shards  map[uint32]*shardHandle
	mapMu   sync.RWMutex
}

// NewStorage creates a new persistence Storage. If enabled is false, a pass-through
// (no-op) instance is returned. If dir is empty, the platform-specific default is used.
// Returns an error if enabled is true and the data directory cannot be created.
func NewStorage(enabled bool, logger log.Logger, dir string) (*Storage, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	if dir == "" {
		dir = getDefaultDir()
		if logger.Enabled(log.LevelDebug) {
			logger.Log(log.LevelDebug, "no persistence dir configured, using default", log.String("dir", dir))
		}
	}
	if !enabled {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "data storage initialized in pass-through mode (data storage disabled)")
		}
		return &Storage{
			enabled: false,
			logger:  logger,
		}, nil
	}
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "data storage initialized", log.String("dir", dir))
	}
	stg := &Storage{
		dir:     dir,
		enabled: true,
		logger:  logger,
		shards:  make(map[uint32]*shardHandle),
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "persistence: failed to create data directory",
				log.String("dir", dir), log.String("error", err.Error()))
		}
		return nil, fmt.Errorf("persistence: mkdir %s: %w", dir, err)
	}
	if logger.Enabled(log.LevelDebug) {
		logger.Log(log.LevelDebug, "persistence: data directory ready", log.String("dir", dir))
	}
	return stg, nil
}

// Enabled reports whether this storage instance will actually write to disk.
func (s *Storage) Enabled() bool {
	return s.enabled
}

// getShard retrieves the shard handle under mapMu. Returns nil if the shard
// has not been opened.
func (s *Storage) getShard(shardID uint32) *shardHandle {
	s.mapMu.RLock()
	h := s.shards[shardID]
	s.mapMu.RUnlock()
	return h
}

// Write appends a SET record to the shard's WAL file. The record includes a
// 16-byte header (key length, value length, TTL), followed by key and value bytes.
// The write and trailing Sync are serialized under the shard's own mutex.
// Returns nil immediately when persistence is disabled.
func (s *Storage) Write(shardID uint32, key string, value []byte, ttl time.Time) error {
	if !s.enabled {
		return nil
	}
	if uint64(len(key)) > uint64(^uint32(0)) {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: key too large",
				log.String("key", key), log.Int("bytes", len(key)), log.Uint("max", ^uint32(0)))
		}
		return fmt.Errorf("persistence: key too large (%d bytes, max %d)", len(key), ^uint32(0))
	}
	if uint64(len(value)) > uint64(^uint32(0)) {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: value too large",
				log.String("key", key), log.Int("bytes", len(value)), log.Uint("max", ^uint32(0)))
		}
		return fmt.Errorf("persistence: value too large (%d bytes, max %d)", len(value), ^uint32(0))
	}
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var header [16]byte
	var ttlNano int64
	if !ttl.IsZero() {
		ttlNano = ttl.UnixNano()
	}
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	binary.LittleEndian.PutUint64(header[8:16], uint64(ttlNano))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: write",
			log.Uint("shard", shardID), log.String("key", key),
			log.Int("key_len", len(key)), log.Int("val_len", len(value)))
	}
	if _, err := h.file.Write(header[:]); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write header failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.WriteString(key); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write key failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.Write(value); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write value failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if err := h.file.Sync(); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: sync failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	return nil
}

// Delete appends a tombstone record to the shard's WAL file. The tombstone
// uses the same 16-byte header format with a sentinel TTL (math.MinInt64) and
// zero-length value. During LoadShard replay, tombstones cause the key to be
// deleted from the in-memory engine.
// Returns nil immediately when persistence is disabled.
func (s *Storage) Delete(shardID uint32, key string) error {
	if !s.enabled {
		return nil
	}
	if uint64(len(key)) > uint64(^uint32(0)) {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: delete key too large",
				log.String("key", key), log.Int("bytes", len(key)), log.Uint("max", ^uint32(0)))
		}
		return fmt.Errorf("persistence: key too large (%d bytes, max %d)", len(key), ^uint32(0))
	}
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var header [16]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], 0)
	binary.LittleEndian.PutUint64(header[8:16], uint64(tombstoneTTL))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: delete",
			log.Uint("shard", shardID), log.String("key", key))
	}
	if _, err := h.file.Write(header[:]); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write delete header failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.WriteString(key); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write delete key failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if err := h.file.Sync(); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: sync delete failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	return nil
}

// OpenShard opens (or creates) the WAL file for the given shard.
// Must be called before Write or LoadShard for that shard.
// Returns nil immediately when persistence is disabled.
func (s *Storage) OpenShard(shardID uint32) error {
	if !s.enabled {
		return nil
	}
	path := filepath.Join(s.dir, fmt.Sprintf("shard_%03d.db", shardID))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: opening shard", log.Uint("shard", shardID), log.String("path", path))
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: failed to open shard",
				log.Uint("shard", shardID), log.String("path", path), log.String("error", err.Error()))
		}
		return err
	}
	s.mapMu.Lock()
	s.shards[shardID] = &shardHandle{file: f}
	s.mapMu.Unlock()
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: shard opened", log.Uint("shard", shardID))
	}
	return nil
}

// LoadShard replays all records from the shard's WAL file into the given engine,
// skipping expired keys and applying tombstones as deletions. Truncated records
// from a crash mid-write are silently skipped.
func (s *Storage) LoadShard(shardID uint32, engine *storage.Engine) error {
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("shard %d not opened", shardID)
	}
	h.mu.Lock()
	f := h.file
	if _, err := f.Seek(0, 0); err != nil {
		h.mu.Unlock()
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		h.mu.Unlock()
		return err
	}
	remaining := fi.Size()
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: loading shard",
			log.Uint("shard", shardID), log.Int64("file_size", remaining))
	}
	h.mu.Unlock()
	header := make([]byte, 16)
	var recordsRead int
	var recordsSkipped int
	for {
		var n int
		n, err = io.ReadFull(f, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("persistence: incomplete header read (%d bytes): %w", n, err)
		}
		remaining -= 16
		keyLen := binary.LittleEndian.Uint32(header[0:4])
		valLen := binary.LittleEndian.Uint32(header[4:8])
		ttlNano := int64(binary.LittleEndian.Uint64(header[8:16]))
		if int64(keyLen)+int64(valLen) > remaining {
			break
		}
		remaining -= int64(keyLen) + int64(valLen)
		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(f, keyBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("persistence: read key: %w", err)
		}
		valBuf := make([]byte, valLen)
		if _, err = io.ReadFull(f, valBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("persistence: read value: %w", err)
		}
		recordsRead++
		ttlVal := int64(ttlNano)
		if ttlVal == tombstoneTTL {
			if s.logger.Enabled(log.LevelDebug) {
				s.logger.Log(log.LevelDebug, "persistence: replaying delete",
					log.Uint("shard", shardID), log.String("key", string(keyBuf)))
			}
			engine.Delete(string(keyBuf))
			recordsSkipped++
			continue
		}
		var duration time.Duration
		if ttlNano != 0 {
			ttl := time.Unix(0, ttlNano)
			duration = time.Until(ttl)
			if duration <= 0 {
				if s.logger.Enabled(log.LevelDebug) {
					s.logger.Log(log.LevelDebug, "persistence: skipping expired key",
						log.Uint("shard", shardID), log.String("key", string(keyBuf)))
				}
				recordsSkipped++
				continue
			}
		}
		if err = engine.Set(string(keyBuf), valBuf, duration); err != nil {
			return err
		}
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: shard loaded",
			log.Uint("shard", shardID), log.Int("records_read", recordsRead),
			log.Int("records_skipped", recordsSkipped), log.Int("records_loaded", recordsRead-recordsSkipped))
	}
	return nil
}

// CloseShard closes the WAL file for the given shard.
func (s *Storage) CloseShard(shardID uint32) error {
	h := s.getShard(shardID)
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.file.Close()
}
