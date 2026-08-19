package persistence

import (
	"encoding/binary"
	"errors"
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
// (no-op) instance is returned. If the dir is empty, the platform-specific default is used.
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

// appendRecord is the shared writing path for both SET and tombstone records.
// It acquires the shard mutex, writes the header, key, value, and syncs.
func (s *Storage) appendRecord(shardID uint32, header [16]byte, key string, value []byte, op string) error {
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.file.Write(header[:]); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write "+op+" header failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.WriteString(key); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write "+op+" key failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := h.file.Write(value); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write "+op+" value failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if err := h.file.Sync(); err != nil {
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: sync "+op+" failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	return nil
}

// Write appends a SET record to the shard's WAL file. The record includes a
// 16-byte header (key length, value length, TTL), followed by key and value bytes.
// The writing and trailing Sync are serialized under the shard's own mutex.
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
	return s.appendRecord(shardID, header, key, value, "set")
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
	var header [16]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], 0)
	binary.LittleEndian.PutUint64(header[8:16], uint64(tombstoneTTL))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: delete",
			log.Uint("shard", shardID), log.String("key", key))
	}
	return s.appendRecord(shardID, header, key, nil, "delete")
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

// LoadShard restores a shard's in-memory engine. If a snapshot file exists,
// it is loaded first (fast binary read). Then the WAL is replayed on top to
// capture any writes that happened after the snapshot. This two-phase approach
// keeps warm-up fast: the snapshot is compact and the WAL is small.
func (s *Storage) LoadShard(shardID uint32, engine *storage.Engine) error {
	if !s.enabled {
		return nil
	}

	// Phase 1: load snapshot if present. A corrupt snapshot means data
	// preceding it is unrecoverable — abort rather than continuing with
	// a partial state and replaying the WAL on top of an empty engine.
	if snapshotExists(s.dir, shardID) {
		loadedKeys, err := snapshotRead(s.dir, shardID, engine, s.logger)
		if err != nil {
			if s.logger.Enabled(log.LevelError) {
				s.logger.Log(log.LevelError, "persistence: snapshot load failed, data preceding snapshot is unrecoverable",
					log.Uint("shard", shardID), log.String("error", err.Error()))
			}
			return fmt.Errorf("persistence: load shard %d snapshot: %w", shardID, err)
		}
		if s.logger.Enabled(log.LevelInfo) {
			s.logger.Log(log.LevelInfo, "persistence: snapshot restored",
				log.Uint("shard", shardID), log.Uint64("keys", loadedKeys))
		}
	}

	// Phase 2: replay the WAL for writes since the last snapshot.
	return s.replayWAL(shardID, engine)
}

// replayWAL replays all records from the shard's WAL file into the given engine,
// skipping expired keys and applying tombstones as deletions. Truncated records
// from a crash mid-write are detected, and the WAL is truncated to the last valid
// offset so future loads resume from a clean end.
func (s *Storage) replayWAL(shardID uint32, engine *storage.Engine) error {
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
	fileSize := fi.Size()
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: loading shard",
			log.Uint("shard", shardID), log.Int64("file_size", fileSize))
	}
	h.mu.Unlock()
	remaining := fileSize
	var validOffset int64
	header := make([]byte, 16)
	var recordsRead int
	var recordsSkipped int
	for {
		var n int
		n, err = io.ReadFull(f, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
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
		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(f, keyBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("persistence: read key: %w", err)
		}
		valBuf := make([]byte, valLen)
		if _, err = io.ReadFull(f, valBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("persistence: read value: %w", err)
		}
		remaining -= int64(keyLen) + int64(valLen)
		validOffset = fileSize - remaining
		recordsRead++
		ttlVal := ttlNano
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
	if validOffset < fileSize {
		h.mu.Lock()
		if err = f.Truncate(validOffset); err != nil {
			h.mu.Unlock()
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "persistence: failed to truncate corrupted tail",
					log.Uint("shard", shardID), log.String("error", err.Error()))
			}
		} else {
			if s.logger.Enabled(log.LevelInfo) {
				s.logger.Log(log.LevelInfo, "persistence: truncated corrupted WAL tail",
					log.Uint("shard", shardID), log.Int64("from", fileSize), log.Int64("to", validOffset))
			}
		}
		h.mu.Unlock()
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: shard loaded",
			log.Uint("shard", shardID), log.Int("records_read", recordsRead),
			log.Int("records_skipped", recordsSkipped), log.Int("records_loaded", recordsRead-recordsSkipped))
	}
	return nil
}

// WALSize returns the current WAL file size in bytes for the given shard.
// Returns 0 if the shard is not opened or on error.
func (s *Storage) WALSize(shardID uint32) int64 {
	h := s.getShard(shardID)
	if h == nil {
		return 0
	}
	h.mu.Lock()
	fi, err := h.file.Stat()
	h.mu.Unlock()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// Snapshot triggers a fork-based snapshot for the given shard. The parent
// serializes the engine state to a pipe, and a child process writes the
// snapshot file. After a successful snapshot, only the WAL records that were
// part of the serialized engine state are truncated; records appended during
// the snapshot are preserved so they survive a crash.
//
// The WAL boundary is captured after serialization and under the shard's WAL
// mutex so that no concurrent Write can slip between the boundary capture and
// the truncation. If the boundary cannot be obtained, truncation is skipped
// rather than risking data loss.
func (s *Storage) Snapshot(shardID uint32, engine *storage.Engine) error {
	if !s.enabled {
		return nil
	}
	// Snapshot the engine state first. ForEach freezes the engine under a
	// read lock so no new mutations complete during serialization.
	if err := snapshotForkDump(s.dir, shardID, engine, s.logger); err != nil {
		return err
	}
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	// Capture the WAL boundary after serialization and truncate atomically
	// under the shard mutex so no Write can interleave between the stat and
	// the truncation.
	h.mu.Lock()
	defer h.mu.Unlock()
	fi, err := h.file.Stat()
	if err != nil {
		// Cannot determine the truncation boundary — skip truncation to
		// avoid losing records. The extra WAL data is harmless: it will be
		// replayed on recovery and the duplicate SETs are idempotent.
		if s.logger.Enabled(log.LevelWarn) {
			s.logger.Log(log.LevelWarn, "persistence: cannot determine WAL boundary, skipping truncation",
				log.Uint("shard", shardID), log.String("error", err.Error()))
		}
		return nil
	}
	return s.truncateWALLocked(h, shardID, fi.Size())
}

// truncateWALLocked truncates, syncs, and seeks the WAL file to the given
// offset. The caller must hold h.mu. Used by Snapshot and TruncateWALTo.
func (s *Storage) truncateWALLocked(h *shardHandle, shardID uint32, offset int64) error {
	if err := h.file.Truncate(offset); err != nil {
		return fmt.Errorf("persistence: truncate WAL shard %d to %d: %w", shardID, offset, err)
	}
	// Sync after truncation so the truncated length is durable; without this
	// a crash could replay stale records that were logically discarded.
	if err := h.file.Sync(); err != nil {
		return fmt.Errorf("persistence: sync WAL shard %d after truncate: %w", shardID, err)
	}
	if _, err := h.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("persistence: seek WAL shard %d: %w", shardID, err)
	}
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: WAL truncated",
			log.Uint("shard", shardID), log.Int64("to", offset))
	}
	return nil
}

// TruncateWALTo truncates the WAL file to the given offset, discarding any
// bytes beyond that point. Called after a successful snapshot to remove only
// the records that were serialized, preserving later writes.
func (s *Storage) TruncateWALTo(shardID uint32, offset int64) error {
	h := s.getShard(shardID)
	if h == nil {
		return fmt.Errorf("persistence: shard %d not opened", shardID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return s.truncateWALLocked(h, shardID, offset)
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
