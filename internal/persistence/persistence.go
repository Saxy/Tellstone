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

type Storage struct {
	dir     string
	enabled bool
	logger  log.Logger
	file    map[uint32]*os.File
	mu      sync.Mutex
}

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
		file:    make(map[uint32]*os.File),
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

func (s *Storage) Enabled() bool {
	return s.enabled
}

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
	s.mu.Lock()
	writer, exist := s.file[shardID]
	if !exist {
		s.mu.Unlock()
		return fmt.Errorf("persistence: shard %d not opened", shardID)
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
	if _, err := writer.Write(header[:]); err != nil {
		s.mu.Unlock()
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write header failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := writer.WriteString(key); err != nil {
		s.mu.Unlock()
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write key failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := writer.Write(value); err != nil {
		s.mu.Unlock()
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write value failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if err := writer.Sync(); err != nil {
		s.mu.Unlock()
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: sync failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	s.mu.Unlock()
	return nil
}

func (s *Storage) Delete(shardID uint32, key string) error {
	if !s.enabled {
		return nil
	}
	s.mu.Lock()
	writer := s.file[shardID]
	var header [16]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], 0)
	binary.LittleEndian.PutUint64(header[8:16], uint64(tombstoneTTL))
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: delete",
			log.Uint("shard", shardID), log.String("key", key))
	}
	if _, err := writer.Write(header[:]); err != nil {
		s.mu.Unlock()
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write delete header failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if _, err := writer.WriteString(key); err != nil {
		s.mu.Unlock()
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: write delete key failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	if err := writer.Sync(); err != nil {
		s.mu.Unlock()
		if s.logger.Enabled(log.LevelError) {
			s.logger.Log(log.LevelError, "persistence: sync delete failed",
				log.Uint("shard", shardID), log.String("key", key), log.String("error", err.Error()))
		}
		return err
	}
	s.mu.Unlock()
	return nil
}

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
	s.file[shardID] = f
	if s.logger.Enabled(log.LevelDebug) {
		s.logger.Log(log.LevelDebug, "persistence: shard opened", log.Uint("shard", shardID))
	}
	return nil
}

func (s *Storage) LoadShard(shardID uint32, engine *storage.Engine) error {
	f := s.file[shardID]
	if f == nil {
		return fmt.Errorf("shard %d not opened", shardID)
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	remaining := fi.Size()
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "persistence: loading shard",
			log.Uint("shard", shardID), log.Int64("file_size", remaining))
	}
	header := make([]byte, 16)
	var recordsRead int
	var recordsSkipped int
	for {
		var n int
		n, err = io.ReadFull(f, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean EOF or a truncated trailing header from a crash mid-write
			}
			return fmt.Errorf("persistence: incomplete header read (%d bytes): %w", n, err)
		}
		remaining -= 16
		keyLen := binary.LittleEndian.Uint32(header[0:4])
		valLen := binary.LittleEndian.Uint32(header[4:8])
		ttlNano := int64(binary.LittleEndian.Uint64(header[8:16]))
		if int64(keyLen)+int64(valLen) > remaining {
			break // truncated trailing record
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
