package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/storage"
)

const (
	osWindows = "windows"
	osDarwin  = "darwin"
)

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
}

func NewStorage(enabled bool, logger log.Logger, dir string) *Storage {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	if dir == "" {
		dir = getDefaultDir()
	}
	if !enabled {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "data storage initialized in pass-through mode (data storage disabled)")
		}
		return &Storage{
			enabled: false,
			logger:  logger,
		}
	}
	if logger.Enabled(log.LevelInfo) {
		logger.Log(log.LevelInfo, "data storage initialized", log.String("data stored in path", dir))
	}
	stg := &Storage{
		dir:     dir,
		enabled: true,
		logger:  logger,
		file:    make(map[uint32]*os.File),
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		if logger.Enabled(log.LevelWarn) {
			logger.Log(log.LevelWarn, "data storage initialization failed (proceed with data storage disabled)")
		}
		return &Storage{
			enabled: false,
			logger:  logger,
		}
	}
	return stg
}

func (s *Storage) Enabled() bool {
	return s.enabled
}

func (s *Storage) Write(shardID uint32, key string, value []byte, ttl time.Time) error {
	if !s.enabled {
		return nil
	}
	writer := s.file[shardID]
	var header [16]byte
	var ttlNano int64
	if !ttl.IsZero() {
		ttlNano = ttl.UnixNano()
	}
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	binary.LittleEndian.PutUint64(header[8:16], uint64(ttlNano))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := writer.WriteString(key); err != nil {
		return err
	}
	if _, err := writer.Write(value); err != nil {
		return err
	}

	return nil
}

func (s *Storage) OpenShard(shardID uint32) error {
	path := filepath.Join(s.dir, fmt.Sprintf("shard_%03d.db", shardID))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	s.file[shardID] = f
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
	header := make([]byte, 16)
	for {
		_, err := f.Read(header)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		keyLen := binary.LittleEndian.Uint32(header[0:4])
		valLen := binary.LittleEndian.Uint32(header[4:8])
		ttlNano := int64(binary.LittleEndian.Uint64(header[8:16]))
		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(f, keyBuf); err != nil {
			return err
		}
		valBuf := make([]byte, valLen)
		if _, err = io.ReadFull(f, valBuf); err != nil {
			return err
		}
		var duration time.Duration
		if ttlNano != 0 {
			ttl := time.Unix(0, ttlNano)
			duration = time.Until(ttl)
			if duration <= 0 {
				continue
			}
		}
		if err = engine.Set(string(keyBuf), valBuf, duration); err != nil {
			return err
		}
	}
	return nil
}
