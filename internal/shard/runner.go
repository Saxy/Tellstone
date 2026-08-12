/*
Package shard
Tellstone Shared-Nothing Shard Layer
File: runner.go
Description: Provides the Shard struct and its lifecycle (Run, Execute, Stop). Each shard holds a single storage.Engine and executes operations synchronously. The shared-nothing design eliminates cross-shard coordination: every key is pinned to exactly one shard via FNV-1a hashing, so the per-shard RWMutex is almost never contended.

Authors:

	Maximilian Hagen
*/
package shard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/persistence"
	"github.com/Saxy/Tellstone/internal/storage"
)

const (
	CmdGet     string = "GET"
	CmdSet     string = "SET"
	CmdDel     string = "DEL"
	CmdPing    string = "PING"
	CmdCommand string = "COMMAND"
	CmdAuth    string = "AUTH"
	CmdRole    string = "ROLE"
	CmdACL     string = "ACL"
)

type Shard struct {
	ID               ID
	Engine           *storage.Engine
	Logger           log.Logger
	Persistence      *persistence.Storage
	connectedClients int64
	totalConnections uint64
	bytesRead        uint64
	bytesWritten     uint64
}

func (s *Shard) IncConnectedClients()     { atomic.AddInt64(&s.connectedClients, 1) }
func (s *Shard) DecConnectedClients()     { atomic.AddInt64(&s.connectedClients, -1) }
func (s *Shard) IncTotalConnections()     { atomic.AddUint64(&s.totalConnections, 1) }
func (s *Shard) AddBytesRead(n uint64)    { atomic.AddUint64(&s.bytesRead, n) }
func (s *Shard) AddBytesWritten(n uint64) { atomic.AddUint64(&s.bytesWritten, n) }

func (s *Shard) ConnectedClients() uint64 { return uint64(atomic.LoadInt64(&s.connectedClients)) }
func (s *Shard) TotalConnections() uint64 { return atomic.LoadUint64(&s.totalConnections) }
func (s *Shard) BytesRead() uint64        { return atomic.LoadUint64(&s.bytesRead) }
func (s *Shard) BytesWritten() uint64     { return atomic.LoadUint64(&s.bytesWritten) }

func envelopeFileName(shardID uint32) string {
	return fmt.Sprintf("shard-%d.env", shardID)
}

func Run(id ID, cfg *config.Config, key []byte, cryptoEngine *crypto.Engine, logger log.Logger, store *persistence.Storage) (*Shard, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	maxBytes := cfg.GetMaxMemBytes()
	if maxBytes > 0 {
		maxBytes = maxBytes / uint64(cfg.GetNumShards())
	}
	shardLogger := log.NewShardLogger(logger, uint32(id))
	if store == nil {
		store, _ = persistence.NewStorage(false, logger, cfg.GetPersistenceDir())
	}
	// In envelope mode the configured key is a KEK: each shard owns a random DEK
	// wrapped by the KEK, so the KEK never touches data and shards share no key
	// material. The DEK is loaded from the shard's envelope file, or generated and
	// stored on the first boot. A changed KEK is rejected here (fail-closed) rather
	// than silently generating fresh DEKs and bricking the dataset.
	if cfg.EncryptionEnabled() && cfg.EnvelopeEnabled() {
		env, err := crypto.NewEnvelope(key, shardLogger)
		if err != nil {
			return nil, fmt.Errorf("shard %d: envelope init: %w", id, err)
		}
		envDir := envelopeDir(cfg)
		dek, err := env.Load(envDir, envelopeFileName(uint32(id)))
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("shard %d: load envelope: %w", id, err)
			}
			if err = env.GenerateDEK(); err != nil {
				return nil, fmt.Errorf("shard %d: generate DEK: %w", id, err)
			}
			if err = env.Store(envDir, envelopeFileName(uint32(id))); err != nil {
				return nil, fmt.Errorf("shard %d: store envelope: %w", id, err)
			}
			dek = env.DEK()
		}
		cryptoEngine, err = crypto.NewEngine(dek, shardLogger)
		if err != nil {
			return nil, fmt.Errorf("shard %d: data engine init: %w", id, err)
		}
	}
	engine := storage.NewEngine(
		cfg.GetEvictTicker(),
		cfg.GetEvictSlots(),
		maxBytes,
		shardLogger,
		cryptoEngine,
	)
	shard := &Shard{
		ID:          id,
		Engine:      engine,
		Logger:      logger,
		Persistence: store,
	}
	if store.Enabled() {
		if err := store.OpenShard(uint32(id)); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "persistence: cannot open shard",
					log.String("error", err.Error()), log.String("shard", fmt.Sprintf("%d", id)))
			}
			return nil, fmt.Errorf("shard %d: open persistence: %w", id, err)
		}
		if err := store.LoadShard(uint32(id), engine); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "persistence: cannot load shard",
					log.String("error", err.Error()), log.String("shard", fmt.Sprintf("%d", id)))
			}
			return nil, fmt.Errorf("shard %d: load persistence: %w", id, err)
		}
	}
	return shard, nil
}

func (s *Shard) Execute(op string, key string, value []byte, ttl time.Duration) Response {
	switch op {
	case CmdGet:
		val, ok := s.Engine.Get(key)
		return Response{Value: val, OK: ok}
	case CmdSet:
		var expiration time.Time
		if ttl > 0 {
			expiration = time.Now().Add(ttl)
		}
		_, keyExisted := s.Engine.Get(key)
		if s.Persistence.Enabled() {
			if err := s.Persistence.Write(uint32(s.ID), key, value, expiration); err != nil {
				return Response{Err: err}
			}
		}
		if err := s.Engine.Set(key, value, ttl); err != nil {
			if s.Persistence.Enabled() && !keyExisted {
				if delErr := s.Persistence.Delete(uint32(s.ID), key); delErr != nil {
					if s.Logger.Enabled(log.LevelError) {
						s.Logger.Log(log.LevelError, "persistence: compensation delete failed after engine rejection",
							log.String("key", key), log.String("error", delErr.Error()))
					}
				}
			}
			return Response{Err: err}
		}
		return Response{OK: true}
	case CmdDel:
		if s.Persistence.Enabled() {
			if err := s.Persistence.Delete(uint32(s.ID), key); err != nil {
				return Response{Err: err}
			}
		}
		s.Engine.Delete(key)
		return Response{OK: true}
	default:
		if s.Logger.Enabled(log.LevelError) {
			s.Logger.Log(log.LevelError, "shard: unknown operation",
				log.String("op", op),
				log.String("key", key),
			)
		}
		return Response{Err: ErrShardStopped}
	}
}

func (s *Shard) Stop(_ context.Context) error {
	s.Engine.Close()
	return nil
}

// envelopeDir returns where per-shard envelope files live: the configured
// persistence dir, or the platform default data dir when none is set. The envelope
// must stay durable independently of the WAL, so it is written even when
// --enable-persistence is disabled.
func envelopeDir(cfg *config.Config) string {
	if dir := cfg.GetPersistenceDir(); dir != "" {
		return dir
	}
	return defaultDataDir()
}

// defaultDataDir mirrors persistence's platform default so envelope files have a
// durable home when the WAL is disabled. Keep in lockstep with
// persistence.getDefaultDir.
func defaultDataDir() string {
	var baseDir string
	switch runtime.GOOS {
	case "windows":
		baseDir = os.Getenv("APPDATA")
	case "darwin":
		baseDir = filepath.Join(os.Getenv("HOME"), "Library", "Application Support")
	default:
		baseDir = filepath.Join(os.Getenv("HOME"), ".local", "share")
	}
	return filepath.Join(baseDir, "tellstone", "data")
}
