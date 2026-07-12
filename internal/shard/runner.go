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
	"fmt"
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

func Run(id ID, cfg *config.Config, cryptoEngine *crypto.Engine, logger log.Logger, store *persistence.Storage) (*Shard, error) {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	maxBytes := cfg.GetMaxMemBytes()
	if maxBytes > 0 {
		maxBytes = maxBytes / uint64(cfg.GetNumShards())
	}
	shardLogger := log.NewShardLogger(logger, uint32(id))
	engine := storage.NewEngine(
		cfg.GetEvictTicker(),
		cfg.GetEvictSlots(),
		maxBytes,
		shardLogger,
		cryptoEngine,
	)
	if store == nil {
		store, _ = persistence.NewStorage(false, logger, "")
	}
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
		if s.Persistence.Enabled() {
			if err := s.Persistence.Write(uint32(s.ID), key, value, expiration); err != nil {
				return Response{Err: err}
			}
		}
		if err := s.Engine.Set(key, value, ttl); err != nil {
			if s.Persistence.Enabled() {
				if delErr := s.Persistence.Delete(uint32(s.ID), key); delErr != nil {
					s.Logger.Log(log.LevelError, "persistence: compensation delete failed after engine rejection",
						log.String("key", key), log.String("error", delErr.Error()))
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
		return Response{Err: ErrShardStopped}
	}
}

func (s *Shard) Stop(_ context.Context) error {
	s.Engine.Close()
	return nil
}
