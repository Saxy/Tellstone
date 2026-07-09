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
	"time"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/storage"
)

type Shard struct {
	ID     ID
	Engine *storage.Engine
	Logger log.Logger
}

func Run(id ID, cfg *config.Config, cryptoEngine *crypto.Engine, logger log.Logger) (*Shard, error) {
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
	return &Shard{
		ID:     id,
		Engine: engine,
		Logger: logger,
	}, nil
}

func (s *Shard) Execute(op string, key string, value []byte, ttl time.Duration) Response {
	switch op {
	case "GET":
		val, ok := s.Engine.Get(key)
		return Response{Value: val, OK: ok}
	case "SET":
		err := s.Engine.Set(key, value, ttl)
		if err != nil {
			return Response{Err: err}
		}
		return Response{OK: true}
	case "DEL":
		s.Engine.Delete(key)
		return Response{OK: true}
	default:
		return Response{Err: ErrShardStopped}
	}
}

func (s *Shard) Stop(ctx context.Context) error {
	s.Engine.Close()
	return nil
}
