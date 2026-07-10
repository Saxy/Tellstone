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
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/storage"
)

const (
	cmdGet string = "GET"
	cmdSet string = "SET"
	cmdDel string = "DEL"
)

type Shard struct {
	ID     ID
	Engine *storage.Engine
	Logger log.Logger

	// Per-shard network metrics updated atomically from gnet event-loop goroutines.
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
	case cmdGet:
		val, ok := s.Engine.Get(key)
		return Response{Value: val, OK: ok}
	case cmdSet:
		err := s.Engine.Set(key, value, ttl)
		if err != nil {
			return Response{Err: err}
		}
		return Response{OK: true}
	case cmdDel:
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
