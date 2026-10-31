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
	"sync"
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
	CmdSetNX   string = "SETNX"
	CmdSetXX   string = "SETXX"
	CmdDel     string = "DEL"
	CmdPing    string = "PING"
	CmdCommand string = "COMMAND"
	CmdAuth    string = "AUTH"
	CmdRole    string = "ROLE"
	CmdACL     string = "ACL"
)

type Shard struct {
	ID          ID
	Engine      *storage.Engine
	Logger      log.Logger
	Persistence *persistence.Storage
	stats       shardCounters
	// actor, when non-nil, owns every mutation of this shard. See EnableActor.
	actor     *actor
	closeOnce sync.Once
}

func (s *Shard) IncConnectedClients()     { atomic.AddInt64(&s.stats.connectedClients, 1) }
func (s *Shard) DecConnectedClients()     { atomic.AddInt64(&s.stats.connectedClients, -1) }
func (s *Shard) IncTotalConnections()     { atomic.AddUint64(&s.stats.totalConnections, 1) }
func (s *Shard) AddBytesRead(n uint64)    { atomic.AddUint64(&s.stats.bytesRead, n) }
func (s *Shard) AddBytesWritten(n uint64) { atomic.AddUint64(&s.stats.bytesWritten, n) }

func (s *Shard) ConnectedClients() uint64 {
	return uint64(atomic.LoadInt64(&s.stats.connectedClients))
}
func (s *Shard) TotalConnections() uint64 { return atomic.LoadUint64(&s.stats.totalConnections) }
func (s *Shard) BytesRead() uint64        { return atomic.LoadUint64(&s.stats.bytesRead) }
func (s *Shard) BytesWritten() uint64     { return atomic.LoadUint64(&s.stats.bytesWritten) }

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
		if err := store.OpenShard(uint32(id), cryptoEngine); err != nil {
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
	if cfg.ShardActorEnabled() {
		shard.EnableActor(cfg.PinShardsEnabled())
	}
	return shard, nil
}

func (s *Shard) Execute(op string, key string, value []byte, ttl time.Duration) Response {
	if op == CmdGet {
		val, ok := s.Engine.Get(key)
		return Response{Value: val, OK: ok}
	}
	if s.actor != nil && isMutation(op) {
		return s.actor.submit(op, key, value, ttl)
	}
	return s.execute(op, key, value, ttl)
}

// BatchOp is one mutation in an ExecuteBatch call. Op must be CmdSet or CmdDel;
// Value and TTL are ignored for CmdDel.
type BatchOp struct {
	Op    string
	Key   string
	Value []byte
	TTL   time.Duration
}

// ExecuteBatch applies a set of mutations to one shard.
//
// All of the batch's WAL records are made durable with a single flush before any
// of them reaches the engine, so a multi-key transaction pays one flush for the
// shard instead of one per key. The engine is only mutated after its durability
// record is on disk, preserving the single-write path's ordering: replay never
// observes a value the engine rejected.
//
// Responses are returned in input order. When the shard has an actor, the batch
// falls back to one actor submission per operation, because the actor owns every
// mutation; that is correct but loses the single-flush win.
func (s *Shard) ExecuteBatch(ops []BatchOp) []Response {
	if len(ops) == 0 {
		return nil
	}
	if s.actor != nil {
		out := make([]Response, len(ops))
		for i, op := range ops {
			out[i] = s.actor.submit(op.Op, op.Key, op.Value, op.TTL)
		}
		return out
	}
	return s.executeBatch(ops)
}

// executeBatch is the direct (non-actor) batch path. It runs on the caller's
// goroutine and must not be called while the shard's actor owns mutations.
func (s *Shard) executeBatch(ops []BatchOp) []Response {
	out := make([]Response, len(ops))

	records := make([]persistence.BatchRecord, len(ops))
	for i, op := range ops {
		switch op.Op {
		case CmdSet:
			var expiration time.Time
			if op.TTL > 0 {
				expiration = time.Now().Add(op.TTL)
			}
			records[i] = persistence.BatchRecord{Key: op.Key, Value: op.Value, Expiration: expiration}
		case CmdDel:
			records[i] = persistence.BatchRecord{Key: op.Key, Delete: true}
		default:
			err := fmt.Errorf("shard: unsupported batch operation %q", op.Op)
			for j := range out {
				out[j] = Response{Err: err}
			}
			return out
		}
	}

	if s.Persistence != nil && s.Persistence.Enabled() {
		if err := s.Persistence.WriteBatch(uint32(s.ID), records); err != nil {
			for i := range out {
				out[i] = Response{Err: err}
			}
			return out
		}
	}

	for i, op := range ops {
		switch op.Op {
		case CmdSet:
			if err := s.Engine.Set(op.Key, op.Value, op.TTL); err != nil {
				// The record is already durable, so a rejected write would be
				// resurrected on replay. Compensate with a tombstone, but only
				// when the key is absent: if another writer stored its own
				// value, a tombstone would discard it.
				s.compensateFailedSet(op.Key, err)
				out[i] = Response{Err: err}
				continue
			}
			out[i] = Response{OK: true}
		case CmdDel:
			out[i] = Response{OK: s.Engine.Delete(op.Key)}
		}
	}
	return out
}

// compensateFailedSet writes a compensating tombstone for a set whose engine
// write was rejected, unless the key exists (another writer won the slot).
func (s *Shard) compensateFailedSet(key string, cause error) {
	if s.Persistence == nil || !s.Persistence.Enabled() {
		return
	}
	if _, exists := s.Engine.Get(key); exists {
		return
	}
	if delErr := s.Persistence.Delete(uint32(s.ID), key); delErr != nil && s.Logger.Enabled(log.LevelError) {
		s.Logger.Log(log.LevelError, "persistence: compensation delete failed after batch engine rejection",
			log.String("key", key), log.String("error", delErr.Error()), log.String("cause", cause.Error()))
	}
}

// isMutation reports whether op changes shard state and so must be serialized
// on the actor when one is enabled.
func isMutation(op string) bool {
	switch op {
	case CmdSet, CmdSetNX, CmdSetXX, CmdDel:
		return true
	}
	return false
}

// execute runs one operation directly on the caller's goroutine. The actor
// calls it for mutations, so it must not re-enter the actor.
func (s *Shard) execute(op string, key string, value []byte, ttl time.Duration) Response {
	switch op {
	case CmdGet:
		val, ok := s.Engine.Get(key)
		return Response{Value: val, OK: ok}
	case CmdSet:
		var expiration time.Time
		if ttl > 0 {
			expiration = time.Now().Add(ttl)
		}
		// keyExisted only decides whether a failed engine write needs a
		// compensating tombstone below, which is a question that exists only
		// when there is a durability record to compensate. Reading it
		// unconditionally cost a second map lookup on every write, and with
		// encryption on it decrypted a value that was then discarded.
		var keyExisted bool
		if s.Persistence != nil && s.Persistence.Enabled() {
			_, keyExisted = s.Engine.Get(key)
			if err := s.Persistence.Write(uint32(s.ID), key, value, expiration); err != nil {
				return Response{Err: err}
			}
		}
		if err := s.Engine.Set(key, value, ttl); err != nil {
			if s.Persistence != nil && s.Persistence.Enabled() && !keyExisted {
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
	case CmdSetNX, CmdSetXX:
		// Conditional writes. The engine evaluates the precondition and the
		// write under one lock, which is what makes the pair atomic; the
		// caller learns the outcome from Response.OK.
		//
		// The durability record is written only after the engine accepted the
		// write, so a rejected conditional leaves nothing for replay to
		// resurrect. A failure there is compensated against the engine, using
		// the version the engine just stored: the rollback is skipped once
		// another writer has stored its own value, so a concurrent write is
		// never silently discarded.
		var expiration time.Time
		if ttl > 0 {
			expiration = time.Now().Add(ttl)
		}
		var outcome storage.SetOutcome
		var err error
		if op == CmdSetNX {
			outcome, err = s.Engine.SetIfAbsent(key, value, ttl)
		} else {
			outcome, err = s.Engine.SetIfPresent(key, value, ttl)
		}
		if err != nil {
			return Response{Err: err}
		}
		if !outcome.Applied {
			return Response{}
		}
		if s.Persistence != nil && s.Persistence.Enabled() {
			if err = s.Persistence.Write(uint32(s.ID), key, value, expiration); err != nil {
				s.Engine.RestoreIf(key, outcome)
				return Response{Err: err}
			}
		}
		return Response{OK: true}
	case CmdDel:
		if s.Persistence != nil && s.Persistence.Enabled() {
			if err := s.Persistence.Delete(uint32(s.ID), key); err != nil {
				return Response{Err: err}
			}
		}
		// The engine reports whether the key existed so the command layer can
		// count DEL hits without a preceding Get.
		return Response{OK: s.Engine.Delete(key)}
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

// New creates a Shard backed by an in-memory storage engine without
// persistence or configuration. It is a convenience constructor used by
// tests and benchmarks.
func New(id int, evictTicker time.Duration, evictSlots int, maxBytes int, cryptoEngine *crypto.Engine, logger log.Logger) *Shard {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	engine := storage.NewEngine(evictTicker, uint32(evictSlots), uint64(maxBytes), logger, cryptoEngine)
	store, _ := persistence.NewStorage(false, logger, "")
	return &Shard{
		ID:          ID(id),
		Engine:      engine,
		Logger:      logger,
		Persistence: store,
	}
}

// EnableActor starts the shard's actor goroutine, which from then on executes
// every mutation. With pin set, the actor pins itself to a CPU chosen from the
// shard ID. Enable it before the shard serves traffic.
func (s *Shard) EnableActor(pin bool) {
	if s.actor != nil {
		return
	}
	pinCore := -1
	if pin {
		pinCore = int(s.ID) % runtime.NumCPU()
	}
	logger := s.Logger
	s.actor = newActor(s.execute, pinCore, func(err error) {
		if logger != nil && logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "shard: core pinning failed",
				log.String("shard", fmt.Sprintf("%d", s.ID)), log.String("error", err.Error()))
		}
	})
}

// Close stops the actor, if any, and the storage engine. It is safe to call
// more than once.
func (s *Shard) Close() {
	s.closeOnce.Do(func() {
		if s.actor != nil {
			s.actor.stop()
		}
		if s.Engine != nil {
			s.Engine.Close()
		}
	})
}

func (s *Shard) Stop(_ context.Context) error {
	s.Close()
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
