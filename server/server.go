/*
Package server
Tellstone Cloud-Native In-Memory Database
File: server.go
Description: High‑level orchestration of all components (app, crypto, logger, network, storage). Exposes public error variables for client misuse and internal failures.

Authors:

	Maximilian Hagen
*/
package server

import (
	"errors"
	"net"
	"time"

	"github.com/Saxy/Tellstone/internal/app/tellstone"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/storage"
)

var (
	// ErrEmptyKey is returned when a client attempts a SET execution with a zero-length identifier.
	ErrEmptyKey = errors.New("set requires a key")
	// ErrStorageFailure is returned when the storage engine rejects data synchronization.
	ErrStorageFailure = errors.New("failed to store inside storage engine")
	// ErrInvalidOpCode is returned when the unpacked message frame contains an unknown engine operation.
	ErrInvalidOpCode = errors.New("unsupported protocol operation")
)

type Server struct {
	app    *tellstone.App
	engine *storage.Engine
}

// NewServer initializes a bare server abstraction waiting for network stack execution.
func NewServer(app *tellstone.App) *Server {
	return &Server{
		app: app,
	}
}

// Run configures cryptography, allocates the storage ring engine, and boots up the gnet event loop.
func (s *Server) Run() {
	var cryptoEngine *crypto.Engine
	logger := s.app.GetLogger()
	cfg := s.app.GetConfig()

	if s.app.EncryptionEnabled() {
		var err error
		cryptoEngine, err = crypto.NewEngine([]byte(cfg.GetEncryptionKey()), logger)
		if err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "crypto engine setup failed", log.String("error", err.Error()))
			}
			panic("crypto engine initialization failed: " + err.Error())
		}
	}
	storageEngine := storage.NewEngine(
		cfg.GetEvictTicker(),
		cfg.GetEvictSlots(),
		cfg.GetMaxMsgSize(),
		logger,
		cryptoEngine,
	)
	s.engine = storageEngine
	defer s.engine.Close()
	srv := network.NewServer(
		cfg.GetAddr(),
		cfg.GetMaxMsgSize(),
		s.networkHandler,
		logger,
	)
	if err := srv.ListenAndServe(); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "tcp error", log.String("error", err.Error()))
		}
	}
}

// networkHandler unmarshals input frames, proxies operations to storage, and generates network responses.
func (s *Server) networkHandler(msg *network.Message) ([]byte, network.MessageType, error) {
	if msg.Type == network.MsgPing {
		return nil, network.MsgPong, nil
	}
	switch msg.Op {
	case network.OpGet:
		val, ok := s.engine.Get(string(msg.Key))
		if !ok {
			return network.ResponseNotFound, network.MsgResponse, nil
		}
		return val, network.MsgResponse, nil
	case network.OpSet:
		if len(msg.Key) == 0 {
			return network.ResponseEmptyKey, network.MsgResponse, ErrEmptyKey
		}
		ttlDuration := time.Duration(msg.TTL) * time.Millisecond
		if err := s.engine.Set(string(msg.Key), msg.Value, ttlDuration); err != nil {
			if s.app.GetLogger().Enabled(log.LevelError) {
				s.app.GetLogger().Log(log.LevelError, "failed to store inside storage engine", log.String("error", err.Error()))
			}
			return network.ResponseStorageFailure, network.MsgResponse, ErrStorageFailure
		}
		return network.ResponseOK, network.MsgResponse, nil
	case network.OpDelete:
		s.engine.Delete(string(msg.Key))
		return network.ResponseOK, network.MsgResponse, nil
	default:
		return network.ResponseInvalidOpCode, network.MsgResponse, ErrInvalidOpCode
	}
}
