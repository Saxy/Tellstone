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
	"net/http"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/app/tellstone"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/metrics"
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
	logger := s.app.GetLogger()
	cfg := s.app.GetConfig()
	cryptoEngine := s.initCrypto()
	s.engine = s.initStorage(cryptoEngine)
	defer s.engine.Close()
	srv := network.NewServer(
		cfg.GetAddr(),
		cfg.GetMaxMsgSize(),
		s.networkHandler,
		logger,
	)
	if cfg.MetricsEnabled() {
		s.startMetricsServer(srv)
	}
	if err := srv.ListenAndServe(); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "tcp error", log.String("error", err.Error()))
		}
	}
}

// initCrypto configures and validates the cryptographic subsystem if enabled.
func (s *Server) initCrypto() *crypto.Engine {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	if !cfg.EncryptionEnabled() {
		return nil
	}
	cryptoEngine, err := crypto.NewEngine([]byte(cfg.GetEncryptionKey()), logger)
	if err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "crypto engine setup failed", log.String("error", err.Error()))
		}
		panic("crypto engine initialization failed: " + err.Error())
	}
	return cryptoEngine
}

// initStorage instantiates the underlying memory engine with the required timing wheels.
func (s *Server) initStorage(cryptoEngine *crypto.Engine) *storage.Engine {
	cfg := s.app.GetConfig()
	return storage.NewEngine(
		cfg.GetEvictTicker(),
		cfg.GetEvictSlots(),
		cfg.GetMaxMemBytes(),
		s.app.GetLogger(),
		cryptoEngine,
	)
}

// startMetricsServer boots the asynchronous Prometheus text exporter on a separate HTTP port.
func (s *Server) startMetricsServer(srv *network.Server) {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	metricsAddr := cfg.GetMetricsAddr()
	collector := metrics.NewCollector(s.engine, srv, s.app.GetLogger())
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		collector.WritePrometheus(w)
	})
	httpSrv := &http.Server{
		Addr:         metricsAddr,
		Handler:      mux,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
	go func() {
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "telemetry infrastructure online", log.String("addr", metricsAddr))
		}
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "metrics server encountered an error", log.String("error", err.Error()))
			}
		}
	}()
}

// networkHandler unmarshals input frames, proxies operations to storage, and generates network responses.
func (s *Server) networkHandler(msg *network.Message) ([]byte, network.MessageType, error) {
	if msg.Type == network.MsgPing {
		return nil, network.MsgPong, nil
	}
	keyStr := *(*string)(unsafe.Pointer(&msg.Key))
	switch msg.Op {
	case network.OpGet:
		val, ok := s.engine.Get(keyStr)
		if !ok {
			return network.ResponseNotFound, network.MsgResponse, nil
		}
		return val, network.MsgResponse, nil
	case network.OpSet:
		if len(msg.Key) == 0 {
			return network.ResponseEmptyKey, network.MsgResponse, ErrEmptyKey
		}
		ttlDuration := time.Duration(msg.TTL) * time.Millisecond
		if err := s.engine.Set(keyStr, msg.Value, ttlDuration); err != nil {
			if s.app.GetLogger().Enabled(log.LevelError) {
				s.app.GetLogger().Log(log.LevelError, "failed to store inside storage engine", log.String("error", err.Error()))
			}
			return network.ResponseStorageFailure, network.MsgResponse, ErrStorageFailure
		}
		return network.ResponseOK, network.MsgResponse, nil
	case network.OpDelete:
		s.engine.Delete(keyStr)
		return network.ResponseOK, network.MsgResponse, nil
	default:
		return network.ResponseInvalidOpCode, network.MsgResponse, ErrInvalidOpCode
	}
}
