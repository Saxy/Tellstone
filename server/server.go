/*
Package server
Tellstone Cloud-Native In-Memory Database
File: server.go
Description: Top-level server orchestration: initializes the shared-nothing shards, router, binary-protocol listener, optional RESP listener, and metrics server. Handles graceful shutdown on SIGINT/SIGTERM.

Authors:

	Maximilian Hagen
*/
package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/app/tellstone"
	"github.com/Saxy/Tellstone/internal/crypto"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/metrics"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/resp"
	"github.com/Saxy/Tellstone/internal/router"
	"github.com/Saxy/Tellstone/internal/shard"
)

var (
	ErrEmptyKey       = errors.New("set requires a key")
	ErrStorageFailure = errors.New("failed to store inside storage engine")
	ErrInvalidOpCode  = errors.New("unsupported protocol operation")
)

type RouterStore struct {
	router *router.Router
}

func (rs *RouterStore) Get(key string) ([]byte, bool) {
	resp := rs.router.Dispatch("GET", key, nil, 0)
	return resp.Value, resp.OK
}

func (rs *RouterStore) Set(key string, value []byte, ttl time.Duration) error {
	resp := rs.router.Dispatch("SET", key, value, ttl)
	return resp.Err
}

func (rs *RouterStore) Delete(key string) {
	rs.router.Dispatch("DEL", key, nil, 0)
}

type Server struct {
	app        *tellstone.App
	router     *router.Router
	shards     []*shard.Shard
	netSrv     *network.Server
	respSrv    *resp.Server
	metricsSrv *http.Server
}

func NewServer(app *tellstone.App) *Server {
	return &Server{
		app: app,
	}
}

func (s *Server) Run() {
	logger := s.app.GetLogger()
	cfg := s.app.GetConfig()
	cryptoEngine := s.initCrypto()
	s.initShards(cryptoEngine)
	s.netSrv = network.NewServer(
		cfg.GetAddr(),
		cfg.GetMaxMsgSize(),
		s.shards,
		s.networkHandler,
		logger,
	)
	if cfg.MetricsEnabled() {
		s.startMetricsServer(s.netSrv)
	}
	if cfg.RESPEnabled() {
		s.startRESPServer()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
		if logger.Enabled(log.LevelInfo) {
			logger.Log(log.LevelInfo, "shutdown signal received, draining connections")
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.GetShutdownTimeout())
		defer cancel()
		s.shutdown(shutdownCtx)
	}()

	if err := s.netSrv.ListenAndServe(); err != nil {
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "tcp error", log.String("error", err.Error()))
		}
	}
}

func (s *Server) shutdown(ctx context.Context) {
	logger := s.app.GetLogger()
	if s.respSrv != nil {
		if err := s.respSrv.Shutdown(ctx); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "resp server shutdown error", log.String("error", err.Error()))
			}
		}
	}
	if s.metricsSrv != nil {
		if err := s.metricsSrv.Shutdown(ctx); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "metrics server shutdown error", log.String("error", err.Error()))
			}
		}
	}
	if err := s.netSrv.Shutdown(ctx); err != nil {
		if logger.Enabled(log.LevelError) {
			logger.Log(log.LevelError, "tcp server shutdown error", log.String("error", err.Error()))
		}
	}
	for _, sh := range s.shards {
		if err := sh.Stop(ctx); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "shard shutdown error",
					log.Uint64("shard_id", uint64(sh.ID)),
					log.String("error", err.Error()),
				)
			}
		}
	}
}

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

func (s *Server) initShards(cryptoEngine *crypto.Engine) {
	cfg := s.app.GetConfig()
	numShards := cfg.GetNumShards()
	logger := s.app.GetLogger()

	s.shards = make([]*shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		sh, err := shard.Run(shard.ID(i), cfg, cryptoEngine, logger)
		if err != nil {
			panic("shard init: " + err.Error())
		}
		s.shards[i] = sh
	}
	s.router = router.New(s.shards)
}

func (s *Server) startMetricsServer(srv *network.Server) {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	metricsAddr := cfg.GetMetricsAddr()
	shardCollectors := make([]*metrics.Collector, len(s.shards))
	for i, sh := range s.shards {
		shardCollectors[i] = metrics.NewShardCollector(uint32(sh.ID), sh, sh.Engine, srv, logger)
	}
	aggregateCollector := metrics.NewAggregateCollector(shardCollectors, srv)
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		aggregateCollector.WritePrometheus(w)
	})
	httpSrv := &http.Server{
		Addr:         metricsAddr,
		Handler:      mux,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
	s.metricsSrv = httpSrv
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

func (s *Server) startRESPServer() {
	cfg := s.app.GetConfig()
	logger := s.app.GetLogger()
	store := &RouterStore{router: s.router}
	respSrv := resp.NewServer(cfg.GetRESPAddr(), store, s.shards, logger)
	s.respSrv = respSrv
	go func() {
		if err := respSrv.ListenAndServe(); err != nil {
			if logger.Enabled(log.LevelError) {
				logger.Log(log.LevelError, "resp server encountered an error", log.String("error", err.Error()))
			}
		}
	}()
}

func (s *Server) networkHandler(msg *network.Message) ([]byte, network.MessageType, error) {
	if msg.Type == network.MsgPing {
		return nil, network.MsgPong, nil
	}
	keyStr := *(*string)(unsafe.Pointer(&msg.Key))
	switch msg.Op {
	case network.OpGet:
		resp := s.router.Dispatch("GET", keyStr, nil, 0)
		if !resp.OK {
			return network.ResponseNotFound, network.MsgResponse, nil
		}
		return resp.Value, network.MsgResponse, nil
	case network.OpSet:
		if len(msg.Key) == 0 {
			return network.ResponseEmptyKey, network.MsgResponse, ErrEmptyKey
		}
		ttlDuration := time.Duration(msg.TTL) * time.Millisecond
		resp := s.router.Dispatch("SET", keyStr, msg.Value, ttlDuration)
		if resp.Err != nil {
			if s.app.GetLogger().Enabled(log.LevelError) {
				s.app.GetLogger().Log(log.LevelError, "failed to store inside storage engine", log.String("error", resp.Err.Error()))
			}
			return network.ResponseStorageFailure, network.MsgResponse, ErrStorageFailure
		}
		return network.ResponseOK, network.MsgResponse, nil
	case network.OpDelete:
		s.router.Dispatch("DEL", keyStr, nil, 0)
		return network.ResponseOK, network.MsgResponse, nil
	default:
		return network.ResponseNotFound, network.MsgResponse, ErrInvalidOpCode
	}
}
