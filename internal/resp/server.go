/*
Package resp
Tellstone Redis-Compatible Wire Protocol
File: server.go
Description: Optional gnet event-loop server speaking RESP2, reusing the shared storage engine
via a small Store interface. Supports PING, GET, SET (with optional EX/PX), and DEL; unknown
commands return an error without dropping the connection. Exists so Tellstone can be driven by
standard Redis tooling (redis-benchmark, memtier_benchmark) for cross-system comparison.

Authors:

	Maximilian Hagen
*/
package resp

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/shard"
	"github.com/panjf2000/gnet/v2"
)

// Store is the subset of the storage engine the RESP server needs. *storage.Engine satisfies
// it directly, which keeps this package decoupled and easy to test with a fake.
type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, ttl time.Duration) error
	Delete(key string)
}

// connState holds per-connection scratch buffers reused across OnTraffic calls so the hot
// path stays allocation-free, plus the assigned shard index for per-shard metrics.
type connState struct {
	out     []byte
	args    [][]byte
	shardID int
}

// Server is an edge-triggered RESP2 listener backed by gnet.
type Server struct {
	gnet.BuiltinEventEngine
	addr   string
	store  Store
	logger log.Logger
	// eng and ready let Shutdown reach the running gnet engine: OnBoot fires once the event
	// loop is accepting connections and hands us the Engine handle we need to stop it; ready
	// is closed at that point so a concurrent Shutdown call can block until it's safe to stop.
	eng              gnet.Engine
	ready            chan struct{}
	connectedClients uint64
	totalConnections uint64
	bytesRead        uint64
	bytesWritten     uint64
	protocolErrors   uint64
	shards           []*shard.Shard
	nextConn         uint64
}

// NewServer creates a RESP server bound to addr that dispatches commands to store.
// shards is optional — if nil, per-shard metrics are not tracked.
func NewServer(addr string, store Store, shards []*shard.Shard, logger log.Logger) *Server {
	if logger == nil {
		logger = log.NewNoOpLogger()
	}
	return &Server{
		addr:   addr,
		store:  store,
		shards: shards,
		logger: logger,
		ready:  make(chan struct{}),
	}
}

// ListenAndServe starts the multi-reactor epoll event loop (blocking).
func (s *Server) ListenAndServe() error {
	if s.logger.Enabled(log.LevelInfo) {
		s.logger.Log(log.LevelInfo, "resp: event-driven engine initializing", log.String("address", s.addr))
	}
	return gnet.Run(s, "tcp://"+s.addr, gnet.WithMulticore(true))
}

// Shutdown gracefully stops the event loop, waiting for in-flight connections to drain or
// ctx to expire. It blocks until ListenAndServe has reached OnBoot, so it is safe to call
// concurrently with ListenAndServe from another goroutine (e.g. a signal handler).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.eng.Stop(ctx)
}

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	close(s.ready)
	return gnet.None
}

func (s *Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, 1)
	atomic.AddUint64(&s.totalConnections, 1)
	shardID := -1
	if len(s.shards) > 0 {
		sid := atomic.AddUint64(&s.nextConn, 1) - 1
		sid = sid % uint64(len(s.shards))
		shardID = int(sid)
		s.shards[sid].IncConnectedClients()
		s.shards[sid].IncTotalConnections()
	}
	c.SetContext(&connState{
		out:     make([]byte, 0, 4096),
		args:    make([][]byte, 0, 8),
		shardID: shardID,
	})
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddUint64(&s.connectedClients, ^uint64(0))
	if st, ok := c.Context().(*connState); ok && st.shardID >= 0 && st.shardID < len(s.shards) {
		s.shards[st.shardID].DecConnectedClients()
	}
	return gnet.None
}

// OnTraffic parses every complete command currently buffered, batches all replies into a
// single write, and advances the inbound buffer once — which makes pipelined workloads
// (redis-benchmark -P / memtier --pipeline) amortize syscalls.
func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	st, _ := c.Context().(*connState)
	if st == nil {
		st = &connState{out: make([]byte, 0, 4096), args: make([][]byte, 0, 8)}
		c.SetContext(st)
	}
	buf, err := c.Peek(-1)
	if err != nil {
		return gnet.Close
	}
	st.out = st.out[:0]
	consumed := 0
	for consumed < len(buf) {
		args, n, perr := Parse(buf[consumed:], st.args)
		if perr != nil {
			if errors.Is(perr, errIncomplete) {
				break
			}
			atomic.AddUint64(&s.protocolErrors, 1)
			if s.logger.Enabled(log.LevelWarn) {
				s.logger.Log(log.LevelWarn, "resp: malformed frame; closing connection",
					log.String("remote_addr", c.RemoteAddr().String()),
				)
			}
			return gnet.Close
		}
		st.args = args[:0] // reuse the backing array next iteration
		consumed += n
		st.out = s.dispatch(args, st.out)
	}
	if consumed == 0 {
		return gnet.None
	}
	if len(st.out) > 0 {
		if _, err := c.Write(st.out); err != nil {
			return gnet.Close
		}
		n := uint64(len(st.out))
		atomic.AddUint64(&s.bytesWritten, n)
		if st.shardID >= 0 && st.shardID < len(s.shards) {
			s.shards[st.shardID].AddBytesWritten(n)
		}
	}
	if _, err := c.Discard(consumed); err != nil {
		return gnet.Close
	}
	n := uint64(consumed)
	atomic.AddUint64(&s.bytesRead, n)
	if st.shardID >= 0 && st.shardID < len(s.shards) {
		s.shards[st.shardID].AddBytesRead(n)
	}
	return gnet.None
}

// dispatch executes a single command and appends its RESP reply to out.
//
// Lookup keys use a zero-copy unsafe string over the argument bytes (which alias the gnet read
// buffer): this is safe because Get does not retain the key, and Set clones the key and copies
// the value before storing them.
func (s *Server) dispatch(args [][]byte, out []byte) []byte {
	if len(args) == 0 {
		return AppendError(out, "ERR empty command")
	}
	cmd := args[0]
	switch {
	case EqualFold(cmd, shard.CmdGet):
		if len(args) != 2 {
			return AppendError(out, "ERR wrong number of arguments for 'get' command")
		}
		key := *(*string)(unsafe.Pointer(&args[1]))
		val, ok := s.store.Get(key)
		if !ok {
			return AppendNullBulk(out)
		}
		return AppendBulk(out, val)

	case EqualFold(cmd, shard.CmdSet):
		if len(args) != 3 && len(args) != 5 {
			return AppendError(out, "ERR wrong number of arguments for 'set' command")
		}
		key := *(*string)(unsafe.Pointer(&args[1]))
		ttl, ok := parseSetTTL(args)
		if !ok {
			return AppendError(out, "ERR syntax error")
		}
		if err := s.store.Set(key, args[2], ttl); err != nil {
			return AppendError(out, "ERR "+err.Error())
		}
		return append(out, respOK...)

	case EqualFold(cmd, shard.CmdDel):
		if len(args) < 2 {
			return AppendError(out, "ERR wrong number of arguments for 'del' command")
		}
		var n int64
		for _, k := range args[1:] {
			ks := *(*string)(unsafe.Pointer(&k))
			if _, ok := s.store.Get(ks); ok {
				s.store.Delete(ks)
				n++
			}
		}
		return AppendInt(out, n)

	case EqualFold(cmd, shard.CmdPing):
		if len(args) >= 2 {
			return AppendBulk(out, args[1])
		}
		return append(out, respPong...)

	case EqualFold(cmd, shard.CmdCommand):
		// redis-cli / some tools probe COMMAND DOCS|COUNT at startup; an empty array keeps
		// the session alive without implementing the introspection surface.
		return append(out, "*0\r\n"...)

	default:
		return AppendError(out, "ERR unknown command '"+string(cmd)+"'")
	}
}

// parseSetTTL extracts the TTL from a SET command. Returns (0, true) for a plain 3-arg SET,
// the parsed duration for a valid "EX <s>" / "PX <ms>" 5-arg SET, and (_, false) on a syntax
// error.
func parseSetTTL(args [][]byte) (time.Duration, bool) {
	if len(args) == 3 {
		return 0, true
	}
	v, err := strconv.Atoi(unsafe.String(unsafe.SliceData(args[4]), len(args[4])))
	if err != nil || v < 0 {
		return 0, false
	}
	switch {
	case EqualFold(args[3], "EX"):
		return time.Duration(v) * time.Second, true
	case EqualFold(args[3], "PX"):
		return time.Duration(v) * time.Millisecond, true
	default:
		return 0, false
	}
}

func (s *Server) ConnectedClients() uint64 { return atomic.LoadUint64(&s.connectedClients) }
func (s *Server) TotalConnections() uint64 { return atomic.LoadUint64(&s.totalConnections) }
func (s *Server) BytesRead() uint64        { return atomic.LoadUint64(&s.bytesRead) }
func (s *Server) BytesWritten() uint64     { return atomic.LoadUint64(&s.bytesWritten) }
func (s *Server) ProtocolErrors() uint64   { return atomic.LoadUint64(&s.protocolErrors) }
