/*
Package network
Tellstone Cloud-Native In-Memory Database
File: pipeline_integration_test.go
Description: Phase 5 (net) pipeline request/response tests over a real transport
pair: forwarded-op roundtrip, framed error responses, keepalive ping/pong, and
concurrent calls sharing one TCP connection (request-ID correlation).

Authors:

	Maximilian Hagen
*/
package network

import (
	"context"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

func mkPipePair(t *testing.T) (*Transport, *Transport) {
	t.Helper()
	logger := log.NewNoOpLogger()
	tr1 := NewTransport("127.0.0.1:0", 1, func(msg *pb.Message) {}, logger)
	tr2 := NewTransport("127.0.0.1:0", 2, func(msg *pb.Message) {}, logger)
	if err := tr1.Listen(); err != nil {
		t.Fatalf("listen1: %v", err)
	}
	if err := tr2.Listen(); err != nil {
		t.Fatalf("listen2: %v", err)
	}
	addr1 := tr1.Addr()
	addr2 := tr2.Addr()
	t.Cleanup(func() {
		tr1.Stop()
		tr2.Stop()
	})
	tr1.Pipeline(1, logger).RegisterHandler(77, func(op OpKind, payload []byte) ([]byte, error) {
		return []byte("ack:" + string(payload)), nil
	})
	tr2.Pipeline(2, logger).RegisterHandler(77, func(op OpKind, payload []byte) ([]byte, error) {
		return []byte("ack:" + string(payload)), nil
	})
	tr1.RegisterPeer(2, addr2)
	tr2.RegisterPeer(1, addr1)
	return tr1, tr2
}

// TestPipelineForwardRoundtrip proves a Call from node 1 to node 2 reaches the
// registered handler and the framed response returns to the caller.
func TestPipelineForwardRoundtrip(t *testing.T) {
	tr1, _ := mkPipePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := tr1.Pipeline(1, log.NewNoOpLogger()).Call(ctx, 2, 77, OpForwardWrite, []byte("hello"))
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(resp) != "ack:hello" {
		t.Fatalf("unexpected response %q", resp)
	}
}

// TestPipelineErrorResponse proves a handler error is framed and returned to
// the caller as an error, not a success payload.
func TestPipelineErrorResponse(t *testing.T) {
	logger := log.NewNoOpLogger()
	tr1 := NewTransport("127.0.0.1:0", 1, func(msg *pb.Message) {}, logger)
	tr2 := NewTransport("127.0.0.1:0", 2, func(msg *pb.Message) {}, logger)
	if err := tr1.Listen(); err != nil {
		t.Fatalf("listen1: %v", err)
	}
	if err := tr2.Listen(); err != nil {
		t.Fatalf("listen2: %v", err)
	}
	t.Cleanup(func() {
		tr1.Stop()
		tr2.Stop()
	})
	tr2.Pipeline(2, logger).RegisterHandler(77, func(op OpKind, payload []byte) ([]byte, error) {
		return nil, errConnectionClosed
	})
	tr1.RegisterPeer(2, tr2.Addr())
	tr2.RegisterPeer(1, tr1.Addr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := tr1.Pipeline(1, logger).Call(ctx, 2, 77, OpForwardWrite, []byte("x"))
	if err == nil {
		t.Fatal("expected handler error to propagate")
	}
}

// TestPipelinePingPong proves keepalive-style pings are answered automatically
// without a registered handler.
func TestPipelinePingPong(t *testing.T) {
	tr1, _ := mkPipePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := tr1.Pipeline(1, log.NewNoOpLogger()).Call(ctx, 2, 0, OpPing, nil)
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("unexpected pong payload %q", resp)
	}
}

// TestPipelineConcurrentCalls pumps many concurrent calls to prove reqID
// correlation survives interleaving on a single TCP connection.
func TestPipelineConcurrentCalls(t *testing.T) {
	tr1, _ := mkPipePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const n = 50
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			payload := []byte{'a' + byte(i%26)}
			resp, err := tr1.Pipeline(1, log.NewNoOpLogger()).Call(ctx, 2, 77, OpForwardWrite, payload)
			if err != nil {
				errs <- err
				return
			}
			if string(resp) != "ack:"+string(payload) {
				errs <- &unexpectedResp{s: string(resp)}
				return
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent call %d: %v", i, err)
		}
	}
}

type unexpectedResp struct{ s string }

func (u *unexpectedResp) Error() string { return "unexpected response " + u.s }
