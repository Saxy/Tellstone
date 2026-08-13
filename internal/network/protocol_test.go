package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/command"
)

const benchAddr = "127.0.0.1:9988"

func TestMain(m *testing.M) {
	srv := NewServer(benchAddr, 0, nil, func(msg *Message, c *command.Ctx) ([]byte, MessageType, error) {
		return msg.Payload, MsgResponse, nil
	}, nil, nil, "", nil, nil, newNoOpAudit())

	go func() {
		_ = srv.ListenAndServe()
	}()
	time.Sleep(100 * time.Millisecond)
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	os.Exit(code)
}

func BenchmarkReadMessageZeroAlloc(b *testing.B) {
	// Build a well-formed MsgRequest frame (Op + keyLen + TTL + key + value) so Decode
	// actually exercises the request-parsing branch instead of failing on the length guard.
	key := []byte("benchmark_key")
	value := []byte("benchmark_value_payload")
	reqPayload := make([]byte, 0, 11+len(key)+len(value))
	reqPayload = append(reqPayload, byte(OpGet))
	var keyLenBuf [2]byte
	binary.BigEndian.PutUint16(keyLenBuf[:], uint16(len(key)))
	reqPayload = append(reqPayload, keyLenBuf[:]...)
	var ttlBuf [8]byte
	binary.BigEndian.PutUint64(ttlBuf[:], 0)
	reqPayload = append(reqPayload, ttlBuf[:]...)
	reqPayload = append(reqPayload, key...)
	reqPayload = append(reqPayload, value...)

	total := 1 + len(reqPayload)
	var msgBuf [256]byte

	binary.BigEndian.PutUint32(msgBuf[:4], uint32(total))
	msgBuf[4] = byte(MsgRequest)
	copy(msgBuf[5:], reqPayload)
	data := msgBuf[:4+total]

	var m Message

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Decode(data, uint64(len(data)), &m)
		if err != nil {
			b.Fatalf("read error: %v", err)
		}
	}
}

func BenchmarkGnetServerHandlerParallel(b *testing.B) {
	payload := []byte("benchdata")
	frame := func() []byte {
		totalLen := 1 + len(payload)
		var hdr [5]byte
		binary.BigEndian.PutUint32(hdr[:4], uint32(totalLen))
		hdr[4] = byte(MsgPing)
		out := make([]byte, 5+len(payload))
		copy(out, hdr[:])
		copy(out[5:], payload)
		return out
	}()
	numCores := runtime.GOMAXPROCS(0)
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", benchAddr)
			if err != nil {
				b.Errorf("dial failed: %v", err)
				return
			}
			defer conn.Close()
			respBuf := make([]byte, 5+len(payload))
			iters := b.N / numCores
			if iters == 0 {
				iters = 1
			}
			for j := 0; j < iters; j++ {
				if _, err := conn.Write(frame); err != nil {
					return
				}
				if _, err := io.ReadFull(conn, respBuf); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
}

// Test Decode returns proper errors for malformed inputs.
func TestDecodeErrors(t *testing.T) {
	if _, err := Decode([]byte{0, 0, 0}, uint64(len([]byte{0, 0, 0})), &Message{}); !errors.Is(err, errShortRead) {
		t.Fatalf("expected errShortRead, got %v", err)
	}
	data := []byte{0, 0, 0, 0, byte(MsgPing)}
	if _, err := Decode(data, uint64(len(data)), &Message{}); !errors.Is(err, errZeroLength) {
		t.Fatalf("expected errZeroLength, got %v", err)
	}
	data = []byte{0, 0, 0, 5, byte(MsgPing), 'a', 'b', 'c'}
	if _, err := Decode(data, uint64(len(data)), &Message{}); !errors.Is(err, errShortRead) {
		t.Fatalf("expected errShortRead for insufficient payload, got %v", err)
	}
}

// Test Write produces the correct wire format.
func TestWriteOutput(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		if err := Write(server, MsgResponse, []byte("data")); err != nil {
			t.Errorf("Write error: %v", err)
		}
		server.Close()
	}()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, client); err != nil {
		t.Fatalf("io.Copy error: %v", err)
	}
	raw := buf.Bytes()
	if len(raw) != 4+1+4 {
		t.Fatalf("unexpected raw length: %d", len(raw))
	}
	length := uint32(raw[0])<<24 | uint32(raw[1])<<16 | uint32(raw[2])<<8 | uint32(raw[3])
	if length != 1+4 {
		t.Fatalf("length prefix mismatch: got %d want %d", length, 5)
	}
	if MessageType(raw[4]) != MsgResponse {
		t.Fatalf("type byte mismatch: got %d want %d", raw[4], MsgResponse)
	}
	if !bytes.Equal(raw[5:], []byte("data")) {
		t.Fatalf("payload mismatch: %s", raw[5:])
	}
}

// Test Read correctly populates a Message using a supplied buffer.
func TestReadPopulatesMessage(t *testing.T) {
	payload := []byte("hello")
	var hdr [5]byte
	total := 1 + len(payload)
	hdr[0] = byte(total >> 24)
	hdr[1] = byte(total >> 16)
	hdr[2] = byte(total >> 8)
	hdr[3] = byte(total)
	hdr[4] = byte(MsgPing)
	data := append(hdr[:], payload...)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		server.Write(data)
		server.Close()
	}()
	var buf [64]byte
	var msg Message
	if err := Read(client, buf[:], &msg); err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if msg.Type != MsgPing {
		t.Fatalf("type mismatch: got %v want %v", msg.Type, MsgPing)
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Fatalf("payload mismatch: got %s want %s", msg.Payload, payload)
	}
}
