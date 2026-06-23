package network

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/gnet/v2"
)

type NOOPLogger struct{}

func (N NOOPLogger) Debugf(format string, args ...any) {}
func (N NOOPLogger) Infof(format string, args ...any)  {}
func (N NOOPLogger) Warnf(format string, args ...any)  {}
func (N NOOPLogger) Errorf(format string, args ...any) {}
func (N NOOPLogger) Fatalf(format string, args ...any) {}

const benchAddr = "127.0.0.1:9988"

func TestMain(m *testing.M) {
	srv := NewServer(benchAddr, func(msg *Message) ([]byte, MessageType, error) {
		return msg.Payload, MsgResponse, nil
	}, nil)

	go func() {
		_ = srv.ListenAndServe()
	}()
	time.Sleep(100 * time.Millisecond)
	os.Exit(m.Run())
}

func BenchmarkReadMessageZeroAlloc(b *testing.B) {
	var payloadArr = [16]byte{'b', 'e', 'n', 'c', 'h', 'm', 'a', 'r', 'k', ' ', 'p', 'a', 'y', 'l', 'o', 'a'}
	payload := payloadArr[:]
	total := 1 + len(payload)
	var msgBuf [256]byte

	binary.BigEndian.PutUint32(msgBuf[:4], uint32(total))
	msgBuf[4] = byte(MsgRequest)
	copy(msgBuf[5:], payload)
	data := msgBuf[:4+total]

	var m Message

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Decode(data, &m)
		if err != nil {
			b.Fatalf("read error: %v", err)
		}
	}
}

type benchClient struct {
	gnet.BuiltinEventEngine
	payload   []byte
	remaining int
	done      chan struct{}
}

func (bc *benchClient) OnBoot(eng gnet.Engine) gnet.Action {
	return gnet.None
}

func (bc *benchClient) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	var hdr [5]byte
	totalLen := 1 + len(bc.payload)
	binary.BigEndian.PutUint32(hdr[:4], uint32(totalLen))
	hdr[4] = byte(MsgPing)

	_, _ = c.Write(hdr[:])
	_, _ = c.Write(bc.payload)
	return nil, gnet.None
}

func (bc *benchClient) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Peek(-1)
	var msg Message
	payloadLen, err := Decode(buf, &msg)
	if err != nil {
		return gnet.None
	}
	_, _ = c.Discard(5 + payloadLen)
	bc.remaining--
	if bc.remaining <= 0 {
		close(bc.done)
		return gnet.Close
	}
	var hdr [5]byte
	totalLen := 1 + len(bc.payload)
	binary.BigEndian.PutUint32(hdr[:4], uint32(totalLen))
	hdr[4] = byte(MsgPing)
	_, _ = c.Write(hdr[:])
	_, _ = c.Write(bc.payload)
	return gnet.None
}

func BenchmarkGnetServerHandlerParallel(b *testing.B) {
	payload := []byte("benchdata")
	numCores := runtime.GOMAXPROCS(0)
	itersPerClient := b.N / numCores
	if itersPerClient == 0 {
		itersPerClient = 1
	}
	clients := make([]*benchClient, numCores)
	engines := make([]gnet.Client, numCores)
	channels := make([]chan struct{}, numCores)
	for i := 0; i < numCores; i++ {
		channels[i] = make(chan struct{})
		clients[i] = &benchClient{
			payload:   payload,
			remaining: itersPerClient,
			done:      channels[i],
		}
		eng, err := gnet.NewClient(clients[i], gnet.WithMulticore(true), gnet.WithLogger(NOOPLogger{}))
		if err != nil {
			b.Fatalf("failed to create client: %v", err)
		}
		if err := eng.Start(); err != nil {
			b.Fatalf("failed to start client engine: %v", err)
		}
		engines[i] = *eng
	}
	time.Sleep(50 * time.Millisecond)
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _ = engines[idx].Dial("tcp", benchAddr)
			<-channels[idx]
		}(i)
	}
	wg.Wait()
	b.StopTimer()
	for i := 0; i < numCores; i++ {
		_ = engines[i].Stop()
	}
}

// Test Decode returns proper errors for malformed inputs.
func TestDecodeErrors(t *testing.T) {
	if _, err := Decode([]byte{0, 0, 0}, &Message{}); !errors.Is(err, errShortRead) {
		t.Fatalf("expected errShortRead, got %v", err)
	}
	data := []byte{0, 0, 0, 0, byte(MsgPing)}
	if _, err := Decode(data, &Message{}); !errors.Is(err, errZeroLength) {
		t.Fatalf("expected errZeroLength, got %v", err)
	}
	data = []byte{0, 0, 0, 5, byte(MsgPing), 'a', 'b', 'c'}
	if _, err := Decode(data, &Message{}); !errors.Is(err, errShortRead) {
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
