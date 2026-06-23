package network

import (
	"encoding/binary"
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
	// Server starten
	srv := NewServer(benchAddr, func(msg *Message) ([]byte, MessageType, error) {
		return msg.Payload, MsgResponse, nil
	})

	go func() {
		_ = srv.ListenAndServe()
	}()

	// Kurze Pause, damit der Epoll-Listener hochfährt
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

// Allokationsfreier Benchmark-Client, der dem Server via gnet einheizt
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
	// Erste Nachricht beim Verbindungsaufbau senden
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

	// Nächsten Request sofort asynchron nachschieben (Pipelines füttern)
	var hdr [5]byte
	totalLen := 1 + len(bc.payload)
	binary.BigEndian.PutUint32(hdr[:4], uint32(totalLen))
	hdr[4] = byte(MsgPing)

	_, _ = c.Write(hdr[:])
	_, _ = c.Write(bc.payload)
	return gnet.None
}

// BenchmarkGnetServerHandlerParallel misst jetzt echten asynchronen Durchsatz
func BenchmarkGnetServerHandlerParallel(b *testing.B) {
	payload := []byte("benchdata")
	numCores := runtime.GOMAXPROCS(0)

	itersPerClient := b.N / numCores
	if itersPerClient == 0 {
		itersPerClient = 1
	}

	// Slices für das Lifecycle-Management der Clients vorallokieren
	clients := make([]*benchClient, numCores)
	engines := make([]gnet.Client, numCores)
	channels := make([]chan struct{}, numCores)

	// 1. SETUP-PHASE: Alle Clients VOR dem Timer initialisieren und starten
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

	// Kurz warten, bis alle Client-Engines im Hintergrund bereit sind
	time.Sleep(50 * time.Millisecond)

	// 2. MESS-PHASE: Jetzt starten wir die Zeitmessung und triggern nur noch das Dialing/I/O
	b.ResetTimer()

	var wg sync.WaitGroup

	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Verbindung asynchron aufbauen – ab hier läuft der OnTraffic-Loop
			_, _ = engines[idx].Dial("tcp", benchAddr)
			<-channels[idx]
		}(i)
	}

	// Warten bis alle Worker ihre zugewiesenen Iterationen abgearbeitet haben
	wg.Wait()

	// Timer SOFORT stoppen – ab hier wird nicht mehr gezählt
	b.StopTimer()

	// 3. TEARDOWN-PHASE: Aufräumen nach dem Benchmark
	for i := 0; i < numCores; i++ {
		_ = engines[i].Stop()
	}
}
