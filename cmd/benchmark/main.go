package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/network"
)

const bucketCount = 371

type histogram struct {
	buckets [bucketCount]uint64
}

func (h *histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	var idx int
	switch {
	case us < 100:
		idx = int(us)
	case us < 1000:
		idx = 100 + int((us-100)/10)
	case us < 10000:
		idx = 190 + int((us-1000)/100)
	case us < 100000:
		idx = 280 + int((us-10000)/1000)
	default:
		idx = bucketCount - 1
	}
	h.buckets[idx]++
}

func (h *histogram) Add(other *histogram) {
	for i := 0; i < bucketCount; i++ {
		h.buckets[i] += other.buckets[i]
	}
}

func (h *histogram) Percentile(p float64, total uint64) time.Duration {
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * p)
	var count uint64
	for i := 0; i < bucketCount; i++ {
		count += h.buckets[i]
		if count >= target {
			return bucketToDuration(i)
		}
	}
	return 100 * time.Millisecond
}

func bucketToDuration(idx int) time.Duration {
	switch {
	case idx < 100:
		return time.Duration(idx) * time.Microsecond
	case idx < 190:
		return time.Duration(100+(idx-100)*10) * time.Microsecond
	case idx < 280:
		return time.Duration(1000+(idx-190)*100) * time.Microsecond
	case idx < 370:
		return time.Duration(10000+(idx-280)*1000) * time.Microsecond
	default:
		return 100 * time.Millisecond
	}
}

type fastRand struct {
	state uint64
}

func newFastRand(seed int64) *fastRand {
	return &fastRand{state: uint64(seed) + 1442695040888963407}
}

func (r *fastRand) Uint64() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *fastRand) Float64() float64 {
	return float64(r.Uint64()>>11) / (1 << 53)
}

func (r *fastRand) Zipf(maxKey uint64, skew float64) uint64 {
	return uint64(math.Pow(r.Float64(), skew)*float64(maxKey)) % maxKey
}

// WorkerStats mit CPU Padding (64 Bytes), um False Sharing auf der Cache Line zu verhindern
type workerStats struct {
	successOps uint64
	errorOps   uint64
	hist       histogram
	_          [8]uint64 // Padding gegen Cache-Line Ping-Pong zwischen CPU-Kernen
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9988", "Target server address")
	concurrency := flag.Int("c", 200, "Number of concurrent workers")
	totalRequests := flag.Int("n", 1000000, "Total number of requests to execute")
	readRatio := flag.Float64("read-ratio", 0.95, "Ratio of GET requests")
	zipfSkew := flag.Float64("skew", 1.5, "Power-law skew")
	flag.Parse()

	reqsPerWorker := *totalRequests / *concurrency
	fmt.Printf("Benchmarking %s with %d workers (%d reqs each)...\n", *addr, *concurrency, reqsPerWorker)

	var wg sync.WaitGroup
	stats := make([]workerStats, *concurrency)

	dummyValue := make([]byte, 32)
	_, _ = rand.Read(dummyValue)

	start := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wStats := &stats[workerID]

			var seedBuf [8]byte
			_, _ = rand.Read(seedBuf[:])
			rng := newFastRand(int64(binary.BigEndian.Uint64(seedBuf[:])))

			conn, err := net.Dial("tcp", *addr)
			if err != nil {
				wStats.errorOps += uint64(reqsPerWorker)
				return
			}
			defer conn.Close()

			var msg network.Message
			var buf [1024]byte

			// Statischer, lokaler Speicherbereich pro Worker für die Key-Slices
			// Verhindert das Aliasing-Risiko komplett
			var staticKey [32]byte
			prefix := []byte("key-")

			for r := 0; r < reqsPerWorker; r++ {
				// Sicheres Schreiben in das statische Array
				copy(staticKey[0:4], prefix)
				keyEnd := strconv.AppendUint(staticKey[4:4], rng.Zipf(50000, *zipfSkew), 10)
				totalKeyLen := 4 + len(keyEnd)
				activeKeySlice := staticKey[:totalKeyLen]

				isRead := rng.Float64() < *readRatio

				if isRead {
					msg = network.Message{Type: network.MsgRequest, Op: network.OpGet, Key: activeKeySlice}
				} else {
					msg = network.Message{Type: network.MsgRequest, Op: network.OpSet, Key: activeKeySlice, Value: dummyValue}
				}

				opStart := time.Now()

				payload := msg.Marshal()
				if _, err := conn.Write(payload); err != nil {
					wStats.errorOps++
					continue
				}

				var resp network.Message
				if err := network.Read(conn, buf[:], &resp); err != nil {
					wStats.errorOps++
					continue
				}

				wStats.hist.Record(time.Since(opStart))
				wStats.successOps++
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	globalHist := histogram{}
	var totalSuccess uint64
	var totalErrors uint64

	for i := 0; i < *concurrency; i++ {
		globalHist.Add(&stats[i].hist)
		totalSuccess += stats[i].successOps
		totalErrors += stats[i].errorOps
	}

	rps := float64(totalSuccess) / duration.Seconds()

	fmt.Println("--- Results ---")
	fmt.Printf("Total Requests (Success): %d\n", totalSuccess)
	fmt.Printf("Total Requests (Failed):  %d\n", totalErrors)
	fmt.Printf("Duration:                 %s\n", duration)
	fmt.Printf("Throughput:               %.2f RPS\n\n", rps)

	if totalSuccess > 0 {
		fmt.Println("--- Latency Percentiles (Histogram) ---")
		fmt.Printf("p50 (Median): %s\n", globalHist.Percentile(0.50, totalSuccess))
		fmt.Printf("p95:          %s\n", globalHist.Percentile(0.95, totalSuccess))
		fmt.Printf("p99:          %s\n", globalHist.Percentile(0.99, totalSuccess))
		fmt.Printf("p99.9:        %s\n", globalHist.Percentile(0.999, totalSuccess))
	}
}
