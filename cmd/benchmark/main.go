/*
Package main
Tellstone Cloud-Native In-Memory Database
File: main.go
Description: Native binary-protocol load generator. Opens multiple TCP connections and drives GET/SET requests against a Tellstone server, reporting throughput and latency percentiles.

Authors:

	Maximilian Hagen
*/
package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	rnd "math/rand/v2"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/network"
)

const bucketCount = 371

// encodeRequest writes a full MsgRequest frame into dst (which must be large enough) and
// returns the number of bytes written. Encoding into a caller-owned buffer keeps the load
// generator allocation-free; allocating a fresh frame per request (as Message.Marshal does)
// makes the benchmark process GC, which shows up as client-side latency in the tail.
//
// Frame layout: [4B total][1B type][1B op][2B keyLen][8B ttl][key][value].
func encodeRequest(dst []byte, op network.OpCode, key, value []byte, ttl int64) int {
	total := 1 + 1 + 2 + 8 + len(key) + len(value) // type byte + payload
	binary.BigEndian.PutUint32(dst[0:4], uint32(total))
	dst[4] = byte(network.MsgRequest)
	dst[5] = byte(op)
	binary.BigEndian.PutUint16(dst[6:8], uint16(len(key)))
	binary.BigEndian.PutUint64(dst[8:16], uint64(ttl))
	copy(dst[16:16+len(key)], key)
	copy(dst[16+len(key):], value)
	return 4 + total
}

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

type workerStats struct {
	successOps uint64
	errorOps   uint64
	hist       histogram
	_          [8]uint64
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
	ttls := make([]int64, *concurrency)

	// The protocol/handler interprets TTL in milliseconds. Generate 1s–3600s TTLs
	// expressed in ms so keys survive the run instead of expiring mid-benchmark and
	// triggering an eviction storm that distorts the latency tail.
	for i := 0; i < *concurrency; i++ {
		ttls[i] = int64((rnd.IntN(3600) + 1) * 1000)
	}
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
			var buf [1024]byte
			var wbuf [1100]byte // reused request frame buffer (keeps the load generator zero-alloc)
			var staticKey [32]byte
			prefix := []byte("key-")
			for r := 0; r < reqsPerWorker; r++ {
				copy(staticKey[0:4], prefix)
				keyEnd := strconv.AppendUint(staticKey[4:4], rng.Zipf(50000, *zipfSkew), 10)
				totalKeyLen := 4 + len(keyEnd)
				activeKeySlice := staticKey[:totalKeyLen]
				isRead := rng.Float64() < *readRatio
				var n int
				if isRead {
					n = encodeRequest(wbuf[:], network.OpGet, activeKeySlice, nil, 0)
				} else {
					n = encodeRequest(wbuf[:], network.OpSet, activeKeySlice, dummyValue, ttls[i])
				}
				opStart := time.Now()
				if _, err := conn.Write(wbuf[:n]); err != nil {
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
