# network package

The **`network`** package provides a high‑performance, zero‑allocation TCP engine built on top of the **gnet** event‑driven networking library. It handles a compact, optimized binary wire protocol designed to transport Tellstone frames with maximum throughput and minimum latency.

## Features
- **Zero‑allocation by default** – Inbound messages are decoded directly out of the live connection ring-buffer views, entirely bypassing the Go heap allocator on the hot path.
- **Edge‑triggered epoll loop** – Powered by `github.com/panjf2000/gnet/v2` to orchestrate 32 event loops smoothly across available CPU cores.
- **Scatter-Gather I/O** – Outbound writes leverage `net.Buffers` to utilize the kernel-level `writev` system call, preventing TCP packet fragmentation and minimizing context switches.
- **Synchronous Client** – Contains an execution-blocking `Client` wrapper designed for low-latency microservice pipelines featuring zero-alloc payload extraction.

## Protocol Overview

```

+----------------+----------------+-----------------+
| uint32 length  | uint8 type     | []byte payload  |
+----------------+----------------+-----------------+

```
- **length** – Total size of the `type` byte + the variable length `payload` (Big-Endian).
- **type** – An 8-bit unsigned integer representing `MessageType` (`MsgPing`, `MsgPong`, `MsgRequest`, `MsgResponse`).
- **payload** – Optional binary array representing data instructions (e.g., Tellstone raw SQL statements).

## Usage Examples

### Server Configuration
```go
package main

import (
	"log"
	"[github.com/Saxy/Tellstone/internal/network](https://github.com/Saxy/Tellstone/internal/network)"
)

func main() {
	// Simple echo handler – replies with a Pong for every Ping.
	handler := func(msg *network.Message) ([]byte, network.MessageType, error) {
		if msg.Type == network.MsgPing {
			return []byte("pong"), network.MsgPong, nil
		}
		return nil, 0, nil
	}

	srv := network.NewServer("127.0.0.1:9988", handler)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

```

### High-Performance Client Execution

```go
package main

import (
	"fmt"
	"time"
	"[github.com/Saxy/Tellstone/internal/network](https://github.com/Saxy/Tellstone/internal/network)"
)

func main() {
	client, err := network.Dial("127.0.0.1:9988", 2*time.Second)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Reusable stack buffers to guarantee 0 allocations during calls
	var scratchpad [1024]byte
	var response network.Message

	err = client.Call(network.MsgPing, []byte("ping"), scratchpad[:], &response)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Received response type: %d, payload: %s\n", response.Type, string(response.Payload))
}

```

## Benchmarks

Micro-benchmarks conducted on a bare-metal **AMD Ryzen 9 9950X (16-Core / 32-Threads)** environment running Linux:

```
goos: linux
goarch: amd64
pkg: [github.com/Saxy/Tellstone/internal/network](https://github.com/Saxy/Tellstone/internal/network)
cpu: AMD Ryzen 9 9950X 16-Core Processor            

BenchmarkReadMessageZeroAlloc-32         856,039,898        1.464 ns/op        0 B/op        0 allocs/op
BenchmarkGnetServerHandlerParallel-32        522,372         2297 ns/op      149 B/op        6 allocs/op

```

### Insights

* **`Decode` execution layer:** Parsing an active protocol packet takes a mere **$1.46\text{ ns}$** with absolute **0 allocations**, maximizing memory density and leaving zero traces for the Go Garbage Collector.
* **Parallel Core Concurrency:** When executing full network event loops in parallel across all 32 threads, operations settle cleanly at **$2297\text{ ns}$**. The minimal 149 bytes allocated here reflect the system's runtime framework lifecycles—completely isolating Tellstone's core execution path from runtime pressure.