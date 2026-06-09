# Tellstone

`Tellstone` is a ultra-high-performance, cloud-native In-Memory Database and Write-Behind Puffer written completely in **Go**.

Inspired by the ancient Runeterran game of focus, tactical placement, and swift decision-making, `Tellstone` acts as a lightning-fast, zero-overhead memory shield designed to run natively inside your Kubernetes clusters. It intercepts aggressive application read/write bursts, serving them in sub-milliseconds, while asynchronously streaming states down to relational backends.

```
       +---------------------------------------------+
       |             Your K8s Cluster                |
       |                                             |
       |  [App Pod] ----( gRPC / Protobuf )---->     |
       |                                             |
       |     +---------------------------------+     |
       |     |        TELLSTONE CORE           |     |
       |     |  (Shared In-Memory Buckets)     |     |
       |     +---------------------------------+     |
       |              |                ^             |
       +--------------|----------------|-------------+
        Async Batch   |                | Active Sync 
        Writes (gRPC) v                | via NATS Stream
       +---------------------------------------------+
       |           Cloud Platform                    |
       |           (e.g., STACKIT Managed DB)        |
       +---------------------------------------------+

```

## ⚡ Why Tellstone?

Modern managed databases (like Managed PostgreSQL, SQLServer or MariaDB/MySQL) abstract infrastructure operational pain but can easily bottleneck when slammed with high-frequency application queries. Traditional caching Layers (like Redis or Valkey) introduce massive operational footprints, complex text-protocol parsing, single-threaded locks, and high CPU-serialization costs.

`Tellstone` strips away 15 years of legacy bloat to provide a **lean, modern, memory-efficient buffering layer** tailored specifically for 2026 Cloud-Native ecosystems.

### Core Architecture Highlights

* **Zero-Copy Protobuf Wire Protocol:** Skips legacy text-protocol string parsing. Clients communicate natively via schema-driven Protobuf structures over raw TCP/gRPC sockets. Data is kept in its raw binary form directly inside the RAM.
* **Lock-Free Linear Scaling:** Utilizes an intensely optimized, concurrently sharded memory engine (256 distinct ring-fenced memory buckets). Dictated by key-hashing, it ensures zero thread contention and scales linearly with your CPU cores.
* **Asynchronous Write-Behind Engine:** Applications receive instantaneous success responses. Changes are buffered in memory, grouped into atomic batches, and streamed smoothly into the destination relational database.
* **NATS-Backed High Availability:** Completely offloads cluster-state coordination, split-brain resolution, and replication logic to a local **NATS JetStream** backplane. If a node drops, it fetches the current line state asynchronously upon boot.
* **Deterministic Active TTLs:** Leverages a lightweight, active Timing Wheel memory cleaner. Dead keys are evicted in $O(1)$ efficiency without blocking execution pipelines.

---

## 🛠️ Planned Project Milestones

### Phase 1: The Core Game (In Progress)

* [ ] Concurrent Sharded Storage Engine in Go (`sync.RWMutex` array).
* [ ] Protobuf API definitions and raw TCP connection multiplexer.
* [ ] Active background cache eviction via a custom Timing Wheel.

### Phase 2: The Tactical Line (Distributed Replication)

* [ ] Event-driven active state synchronization using NATS JetStream.
* [ ] Fast binary snapshots for quick node-reboots.
* [ ] Client SDKs for Go.

### Phase 3: The King's Gambit (Database Mirroring)

* [ ] Configurable Write-Through / Write-Behind storage workers.
* [ ] Native driver integration for SQL databases.
* [ ] Resilient backoff and retry mechanisms for persistent layer connection drops.

---

## 🚀 Vision

The long-term goal of `Tellstone` is to become the definitive **in-cluster accelerator** for applications sitting in cloud topologies. It keeps data handling local, secure, and blazingly fast, reducing downstream cloud-database instance sizes and transactional costs by up to **80%**.

## 🤝 Contributing

`Tellstone` is currently in its early structural architectural design phase. Blueprints, feedback regarding networking layouts, and protocol optimizations are highly welcome. Feel free to open an issue or start a discussion thread.

---

*“A contest of focus. Keep yours made of steel.”* — **Tellstone**
