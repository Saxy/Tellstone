# Tellstone

`Tellstone` is a ultra‑high‑performance, cloud‑native **in‑memory key/value store** written entirely in **Go**. It provides a compact binary TCP protocol (no Protobuf, no text‑based parsing) and a sharded, lock‑free storage engine with optional TTL eviction.

```
       +---------------------------------------------+
       |             Your K8s Cluster                |
       |                                             |
       |  [App Pod] ----( TCP binary )---->          |
       |                                             |
       |     +---------------------------------+     |
       |     |        TELLSTONE CORE           |     |
       |     |  (Sharded In‑Memory Buckets)    |     |
       |     +---------------------------------+     |
       |              |                ^             |
       +--------------|----------------|-------------+
        Client writes/reads               |
        via TCP binary protocol           |
       +---------------------------------------------+
       |           Cloud Platform                    |
       |           (e.g., any backend DB)           |
       +---------------------------------------------+
```

## ⚡ Why Tellstone?

Many managed databases (PostgreSQL, MySQL, etc.) become bottlenecks under high‑frequency workloads. Traditional caches such as Redis add operational complexity and often use single‑threaded text protocols. Tellstone offers a **lean, modern, memory‑efficient buffer** that:

* **Zero‑Copy Binary Protocol** – Direct binary messages avoid the overhead of text parsing or Protobuf marshalling.
* **Lock‑Free Linear Scaling** – The storage engine uses 256 sharded buckets indexed by key‑hash, giving near‑linear scalability across CPU cores.
* **Configurable TTL Eviction** – An active timing‑wheel (chronometer) evicts expired keys in O(1) without blocking.
* **Metrics & Tracing** – Built‑in collector provides engine and network snapshots; optional OpenTelemetry tracing.
* **Adjustable Message Size** – `--max‑msg‑size` (or `TSD_MAX_MSG_SIZE`) lets you set a per‑connection limit (default 16 MiB).

### Core Architecture Highlights

* **Binary TCP Protocol** – Simple `MsgRequest`/`MsgResponse` frames containing operation (`GET`, `SET`, `DEL`), TTL, key and value.
* **Sharded Storage Engine** – Concurrent map with per‑shard mutexes; supports `Set`, `Get`, `Delete` and statistics (`HitCount`, `MissCount`, etc.).
* **Optional Write‑Behind** – Not part of the current release; future work may add async persistence.
* **Metrics & Tracing** – Exportable snapshots via the `metrics` package and optional OpenTelemetry exporter.


## 🛠️ Planned Project Milestones

### Phase 1: Core Engine (Completed)

* Sharded in‑memory storage engine with TTL eviction.
* Binary TCP protocol implementation.
* Configurable max message size flag.
* Basic metrics collector and OpenTelemetry tracing support.

### Phase 2: Distributed Features (Future)

* Event‑driven replication using a message bus (e.g., NATS JetStream).
* Write‑through / write‑behind persistence to external databases.
* Client SDKs for Go and other languages.


## 🚀 Vision

Tellstone aims to become the go‑to **in‑cluster accelerator** for cloud‑native applications, reducing latency and off‑loading traffic from downstream databases. By keeping data handling local, secure, and fast, it can cut downstream database load and cost.

## 🤝 Contributing

Tellstone is still evolving. Contributions are welcome – especially around networking, replication, and persistence. Open an issue or start a discussion to share ideas.


*“A contest of focus. Keep yours made of steel.”* — **Tellstone**