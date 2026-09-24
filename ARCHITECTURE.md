# Architecture

This document describes Tellstone's internal structure and how requests flow through the system.

## Overview

Tellstone is a shared-nothing, in-memory key/value store with a native binary protocol frontend
feeding into a single storage engine per shard. A PostgreSQL-wire (SQL) frontend is in progress
(ADR-012); the former Redis-compatible (RESP2) frontend was removed.

```
                    ┌──────────────────────────────────┐
                    │          Your Application         │
                    └───────────┬───────────────────────┘
                                │
                         Binary :9988
                                │
                    ┌───────────▼──────────┐
                    │    network.Server    │
                    │      (gnet)          │
                    └───────────┬──────────┘
                                │
                    ┌───────────▼──────────┐
                    │    router.Router     │
                    │  FNV-1a → shard      │
                    └───────────┬──────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
     ┌────────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
     │   Shard 0       │  │   Shard 1       │  │   Shard N       │
     │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │
     │ │storage.Engine│ │  │ │storage.Engine│ │  │ │storage.Engine│ │
     │ │ map[string]  │ │  │ │ map[string]  │ │  │ │ map[string]  │ │
     │ │ sync.RWMutex │ │  │ │ sync.RWMutex │ │  │ │ sync.RWMutex │ │
     │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │
     │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │
     │ │chronometer  │ │  │ │chronometer  │ │  │ │chronometer  │ │
     │ │(timing wheel)│ │  │ │(timing wheel)│ │  │ │(timing wheel)│ │
     │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │
     │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │
     │ │persistence  │ │  │ │persistence  │ │  │ │persistence  │ │
     │ │(WAL)        │ │  │ │(WAL)        │ │  │ │(WAL)        │ │
     │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │
     │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │
     │ │crypto       │ │  │ │crypto       │ │  │ │crypto       │ │
     │ │(ChaCha20)   │ │  │ │(ChaCha20)   │ │  │ │(ChaCha20)   │ │
     │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │
     └─────────────────┘  └─────────────────┘  └─────────────────┘
```

## Package Reference

### Top-level

| Package | Path | Purpose |
|---------|------|---------|
| `cmd/tellstone` | `cmd/tellstone/` | Main entry point. Parses flags, configures runtime (GC, memory limits, profiling), creates and starts the server. |
| `cmd/benchmark` | `cmd/benchmark/` | Standalone native-binary-protocol load generator with Zipfian key distribution. |
| `cmd/example/client` | `cmd/example/client/` | Minimal example: SET/GET/DELETE via the binary protocol. |
| `server` | `server/` | Top-level orchestrator. Creates shards, router, binary listener, metrics server. Handles graceful shutdown. |
| `config` | `config/` | CLI flags with env-var fallbacks. Includes `ByteSize` parser for human-readable sizes (`16MiB`, `1GiB`). |
| `logger` | `logger/` | Bridges internal `log.Logger` to Go's `slog`. |

### Internal

| Package | Path | Purpose |
|---------|------|---------|
| `app/tellstone` | `internal/app/tellstone/` | Bootstrap: holds logger + config, prints ASCII startup banner. |
| `log` | `internal/log/` | Logging abstraction. Defines `Logger` interface, `ShardLogger` (injects shard ID), `NoOpLogger`, `GnetAdapter`. |
| `router` | `internal/router/` | Routes requests to shards via FNV-1a hash. `Dispatch(op, key, value, ttl) → Response`. |
| `shard` | `internal/shard/` | Shared-nothing shard. Owns an engine + persistence + logger. `Execute()` dispatches GET/SET/DEL. Tracks per-shard atomic metrics. |
| `storage` | `internal/storage/` | Core engine: `map[string]Item` + `sync.RWMutex`. TTL eviction, memory ceiling, at-rest encryption. Also contains the `Chronometer` timing wheel. |
| `network` | `internal/network/` | Binary protocol server (gnet, edge-triggered epoll). Zero-alloc decode from ring buffer. Wire format codec. Synchronous `Client`. |
| `protocol` | `internal/protocol/` | SQL-text-to-KV translator. Parses `SELECT`/`INSERT`/`DELETE` into operations (experimental/frontend path; superseded by the PostgreSQL-wire plan in ADR-012). |
| `persistence` | `internal/persistence/` | Per-shard append-only WAL. Crash recovery with replay, tombstone deletes, truncation of corrupted tails. |
| `crypto` | `internal/crypto/` | ChaCha20-Poly1305 encryption. `EncryptInPlace` / `DecryptInPlace`. Pass-through mode when disabled (zero overhead). |
| `tls` | `internal/tls/` | TLS 1.3/mTLS transport, gnet connection adapter, and automatic certificate/key/CA rotation through a shared atomic config store. |
| `metrics` | `internal/metrics/` | Prometheus exporter. Per-shard `Collector` + `AggregateCollector` (includes Go runtime stats). Hand-written exposition text. |
| `audit` | `internal/audit/` | Structured audit logging. Event-type filter (`--audit-events`), JSON engine emitting `"level":"AUDIT"` lines, rotating and optionally encrypted file writer. |
| `oauth` | `internal/oauth/` | OIDC connection-time token verification. Provider registry, generic OIDC verifier (discovery + cached JWKS, RS256/ES256), `google`/`stackit` presets. See [`internal/oauth/README.md`](internal/oauth/README.md). |
| `trace` | `internal/trace/` | OpenTelemetry wrapper. `Tracer`/`Span` interfaces with `NoOpTracer` (zero-alloc) and `OTelTracer` (OTLP/gRPC). |
| `version` | `internal/version/` | Build-time version/commit/date via `-ldflags`. |

## Request Flow

### Binary Protocol (port 9988)

1. Client connects via TCP (gnet `OnOpen` assigns connection to a shard for metrics tracking).
2. `network.Server.OnTraffic` fires on incoming data (edge-triggered epoll).
3. `Decode()` parses the frame directly from gnet's ring buffer — zero heap allocations.
   - Frame: `[4B length][1B type][1B op][2B keyLen][8B TTL][key][value]`
4. `server.networkHandler()` dispatches:
   - `MsgPing` → returns `MsgPong`
   - `MsgRequest` with OpGet/OpSet/OpDelete → calls `router.Dispatch()`
5. `router.Dispatch()` hashes the key with FNV-1a, selects shard: `sid = FNV-1a(key) % numShards`.
6. `shard.Execute()` runs synchronously:
   - **GET**: `engine.Get(key)` → lazy expiry check → decrypt if crypto enabled
   - **SET**: `engine.Set(key, value, ttl)` → WAL write if persistence enabled → `chronometer.Register()` if TTL > 0
   - **DEL**: WAL tombstone if persistence enabled → `engine.Delete(key)`
7. Response flows back up, `Write()` sends it (fast-path coalescing for payloads < 512 bytes).

## Key Design Decisions

### Shared-Nothing Sharding

Each shard owns a single `map[string]Item` + `sync.RWMutex`. Keys are pinned to a shard
via FNV-1a hashing, so the lock is almost never contended. Default shard count equals
`runtime.NumCPU()`. No cross-shard coordination, no channel round-trips.

### Zero-Allocation Hot Path

- `Decode()` slices directly into gnet's ring buffer (no copies).
- Request buffers use stack-allocated `[N]byte` arrays.
- `Write()` coalesces header + payload under 512 bytes into a single buffer.
- GC is disabled by default (`GOGC=-1`), with `debug.SetMemoryLimit` as a safety valve.

### Timing Wheel Eviction

Instead of per-key timers, a circular timing wheel (`Chronometer`) groups keys into
configurable slots (default 256). Per-slot locks are cache-line-padded (`[56]byte`) to
prevent false sharing. A background goroutine advances the wheel each tick, batch-deleting
expired keys. Registration is append-only and O(1).

### Opt-In Features

Every optional feature is disabled by default and has zero overhead when off:

| Feature | Flag | Default |
|---------|------|---------|
| TLS / mTLS | `--tls-cert`, `--tls-key`, `--tls-ca` | off |
| RBAC | `--rbac-config` | off |
| OAuth / OIDC | `--oauth-provider`, `--oauth-issuer` | off |
| Encryption | `--enable-encryption` | off |
| Metrics | `--enable-metrics` | off |
| Persistence | `--enable-persistence` | off |
| Tracing | `--trace-ratio 0` | off |
| Audit logging | `--enable-audit` | off |

### Role-Based Access Control (RBAC)

Opt-in via `--rbac-config` / `TSD_RBAC_CONFIG`. When a policy file is configured, the binary
listener switches from the shared `--require-pass` password to per-user credentials and
role-based command gating; without one, the server keeps its legacy zero-overhead path. The
file (YAML or JSON) is loaded once at startup and hot-reloaded on SIGHUP with a single atomic
swap, so a rejected file never half-applies and running connections keep their pinned sessions.

**Permission model.** Commands map to bit positions in a dynamic `[]uint64` bitset
(`internal/rbac`). IDs are stable wire values — append-only, never renumbered or reused. Roles
expand their rules into a bitset at creation time, so the authorization hot path is one bit test
plus a prefix scan: `IsAllowed` runs in single-digit nanoseconds with zero allocations
(see `internal/rbac/bench_test.go`).

**Rule syntax.** `ROLE CREATE <name> <rule>...` takes Redis-style tokens left to right:
`+cmd` / `-cmd` grant or revoke one command, `+@cat` / `-@cat` a whole category, and `~prefix`
whitelists a key namespace. The bulk categories:

| Category | Commands |
|----------|----------|
| `login` | AUTH, PING, COMMAND |
| `read` | GET, INFO |
| `write` | SET, DEL |
| `readwrite` | GET, SET, DEL, PING, COMMAND, INFO, AUTH |
| `operator` | readwrite + FLUSH |
| `maintenance` | FLUSH, SHUTDOWN, CONFIG, DEBUG, MONITOR |
| `admin` | AUTH, ROLE, ACL, USER, GRANT, REVOKE |
| `all` / `none` | every registered command / nothing |

**Namespaces.** A role's `~prefix` list is a whitelist: any non-empty list allows only matching
key prefixes (default-deny); an empty list or explicit `~*` allows every key. This stops a role
that may `SET` from writing outside its prefix.

**Fail-closed.** A session resolves its role once at handshake (AUTH, or the nopass `default`
user) and pins it for the connection's lifetime — a policy hot-swap never re-evaluates
mid-session. A session with no role is deny-all. Denials are reported per command (`-NOPERM`, or
`-NOAUTH` for unauthenticated data commands) rather than by dropping the connection, so the
client sees exactly which permission it lacks. Passwords are bcrypt hashes verified with
`bcrypt.CompareHashAndPassword` (constant-time by construction); hashes are never rendered by
`ROLE LIST` / `ROLE GETUSER`.

**Integration seams.** `SessionContext` carries `RoleName` and `Username`, consumed by the audit
engine for `auth_success`, `auth_failure`, and `acl_deny` events (see Audit Logging). Federated
identity is wired at the same seam: an OIDC `id_token` presented to `AUTH` is verified by
`internal/oauth`, its claims resolve to a role through the policy's `oauth.rules` (first match
wins), and the token's `sub` becomes the session username (see
[`internal/oauth/README.md`](internal/oauth/README.md)). Planned integration: API keys intersect
role permissions (most restrictive wins).

### Audit Logging

Opt-in via `--enable-audit` / `TSD_ENABLE_AUDIT`. One shared `audit.LogEngine` is built in
`server` and passed to the binary listener. It is **always non-nil**: without the
flag it is a disabled no-op whose `Record()` returns on a single bool comparison — no writer, no
encoder, no allocation — so the listener calls it unconditionally with no `nil` guard on the
dispatch path.

**Event types.** `connect`, `disconnect`, `auth_success`, `auth_failure`, `acl_deny`, and
`command`. The filter is parsed once at startup from `--audit-events` (default `auth,acl`;
`all` enables every event) and consulted per `Record()`. The `command` event is the only one with
per-command dispatch overhead and is off by default. Each line is one JSON object with
`"level":"AUDIT"`, distinguishing it from operational INFO/WARN/ERROR logs.

**Concurrency.** `Record()` and `Close()` are serialized by a mutex, so a gnet event loop never
races file rotation or shutdown. `Close()` runs at the end of `server.shutdown()`, only after the
listener is stopped, so no in-flight write can race the file close.

**Zero-copy keys.** Command and key strings alias the gnet buffer via the same `unsafe`
slice-header pattern used on the dispatch paths, consumed synchronously by the encoder before the
frame is discarded — no allocation is added to the audit path.

**File writer.** `--audit-log-path` selects `stdout` or a directory. File names are generated
(`<unix-nanoseconds>_<8-hex-dir-hash>_tsd.log`), created `0600`, and rotated at 50 MiB by closing
and reopening a fresh file in the same directory — rotation never truncates or renames history.
When `--enable-encryption` is set, every record is sealed with the crypto engine before it is
flushed. A failed file open falls back to stdout with an error log.

### Event-Driven Networking

The binary protocol server uses **gnet** (edge-triggered epoll), not `net.Conn` per-goroutine.
This gives Linux-level performance with multi-reactor multicore support
(`gnet.WithMulticore(true)`).

### TLS Transport and Certificate Rotation

When TLS is configured, one filesystem watcher monitors the distinct parent directories of the
certificate, private key, and optional client CA. A complete replacement config is validated and
published through a shared atomic pointer after a 500 ms debounce. The listener loads the pointer
when accepting a connection. Established TLS connections retain their original state while later
accepts use the rotated material. Parent-directory watching detects direct writes, atomic
renames, and Kubernetes projected Secret `..data` symlink swaps.

### Persistence (WAL)

Per-shard append-only write-ahead log. Writes are serialized under a per-file mutex and
fsynced after every record. On crash recovery, truncated/corrupted tails are detected
and the file is truncated to the last valid offset. Tombstone records (delete markers)
use a sentinel TTL of `math.MinInt64`.

### Encryption

ChaCha20-Poly1305 with a 32-byte key. Every value is encrypted on SET (nonce + ciphertext +
auth tag). Decryption happens lazily on GET. When disabled, pass-through mode means zero overhead.
