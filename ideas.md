
### Metrics (verified against source)
**Exposed:** 7 engine, 4 per-shard network, runtime, TLS cert, RBAC.

**Tracked but NOT on /metrics:**
- `storage.Engine.CryptoEncryptedBytes()`/`CryptoDecryptedBytes()` (`engine.go:306-307`)
- `Chronometer.Overflows()` — captured into snapshot at `metrics.go:81`, never written
- `network.Server.ProtocolErrors()`/`HandlerErrors()` — captured at `metrics.go:100-101`, never written
- RESP server — all 5 counters tracked (`resp/server.go:606-610`) but the server is never passed to metrics, so RESP `protocolErrors` are invisible

**Wiring bugs:**
- Production uses only shard collectors (`startMetricsServer` in `server/server.go`); `AggregateCollector.WritePrometheus` never renders its stored `networkServer`, so plain `tellstone_network_*` never appears — only `tellstone_shard_N_*`. `NewCollector` (network-only) is test-only.
- Shard counters are incremented by BOTH binary and RESP listeners, conflating both.

**Not tracked at all:** router (no latency/command stats), SIGHUP/RBAC reload (TLS has `ReloadTotal`/`ReloadErrorsTotal`; RBAC has nothing), startup/shutdown, persistence (WAL records counted then discarded), crypto package, protocol parser.

### Logging
- Solid infra: `log.Logger` interface, `SlogAdapter`, `GnetAdapter`, `shard_logger.go` (shard_id injection).
- Gaps: `ErrEngineFull` never logged; per-shard lifecycle logs missing; **no audit log at all** — RBAC ROLE mutations, NOPERM denials, AUTH successes, SIGHUP policy reload not logged consistently.

### Tracing
- `internal/trace/trace.go` OTel wrapper is complete but **never called in production** — only tests. `startup.go` just logs "OTLP/gRPC Active" vs "NoOp"; `--trace-ratio`/`TSD_TRACE_RATIO` wired in config but no spans, no propagator, no context propagation anywhere.

---

How do you want to proceed? Options:

1. **Fix the two metrics wiring bugs** (rendering `networkServer`, exposing RESP + ProtocolErrors/HandlerErrors + CryptoBytes + Overflows) — small, contained.
2. **Add the audit log** (biggest gap, RBAC-aligned).
3. **Wire tracing into production** (InitTracer + request spans).
4. **All of the above, in that order.**

Note: per AGENTS.md the hot path must stay allocation-free — any counter additions on the request path need `-benchmem` verification.