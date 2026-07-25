# internal/tls — TLS 1.3 Transport Security

## What

TLS 1.3 transport encryption for Tellstone's binary and RESP2 listeners. This is a
stripped fork of [gnet-io/tls](https://github.com/gnet-io/tls) (itself a fork of
Go's `crypto/tls`) optimized for gnet's epoll-based event-loop I/O model.

## Why

Tellstone is single-binary, zero-dependency, cloud-native infrastructure.
TLS is not optional in production. This package provides:

- **TLS 1.3 only** — no legacy protocol versions, no cipher suite negotiation complexity
- **Zero-allocation fast path** — `readBuf` pre-allocated read buffer avoids per-record allocations
- **gnet-compatible** — returns `ErrNotEnough` instead of blocking, integrates with gnet's
  `InboundBuffered()` / `Peek()` for non-blocking reads
- **mTLS by config** — adding `--tls-ca` flag enables mutual TLS with client certificate verification

## Architecture

```text
Tellstone binary
  └─ server.Server
       ├─ internal/network.Server   (binary protocol — gnet event-loop)
       │    └─ tls.NewGnetConnAdapter(conn)  → tls.Server / tls.Client
       └─ internal/resp.Server      (RESP2 protocol — gnet event-loop)
            └─ tls.NewGnetConnAdapter(conn)  → tls.Server / tls.Client
```

## What was removed

This fork has been aggressively stripped to minimize code surface:

- **TLS 1.0, 1.1, 1.2** — all protocol versions except 1.3
- **QUIC** — entire QUIC transport integration
- **Key agreement** — RSA key exchange, `key_agreement.go`
- **PRF** — pseudo-random function for TLS 1.2, `prf.go`
- **Certificate generation** — `generate_cert.go` (use `cmd/tellstone` certs instead)
- **Legacy cipher suites** — RC4, CBC, 3DES, RSA key exchange suites
- **Renegotiation** — TLS 1.2 renegotiation support
- **Downgrade protection** — version downgrade detection logic

## Key types

- `GnetConnAdapter` — bridges `gnet.Conn` to `net.Conn` for use with the TLS library
- `NewGnetConnAdapter(gconn)` — constructor; requires gnet.Conn with `Peek` and `InboundBuffered`
- `BuildConfig(certPath, keyPath, caPath)` — builds `*tls.Config` from file paths
- `IsMTLS(config)` — returns true if CA certificate is configured (mutual TLS)

## Configuration

```bash
# Server-only TLS
--tls-cert /path/to/server.crt --tls-key /path/to/server.key

# Mutual TLS (mTLS)
--tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-ca /path/to/ca.crt
```

Environment variables: `TELLSTONE_TLS_CERT`, `TELLSTONE_TLS_KEY`, `TELLSTONE_TLS_CA`

## Performance

Benchmark results on AMD Ryzen 9 9950X (16-core):
- Plaintext: 2 allocs/op
- TLS 1.3: 3 allocs/op (1 extra from TLS encryption internals, not eliminable from our side)
