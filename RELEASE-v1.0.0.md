# Tellstone v1.0.0

The first stable release of Tellstone — a high-performance, cloud-native in-memory key/value store written in Go.

## Highlights

- **Dual protocol support**: Zero-copy binary protocol + Redis-compatible RESP2
- **Shared-nothing architecture**: One goroutine per shard, no cross-shard coordination
- **Zero-allocation hot path**: Flat-matrix timing wheel, in-place value maps, allocation-free parser
- **Optional encryption**: ChaCha20-Poly1305 at-rest encryption, zero allocs/op
- **WAL persistence**: Per-shard append-only write-ahead log for crash recovery
- **Prometheus metrics**: Per-shard + aggregate exporter, zero-alloc serialization
- **OpenTelemetry tracing**: OTLP/gRPC integration with configurable sampling

## Performance

| Scenario | vs Redis |
|----------|----------|
| Bare-metal 32 CPUs | **11.53x** Redis |
| Bare-metal 16 CPUs | **5.98x** Redis |
| Cloud SDN (4–60 threads) | 1.01–1.05x Redis |
| Native binary protocol | up to 1.47M RPS |

## Install

### APT (Debian/Ubuntu)

```bash
curl -fsSL https://saxy.github.io/tellstone-apt/key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/tellstone.gpg
echo "deb [signed-by=/usr/share/keyrings/tellstone.gpg] https://saxy.github.io/tellstone-apt stable main" \
  | sudo tee /etc/apt/sources.list.d/tellstone.list
sudo apt update && sudo apt install tellstone
```

### Homebrew (macOS/Linux)

```bash
brew tap Saxy/tellstone-tap
brew install tellstone
```

### Binary downloads

Pre-built binaries for Linux, macOS, and Windows (amd64/arm64) are available on the [GitHub Releases](https://github.com/Saxy/Tellstone/releases) page.

### Build from source

```bash
git clone https://github.com/Saxy/Tellstone
cd Tellstone
task build   # -> ./bin/tellstone
```

## What's included

### Binaries

| OS | Arch | Archive |
|----|------|---------|
| Linux | amd64 | `tellstone_v1.0.0_linux_amd64.tar.gz` |
| Linux | arm64 | `tellstone_v1.0.0_linux_arm64.tar.gz` |
| macOS | amd64 | `tellstone_v1.0.0_darwin_amd64.tar.gz` |
| macOS | arm64 | `tellstone_v1.0.0_darwin_arm64.tar.gz` |
| Windows | amd64 | `tellstone_v1.0.0_windows_amd64.zip` |

### Packages

- `.deb` for Debian/Ubuntu (amd64, arm64)

### Checksums

SHA-256 checksums are provided in `checksums.txt`.

## Configuration

All settings are configurable via CLI flags or environment variables:

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-addr` | `TSD_ADDR` | `127.0.0.1:9988` | TCP listen address |
| `-enable-resp` | `TSD_ENABLE_RESP` | `false` | Enable Redis-compatible RESP listener |
| `-resp-addr` | `TSD_RESP_ADDR` | `127.0.0.1:6379` | RESP listener address |
| `-enable-metrics` | `TSD_ENABLE_METRICS` | `false` | Enable Prometheus exporter |
| `-enable-encryption` | `TSD_ENABLE_ENCRYPTION` | `false` | Enable ChaCha20-Poly1305 encryption |
| `-enable-persistence` | `TSD_ENABLE_PERSISTENCE` | `false` | Enable WAL persistence |
| `-shards` | `TSD_NUM_SHARDS` | `GOMAXPROCS` | Number of shared-nothing shards |

See `tellstone -h` for the full list.

## Changelog

### Features

- Shared-nothing shard architecture with FNV-1a hash routing
- RESP2 protocol (GET/SET/DEL/PING with EX/PX expiry)
- Zero-copy binary protocol with 11-byte frame header
- ChaCha20-Poly1305 encryption (zero allocs)
- Per-shard append-only WAL with TTL-aware replay
- Prometheus metrics exporter (per-shard + aggregate)
- OpenTelemetry gRPC tracing
- Graceful shutdown on SIGINT/SIGTERM
- Configurable memory limits for Kubernetes deployments
- Zero-GC hot path mode

### Performance

- Zero-allocation flat-matrix timing wheel for TTL eviction
- Striped per-slot locking on chronometer
- Single-map + RWMutex per shard engine
- Allocation-free SQL-style protocol parser (11–26ns/parse)
- Synchronous client for native binary protocol

### Infrastructure

- GitHub Actions CI (build, vet, test, race detection)
- GoReleaser for cross-platform binary builds
- APT repository for Debian/Ubuntu
- Task-based build system with profiling support

## Links

- [GitHub](https://github.com/Saxy/Tellstone)
- [Documentation](https://tellstone.io/docs/getting-started/introduction/)
- [Benchmarks](https://tellstone.io/benchmarks)
- [Discord](https://discord.gg/kEf78DUFX)

## License

Apache-2.0
