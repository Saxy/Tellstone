# Contributing to Tellstone

Thanks for your interest in contributing! This document covers the process for getting your changes merged.

## Core Principles

Every contribution should align with Tellstone's design philosophy. Keep these in mind when proposing changes:

| Principle | What it means |
|-----------|---------------|
| **Zero Allocation** | The hot path must not allocate. Reuse buffers, use stack-allocated types, avoid `make()` in request handling. |
| **Shared-Nothing First** | Data lives in per-shard maps. No cross-shard coordination, no shared mutable state. |
| **Opt-In Everything** | Features like encryption, metrics, persistence, and RESP are disabled by default. Don't force overhead on users who don't need it. |
| **Single Binary** | No external dependencies at runtime. The binary is self-contained. |
| **Minimal Third-Party Dependencies** | Prefer the standard library. Every dependency is a liability — security surface, build complexity, license risk. Justify any new dep. |
| **Security by Default** | When a feature is enabled, it should be secure out of the box. No weak defaults. |
| **Cloud-Native** | Design for Kubernetes, ephemeral containers, horizontal scaling. No local-state assumptions. |

## Developer Certificate of Origin (DCO)

Tellstone uses the [DCO](https://developercertificate.org/) to ensure all contributions are properly licensed. You **must** sign off every commit:

```bash
git commit -s -m "your commit message"
```

This adds a `Signed-off-by: Your Name <your@email.com>` line to the commit, certifying that you have the right to submit the contribution under the project's license (Apache 2.0).

### DCO sign-off requirements

- Every commit in a PR must be signed off (`-s` flag).
- Use your **real name** and a **valid email address** (no pseudonyms, no noreply emails).
- If you forgot to sign off, amend the commit:
  ```bash
  git commit --amend -s
  # or for the last N commits:
  git rebase --signoff HEAD~N
  ```
- PRs with unsigned commits will not be merged.

## Getting Started

### Prerequisites

- **Go 1.26+**
- **[task](https://taskfile.dev)** (go-task) — optional but recommended

### Codebase Overview

See [ARCHITECTURE.md](ARCHITECTURE.md) for a full description of the package structure,
request flow (binary and RESP), key types, and design decisions.

### Setup

```bash
git clone https://github.com/Saxy/Tellstone.git
cd Tellstone
task build        # or: go build -o bin/tellstone ./cmd/tellstone
task test         # verify everything passes
```

### Development Workflow

```bash
task check        # vet + race tests — run before every commit
task fmt          # auto-format
task test:race    # race detector tests
task bench:resp   # benchmark (for performance-sensitive changes)
```

## Making Changes

### Branch Naming

Use descriptive branch names:

```
feat/resp-pipeline-support
fix/shard-hash-collision
perf/wal-zero-alloc-write
docs/benchmark-methodology
```

### Commit Messages

This project follows [Conventional Commits](https://www.conventionalcommits.org/). Every commit message must match this format:

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]

Signed-off-by: Your Name <your@email.com>
```

**Types:**

| Type | Use for |
|------|---------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `perf` | Performance improvement (no behavior change) |
| `refactor` | Code restructuring (no behavior change, no feature, no fix) |
| `docs` | Documentation only |
| `test` | Adding or updating tests |
| `chore` | Build process, tooling, dependencies |
| `ci` | CI/CD configuration |
| `build` | Build system or external dependencies |
| `style` | Formatting, whitespace (no logic change) |

**Scopes** (project-specific):

| Scope | Area |
|-------|------|
| `resp` | RESP2 protocol |
| `binary` | Binary protocol |
| `storage` | Storage engine / TTL |
| `shard` | Shard runner / shared-nothing |
| `router` | Request routing / FNV-1a |
| `persistence` | WAL / crash recovery |
| `crypto` | ChaCha20-Poly1305 encryption |
| `metrics` | Prometheus exporter |
| `trace` | OpenTelemetry tracing |
| `cli` | Flags / configuration |
| `server` | Server lifecycle |

**Examples:**

```
feat(resp): add EXPIRE command support

perf(storage): reduce heap allocations in timing wheel

fix(persistence): resolve WAL replay race on startup

docs: update benchmark methodology section

chore(deps): bump go.mod to Go 1.26
```

**Rules:**
- Description is lowercase, imperative mood, no period, max 72 characters.
- Reference issues in the footer: `Closes #123`, `Fixes #456`.
- Breaking changes must include `BREAKING CHANGE:` in the footer or append `!` after the type/scope: `feat(resp)!: remove deprecated HELLO command`.
- Do not include AI-generated or co-authored trailers (`Co-authored-by: Claude ...`, `Generated with ...`). Such tags create legal and licensing gray areas, and only a person can certify the DCO. See [AGENTS.md](AGENTS.md).

### Code Standards

- Follow existing code conventions — look at neighboring files for style.
- Keep function signatures minimal. Prefer returning errors over panicking.
- Use `sync.RWMutex` patterns consistent with the existing shard architecture.
- Avoid `interface{}` where a concrete type works.
- Benchmarks go in `_test.go` files alongside the code they measure.

### Commenting Style

The codebase is **heavily commented**, especially around performance-critical sections. Three
styles are used consistently:

1. **Block header comments** — every file begins with a C-style block:
   ```go
   /*
   Package <name>
   Tellstone Cloud-Native In-Memory Database
   File: <filename>.go
   Description: <summary>
   Authors:
       Name
   */
   ```

2. **GoDoc comments** — on exported functions and types:
   ```go
   // Decode parses a full protocol frame from an existing byte slice.
   // Guarantees 0 heap allocations by slicing directly into the ring buffer.
   func Decode(...) (int, error) { ... }
   ```

3. **Inline comments** — explain *why*, not *what*. Especially around:
   - Allocation-sensitive code (`// Use a fixed stack allocation...`)
   - Cache-line padding (`// slotLock pads a sync.Mutex out to a full cache line...`)
   - Kubernetes/container considerations (`// CRITICAL FOR DEPLOYMENT...`)
   - Non-obvious trade-offs

When in doubt, comment. Architectural reasoning and performance rationale should always
be documented — future contributors (and your future self) will thank you.

### Performance

For any change touching the hot path (storage, router, shard execution, protocol parsing):

1. Run benchmarks **before** and **after** your change.
2. Include before/after numbers in your PR description.
3. Ensure zero new allocations on the hot path (`go test -benchmem`).
4. Pin server and load generator to disjoint cores for accurate numbers (see `task bench:resp`).

```bash
# Quick allocation check
go test -bench=BenchmarkYourThing -benchmem ./path/to/pkg

# Full benchmark with core pinning
task bench:resp
```

## Pull Request Process

1. **Fork and branch** from `main`.
2. **Make your changes**, following the standards above.
3. **Run checks** — `task check` must pass with zero warnings.
4. **Sign off all commits** — every commit must have a `Signed-off-by` line.
5. **Open a PR** against `main` using the PR template.
6. **Fill out the PR template** completely — especially the benchmark table for performance changes.
7. **Respond to review feedback** promptly.

If you used a coding agent for any part of the change, follow [AGENTS.md](AGENTS.md).

PRs that introduce new dependencies, change the public API, or affect performance will receive extra scrutiny. This is by design — it protects the project's core principles.

## Reporting Issues

Use the issue templates in `.github/ISSUE_TEMPLATE/`:

| Template | Use for |
|----------|---------|
| Bug Report | Unexpected behavior, crashes, incorrect results |
| Feature / Enhancement | New features or improvements |
| Vulnerability Report | Security issues (see [SECURITY.md](SECURITY.md) for critical vulns) |
| Proposal | Large design changes or architectural decisions |
| Telemetry / Observability | Metrics, tracing, logging issues |
| Profiling | CPU/memory profiling, performance regressions |

## License

By contributing to Tellstone, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE), and you certify this via the DCO sign-off as described above.

---

*“A contest of focus. Keep yours made of steel.”* — **Tellstone**
