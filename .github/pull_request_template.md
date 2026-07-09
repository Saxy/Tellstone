## Description

<!-- Clear and concise description of what this PR changes or introduces -->

**Component:** (e.g., Networking/RESP, Storage Engine, Router/Shard, CLI, Build/CI)

**Type of Change:**
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Performance optimization (no change in behavior, improved speed/memory)
- [ ] Refactoring (no functional changes, code cleanup)
- [ ] Build / CI / Documentation

---

## Related Issue

<!-- Link to the issue this PR addresses, if any. Use "Closes #123" to auto-close. -->

---

## Technical Deep Dive & Context

<!-- Explain how you implemented it, especially if it touches:
  - Memory management or allocation patterns
  - Lock-free / concurrent data structures
  - Network primitives or protocol parsing
  - The shared-nothing shard architecture
  - Any trade-offs or alternatives considered
-->

---

## Performance & Benchmarks (If Applicable)

<!--
For performance-sensitive changes, provide before/after numbers.
Include go test -bench output, memtier_benchmark results, or native binary-protocol benchmark results.
If there is no measurable change, state "no measurable change".
-->

**Workload:** (e.g., 8t x 100c, pipeline 8, 1:9, 200k keys Gaussian)

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Throughput | ? ops/s | ? ops/s | ?% |
| p50 latency | ? ms | ? ms | ?% |
| p99 latency | ? ms | ? ms | ?% |

```text
// Paste benchmark output here
```

---

## How Has This Been Tested?

<!-- Describe the testing you performed:
  - go test ./... output
  - go test -race ./... output
  - Manual testing steps, if any
  - Any edge cases considered
-->

---

## Checklist

- [ ] My code follows the existing code style of this project
- [ ] I have added tests that prove my fix/feature works
- [ ] New and existing tests pass locally (`go test ./...` and `go test -race ./...`)
- [ ] I have updated the documentation (README, comments, or any relevant docs)
- [ ] My changes generate no new `go vet` warnings
- [ ] Any breaking changes are documented and communicated
