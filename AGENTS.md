# AGENTS.md

Instructions for coding agents (Claude Code, Cursor, Copilot, Codex, Aider) working in this
repository. Read this before making any change.

Human contributors: [CONTRIBUTING.md](CONTRIBUTING.md) is the full guide and the source of
truth for everything summarised here.

## Commits and attribution

### No AI attribution

Do not add AI attribution trailers or footers to commits or pull request descriptions. None
of the following, in any form:

```text
Co-authored-by: Claude <noreply@anthropic.com>
Co-Authored-By: Claude Code <...>
Generated with Claude Code
🤖 Generated with ...
```

AI-generated/co-authored tags create legal and licensing gray areas. This project uses the
[DCO](https://developercertificate.org/): `Signed-off-by` is a certification by a real
person who has the right to submit the work under Apache 2.0. A model is not a legal person
and cannot make that certification, so naming one as an author muddies the provenance of
every file it touches.

The human contributor is the author and signs off. How the patch was drafted is not
recorded in the commit.

### Sign off every commit

```bash
git commit -s
```

Use a real name and a valid email — no pseudonyms, no `noreply` addresses. Commits without
a `Signed-off-by` line are not merged. To fix commits you already made:

```bash
git commit --amend -s          # the last commit
git rebase --signoff HEAD~3    # the last 3 commits
```

### Commit message format

[Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(<scope>): <description>
```

| Field | Values |
|-------|--------|
| Type | `feat` `fix` `perf` `refactor` `docs` `test` `chore` `ci` `build` `style` |
| Scope | `resp` `binary` `storage` `shard` `router` `persistence` `crypto` `metrics` `trace` `cli` `server` |

- The scope is optional. Omit it when the change is not specific to one area, as
  `CONTRIBUTING.md` does with `docs: update benchmark methodology section`.
- Description: lowercase, imperative mood, no period, max 72 characters.
- Reference issues in the footer: `Closes #123`.
- Breaking changes: append `!` after the type/scope, or put `BREAKING CHANGE:` in the
  footer.

## Before you commit

```bash
task check    # go vet + race tests — must pass
task fmt      # format
```

Without the `task` CLI, run the equivalents:

```bash
go vet ./...
go test -race ./...
go fmt ./...
```

## Codebase rules

- **The hot path must not allocate.** Request handling in `internal/storage`,
  `internal/router`, `internal/shard`, `internal/resp`, and `internal/network` is
  allocation-free by design. No `make()`, no string conversions, no `interface{}` boxing
  there. Verify with `go test -bench=. -benchmem ./path/to/pkg` and put before/after
  numbers in the pull request.
- **Never invent benchmark numbers.** If you did not run a benchmark, say so.
- **Every file opens with the C-style block header.** Copy the exact shape from a
  neighbouring file in the same package — the second line is a component title
  (`Tellstone Request Router`), not a fixed string.
- **Comment why, not what**, especially around allocation-sensitive code, cache-line
  padding, and non-obvious trade-offs. This codebase is heavily commented; match the
  density of the file you are editing.
- **Prefer the standard library.** Every dependency is a liability — security surface,
  build complexity, license risk. A new one needs justification in the pull request.
- **Opt-in features stay opt-in.** Encryption, metrics, persistence, RESP, and TLS are all
  disabled by default. Do not add overhead for users who have not enabled a feature.
- **Read [ARCHITECTURE.md](ARCHITECTURE.md)** before changing anything at the shard/router
  boundary.

## Coding Principles

### 1. Think Before Coding
Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First
Minimum code that solves the problem. Nothing speculative.

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.
- Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes
Touch only what you must. Clean up only your own mess.

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.
- The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution
Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"

For multi-step tasks, state a brief plan with clear verification steps.

## Where to look

| File | What's in it |
|------|--------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Package reference, request flow (binary + RESP), key design decisions |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Full contribution guide: DCO, commit conventions, code standards, PR process |
| [ROADMAP.md](ROADMAP.md) | Planned work and priorities |
| [Taskfile.yml](Taskfile.yml) | Build, test, benchmark, and profiling targets |
| [.github/pull_request_template.md](.github/pull_request_template.md) | What a pull request description must contain |
