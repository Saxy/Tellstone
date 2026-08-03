
# 📦 RBAC Package – README

**Location:** `internal/rbac/README.md`

**Purpose:** Explain the design, usage, and performance characteristics of Tellstone's role-based access control subsystem: policy loading, role parsing, per-connection authorization, and runtime `ROLE` management — with a zero-allocation, fail-closed authorization hot path.

---

## 🚀 Overview

The `rbac` package decides *who may run which command on which key*. It is deliberately Redis-ACL-flavored:

- **Roles** are named bundles of command permissions and key-namespace whitelists. Rules are Redis-style tokens: `+cmd` / `-cmd` grant or revoke one command, `+@cat` / `-@cat` a whole category, `~prefix` a key namespace.
- **Users** are principals bound to exactly one role with an optional bcrypt password hash. A hash is never rendered by `ROLE LIST` or `ROLE GETUSER`.
- **Policy is immutable and hot-swapped.** A `PolicyStore` is an immutable snapshot; writers build a complete replacement and publish it with a single atomic pointer store, so readers never block and never observe a half-applied policy. `SIGHUP` reloads the policy file the same way.
- **Fail-closed.** No role means deny-all; a missing namespace whitelist match means deny.

### Key Features

* **Zero-Allocation Hot Path:** `SessionContext.IsAllowed` is one bit test plus a prefix scan over raw key bytes — no allocations, no locks (verified: `0 B/op, 0 allocs/op`, ~3 ns).
* **Immutable Snapshots:** Readers call `Store.Load` lock-free and operate on a stable snapshot; mutations (ROLE commands, SIGHUP reloads) build a clone and swap it in one operation.
* **Session Pinning:** A connection's role is resolved once at handshake and pinned for its lifetime; a later policy swap never changes an in-flight session.
* **Fail-Closed by Default:** Unknown user, missing role, or no namespace match all deny. Unauthenticated data commands get `-NOAUTH`, permission denials get `-NOPERM`.
* **Hot Reload:** `SIGHUP` re-parses the policy file and atomically publishes the new snapshot, serialized against concurrent `ROLE` mutations.
* **Runtime Management:** `ROLE CREATE/SETUSER/DELUSER/DELETE/LIST/GETUSER` mutate the live policy without a restart.

---

## ⚡ Quick Start

### Policy file (YAML or JSON)

```yaml
roles:
  - name: admin
    rules: ["+@all", "~*"]
  - name: readonly
    rules: ["+@read", "~*"]
users:
  - name: admin
    role: admin
    password: "$2a$10$..."   # bcrypt hash; nopass: true for passwordless
default_role: readonly        # optional fallback for users without an explicit role
```

### Loading and authorizing

```go
import "github.com/Saxy/Tellstone/internal/rbac"

policy, _ := rbac.LoadFile("policy.yaml")     // also Parse([]byte) for embedded bytes
store := rbac.NewStore(policy)                // seed the atomic holder

// At handshake: resolve and pin the connection's role.
role := store.Load().RoleFor("alice")         // nil → fail-closed deny-all
sc := rbac.NewSessionContext("alice", role)

// Hot path: one bit test + prefix scan, zero allocations.
// Hot path: one bit test + prefix scan, zero allocations.
if !sc.IsAllowed(rbac.CmdGet, key) {          // key []byte
    // protocol layer replies -NOPERM and returns
}

// Mutations build a clone and swap it atomically.
store.CreateRole("operator", []string{"+@readwrite", "~cache:"})
store.SetUser("bob", "operator", rbac.PasswordFromOpts([][]byte{[]byte(">bobpw")}))
```

### Rule parsing

```go
role, _ := rbac.ParseRole("cache-manager", "+@readwrite", "-set", "~cache:")
```

`-` rules override `+` rules regardless of order; `~*` (or an empty whitelist) allows every key.

---

## 🛠️ API Summary

```go
// Policy loading (YAML or JSON; validated — bad files never half-apply)
func LoadFile(path string) (*PolicyStore, error)
func Parse(data []byte) (*PolicyStore, error)

// Policy snapshot — immutable once built
type PolicyStore struct { Roles map[string]*Role; Users map[string]*User; Default *Role }
func (p *PolicyStore) RoleFor(username string) *Role   // explicit → default → nil (fail-closed)
func (p *PolicyStore) UserFor(username string) *User
func (p *PolicyStore) NoPassDefault() bool             // passwordless "default" user exists
func (p *PolicyStore) Clone() *PolicyStore

// Atomic hot-swap holder — Load never blocks, never allocates
type Store struct { /* atomic pointer + mutation mutex */ }
func NewStore(policy *PolicyStore) *Store
func (s *Store) Load() *PolicyStore
func (s *Store) Store(policy *PolicyStore)
func (s *Store) Reload(policy *PolicyStore)            // SIGHUP path, serialized vs. mutations
func (s *Store) CreateRole(name string, rules []string) error
func (s *Store) SetUser(username, roleName string, passHash []byte) error
func (s *Store) DelUser(username string) error // rejects deleting the last ACL-management user
func (s *Store) DeleteRole(name string) error

// Per-connection authorization state — pinned at handshake
type SessionContext struct { Username, RoleName string }
func NewSessionContext(username string, role *Role) *SessionContext
func (s *SessionContext) AllowsCommand(cmd uint16) bool // keyless commands (ROLE, PING, AUTH)
func (s *SessionContext) IsAllowed(cmd uint16, key []byte) bool

// Roles and rules
func ParseRole(name string, rules ...string) (*Role, error)
type Role struct { Name string; Permissions Bitset; Namespaces [][]byte }
func (r *Role) AllowsKey(key []byte) bool
func (r *Role) GrantedCommands() []string
func (r *Role) IncCommands(); func (r *Role) Commands() uint64

// Bitset of granted commands (zero-alloc membership test)
type Bitset []uint64
func NewBitset(commands []uint16) Bitset
func (b Bitset) Has(id uint16) bool
func (b *Bitset) Set(id uint16); func (b Bitset) Clear(id uint16)

// Command registry and categories
func LookupCommand(name string) (uint16, bool)
func CommandName(id uint16) string
func Category(name string) []uint16

// Passwords
type User struct { Role string; PasswordHash []byte }
func PasswordFromOpts(opts [][]byte) ([]byte, error) // ">pw" hashes, "nopass" clears

// Store-wide denial counters (atomic, no locks)
func (s *Store) IncAuthFailure(); func (s *Store) AuthFailures() uint64
func (s *Store) IncDenied(); func (s *Store) DeniedCommands() uint64
func (s *Store) RoleCommandCounts() map[string]uint64

// ACL LOG — the auth-failure buffer behind ACL LOG (mutex-protected, lives
// across policy hot-swaps). LogAuthFailure records an entry and bumps the
// auth-failure counter; AuthLog returns the entries oldest-first.
type AuthLogEntry struct { Timestamp time.Time; Username, RemoteAddr, Reason string }
func (s *Store) LogAuthFailure(username, remoteAddr, reason string)
func (s *Store) AuthLog() []AuthLogEntry
```

---

## 📈 Performance & Benchmarks

The authorization hot path (`IsAllowed`) is the strictest requirement in the package: **zero allocations, single-digit nanoseconds, no locks**. It is a single bit test in the role's permission bitset plus a prefix scan over the raw key bytes.

**Latest benchmark run (2026-08-01)** on an **AMD Ryzen 9 9950X (16 cores, 32 threads, Zen 5)**:

```text
goos: linux
goarch: amd64
pkg: github.com/Saxy/Tellstone/internal/rbac
cpu: AMD Ryzen 9 9950X 16-Core Processor

BenchmarkIsAllowedAllowed-32            895,796,976          3.114 ns/op          0 B/op          0 allocs/op
BenchmarkIsAllowedDeniedByPrefix-32     629,547,681          3.533 ns/op          0 B/op          0 allocs/op
```

Run them yourself:

```bash
go test -bench=. -benchmem ./internal/rbac/
```

---

## 🛡️ Built-in Categories

`+@cat` expands to the category's registered commands. `-` rules always win, regardless of order.

| Category | Grants |
|----------|--------|
| `login` | AUTH, PING, COMMAND |
| `read` | GET, INFO |
| `write` | SET, DEL |
| `readwrite` | read + write + login |
| `operator` | readwrite + FLUSH |
| `maintenance` | FLUSH, SHUTDOWN, CONFIG, DEBUG, MONITOR |
| `admin` | AUTH, ROLE, ACL, USER, GRANT, REVOKE |
| `all` / `none` | every registered command / nothing |

---

## 🔑 Password Handling

* Policy-file passwords are **bcrypt hashes** (`$2a$10$...`); `Parse` validates the format cheaply via prefix + length checks, full verification happens at the first `AUTH`.
* `nopass: true` (or the `nopass` SETUSER option) marks a passwordless user (Redis ACL semantics). A passwordless `default` user lets connections start authenticated and inherit its effective role.
* `ROLE SETUSER bob role '>bobpw'` bcrypt-hashes the plaintext server-side, so runtime-created users need no tooling.
* Generate a hash with `htpasswd -nbBC 10 "" PASSWORD | tr -d ':\n'` or `mkpasswd -m bcrypt PASSWORD`. **Do not** use `openssl passwd` — it emits SHA-512-crypt, not bcrypt.

---

## 📂 Package Contents

```
policy.go    – PolicyStore (immutable snapshot) and Store (atomic hot-swap holder)
role.go      – Role type, namespace whitelist matching
session.go   – SessionContext, the pinned per-connection authorization state
parser.go    – Rule token parsing (+cmd / -@cat / ~prefix)
cmd.go       – Command registry, LookupCommand/CommandName, categories
config.go    – YAML/JSON policy loading with full validation
user.go      – User type and the SETUSER password-option parser
manager.go   – ROLE mutation helpers (CreateRole, SetUser, DelUser, DeleteRole)
rbac.go      – Bitset command-permission container
metrics.go   – Atomic per-role command counts and store-wide denial counters
*_test.go    – Unit tests (incl. race) and bench_test.go (hot-path benchmarks)
```

---

## 🔨 Development & Testing

```bash
# Unit tests, including the authorization semantics and fail-closed behavior
go test -v ./internal/rbac/

# Race-enabled (the atomic snapshot swap is the core concurrency contract)
go test -race ./internal/rbac/

# Hot-path benchmark: must stay at 0 allocs/op
go test -bench=. -benchmem ./internal/rbac/
```

---

## 📌 Architectural Constraints & Boundaries

* **The hot path must not allocate.** `IsAllowed`, `AllowsCommand`, and `Store.Load` are allocation-free by design. Allocation-free is verified by `-benchmem`; a regression here is a regression in the request path of the whole server.
* **Fail-closed everywhere.** A nil role, an unknown user, or a non-matching namespace must deny — never allow. `RoleFor` returns `nil` for "no role applies" and callers treat that as deny-all.
* **Readers never block.** `Load` is a lock-free atomic load; correctness comes from whole-snapshot swap, so readers always see a complete policy.
* **Mutations are serialized.** `CreateRole`/`SetUser`/`DelUser`/`DeleteRole` and `Reload` all take `mu` so a `SIGHUP` reload can never land between a mutation's clone and its republish and silently discard the other operation.
* **Hashes are secrets.** A `User`'s `PasswordHash` is never rendered by `ROLE LIST` or `ROLE GETUSER`; only role assignment and the AUTH path touch it.

*This document accurately represents the authorization core of Tellstone as of 2026-08-03.*
