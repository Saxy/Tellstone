/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: txn.go
Description: Transaction state and the MVCC overlay a transaction block executes
against (Phase 12).

A transaction is a buffered write set over the shared store. BEGIN allocates a
start timestamp from the node's timestamp oracle; every statement inside the
block reads through an overlay that layers the transaction's own buffered writes
over the committed store, so the block sees its own uncommitted effects while no
other session can. COMMIT validates the write set against every key another
transaction committed since this one started, then applies the whole set under a
single lock; ROLLBACK discards it.

The store offers atomic single-key operations only, so this file supplies the
transaction the storage layer does not: read-your-writes visibility, and
write-write plus read-write conflict detection on the way out.

Scope of this stage, stated plainly rather than hidden:
  - Conflict detection is node-local. Two transactions on one node conflict
    correctly; a transaction whose writes are served by another node does not
    yet detect a concurrent writer there. Cross-region two-phase commit is the
    remaining Phase 12 work.
  - A commit that spans keys is validated as a whole and then applied key by
    key. The storage layer has no atomic multi-key write, so this is not a
    single atomic commit at the storage level; conflict detection is what keeps
    the applied result consistent.
  - MVCC versions catalog-table row keys, so a block reads column values as of
    its start timestamp (see mvcc.go). The implicit "tellstone" table is a raw
    single-key table shared with the binary frontend and is left unversioned;
    its reads still fall back to read-set validation at commit.
*/
package sql

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Saxy/Tellstone/internal/mvcc"
)

// TimestampOracle supplies strictly increasing timestamps for transaction start
// and commit ordering. Cluster deployments wire the placement driver's TSO
// through SetTimestampOracle; standalone deployments keep the local fallback.
type TimestampOracle interface {
	// Next returns the next timestamp. Values must be strictly greater than
	// every value returned before, so a later transaction always orders after
	// an earlier one.
	Next() (uint64, error)
}

// txnLogicalBits matches the cluster TSO's logical counter width, so a
// standalone timestamp and a cluster timestamp are laid out identically and
// compare the same way.
const txnLogicalBits = 20

const txnLogicalMask = (uint64(1) << txnLogicalBits) - 1

// packTimestamp combines physical milliseconds and a logical counter into one
// comparable uint64, mirroring cluster.PackTimestamp.
func packTimestamp(physicalMS, logical uint64) uint64 {
	return physicalMS<<txnLogicalBits | logical&txnLogicalMask
}

// localOracle is the standalone timestamp source: physical milliseconds from
// the wall clock with a logical counter for the sub-millisecond and
// clock-regression cases. It keeps the monotonic guarantee a wall clock alone
// does not.
type localOracle struct {
	last atomic.Uint64
}

func (o *localOracle) Next() (uint64, error) {
	for {
		now := packTimestamp(uint64(time.Now().UnixMilli()), 0)
		prev := o.last.Load()
		if now <= prev {
			// The clock did not advance (or moved backwards); burn a logical
			// step on top of the last value so ordering still holds.
			now = prev + 1
		}
		if o.last.CompareAndSwap(prev, now) {
			return now, nil
		}
	}
}

// txnWrite is one buffered mutation of a transaction.
//
// mvcc marks a mutation of a catalog-table key, whose committed form is a list
// of versions rather than a single value. Such a write is applied by appending a
// frame at the commit timestamp instead of overwriting the key, which is what
// lets a concurrent reader still resolve the old value at its own snapshot. A
// raw write (the implicit table) has no versions and overwrites as before.
type txnWrite struct {
	value   []byte
	deleted bool
	mvcc    bool
}

// txnState is one open transaction block on a connection.
//
// It is its own Store: statement execution that runs inside the block reads and
// writes through it, and the committed store is only touched when the block
// commits. That is what makes a write issued after BEGIN invisible to every
// other session until COMMIT, and what makes ROLLBACK total.
type txnState struct {
	startTS uint64
	base    Store

	writes map[string]*txnWrite
	order  []string

	// reads is the set of committed keys a lookup or MVCC range read observed
	// during the block. It is validated at commit against the same table the
	// write set is, which closes the read-write anomaly the write-only check
	// would leave: a block that read a row another transaction replaced cannot
	// commit a decision derived from the replaced value. A raw range read (the
	// implicit table) is not tracked -- only point lookups there carry this
	// guarantee.
	reads map[string]struct{}

	// failed marks a block that hit an error. PostgreSQL semantics: every
	// statement after a failure is refused until the block ends, and a COMMIT
	// of a failed block is reported as ROLLBACK.
	failed bool

	// localGUCs holds the SET LOCAL overrides issued during the block. They are
	// discarded when the block ends, whether by COMMIT or ROLLBACK, so the
	// session value set before the block shows through again (ADR-016 D6).
	localGUCs map[string]string
}

func newTxnState(startTS uint64, base Store) *txnState {
	return &txnState{
		startTS:   startTS,
		base:      base,
		writes:    map[string]*txnWrite{},
		reads:     map[string]struct{}{},
		localGUCs: map[string]string{},
	}
}

// record buffers one mutation, remembering the first time a key is named so the
// commit applies it once and in a deterministic order.
func (t *txnState) record(key string, w *txnWrite) {
	if _, ok := t.writes[key]; !ok {
		t.order = append(t.order, key)
	}
	t.writes[key] = w
}

// markRead records that the block observed a committed key. The MVCC view calls
// this on every key it resolves, so a read of a versioned key is validated at
// commit even when the block never writes it.
func (t *txnState) markRead(key string) {
	t.reads[key] = struct{}{}
}

// lookup reads a key through the overlay: a buffered write wins over the
// committed store, and a buffered delete hides it.
func (t *txnState) lookup(key string) ([]byte, bool, error) {
	t.reads[key] = struct{}{}
	if w, ok := t.writes[key]; ok {
		if w.deleted {
			return nil, false, nil
		}
		return w.value, true, nil
	}
	return t.base.GetErr(key)
}

// ---- Store implementation ----

func (t *txnState) GetErr(key string) ([]byte, bool, error) {
	return t.lookup(key)
}

func (t *txnState) Set(key string, value []byte, _ time.Duration) error {
	t.record(key, &txnWrite{value: append([]byte(nil), value...)})
	return nil
}

func (t *txnState) SetIfAbsent(key string, value []byte, _ time.Duration) (bool, error) {
	_, ok, err := t.lookup(key)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	t.record(key, &txnWrite{value: append([]byte(nil), value...)})
	return true, nil
}

func (t *txnState) SetIfPresent(key string, value []byte, _ time.Duration) (bool, error) {
	_, ok, err := t.lookup(key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	t.record(key, &txnWrite{value: append([]byte(nil), value...)})
	return true, nil
}

func (t *txnState) Delete(key string) (bool, error) {
	_, ok, err := t.lookup(key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	t.record(key, &txnWrite{deleted: true})
	return true, nil
}

// ScanPrefix serves a range read from the overlay: the committed entries in the
// prefix with the transaction's own buffered writes and deletes layered on top,
// returned in key order. The merged set is materialized before the callback
// runs, so the callback may itself issue reads or buffered writes.
func (t *txnState) ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error) {
	type entry struct {
		value   []byte
		deleted bool
	}
	merged := map[string]entry{}
	_, err := t.base.ScanPrefix(prefix, func(key, value []byte) bool {
		merged[string(key)] = entry{value: append([]byte(nil), value...)}
		return true
	})
	if err != nil {
		return 0, err
	}
	for key, w := range t.writes {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if w.deleted {
			merged[key] = entry{deleted: true}
		} else {
			merged[key] = entry{value: w.value}
		}
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	n := 0
	for _, key := range keys {
		e := merged[key]
		if e.deleted {
			continue
		}
		if !fn([]byte(key), e.value) {
			break
		}
		n++
	}
	return n, nil
}

// txnCount reports how many distinct keys the transaction has buffered.
func (t *txnState) txnCount() int { return len(t.order) }

// commitStore applies a transaction's write set to the store after the conflict
// check, assigning each key the same commit timestamp.
//
// The node-local lastCommit map is the conflict oracle: a key whose last
// committed timestamp is newer than this transaction's start timestamp was
// changed by a transaction that ran concurrently, so this one must abort rather
// than silently overwrite it. The check and the apply share one lock, so two
// conflicting commits cannot both pass.
type commitTable struct {
	mu         sync.Mutex
	lastCommit map[string]uint64
}

func newCommitTable() *commitTable {
	return &commitTable{lastCommit: map[string]uint64{}}
}

// SetTimestampOracle installs the timestamp source transactions use. It must be
// called before the listener starts serving; a nil oracle is ignored so the
// standalone fallback survives a wiring gap.
func (s *Server) SetTimestampOracle(tso TimestampOracle) {
	if tso != nil {
		s.tso = tso
	}
}

// nextTS allocates one timestamp, mapping an oracle failure onto the SQLSTATE a
// client can retry.
func (s *Server) nextTS() (uint64, error) {
	ts, err := s.tso.Next()
	if err != nil {
		return 0, &pgError{code: errIoError, msg: err.Error()}
	}
	return ts, nil
}

// commitTxn validates and applies a transaction's buffered write set.
//
// Conflict detection happens first and in full: if any key the transaction
// wrote was committed by another transaction after this one started, the whole
// transaction aborts and nothing is applied. Only once every key is known to be
// uncontended is the write set applied, each key stamped with one commit
// timestamp so a later transaction's start timestamp orders correctly against
// it.
func (s *Server) commitTxn(t *txnState) error {
	if t.txnCount() == 0 {
		return nil
	}
	commitTS, err := s.nextTS()
	if err != nil {
		return err
	}
	s.commit.mu.Lock()
	defer s.commit.mu.Unlock()
	// A key read during the block that another transaction committed after this
	// one started makes the block's view stale, even if this block wrote a
	// different key: committing would record a decision derived from a value
	// that no longer holds. That is the read-write arm of serializability.
	//
	// The check consults two oracles. The lastCommit map catches writers that
	// committed through this node's transaction path. The newer frame timestamp
	// catches a version appended by an autocommit write, which never touches the
	// map: on a versioned key the committed value itself records when it changed.
	for key := range t.reads {
		conflict, err := s.keyCommittedAfter(key, t.startTS)
		if err != nil {
			return err
		}
		if conflict {
			return &pgError{
				code: errSerializationFailure,
				msg:  "could not serialize access due to concurrent read",
			}
		}
	}
	for _, key := range t.order {
		conflict, err := s.keyCommittedAfter(key, t.startTS)
		if err != nil {
			return err
		}
		if conflict {
			return &pgError{
				code: errSerializationFailure,
				msg:  "could not serialize access due to concurrent update",
			}
		}
	}
	for _, key := range t.order {
		w := t.writes[key]
		var serr error
		switch {
		case w.mvcc:
			// Append a version rather than overwrite: a reader at an older
			// snapshot must still be able to resolve the value it saw.
			raw, _, gerr := s.store.GetErr(key)
			if gerr != nil {
				return &pgError{code: errIoError, msg: gerr.Error()}
			}
			serr = s.store.Set(key, mvcc.Append(raw, commitTS, w.value, w.deleted), 0)
		case w.deleted:
			_, serr = s.store.Delete(key)
		default:
			serr = s.store.Set(key, w.value, 0)
		}
		if serr != nil {
			return &pgError{code: errIoError, msg: serr.Error()}
		}
		s.commit.lastCommit[key] = commitTS
	}
	return nil
}

// keyCommittedAfter reports whether a key changed after startTS, closing the
// race the transaction's own write started. It reads the node-local commit map
// first and then the committed value: on a versioned key the newest frame
// carries the timestamp of the last write, so an autocommit writer is caught
// even though it never records into the map.
func (s *Server) keyCommittedAfter(key string, startTS uint64) (bool, error) {
	if last, ok := s.commit.lastCommit[key]; ok && last > startTS {
		return true, nil
	}
	raw, ok, err := s.store.GetErr(key)
	if err != nil {
		return false, &pgError{code: errIoError, msg: err.Error()}
	}
	if ok && mvcc.Latest(raw) > startTS {
		return true, nil
	}
	return false, nil
}
