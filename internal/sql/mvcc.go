/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: mvcc.go
Description: The MVCC view catalog-table statements execute against (Phase 12).

A catalog row is a key range and each column is one key. The version encoding
lives in internal/mvcc; this file is the Store that applies it to the SQL
frontend.

Scope. This applies to catalog-table rows, whose shape the schema layer knows.
The implicit Phase 8 "tellstone" table is a raw single-key table shared with the
binary frontend, so it is deliberately left unversioned: versioning it would put
the frame envelope where the other protocol expects the caller's bytes. The
mvccView's snapshot and commit timestamp come from the same timestamp oracle the
transaction layer uses, so a version's timestamp orders against a transaction's
start timestamp.
*/
package sql

import (
	"sort"
	"strings"
	"time"

	"github.com/Saxy/Tellstone/internal/mvcc"
)

// mvccView presents a store's catalog-row data at a read snapshot and writes
// versions at a commit timestamp.
//
// With an overlay (an open transaction) reads prefer the transaction's buffered
// writes and otherwise resolve committed data at the block's start timestamp;
// writes are buffered, not applied, and are versioned when the block commits.
// Without an overlay (autocommit) reads resolve at the latest snapshot and
// writes version immediately under this statement's commit timestamp.
type mvccView struct {
	base    Store
	overlay *txnState
	commit  *commitTable
	// snapshot is the timestamp committed data is resolved at, and writeTS the
	// timestamp an autocommit version is stamped with. Inside a block the
	// snapshot is the block's start timestamp and writeTS is unused, because
	// writes are buffered until the block commits.
	snapshot uint64
	writeTS  uint64
}

// appendLocked versions a key at the view's write timestamp, reading the current
// value and writing it back. The caller holds the key's stripe lock for an
// autocommit write; a buffered write never reaches here.
func (v *mvccView) appendLocked(key string, value []byte, tomb bool) error {
	raw, _, err := v.base.GetErr(key)
	if err != nil {
		return err
	}
	var horizon uint64
	if v.commit != nil {
		horizon = v.commit.horizon()
	}
	return v.base.Set(key, mvcc.Append(raw, v.writeTS, value, tomb, horizon), 0)
}

func (v *mvccView) GetErr(key string) ([]byte, bool, error) {
	if v.overlay != nil {
		v.overlay.markRead(key)
		if w, ok := v.overlay.writes[key]; ok {
			if w.deleted {
				return nil, false, nil
			}
			return w.value, true, nil
		}
	}
	raw, ok, err := v.base.GetErr(key)
	if err != nil || !ok {
		return nil, false, err
	}
	val, present := mvcc.Resolve(raw, v.snapshot)
	return val, present, nil
}

// Set/SetIfAbsent/SetIfPresent/Delete serialize an autocommit mutation against
// every other writer of the same key: a version append reads the current value
// and writes it back, and a conditional write reads existence and then writes,
// so without the lock two autocommit statements could lose a version or both
// claim the same key. The lock is the key's stripe, not a global lock, so
// autocommit writes to different keys proceed concurrently. A buffered write
// inside a block touches nothing and takes no lock; the commit path takes it
// instead.
func (v *mvccView) Set(key string, value []byte, _ time.Duration) error {
	if v.overlay != nil {
		v.overlay.record(key, &txnWrite{value: append([]byte(nil), value...), mvcc: true})
		return nil
	}
	if v.commit != nil {
		v.commit.locks.lock(key)
		defer v.commit.locks.unlock(key)
	}
	return v.appendLocked(key, value, false)
}

func (v *mvccView) SetIfAbsent(key string, value []byte, _ time.Duration) (bool, error) {
	if v.overlay != nil {
		// Existence is checked through the view, not the overlay's raw base:
		// the base still holds a version that the snapshot resolves absent (a
		// row deleted before the block started, or deleted then re-inserted
		// inside it), and a raw read would call that present and refuse an
		// insert PostgreSQL allows. GetErr also records the read for commit
		// validation.
		if _, ok, err := v.GetErr(key); err != nil || ok {
			return false, err
		}
		v.overlay.record(key, &txnWrite{value: append([]byte(nil), value...), mvcc: true})
		return true, nil
	}
	if v.commit != nil {
		v.commit.locks.lock(key)
		defer v.commit.locks.unlock(key)
	}
	if _, ok, err := v.GetErr(key); err != nil || ok {
		return false, err
	}
	return true, v.appendLocked(key, value, false)
}

func (v *mvccView) SetIfPresent(key string, value []byte, _ time.Duration) (bool, error) {
	if v.overlay != nil {
		// As in SetIfAbsent, the check goes through the view so a row whose
		// latest version the snapshot resolves absent is not treated as present.
		if _, ok, err := v.GetErr(key); err != nil || !ok {
			return false, err
		}
		v.overlay.record(key, &txnWrite{value: append([]byte(nil), value...), mvcc: true})
		return true, nil
	}
	if v.commit != nil {
		v.commit.locks.lock(key)
		defer v.commit.locks.unlock(key)
	}
	if _, ok, err := v.GetErr(key); err != nil || !ok {
		return false, err
	}
	return true, v.appendLocked(key, value, false)
}

func (v *mvccView) Delete(key string) (bool, error) {
	_, existed, err := v.GetErr(key)
	if err != nil {
		return false, err
	}
	if v.overlay != nil {
		v.overlay.record(key, &txnWrite{deleted: true, mvcc: true})
		return existed, nil
	}
	if v.commit != nil {
		v.commit.locks.lock(key)
		defer v.commit.locks.unlock(key)
	}
	// Write the tombstone even when the read said the key was absent. That read
	// may have been served best-effort and gone stale, and skipping the version
	// would leave a live key behind that makes the row reappear and the table
	// look non-empty. An extra tombstone on a genuinely absent key resolves
	// absent, so it is harmless.
	if err := v.appendLocked(key, nil, true); err != nil {
		return existed, err
	}
	return existed, nil
}

// ScanPrefix resolves each committed key in the prefix at the snapshot, drops
// the ones that are absent then, and layers the transaction's buffered writes
// on top, all in key order.
func (v *mvccView) ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error) {
	type entry struct {
		value   []byte
		deleted bool
	}
	merged := map[string]entry{}
	_, err := v.base.ScanPrefix(prefix, func(key, value []byte) bool {
		if v.overlay != nil {
			// A range read observes these keys just as a point lookup would, so
			// record them: a block that scans a row and then commits a decision
			// derived from it must be validated against concurrent writers too.
			v.overlay.markRead(string(key))
		}
		if val, present := mvcc.Resolve(value, v.snapshot); present {
			merged[string(key)] = entry{value: append([]byte(nil), val...)}
		} else {
			merged[string(key)] = entry{deleted: true}
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if v.overlay != nil {
		for key, w := range v.overlay.writes {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if w.deleted {
				merged[key] = entry{deleted: true}
			} else {
				merged[key] = entry{value: w.value}
			}
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
