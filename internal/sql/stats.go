/*
File: stats.go
*/

package sql

import (
	"errors"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

// statsStore holds per-table row counts, collected by ANALYZE and consulted by
// the cost estimator.
//
// It is deliberately **local and not replicated** (ADR-014 decision 5). A row
// count is an estimate input, not a correctness input: the planner must produce
// the same *plan shape* whether or not statistics exist, and it does, because
// every method here remains correct when fed a guess. What differs is the cost
// number shown by EXPLAIN and the order the methods are compared in.
//
// That property is what makes the cheap option safe. Replicating the count would
// need a new FSM opcode, a schema version bump, and a rule about which node's
// count wins when replicas disagree -- all to move a number that is already
// allowed to be wrong. The cost is that two nodes can estimate the same table
// differently, so EXPLAIN output is node-local. A client comparing plans across
// nodes is comparing plans from different estimators.
//
// The counts live in memory and do not survive a restart. A table that has not
// been analyzed behaves as unanalyzed, which is the same state as a fresh
// table, so nothing has to distinguish "never counted" from "counted before the
// restart".
type statsStore struct {
	mu    sync.RWMutex
	byKey map[string]TableStats
}

// statsKey identifies a table's statistics. The database is included even though
// every catalog table today lives in DefaultDB, because keyspace allows more
// than one and a key that silently merged two databases would be wrong the day
// a second exists.
func statsKey(db, table string) string { return db + "/" + table }

func newStatsStore() *statsStore {
	return &statsStore{byKey: make(map[string]TableStats)}
}

// Put records a freshly collected row count.
func (s *statsStore) Put(db, table string, st TableStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey[statsKey(db, table)] = st
}

// Get returns the recorded statistics. The bool reports whether ANALYZE has ever
// run for the table on this node, which is what separates "0 rows" from
// "unknown".
func (s *statsStore) Get(db, table string) (TableStats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.byKey[statsKey(db, table)]
	return st, ok
}

// Forget drops the count for one table. A DROP TABLE calls it, because keeping
// the old count would misprice every query against the new contents: a table
// recreated with one row would keep the estimate -- and so the plan choice -- of
// a table that held a million.
//
// The entry is removed rather than set to zero, and the distinction matters.
// Zero rows is a measured fact about a table that exists; a dropped table is not
// a table with no rows. Reporting the second would let a dropped-and-recreated
// table look empty to a planner entitled to assume otherwise.
func (s *statsStore) Forget(db, table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byKey, statsKey(db, table))
}

// analyzeTable counts the rows of one table by counting distinct row ids under
// its table prefix.
//
// Row *ids* are counted rather than primary key values. A row is a group of
// column keys sharing a row prefix, so counting every key would report a
// five-column row five times -- an estimate that is wrong by the width of every
// table, which is exactly the kind of error that makes a cost model choose the
// wrong plan. Deduplicating by row id is therefore not an optimisation, it is
// what makes the number mean what it says.
func (s *Server) analyzeTable(sch *Schema) (TableStats, error) {
	prefix := keyspace.TablePrefix(sch.DB, sch.Table)
	// The scan callback may run under the store's lock, so nothing is retained
	// past it. Tracking the previous row id as a string is enough, since the
	// callback receives keys in ascending order and every key of one row shares
	// a contiguous run.
	var (
		prev    string
		rows    int64
		columns int64
	)
	_, err := s.store.ScanPrefix(prefix, func(key, _ []byte) bool {
		id, ok := rowIDFromKey(prefix, key)
		if !ok {
			// A key under the table prefix that names no row cannot happen
			// through this package's writers. Counting it anyway would inflate
			// the total, and skipping it keeps the number an upper bound on real
			// rows rather than on unknown keys.
			return true
		}
		if id != prev {
			rows++
			prev = id
		}
		columns++
		return true
	})
	if err != nil {
		return TableStats{}, err
	}
	st := TableStats{
		Rows:     uint64(rows),
		Analyzed: true,
		AtUnix:   time.Now().Unix(),
	}
	st.Keys = columns
	s.stats.Put(sch.DB, sch.Table, st)
	return st, nil
}

// analyzeAll collects statistics for every table in the catalog. It is what a
// bare ANALYZE means.
func (s *Server) analyzeAll() ([]TableStats, error) {
	tables, err := s.catalog().Tables()
	if err != nil {
		return nil, s.schemaError(err)
	}
	out := make([]TableStats, 0, len(tables))
	for _, name := range tables {
		sch, err := s.catalog().Get(name)
		if err != nil {
			if errors.Is(err, ErrNoSuchTable) {
				// A concurrent DROP. Skipping is right: the goal is to describe
				// the tables that exist, and this one no longer does.
				continue
			}
			return nil, s.schemaError(err)
		}
		st, err := s.analyzeTable(sch)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

// statsFor returns the statistics to plan with: the local count when ANALYZE has
// run, and an explicitly unanalyzed placeholder otherwise.
//
// The placeholder carries the default row estimate rather than zero, so an
// unanalyzed table is not mistaken for an empty one. A cost model that priced a
// missing count at zero rows would conclude that every query against an
// unanalyzed table is free, and would choose a full scan for a table that is in
// fact large.
func (s *Server) statsFor(sch *Schema) *TableStats {
	if st, ok := s.stats.Get(sch.DB, sch.Table); ok {
		return &st
	}
	return &TableStats{Rows: defaultRowsForUnmeasured, Analyzed: false}
}
