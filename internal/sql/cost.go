/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: cost.go
Description: The Phase 10 cost model and the table statistics it reads
(ADR-014 decisions 2 and 6).

The model is byte-based on purpose. The roadmap's version counts network hops,
but hop cost is not knowable at plan time on a cluster whose regions move, and
the planner is topology-agnostic so that a plan is reproducible under EXPLAIN and
does not have to be rebuilt when a region moves. What is knowable is how many
bytes a method has to read, which is what decides between a range and a full
scan today.

There are no histograms (ADR-014 decision 6). A histogram's only consumer is a
choice between an index scan and a table scan, and that choice does not exist
until Phase 13 creates an index. So selectivity for a non-key predicate is a
documented constant rather than a measurement, and the constants are named so
that a reader can see they are guesses. They are deliberately not presented as
statistics-derived: an estimate that looks measured is worse than one that
admits it is a guess, because nobody corrects a number they believe.
*/
package sql

import (
	"fmt"
	"math"
	"time"
)

// TableStats is what ANALYZE records about one table.
type TableStats struct {
	// Rows is the row count as of the last ANALYZE.
	Rows uint64
	// Analyzed reports whether ANALYZE has ever run for this table. It is
	// separate from Rows because zero is a real count -- an empty table -- and
	// must not be confused with "never measured", which is what a planner
	// needs in order to say so.
	Analyzed bool
	// Keys is how many column keys were seen while counting Rows. It is not
	// used to price anything -- the estimator works in rows -- but it is what
	// ANALYZE reports, because a row count alone cannot show a client that the
	// scan was as wide as it claims.
	Keys int64
	// AtUnix is when the count was taken, in seconds. Statistics go stale as
	// rows are written, and a planner that reports an age is more useful than
	// one that presents a stale count as current.
	AtUnix int64
}

// Cost is an estimate in abstract units. The unit is deliberately "bytes read",
// so the numbers are comparable across methods on one table and meaningless
// across tables. Absolute values are not latency.
type Cost struct {
	// Startup is the cost to produce the first row, and Total is the cost to
	// produce all of them. A plan is only ever latency-bound in its startup, so
	// separating them is what lets a future nested-loop join prefer the plan
	// with the lower startup even when its total is worse.
	Startup float64
	Total   float64
	// Rows is the estimated output cardinality, and Bytes the estimated bytes
	// read to produce it.
	Rows  float64
	Bytes float64
}

// Selectivity assumptions for predicates the statistics cannot inform.
//
// These are constants, not measurements. They are named to read as constants,
// and each is commented with what it assumes, because the day Phase 13 needs
// real selectivity this is the list to replace with histogram lookups.
const (
	// selKeyEquality assumes a primary key equality selects exactly the one row
	// it names. That is not a guess: it is the layout's guarantee, since the
	// primary key *is* the row id and a row id is unique.
	selKeyEquality = 1
	// selKeyRange assumes integer row ids are spread evenly across their
	// domain. For an auto-generated TSO id the rows are contiguous, so this
	// over-estimates the rows a range returns and therefore under-estimates its
	// cost -- which is the safe direction, since it makes the planner prefer a
	// range it should have avoided only when the range really is small.
	selKeyRange = 1.0 / 4
	// selGenericCompare assumes a comparison on a non-key column admits a
	// quarter of the table. Without a histogram there is nothing to do better
	// with, and this is close enough that the choice between two methods
	// neither of which can use an index rarely matters.
	selGenericCompare = 0.25
	// selNullTest assumes a nullable column is NULL for a tenth of the rows.
	// NULL is the absence of a key (ADR-013), so this is a guess about how often
	// a column is simply not written.
	selNullTest = 0.1
	// selLikePrefix assumes a prefix match admits a tenth of the table.
	selLikePrefix = 0.1
)

// defaultRowsForUnmeasured is what the planner assumes about a table ANALYZE
// has never seen. It is a deliberate non-zero value: a zero would make an
// unmeasured table cost nothing to scan, and the planner would then prefer it
// over a measured one for reasons that have nothing to do with the data.
const defaultRowsForUnmeasured = 1000

// rowBytes estimates the stored size of one row: every column key contributes
// its name plus the separator, and every column value contributes a width
// estimate for its type.
//
// The value widths are per-type guesses because there is no sampled data to
// average. What matters for planning is their *relative* size, which is a
// property of the schema: an INT column costs about 8 bytes however the data
// happens to fall, and a VARCHAR column's cost tracks its declared width.
func rowBytes(sch *Schema) float64 {
	total := 0.0
	for _, c := range sch.Columns {
		// The column name and its separator appear in the key.
		total += float64(len(c.Name)) + 1
		total += valueWidthGuess(c.Type)
	}
	return total
}

func valueWidthGuess(t ColumnType) float64 {
	switch t {
	case TypeInt, TypeBigInt, TypeTimestamp, TypeBool:
		// All of these are fixed-width encodings (ADR-013 section 4).
		return 8
	case TypeFloat:
		return 8
	case TypeVarchar:
		// A guess about text: short labels dominate, but the tail is long.
		// There is no declared length on a VARCHAR here to use instead.
		return 16
	case TypeBytes, TypeJSONB:
		return 32
	}
	return 16
}

// tableRows reports the row count to plan against, and whether it was measured.
func (o *optimizer) tableRows() (float64, bool) {
	if o.stats != nil && o.stats.Analyzed {
		return float64(o.stats.Rows), true
	}
	return defaultRowsForUnmeasured, false
}

// finish attaches a cost and a cardinality to a chosen plan.
//
// It is the single place cost is assigned, so every access method is costed the
// same way and no method can acquire an accidental free pass by being written
// before the estimator.
func (o *optimizer) finish(p *physicalPlan) (*physicalPlan, error) {
	rows, measured := o.tableRows()
	rb := rowBytes(o.sch)

	switch p.Kind {
	case PlanPointLookup:
		// Exactly one row, and one row's worth of keys to read. The descent
		// cost is not modelled: it is the same for every method and so cannot
		// change the choice between them.
		p.Rows = 1
		p.Cost = Cost{Startup: rb, Total: rb, Rows: 1, Bytes: rb}

	case PlanRangeScan:
		if p.EmptyRange {
			// No bytes are read. A range that cannot contain anything is
			// answered without touching the table, and saying so is the point
			// of EmptyRange being explicit.
			p.Rows = 0
			p.Cost = Cost{Startup: 0, Total: 0, Rows: 0, Bytes: 0}
			break
		}
		est := rows * selKeyRange
		// The residual filter discards rows the range returned, so a range with
		// a filter estimates fewer output rows than it reads. Reading more
		// rows than it keeps is the case worth recording: the cost is in the
		// bytes, not the survivors.
		if p.Filter != nil {
			est *= selectivityOf(p.Filter, rows, false)
		}
		p.Rows = est
		p.Cost = Cost{Startup: rb, Total: est * rb, Rows: est, Bytes: est * rb}

	case PlanFullScan:
		est := rows
		if p.Filter != nil {
			est = rows * selectivityOf(p.Filter, rows, true)
		}
		p.Rows = est
		// A full scan reads every row and *returns* the ones the filter keeps.
		// Pricing it by the output count is the bug this comment exists to
		// prevent: it makes a selective full scan look cheaper than the range
		// it should lose to, and the two estimates are then compared on
		// different quantities. Bytes are driven by rows read, which is all of
		// them; only the returned cardinality shrinks.
		p.Cost = Cost{Startup: rb, Total: rows * rb, Rows: est, Bytes: rows * rb}
	}

	// Recorded on the plan so EXPLAIN can distinguish a measured estimate from
	// an assumed one instead of printing both identically.
	p.Measured = measured
	if !measured {
		p.CostNote = fmt.Sprintf("rows estimated, table never analyzed")
	}
	return p, nil
}

// selectivityOf estimates the fraction of rows a predicate admits.
//
// rows is the table's row count, needed only to recognise a primary key
// equality. allowGeneric is false for a range that already applied the key
// constraint: a residual predicate over a range must not be charged the full
// generic cost twice.
func selectivityOf(e Expr, rows float64, allowGeneric bool) float64 {
	if e == nil {
		return 1
	}
	switch t := e.(type) {
	case *TrueExpr:
		return 1
	case *FalseExpr:
		return 0
	case *CmpExpr:
		return selGenericCompare
	case *NullTestExpr:
		return selNullTest
	case *LikeExpr:
		return selLikePrefix
	case *BoolExpr:
		switch t.Op {
		case BoolAnd:
			// Independent predicates are treated as independent. They are not
			// in general, and with no histograms there is nothing to correct
			// for the correlation -- so the product is a lower bound on the
			// rows, which is the direction that over-states cost.
			out := 1.0
			for _, arg := range t.Args {
				out *= selectivityOf(arg, rows, allowGeneric)
			}
			return out
		case BoolOr:
			// P(a) + P(b) - P(a)P(b): the union of two sets, which is what
			// makes OR cheaper than AND here even though it is not cheaper to
			// evaluate. An OR of overlapping predicates is under-counted by
			// the overlap term, so this is an upper bound.
			out := 0.0
			for _, arg := range t.Args {
				s := selectivityOf(arg, rows, allowGeneric)
				out = out + s - out*s
			}
			return out
		case BoolNot:
			return 1 - selectivityOf(t.Args[0], rows, allowGeneric)
		}
	}
	_ = rows
	return selGenericCompare
}

// statsAge renders how old a statistics reading is, for EXPLAIN. An unmeasured
// table has no age, which is reported differently on purpose: "never analyzed"
// is something a user can fix, and an age in days is not.
func statsAge(s *TableStats, now time.Time) string {
	if s == nil || !s.Analyzed {
		return "never analyzed"
	}
	d := now.Sub(time.Unix(s.AtUnix, 0))
	switch {
	case d < time.Hour:
		return fmt.Sprintf("analyzed %s ago", d.Round(time.Minute))
	case d < 48*time.Hour:
		return fmt.Sprintf("analyzed %d hours ago", int(d.Hours()))
	default:
		return fmt.Sprintf("analyzed %d days ago", int(d.Hours()/24))
	}
}

// clampRows keeps a cardinality estimate usable. A negative or NaN row count
// would make a cost comparison meaningless, and an estimate that cannot be
// compared is worse than a crude one.
func clampRows(f float64) float64 {
	if math.IsNaN(f) || f < 0 {
		return 0
	}
	return f
}
