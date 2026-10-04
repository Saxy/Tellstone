package sql

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// explainText runs an EXPLAIN and returns the plan as text.
func explainText(t *testing.T, cl *tclient, q string) string {
	t.Helper()
	frames := queryOK(t, cl, "EXPLAIN "+q, "EXPLAIN")
	data := findFrame(frames, msgDataRow)
	if data == nil {
		t.Fatalf("EXPLAIN %s returned no DataRow", q)
	}
	row, err := parseDataRow(data.val)
	if err != nil {
		t.Fatalf("EXPLAIN %s: %v", q, err)
	}
	if len(row) != 1 {
		t.Fatalf("EXPLAIN %s: got %d columns, want 1", q, len(row))
	}
	return string(row[0])
}

func seedExplainServer(t *testing.T, cl *tclient, table string) {
	t.Helper()
	queryOK(t, cl, fmt.Sprintf(
		`CREATE TABLE %s (id BIGINT PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT true)`, table), "CREATE TABLE")
	for i := 1; i <= 40; i++ {
		queryOK(t, cl, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (%d, 'n%d')`, table, i, i), "INSERT 0 1")
	}
}

// The plan EXPLAIN reports has to be the plan execution would choose, so the
// access methods it names are the ones the planner actually selected.
func TestExplainNamesTheChosenAccessMethod(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")

	for _, tc := range []struct {
		where string
		want  string
	}{
		{"id = 7", "Index Scan"},
		{"id > 10", "Range Scan"},
		{"id BETWEEN 10 AND 20", "Range Scan"},
		{"name = 'n7'", "Seq Scan"},
		{"id = 7 AND name = 'n7'", "Index Scan"},
	} {
		got := explainText(t, cl, "SELECT name FROM users WHERE "+tc.where)
		if !strings.Contains(got, tc.want) {
			t.Errorf("EXPLAIN ... WHERE %s = %q, want it to name %q", tc.where, got, tc.want)
		}
		if !strings.Contains(got, "on users") {
			t.Errorf("EXPLAIN ... WHERE %s does not name the table: %q", tc.where, got)
		}
	}
}

// EXPLAIN must not read anything. Asserted by pointing it at a row that is
// there and checking the answer is a plan and not the row: an EXPLAIN that
// executed would be indistinguishable from a query for everything this phase
// can already answer, and would make EXPLAIN useless for its purpose.
func TestExplainDoesNotExecute(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")

	got := explainText(t, cl, "SELECT name FROM users WHERE id = 7")
	if strings.Contains(got, "n7") {
		t.Fatalf("EXPLAIN returned row data, so it executed the query: %q", got)
	}
	if !strings.Contains(got, "cost=") {
		t.Fatalf("EXPLAIN output has no cost: %q", got)
	}
}

// A range EXPLAIN has to show which side is bounded, because an open bound is
// the difference between a range and a full scan wearing a range's name.
func TestExplainRangeShowsOpenAndClosedBounds(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")

	bounded := explainText(t, cl, "SELECT name FROM users WHERE id >= 10 AND id <= 20")
	if strings.Contains(bounded, "unbounded") {
		t.Errorf("two-sided range reports an open bound: %q", bounded)
	}
	openEnded := explainText(t, cl, "SELECT name FROM users WHERE id > 10")
	if !strings.Contains(openEnded, "unbounded") {
		t.Errorf("open-ended range does not say so: %q", openEnded)
	}
}

// Statistics are node-local and are absent until ANALYZE runs, so EXPLAIN has to
// distinguish "assumed" from "measured". Before ANALYZE it must not present a
// guess as a reading.
func TestExplainDistinguishesEstimatedFromAnalyzed(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")

	before := explainText(t, cl, "SELECT name FROM users WHERE name = 'n7'")
	if !strings.Contains(before, "never analyzed") {
		t.Errorf("un-analyzed table does not report the estimate: %q", before)
	}

	queryOK(t, cl, "ANALYZE users", "ANALYZE 1")

	after := explainText(t, cl, "SELECT name FROM users WHERE name = 'n7'")
	if strings.Contains(after, "never analyzed") {
		t.Errorf("analyzed table still claims it never was: %q", after)
	}
	if !strings.Contains(after, "40 rows") {
		t.Errorf("analyzed plan does not report the row count: %q", after)
	}
}

// ANALYZE has to count rows, not keys. A four-column row is four keys, so a
// counter that tallies keys would report 160 for 40 rows -- and a cost model
// fed that would price every query four times too high.
func TestAnalyzeCountsRowsNotKeys(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")

	queryOK(t, cl, "ANALYZE users", "ANALYZE 1")
	st, ok := srv.stats.Get(DefaultDB, "users")
	if !ok {
		t.Fatal("ANALYZE recorded no statistics")
	}
	if st.Rows != 40 {
		t.Fatalf("ANALYZE counted %d rows, want 40 (it counted keys: %d)", st.Rows, st.Keys)
	}
	if uint64(st.Keys) <= st.Rows {
		t.Fatalf("Keys (%d) should exceed Rows (%d); the two are not being tracked separately", st.Keys, st.Rows)
	}
	if !st.Analyzed || st.AtUnix == 0 {
		t.Fatalf("statistics not marked as collected: %+v", st)
	}
}

// A bare ANALYZE means every table, and each gets its own count.
func TestAnalyzeWithoutTableCoversEveryTable(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")
	seedExplainServer(t, cl, "orders")

	queryOK(t, cl, "ANALYZE", "ANALYZE 80")
	for _, name := range []string{"users", "orders"} {
		st, ok := srv.stats.Get(DefaultDB, name)
		if !ok || st.Rows != 40 {
			t.Errorf("%s: stats %+v (present=%v), want 40 rows", name, st, ok)
		}
	}
}

func TestAnalyzeRejectsUnknownTable(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	queryErr(t, cl, "ANALYZE nosuch", errUndefinedTable)
}

// VACUUM is the other half of the same parse node and is not the same thing:
// this layout deletes keys outright and has nothing to reclaim.
func TestVacuumIsRefused(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	msg := queryErr(t, cl, "VACUUM", errSyntax)
	if !strings.Contains(msg, "VACUUM") {
		t.Errorf("refusal should name VACUUM: %q", msg)
	}
}

// EXPLAIN ANALYZE would have to execute, so it is refused rather than silently
// degraded to a plan of a query that never ran.
func TestExplainAnalyzeIsRefused(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	msg := queryErr(t, cl, "EXPLAIN ANALYZE SELECT name FROM users WHERE id = 1", errSyntax)
	if !strings.Contains(msg, "EXPLAIN ANALYZE") {
		t.Errorf("refusal should name EXPLAIN ANALYZE: %q", msg)
	}
}

// There is no row-access plan for DDL or a transaction, so EXPLAIN of one has
// nothing to show.
func TestExplainRefusesStatementsWithNoPlan(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	queryErr(t, cl, "EXPLAIN CREATE TABLE t (id BIGINT)", errSyntax)
	queryErr(t, cl, "EXPLAIN BEGIN", errSyntax)
}

// A row count is an estimate input, so dropping a table must not leave its old
// count behind: a recreated table holding one row would keep the plan choices of
// a table that held a million.
func TestDropTableForgetsItsStatistics(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")
	queryOK(t, cl, "ANALYZE users", "ANALYZE 1")

	if st, _ := srv.stats.Get(DefaultDB, "users"); st.Rows != 40 {
		t.Fatalf("setup: stats = %+v, want 40 rows", st)
	}
	// Phase 9 refuses to drop a populated table, so the rows go first. The
	// statistics must still be forgotten: emptying a table is not dropping it.
	for i := 1; i <= 40; i++ {
		queryOK(t, cl, fmt.Sprintf(`DELETE FROM users WHERE id = %d`, i), "DELETE 1")
	}
	queryOK(t, cl, "DROP TABLE users", "DROP TABLE")
	if _, ok := srv.stats.Get(DefaultDB, "users"); ok {
		t.Error("statistics survived the table being dropped")
	}
}

// The rendered cost has to be readable and to actually carry a cost. A plan
// printing "cost=0.00..0.00" for a forty-row table would be a formatting bug
// that reads like a free query.
func TestExplainCostsAreNonZeroForAScan(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")

	got := explainText(t, cl, "SELECT name FROM users WHERE name = 'n7'")
	if !strings.Contains(got, "cost=") {
		t.Fatalf("no cost in %q", got)
	}
	if strings.Contains(got, "..0.00 rows=") {
		t.Errorf("full scan costed zero bytes: %q", got)
	}
}

func TestStatsAgeReportsUnmeasuredDistinctly(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	if got := statsAge(&TableStats{Analyzed: false}, now); got != "never analyzed" {
		t.Errorf("unmeasured = %q", got)
	}
	if got := statsAge(nil, now); got != "never analyzed" {
		t.Errorf("nil stats = %q", got)
	}
	fresh := &TableStats{Analyzed: true, AtUnix: now.Add(-2 * time.Minute).Unix()}
	if got := statsAge(fresh, now); !strings.Contains(got, "2m0s") {
		t.Errorf("two-minute-old stats = %q", got)
	}
	old := &TableStats{Analyzed: true, AtUnix: now.Add(-72 * time.Hour).Unix()}
	if got := statsAge(old, now); !strings.Contains(got, "3 days") {
		t.Errorf("three-day-old stats = %q", got)
	}
}

// RowDescription declares len(Cols) fields and the DataRow carries the cells, so
// a mismatch between them is a protocol violation. A real client rejects it
// ("unexpected field count in D message") and no in-process test notices,
// because parsing a DataRow does not involve parsing its description -- this
// only surfaced when EXPLAIN was first run through psql. So the widths are
// compared directly here.
func TestExplainRowDescriptionMatchesDataRowWidth(t *testing.T) {
	srv, _ := newTestServer(t, srvOpts{})
	cl := dialServer(t, srv.Addr())
	cl.startupTrust("default")
	seedExplainServer(t, cl, "users")

	for _, q := range []string{
		"EXPLAIN SELECT name FROM users WHERE id = 7",
		"EXPLAIN SELECT name FROM users WHERE id > 10",
		"EXPLAIN SELECT name FROM users",
	} {
		plan, err := Translate(q)
		if err != nil {
			t.Fatalf("%s: translate: %v", q, err)
		}
		if len(plan.Cols) != 1 {
			t.Errorf("%s: RowDescription would declare %d fields, want 1", q, len(plan.Cols))
			continue
		}
		// execExplain directly rather than through execute, which needs a
		// connection this test has no reason to build.
		out, err := srv.execExplain(plan)
		if err != nil {
			t.Fatalf("%s: execute: %v", q, err)
		}
		if len(out.row) != len(plan.Cols) {
			t.Errorf("%s: RowDescription declares %d fields but the DataRow carries %d",
				q, len(plan.Cols), len(out.row))
		}
		// out.row[0] is the single cell's *bytes*, not a column count, so this
		// only asserts the plan is non-empty.
		if len(out.row[0]) == 0 {
			t.Errorf("%s: plan text is empty", q)
		}
	}
}
