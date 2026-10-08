/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: executor_bench_test.go
Description: The Phase 11 executor benchmarks: the scan engine's full-table read
(BenchmarkScanSelect) and the hash join's build and probe over the real server
(BenchmarkHashJoin). Both seed their tables through production paths so the key
layout, scan order and wire delivery they measure are the ones a client sees.
*/
package sql

import (
	"fmt"
	"testing"

	"github.com/Saxy/Tellstone/internal/keyspace"
	"github.com/Saxy/Tellstone/internal/storage"
)

// BenchmarkScanSelect measures the Phase 11 scan engine: a full scan of every
// row, walked by prefix, batched into the reused chunk, drained into the result
// set with one wire render per surviving cell. The wire client is left out of
// the loop (the step 1 pivot made framing cheap and second order); storage is
// governed by internal/storage's own benchmarks. The table is seeded straight
// into the real engine the way insertRow would write it, so the key layout and
// scan order are production-accurate.
func BenchmarkScanSelect(b *testing.B) {
	for _, rows := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			store := &engineStore{e: storage.NewEngine(0, 0, 0, testLogger(), nil)}
			srv := NewServer("127.0.0.1:0", store, nil, nil, nil, nil, nil, testLogger(), false)
			b.Cleanup(srv.Close)

			sch := &Schema{
				DB: DefaultDB, Table: "t", PrimaryKey: 0,
				Columns: []Column{
					{Name: "id", Type: TypeBigInt},
					{Name: "a", Type: TypeVarchar},
					{Name: "b", Type: TypeBigInt},
				},
			}
			if err := sch.Validate(); err != nil {
				b.Fatalf("schema: %v", err)
			}
			for i := 0; i < rows; i++ {
				id, err := EncodeValue(TypeBigInt, int64(i))
				if err != nil {
					b.Fatalf("encode pk: %v", err)
				}
				bv, err := EncodeValue(TypeBigInt, int64(i))
				if err != nil {
					b.Fatalf("encode b: %v", err)
				}
				cells := rowCells{
					{value: id, set: true},
					{value: []byte(fmt.Sprintf("row-%d", i)), set: true},
					{value: bv, set: true},
				}
				if err := srv.insertRow(sch, keyspace.EncodeIntRowID(int64(i)), cells); err != nil {
					b.Fatalf("insertRow: %v", err)
				}
			}

			plan := &Plan{
				Kind:   StmtSelect,
				Table:  "t",
				Schema: sch,
				Cols:   []string{"id", "a", "b"},
				Phys:   &physicalPlan{Kind: PlanFullScan, Schema: sch},
			}
			n := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := srv.execScan(plan, nil)
				if err != nil {
					b.Fatalf("execScan: %v", err)
				}
				n += len(out.rows)
			}
			if n == 0 {
				b.Fatal("scan produced no rows")
			}
		})
	}
}

func BenchmarkHashJoin(b *testing.B) {
	srv, _ := newTestServer(b, srvOpts{})
	cl := dialServer(b, srv.Addr())
	cl.startupTrust("default")
	queryOK(b, cl, `CREATE TABLE j1 (id BIGINT PRIMARY KEY, x BIGINT)`, "CREATE TABLE")
	queryOK(b, cl, `CREATE TABLE j2 (id BIGINT PRIMARY KEY, j1_id BIGINT)`, "CREATE TABLE")
	for i := int64(1); i <= 1000; i++ {
		queryOK(b, cl, fmt.Sprintf(`INSERT INTO j1 (id, x) VALUES (%d, %d)`, i, i), "INSERT 0 1")
		queryOK(b, cl, fmt.Sprintf(`INSERT INTO j2 (id, j1_id) VALUES (%d, %d)`, i, i), "INSERT 0 1")
	}
	q := `SELECT j1.x FROM j1 JOIN j2 ON j1.id = j2.j1_id WHERE j2.id <= 1000`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selectRows(b, cl, q, 1000)
	}
}
