package sql

/*
Tellstone PostgreSQL Wire Frontend
File: benchmark_pk_test.go
Description: BenchmarkPGSimpleQueryPK, the Phase 0 primary-key read benchmark:
the pgbench "SELECT ... WHERE pk = $1" shape, executed through the real planner
and row assembler against a real shard layer.

Why a router-backed store rather than engineStore: a standalone deployment
reaches storage through server.RouterStore, and Router.Dispatch hashes the
*whole* key, so a row's column keys scatter across every shard. Reconstructing
one row is therefore an all-shards walk whose results are collected and merged,
and that collection -- not the engine -- is what a PK read actually pays for.
A single-engine store would hide exactly the cost this benchmark exists to show,
which is why executor_bench_test.go's engineStore is not reused here.

It lives in package sql because the executor entry points it measures
(execSelectRow, readRow) are unexported.

Authors:

	Maximilian Hagen
*/

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/keyspace"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/router"
	"github.com/Saxy/Tellstone/internal/shard"
)

// routerStore adapts a router.Router to the Store seam exactly the way
// server.RouterStore does. It is duplicated here rather than imported because
// server.RouterStore's router field is unexported and package server has no
// constructor for one; the method bodies are a line-for-line match with
// server/server.go so the path under measurement is the production path.
type routerStore struct {
	r *router.Router
}

func (s *routerStore) GetErr(key string) ([]byte, bool, error) {
	resp := s.r.Dispatch(shard.CmdGet, key, nil, 0)
	return resp.Value, resp.OK, resp.Err
}

func (s *routerStore) Set(key string, value []byte, ttl time.Duration) error {
	return s.r.Dispatch(shard.CmdSet, key, value, ttl).Err
}

func (s *routerStore) SetIfAbsent(key string, value []byte, ttl time.Duration) (bool, error) {
	resp := s.r.Dispatch(shard.CmdSetNX, key, value, ttl)
	return resp.OK, resp.Err
}

func (s *routerStore) SetIfPresent(key string, value []byte, ttl time.Duration) (bool, error) {
	resp := s.r.Dispatch(shard.CmdSetXX, key, value, ttl)
	return resp.OK, resp.Err
}

func (s *routerStore) Delete(key string) (bool, error) {
	resp := s.r.Dispatch(shard.CmdDel, key, nil, 0)
	return resp.OK, resp.Err
}

func (s *routerStore) ScanPrefix(prefix string, fn func(key, value []byte) bool) (int, error) {
	return s.r.ScanPrefix(prefix, fn), nil
}

// newRouterStore builds a store over numShards real shards.
func newRouterStore(b *testing.B, numShards int) *routerStore {
	b.Helper()
	cfg := config.LoadConfig([]string{fmt.Sprintf("-shards=%d", numShards)})
	shards := make([]*shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		sh, err := shard.Run(shard.ID(i), cfg, nil, nil, log.NewNoOpLogger(), nil)
		if err != nil {
			b.Fatalf("shard %d: %v", i, err)
		}
		b.Cleanup(func() { _ = sh.Stop(context.Background()) })
		shards[i] = sh
	}
	return &routerStore{r: router.New(shards)}
}

// BenchmarkPGSimpleQueryPK measures the PK point lookup: plan already resolved,
// one row addressed by primary key, read through readRow's prefix scan and
// rendered to wire cells. This is the pgbench read-only hot statement and the
// benchmark Phase 1's Router.ScanPrefix work is judged on.
func BenchmarkPGSimpleQueryPK(b *testing.B) {
	for _, shards := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			const rows = 1000
			store := newRouterStore(b, shards)
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
				if err := insertRow(srv.store, sch, keyspace.EncodeIntRowID(int64(i)), cells); err != nil {
					b.Fatalf("insertRow: %v", err)
				}
			}

			// A different row each iteration, so the measurement is a point
			// lookup rather than one prefix the caches have memorized. The ids
			// are encoded up front: the encoding allocates, and this benchmark
			// exists to report the read path's allocations, not the harness's.
			rowIDs := make([]string, rows)
			for i := range rowIDs {
				rowIDs[i] = keyspace.EncodeIntRowID(int64(i))
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rowID := rowIDs[i%rows]
				cells, found, err := readRow(srv.store, sch, rowID, nil)
				if err != nil {
					b.Fatalf("readRow: %v", err)
				}
				if !found || len(cells) != 3 {
					b.Fatalf("row %d: found=%v cells=%d", i%rows, found, len(cells))
				}
			}
		})
	}
}
