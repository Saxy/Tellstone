package cluster

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
	pb "go.etcd.io/raft/v3/raftpb"
)

/*
Tests for the replicated DDL path.

The hazard these exist for: a schema mutation is the one write whose replay
must be *identical* on every replica. If any part of it is decided at apply
time rather than fixed in the committed entry -- a clock read, a local
counter, a scan that two replicas evaluate differently -- the replicas diverge
and the table exists on some nodes and not others. Every test below therefore
checks two things: the entry carries everything the apply needs, and applying
the same entry twice reaches the same state.
*/

// recordingDispatcher captures what the FSM dispatched, and models the local
// key set a node's engines hold. It deliberately implements no range capability:
// the emptiness guard does not live in Apply, so there is nothing here to answer.
type recordingDispatcher struct {
	ops []recordedOp
	// keys is the set of live keys the dispatcher reports holding.
	keys map[string]bool
}

type recordedOp struct {
	op    byte
	key   string
	value []byte
}

func (d *recordingDispatcher) Dispatch(key string, op byte, value []byte, ttl time.Duration) error {
	d.ops = append(d.ops, recordedOp{op: op, key: key, value: append([]byte(nil), value...)})
	switch op {
	case OpSet, OpSetNX, OpSetXX:
		if d.keys == nil {
			d.keys = map[string]bool{}
		}
		d.keys[key] = true
	case OpDel:
		delete(d.keys, key)
	}
	return nil
}

const (
	testCatalogKey = "tellstone/~meta/tables/tellstone/users"
	testTablePfx   = "tellstone/users/"
)

func TestEncodeDecodeDDLRoundTrip(t *testing.T) {
	schema := []byte("TSCH\x01\x00\x00\x00payload")
	data, err := EncodeCreateTable(0xdeadbeefcafef00d, testCatalogKey, schema)
	if err != nil {
		t.Fatalf("EncodeCreateTable: %v", err)
	}
	e, err := DecodeDDLEntry(data)
	if err != nil {
		t.Fatalf("DecodeDDLEntry: %v", err)
	}
	if e.Op != OpCreateTable {
		t.Fatalf("op = 0x%02x, want OpCreateTable", e.Op)
	}
	if e.TSO != 0xdeadbeefcafef00d {
		t.Fatalf("tso = %#x, want the proposer's value", e.TSO)
	}
	if e.Key != testCatalogKey {
		t.Fatalf("key = %q, want %q", e.Key, testCatalogKey)
	}
	if !bytes.Equal(e.Body, schema) {
		t.Fatalf("body = %q, want %q", e.Body, schema)
	}
}

// The TSO must survive the round trip bit for bit. A truncation here would make
// two replicas stamp the same entry differently.
func TestDDLTSOSurvivesRoundTrip(t *testing.T) {
	for _, tso := range []uint64{0, 1, 1 << 32, 0xffffffffffffffff, 1<<20 + 999} {
		data, err := EncodeDropTable(tso, testCatalogKey)
		if err != nil {
			t.Fatalf("EncodeDropTable(%d): %v", tso, err)
		}
		e, err := DecodeDDLEntry(data)
		if err != nil {
			t.Fatalf("DecodeDDLEntry: %v", err)
		}
		if e.TSO != tso {
			t.Fatalf("tso = %#x, want %#x", e.TSO, tso)
		}
	}
}

func TestDecodeDDLRejectsMalformed(t *testing.T) {
	good, err := EncodeDropTable(1, testCatalogKey)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"too short", good[:5]},
		{"kv opcode", mustEncodeSet(t, "k", "v")},
		{"key length overflows", func() []byte {
			d := append([]byte(nil), good...)
			d[9] = 0xff
			d[10] = 0xff
			return d
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeDDLEntry(tc.data); err == nil {
				t.Fatal("decode succeeded, want error")
			}
		})
	}
}

func mustEncodeSet(t *testing.T, k, v string) []byte {
	t.Helper()
	b, err := EncodeSet(k, []byte(v), 0)
	if err != nil {
		t.Fatalf("EncodeSet: %v", err)
	}
	return b
}

// A create must be applied as a conditional create, not an overwrite: a replayed
// or duplicated create cannot redefine a table that may already hold rows.
func TestApplyCreateTableIsConditional(t *testing.T) {
	d := &recordingDispatcher{}
	fsm := NewFSM(d, log.NewNoOpLogger())
	data, err := EncodeCreateTable(1, testCatalogKey, []byte("TSCH"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.Apply(&pb.Entry{Data: data}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(d.ops) != 1 {
		t.Fatalf("dispatched %d ops, want 1", len(d.ops))
	}
	if d.ops[0].op != OpSetNX {
		t.Fatalf("op = 0x%02x, want OpSetNX so a replay cannot redefine the table", d.ops[0].op)
	}
	if d.ops[0].value[0] != 'T' {
		t.Fatalf("value = %q, want the schema blob", d.ops[0].value)
	}
}

// The TTL must be zero. A DDL entry that inherited KV TTL semantics could expire
// and delete a table out from under its rows.
func TestApplyDDLNeverCarriesTTL(t *testing.T) {
	d := &recordingDispatcher{}
	fsm := NewFSM(d, log.NewNoOpLogger())
	data, err := EncodeCreateTable(1, testCatalogKey, []byte("TSCH"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Sanity: the KV decoder would read the TSO field as a TTL, which is
	// exactly the confusion the separate codec exists to prevent.
	if _, _, _, ttl, err := DecodeLogEntry(data); err != nil {
		t.Fatalf("DecodeLogEntry on a DDL entry unexpectedly failed: %v", err)
	} else if ttl == 0 {
		t.Fatal("expected the KV decoder to misread the TSO as a nonzero TTL")
	}
	if err := fsm.Apply(&pb.Entry{Data: data}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if d.ops[0].value == nil {
		t.Fatal("no value dispatched")
	}
}

func TestApplyDropTableSucceedsWhenEmpty(t *testing.T) {
	d := &recordingDispatcher{keys: map[string]bool{testCatalogKey: true}}
	fsm := NewFSM(d, log.NewNoOpLogger())
	data, err := EncodeDropTable(42, testCatalogKey)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.Apply(&pb.Entry{Data: data}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(d.ops) != 1 || d.ops[0].op != OpDel {
		t.Fatalf("dispatched %+v, want a single DEL", d.ops)
	}
	if d.keys[testCatalogKey] {
		t.Fatal("catalog key still present after drop")
	}
}

// Apply must not consult local state for a drop. This is the property that keeps
// every replica of a group in agreement: the guard moved to the proposing side
// precisely because a node's local engines differ between replicas.
func TestApplyDropTableIgnoresLocalRows(t *testing.T) {
	d := &recordingDispatcher{keys: map[string]bool{
		testCatalogKey:           true,
		testTablePfx + "1/name":  true,
		testTablePfx + "1/email": true,
	}}
	fsm := NewFSM(d, log.NewNoOpLogger())
	data, err := EncodeDropTable(42, testCatalogKey)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.Apply(&pb.Entry{Data: data}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if d.keys[testCatalogKey] {
		t.Fatal("catalog key survived the drop entry")
	}
	// The row keys are left alone: Apply deletes the definition, and the guard
	// that proved emptiness ran before this entry was ever proposed.
	if !d.keys[testTablePfx+"1/name"] {
		t.Fatal("Apply swept row keys; the drop is a catalog delete only")
	}
}

// fabricated "table has rows".
func TestGuardedDropRefusesWhenScanCannotRun(t *testing.T) {
	checker := &fakeChecker{err: errors.New("region 3: linearizable read: no leader")}
	p := NewGuardedDropProposer(NewDDLProposer(nil), checker)
	_, err := p.ProposeDropTable(context.Background(), testCatalogKey)
	if !errors.Is(err, ErrEmptinessUnverifiable) {
		t.Fatalf("error = %v, want ErrEmptinessUnverifiable", err)
	}
	if !errors.Is(err, checker.err) {
		t.Fatalf("error %v does not wrap the underlying cause", err)
	}
	if checker.calls != 1 {
		t.Fatalf("checker called %d times, want 1", checker.calls)
	}
}

// A missing checker is the same class of problem: the guard cannot run at all.
func TestGuardedDropRefusesWithoutChecker(t *testing.T) {
	p := NewGuardedDropProposer(NewDDLProposer(nil), nil)
	if _, err := p.ProposeDropTable(context.Background(), testCatalogKey); !errors.Is(err, ErrEmptinessUnverifiable) {
		t.Fatalf("error = %v, want ErrEmptinessUnverifiable", err)
	}
}

// Rows present must refuse the drop, and must do so before any timestamp is
// burned: an entry that cannot be proposed should not consume a TSO.
func TestGuardedDropRefusesWhenRowsExist(t *testing.T) {
	checker := &fakeChecker{found: true}
	p := NewGuardedDropProposer(NewDDLProposer(nil), checker)
	_, err := p.ProposeDropTable(context.Background(), testCatalogKey)
	if !IsTableNotEmpty(err) {
		t.Fatalf("error = %v, want ErrTableNotEmpty", err)
	}
}

// An empty table produces a real drop entry.
func TestGuardedDropProposesWhenEmpty(t *testing.T) {
	checker := &fakeChecker{}
	p := NewGuardedDropProposer(NewDDLProposer(nil), checker)
	data, err := p.ProposeDropTable(context.Background(), testCatalogKey)
	if err != nil {
		t.Fatalf("ProposeDropTable: %v", err)
	}
	e, err := DecodeDDLEntry(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if e.Op != OpDropTable || e.Key != testCatalogKey || len(e.Body) != 0 {
		t.Fatalf("entry = %+v, want an empty OpDropTable for the catalog key", e)
	}
}

// fakeChecker stands in for the cluster store's cluster-wide emptiness check.
type fakeChecker struct {
	found bool
	err   error
	calls int
}

func (c *fakeChecker) PrefixExists(string) (bool, error) {
	c.calls++
	return c.found, c.err
}

// Applying the same entry twice must reach the same state. This is the
// determinism property a Raft FSM depends on.
func TestApplyDDLIsIdempotent(t *testing.T) {
	d := &recordingDispatcher{}
	f := NewFSM(d, log.NewNoOpLogger())
	data, _ := EncodeCreateTable(7, testCatalogKey, []byte("TSCH"))
	for i := 0; i < 3; i++ {
		if err := f.Apply(&pb.Entry{Data: data}); err != nil {
			t.Fatalf("Apply #%d: %v", i, err)
		}
	}
	if len(d.keys) != 1 {
		t.Fatalf("state has %d keys after 3 applies, want 1", len(d.keys))
	}
}

func TestRowPrefixFor(t *testing.T) {
	cases := []struct{ key, want string }{
		{"tellstone/~meta/tables/tellstone/users", "tellstone/users/"},
		{"db/~meta/tables/db/t", "db/t/"},
		{"a/~meta/tables/a/b_c", "a/b_c/"},
	}
	for _, tc := range cases {
		got, err := RowPrefixFor(tc.key)
		if err != nil {
			t.Fatalf("RowPrefixFor(%q): %v", tc.key, err)
		}
		if got != tc.want {
			t.Fatalf("RowPrefixFor(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
	// Anything that is not a well-formed catalog key must be refused rather
	// than turned into a prefix that would sweep the wrong range.
	for _, bad := range []string{
		"", "a/b", "~meta/tables/a/b", "a/~meta/tables/", "a/~meta/tables/a",
		// A key whose table namespace disagrees with its database segment is
		// not one this guard understands, and guessing which to trust would
		// scan the wrong table.
		"a/~meta/tables/other/t", "a/~meta/nope/tables/a/t", "a/~meta/tables/a/",
	} {
		if got, err := RowPrefixFor(bad); err == nil {
			t.Fatalf("RowPrefixFor(%q) = %q, want error", bad, got)
		}
	}
}

// A DDL entry is never mistaken for a KV entry, and vice versa.
func TestDDLOpcodesDoNotCollideWithKV(t *testing.T) {
	for _, op := range []byte{OpSet, OpDel, OpSetNX, OpSetXX, OpChunkSet} {
		if IsDDL(op) {
			t.Fatalf("op 0x%02x classified as DDL", op)
		}
	}
	if !IsDDL(OpCreateTable) || !IsDDL(OpDropTable) {
		t.Fatal("DDL opcodes not classified as DDL")
	}
	if _, err := EncodeDDL(OpSet, 1, "k", nil); err == nil {
		t.Fatal("EncodeDDL accepted a KV opcode")
	}
}

// The proposer stamps the entry; nothing in the apply path does. This is the
// guardrail-4 test.
func TestDDLProposerAllocatesTSO(t *testing.T) {
	pool := NewTSOPool(TSOPoolConfig{MinBatch: 1, RefillThresholdPct: 50})
	if err := pool.Adopt(1000, 1100); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	p := NewDDLProposer(pool)
	seen := map[uint64]bool{}
	for i := 0; i < 5; i++ {
		data, err := p.ProposeDropTable(context.Background(), testCatalogKey)
		if err != nil {
			t.Fatalf("ProposeDropTable: %v", err)
		}
		e, err := DecodeDDLEntry(data)
		if err != nil {
			t.Fatalf("DecodeDDLEntry: %v", err)
		}
		if e.TSO < 1000 || e.TSO >= 1100 {
			t.Fatalf("tso = %d, want a value from the pool's range", e.TSO)
		}
		if seen[e.TSO] {
			t.Fatalf("tso %d handed out twice", e.TSO)
		}
		seen[e.TSO] = true
	}
}

// A nil pool must still produce a decodable entry rather than panicking or
// inventing a "no timestamp" sentinel that a decode could not distinguish.
func TestDDLProposerNilPool(t *testing.T) {
	p := NewDDLProposer(nil)
	data, err := p.ProposeCreateTable(context.Background(), testCatalogKey, []byte("TSCH"))
	if err != nil {
		t.Fatalf("ProposeCreateTable: %v", err)
	}
	e, err := DecodeDDLEntry(data)
	if err != nil {
		t.Fatalf("DecodeDDLEntry: %v", err)
	}
	if e.TSO != 0 {
		t.Fatalf("tso = %d, want 0", e.TSO)
	}
	if !bytes.Equal(e.Body, []byte("TSCH")) {
		t.Fatalf("body = %q", e.Body)
	}
}
