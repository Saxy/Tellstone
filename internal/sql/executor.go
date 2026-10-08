/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: executor.go
Description: The Phase 11 executor contract: a Volcano pull pipeline shaped for
zero-allocation steady state. The engine's rule is that raw execution speed and
zero allocations in the hot path come before ease of writing an operator, so
the interface is pass-by-reference end to end — no operator constructs a Row
per call, and no operator hands a caller memory it allocated. A statement runs
through a fixed set of operator buffers owned by one ExecCtx, and only the
result sink keeps bytes past the end of the pipeline.

The contract is declared before the operators so Phase 11 work lands against a
fixed seam rather than growing one. Each decision in ADR-014 and the Phase 11
plan is reflected here: buffered multi-row delivery (the wire emits many
DataRows from one materialized buffer), a full-table and a primary-key-range
scan built on Store.ScanPrefix's callback with its return-false early stop,
residual filters evaluated on storage bytes rather than decoded values (D5),
and the D1 batch/dst shape that makes the rest possible.
*/
package sql

import "hash/maphash"

// ExecCtx is the ownership boundary of one statement execution. Everything an
// operator needs that is not the row data itself lives here: the bump arena
// that backs all scratch memory.
//
// A fresh ExecCtx is prepared for each statement and reset afterwards; it is a
// pool, not a new allocation per statement. Its memory is deliberately not
// reachable from a Chunk that outlives the owning operator, so a consumer can
// never accidentally retain scratch past the statement.
type ExecCtx struct {
	arena *arena
}

// arena is the bump allocator that owns every byte of scratch memory an
// operator borrows while a statement runs -- the join's row region, and later
// the top-N heap, the group table, projection buffers. It is reset wholesale at
// statement end, which is what makes the steady-state allocation count zero:
// nothing an operator builds during execution is garbage-collected mid-query,
// and nothing allocates per row.
//
// It has two carving shapes because the join needs two: alloc returns a fixed
// slice the caller keeps (the future fixed carvings), while growRegion extends
// the one growable carve a statement may hold. A grow copies the region into a
// larger backing array, so everything the join stores into it -- row offsets,
// key offsets -- has to be relative to the region's start rather than to Go
// pointers, and it is: the hash nodes index bytes, not addresses. A statement
// that needs no scratch keeps both fields nil, which is every statement but a
// join today.
type arena struct {
	buf    []byte
	region []byte
}

// newArena prepares an arena with room for a plausible build side, so a small
// join does not spend its first few rows growing the region.
func newArena(hint int) *arena {
	if hint < 256 {
		hint = 256
	}
	return &arena{region: make([]byte, 0, hint)}
}

// alloc carves n fixed bytes of scratch, 8-byte aligned.
func (a *arena) alloc(n int) []byte {
	off := (len(a.buf) + 7) &^ 7
	if off+n > cap(a.buf) {
		c := cap(a.buf)
		if c == 0 {
			c = 256
		}
		for c < off+n {
			c *= 2
		}
		nb := make([]byte, len(a.buf), c)
		copy(nb, a.buf)
		a.buf = nb
	}
	a.buf = a.buf[:off+n]
	return a.buf[off : off+n]
}

// growRegion extends the arena's growable region by n bytes and returns the
// whole region. The caller must re-read every slice it holds of the old
// region afterwards: growth copies, and the old backing array is dead.
func (a *arena) growRegion(n int) []byte {
	if len(a.region)+n > cap(a.region) {
		c := cap(a.region)
		if c == 0 {
			c = 256
		}
		for c < len(a.region)+n {
			c *= 2
		}
		nr := make([]byte, len(a.region), c)
		copy(nr, a.region)
		a.region = nr
	}
	a.region = a.region[:len(a.region)+n]
	return a.region
}

// reset drops every carving back to empty. The backing arrays stay, so the
// next statement reuses them instead of re-allocating.
func (a *arena) reset() {
	a.buf = a.buf[:0]
	a.region = a.region[:0]
}

// rowAssembler groups the scan callback's key stream into rows. A catalog row
// is several keys under one row-id prefix (ADR-013), so a table scan cannot
// emit a row from a single callback invocation: it segments the stream by row
// id and flushes a row only when the next row id arrives. The assembler owns
// the per-row cell buffer and reuses it between rows.
//
// The cells are accumulated in schema columns for the D5 residual filter to
// read, and only the projected ordinals are copied into the chunk payload, so a
// row the filter rejects copies nothing at all.
type rowAssembler struct {
	sch *Schema
	// prefix is the table's key prefix, which every row key shares no matter
	// where the scan started. The scan's lo may begin mid-table (a range), and
	// the row id always sits directly under this prefix.
	prefix string
	cells  []rowValue
	// cur is the escaped row id of the row being assembled. Its bytes are a
	// copy: the scan buffer they came from is only valid for the callback.
	cur []byte
	// proj and filt are the statement's projection and residual filter,
	// applied at flush time.
	proj []int
	filt *compiledNode
	dst  *Chunk
	// err carries a store or emit failure out of the scan callback, so the
	// pass can stop as soon as one happens and still report it afterwards.
	err error
}

// Cell is one projected value, addressed by pointer and length into a buffer
// the owning operator controls. It is deliberately not an owned []byte: a Cell
// copied out of the chunk payload that should survive past the chunk has to be
// appended to the result sink, and nothing else may retain it.
type Cell struct {
	p []byte
}

// BatchRows caps every Chunk an operator fills. A fixed batch is what lets the
// pull model amortize interface-call cost without ever building a []Row; a
// scan fills up to this many rows per Next, and the wire path drains a chunk
// one DataRow at a time.
const BatchRows = 64

// Chunk is a batch of rows passed down the pipeline by reference. Its payload
// is one contiguous buffer owned by the operator that produced it, so a
// consumer reads without allocating; Rows==0 means the source is exhausted.
//
// NULL is recorded as a bitmap bit rather than as len==0, because an empty
// string is a real value in this layout and a NULL column is the absence of its
// key, not the presence of an empty value (ADR-013).
type Chunk struct {
	Rows  uint32
	NCols uint16
	data  []byte
	nulls []uint64
}

// Operator is one pull node in the pipeline. It fills dst with up to BatchRows
// rows and returns the count; a returned 0 drains it. Callers pass the same
// Chunk down multiple times, and operators write into it in place, so the
// interface and its use sites together make a per-row allocation impossible.
type Operator interface {
	// Next reads the next batch of rows into dst. A zero return means the
	// source is exhausted, which is also the cancellation signal: a LIMIT
	// operator stops pulling, and the scan's callback stops a storage walk by
	// returning false. The returned error is terminal.
	Next(ctx *ExecCtx, dst *Chunk) (uint32, error)

	// Close resets any operator memory it borrowed and marks the node done. It
	// must not allocate; it returns an error only to report a flush that was
	// deferred to close time.
	Close(ctx *ExecCtx) error
}

// compiledFilter is one leaf of the D5 residual filter, evaluated on storage
// bytes.
//
// A comparison's literal is re-encoded into the column's storage encoding once
// per plan (at Bind when the value is a parameter), and evaluation then
// compares the raw bytes of the assembled cell against that encoding: equality
// is a byte compare, order is a byte compare because the encodings are
// order-preserving (ADR-013 §4), and a prefix LIKE is a HasPrefix. A null test
// reads whether the column's key is present. No DecodeValue, no boxing, no
// allocation — a row that fails the filter never leaves the assembler as a
// value.
type compiledFilter struct {
	enc  []byte // storage encoding of the literal side (CmpExpr)
	col  int    // schema ordinal the filter reads
	op   CmpOp  // OpEq / OpLt / OpLe / OpGt / OpGe
	like []byte // LikeExpr prefix (trailing % stripped), when this is a LIKE
	null bool   // set for a NullTestExpr: test presence instead of a value
	not  bool   // NullTestExpr direction: true is IS NOT NULL
}

// compiledNode is the compiled residual filter: a boolean tree over the leaf
// comparisons above, plus the two constants. The tree is the expr.go tree with
// each literal side resolved to bytes, so AND/OR/NOT keep their meaning and an
// always-false node that folds from `col <op> NULL` stays representable.
type compiledNode struct {
	f    *compiledFilter // leaf test when non-nil
	op   BoolOp          // boolean connective when f == nil and lit is false
	args []*compiledNode // children of a connective
	lit  bool            // TrueExpr / FalseExpr constant when set
	val  bool            // the constant's truth value
}

// tableScan is the D1/D5 scan operator: a point lookup, a primary-key range,
// or a full table scan, all the same tree with different seek/beg/hi bounds.
//
// Store.ScanPrefix matches keys by prefix and offers no seek or resume point,
// so the store cannot be asked for "everything from row 2 onward": a row
// past row 2 does not begin with row 2's prefix. The scan therefore seeks the
// longest common prefix of its two row bounds -- which for a wide range is just
// the table prefix -- and lets the callback skip keys below beg and stop at hi,
// so the yield is exactly the range even though the store walked more of the
// tree. hi is the same exclusive cutoff ScanPrefix's own early stop provides,
// and the one LIMIT pulls on.
//
// The scan is push-shaped rather than a pull operator, because Store.ScanPrefix
// offers no resume point: it can only walk a prefix to its end, and the set of
// rows past a given id shares no single prefix. A pull over a table would
// therefore re-scan everything below the current batch on each Next call, which
// is quadratic in the table size. Instead the scan fills a reused Chunk and
// hands each batch to a sink function as it fills, stopping only at the hi
// bound or when a sink (or a future LIMIT) returns false. The join retrofit
// confirmed this shape rather than replacing it: a join's two sides are scans
// with sink functions too, because neither side can resume a ScanPrefix walk
// any more than a plain scan can.
type tableScan struct {
	store Store
	sch   *Schema
	seek  []byte // ScanPrefix prefix: the scan's common-prefix lower limit
	beg   []byte // inclusive row lower bound; keys below it are skipped
	hi    []byte // exclusive key cutoff; nil = scan to the end
	proj  []int
	filt  *compiledNode
	as    *rowAssembler
	chunk *Chunk
}

// limit stops the pipeline after skip+take rows. It does no buffering: it
// simply stops pulling its source, which propagates as the scan callback
// returning false. That is what makes a bounded query cheap instead of reading
// the whole table.
//
// Implemented in Phase 11.
type limit struct {
	src  Operator
	skip uint64
	take uint64
	seen uint64
}

// topN implements ORDER BY ... LIMIT N without sorting the result set: a
// fixed-capacity array heap of size N, carved from the arena at Open, fed by
// one pass over the source. Order is decided by comparing storage encodings —
// the same byte order the scan and the filters use — so no decoded values are
// ever materialized to sort.
//
// Implemented in Phase 11.
type topN struct {
	src  Operator
	less func(ctx *ExecCtx, a, b *Cell) int
	heap []rowRef
}

// hashJoin is the D4 single-pass INNER equi-join: the build side is scanned
// once into a bucket-chained hash table whose row bytes live in the arena
// region, then the probe side streams through one scan whose sink emits matched
// rows into the output batch.
//
// It is push-shaped for the same reason the scan is: Store.ScanPrefix offers no
// resume point, so a pull over either side would re-walk everything below the
// current batch on each Next. The declared build/probe Operator seam from the
// original contract did not survive that, which is why this is a scan-driven
// structure rather than two pull nodes.
//
// The buckets are sized from the build side's row estimate and deliberately
// never rehashed: a chain degrades to linear rather than to a wrong answer, and
// rehashing mid-scan would move rows the probe is walking.
type hashJoin struct {
	seed    maphash.Seed
	bKey    int // build-side key ordinal, in schema order
	pKey    int // probe-side key ordinal, in schema order
	buckets []int32
	nodes   []hashNode
}

// hashNode is one build-side row in a bucket chain. next is the previous head
// of the chain (or -1), so insertion is a push at the front and the chain order
// is the build order.
//
// off, koff and klen are offsets into the arena region rather than pointers:
// the region grows while the build side is read, and growth copies the backing
// array. Offsets survive that; pointers would silently read the dead copy.
type hashNode struct {
	next int32
	off  uint32 // serialized row start within the region
	koff uint32 // key cell's value bytes within the region
	klen uint32
}

// hashAgg is the D3 grouping operator: numeric accumulators (int64/float64)
// over a flat group table in the arena, producing one output row per group.
// Group keys are storage-encoded bytes, so grouping compares encodings rather
// than decoded values, exactly like the filters and the joins.
//
// Implemented in Phase 11.
type hashAgg struct {
	src    Operator
	groups []groupRef
	fns    []aggFn
}

// rowRef addresses one row inside a chunk payload or the result sink: offset
// and cell count into a contiguous buffer. It is what the sort/heap/group
// structures store instead of owning row bytes.
type rowRef struct {
	off uint32
	n   uint16
}

// groupRef is one GROUP BY output grouping inside the aggregate's group table:
// the group key's position and the accumulator slot for its aggregate state.
type groupRef struct {
	off uint32
	agg int32
}

// aggFn names a supported aggregate. The set is the Phase 11 core: COUNT, SUM,
// MIN, MAX, AVG, each paired with a numeric accumulator.
type aggFn uint8

const (
	aggCount aggFn = iota
	aggSum
	aggMin
	aggMax
	aggAvg
)
