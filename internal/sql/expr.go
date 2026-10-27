/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: expr.go
Description: The WHERE-clause expression tree, and the recursive translation of
the PG parse tree into it (ADR-014 decision 4). This replaces the single
`rowPredicate` equality check that Phase 9 shipped, because one shape cannot
express what a filter is for: `a IS NULL OR b IS NULL` is not an equality on
either column, and flattening it to one would answer a different question than
the client asked.

The tree is deliberately small -- comparisons, boolean connectives, null tests
and prefix LIKE -- because every node here is something the planner can either
turn into a key range or evaluate as a residual filter. A shape that can be
neither is refused at translation rather than approximated, so an unsupported
predicate surfaces as an error naming the shape instead of as a slow query that
returns the wrong rows.

Translation is pure. It does not consult the catalog and does not check that a
named column exists, because a prepared statement may be parsed before the
table it names has been created (ADR-013). Column resolution is the planner's
job, where the schema is available.
*/
package sql

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Expr is a WHERE-clause expression.
//
// A nil Expr is not an error: it is the absence of a filter, which plans as a
// scan with nothing to evaluate. That is distinct from a filter that is always
// false, which the tree has to be able to represent -- `WHERE 1 = 0` is not an
// unfiltered scan, it is a scan that matches nothing.
type Expr interface {
	// exprNode is unexported so that only this package can build an Expr. The
	// planner walks a tree it did not parse, and an unexported marker means a
	// future node type cannot be smuggled in from a test without the walker
	// learning to handle it.
	exprNode()
}

// CmpOp is a comparison operator. The set is exactly the operators a key range
// can express, because every one of them has an order-preserving encoding
// (ADR-013 section 4). `!=` is absent deliberately: it is not expressible as a
// single range, so it is a residual filter over whatever the other side
// selects, and a tree node for it would pretend to more than it can do.
type CmpOp uint8

const (
	OpEq CmpOp = iota
	OpLt
	OpLe
	OpGt
	OpGe
)

func (o CmpOp) String() string {
	switch o {
	case OpEq:
		return "="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	}
	return "?"
}

// invert returns the operator that holds when neither operand moved. Used when
// a predicate arrives with the value on the left (`5 = id`), which PostgreSQL
// accepts and which the planner would otherwise read as a comparison of a
// literal against a column.
func (o CmpOp) invert() (CmpOp, bool) {
	switch o {
	case OpLt:
		return OpGt, true
	case OpLe:
		return OpGe, true
	case OpGt:
		return OpLt, true
	case OpGe:
		return OpLe, true
	}
	// `=` is its own inverse; `!=` has no node.
	return o, true
}

// CmpExpr is `column <op> value`.
type CmpExpr struct {
	Col string
	Op  CmpOp
	Val ValRef
}

func (*CmpExpr) exprNode() {}

// BoolOp is a boolean connective.
type BoolOp uint8

const (
	BoolAnd BoolOp = iota
	BoolOr
	BoolNot
)

func (o BoolOp) String() string {
	switch o {
	case BoolAnd:
		return "AND"
	case BoolOr:
		return "OR"
	case BoolNot:
		return "NOT"
	}
	return "?"
}

// BoolExpr is a connective over sub-expressions. NOT is unary and carries the
// single operand in Args.
type BoolExpr struct {
	Op   BoolOp
	Args []Expr
}

func (*BoolExpr) exprNode() {}

// NullTestExpr is `column IS NULL` or `column IS NOT NULL`.
//
// This is a key-existence question, not a value comparison. NULL is the absence
// of a column key (ADR-013), so `IS NULL` asks whether the key is missing. That
// is why a null test can be evaluated at all when the value bytes do not exist
// to compare against, and why it cannot be folded into an equality: there is no
// value for `a = NULL` to carry.
type NullTestExpr struct {
	Col string
	Not bool
}

func (*NullTestExpr) exprNode() {}

// LikeExpr is `column LIKE pattern` and `column NOT LIKE pattern`.
//
// Any pattern shape is accepted: a pattern whose only wildcard is a trailing
// `%` compiles to a prefix test the planner may also use as a key range, and
// anything wider (`_`, a `%` in the middle, a leading wildcard) compiles to a
// residual matcher that tests every row the scan yields. Neither shape can
// return rows the client did not ask for: the wide one is slower, not wrong
// (ADR-014 guardrail 6). A backslash escapes the next pattern byte, which is
// how `LIKE 'pg\_temp%'` names a literal underscore.
type LikeExpr struct {
	Col string
	// Val is the pattern. It is a ValRef rather than a string so that a
	// parameterized pattern (`LIKE $1`) can be carried; its shape is decided
	// when the value is known, at plan time for a literal and at Bind for a
	// parameter.
	Val ValRef
	// Not inverts the match, which is what NOT LIKE means. It lives on the
	// node rather than in a wrapper because the parser reports NOT LIKE as
	// the same expression kind as LIKE with the opposite operator.
	Not bool
}

func (*LikeExpr) exprNode() {}

// NeExpr is `column <> value`, the inequality the comparison set deliberately
// does not include: it cannot narrow a key range, so it exists only as a
// residual leaf that tests one cell against the literal's encoding. A NULL
// column yields UNKNOWN and so matches no row, exactly as `=` does.
type NeExpr struct {
	Col string
	Val ValRef
}

func (*NeExpr) exprNode() {}

// ColCmpExpr is `column <op> column`: a comparison between two columns of the
// same row. It exists because a client's introspection queries join tables
// with the condition written in the WHERE clause rather than in an ON -- the
// comma join `FROM a, b WHERE a.x = b.y` is the shape PostgreSQL's own
// metadata queries use -- and because a compound ON clause names more than
// one pair of columns. The two sides are compared as bytes, which is only
// meaningful when both columns share a type; the compiler refuses the
// comparison otherwise rather than comparing encodings that do not order the
// same way.
type ColCmpExpr struct {
	Left  string
	Right string
	Op    CmpOp
	// Not marks the inequality `<>`, which the comparison operator set does
	// not carry: a residual leaf can express it, a key range cannot.
	Not bool
}

func (*ColCmpExpr) exprNode() {}

// RegexExpr is `column ~ pattern` and `column !~ pattern`: the regular
// expression match.
//
// The pattern is a POSIX regular expression, which is what PostgreSQL's own
// metadata queries use to exclude the system schemas (`nspname !~ '^pg_'`).
// The match is decided against the column's text, so it is a match on a text
// column rather than on an arbitrary value's encoding.
type RegexExpr struct {
	Col string
	Val ValRef
	// Not inverts the match, which is what !~ means.
	Not bool
}

func (*RegexExpr) exprNode() {}

// BoolColExpr is a boolean column used as a predicate in its own right:
// `WHERE dropped`, or the `NOT dropped` that is its negation.
//
// PostgreSQL allows a boolean column where a condition is expected, and a
// client's introspection queries use it: `AND NOT a.attisdropped` is how a
// column's own flag is consulted. The cell decides: a NULL flag is UNKNOWN
// and so matches no row, exactly as a comparison against NULL does.
type BoolColExpr struct {
	Col string
}

func (*BoolColExpr) exprNode() {}

// TrueExpr matches every row. It exists so that a filter the planner proves
// always true is representable without collapsing the tree to a nil Expr: nil
// means "no filter was written", and reporting that for a statement that did
// write one would make EXPLAIN describe a different statement than the client
// sent.
type TrueExpr struct{}

func (*TrueExpr) exprNode() {}

// translateExpr converts a WHERE clause into an expression tree. A nil node --
// no WHERE clause at all -- yields a nil Expr, which is the unfiltered scan.
//
// A nil Expr is not an error. Absent a filter is the unfiltered scan, which is
// a real query shape, and treating it as an error would make `SELECT * FROM t`
// unplannable rather than plannable-and-expensive.
func translateExpr(node *pg_query.Node) (Expr, error) {
	if node == nil {
		return nil, nil
	}
	switch {
	case node.GetBoolExpr() != nil:
		return translateBoolExpr(node.GetBoolExpr())
	case node.GetNullTest() != nil:
		return translateNullTest(node.GetNullTest())
	case node.GetAExpr() != nil:
		return translateAExpr(node.GetAExpr())
	case node.GetColumnRef() != nil:
		// A column in a condition's own right: PostgreSQL reads a boolean
		// column as the condition it holds.
		name, ok := plainColumn(node)
		if !ok {
			return nil, fmt.Errorf("%w: a condition must name a plain column", errUnsupported)
		}
		return &BoolColExpr{Col: name}, nil
	}
	// A bare boolean constant is a filter in its own right: `WHERE TRUE` admits
	// every row and `WHERE FALSE` admits none. The parser spells these as an
	// A_Const carrying a boolval, which is the same node a boolean *value*
	// arrives as -- so without this a statement like `a = 1 OR TRUE` would be
	// refused for containing a shape it does have an answer for.
	if ac := node.GetAConst(); ac != nil && ac.GetBoolval() != nil {
		if ac.GetBoolval().GetBoolval() {
			return &TrueExpr{}, nil
		}
		return &FalseExpr{}, nil
	}
	return nil, fmt.Errorf("%w: this WHERE clause is not a comparison, a boolean combination, a null test or a LIKE", errUnsupported)
}

func translateBoolExpr(be *pg_query.BoolExpr) (Expr, error) {
	// PostgreSQL flattens a chain of same-operator conjunctions into one node
	// with many args, so `a AND b AND c` arrives as one AND_EXPR. Recursing per
	// arg keeps nesting uniform for the walker rather than special-casing the
	// flattened shape here.
	args := make([]Expr, 0, len(be.GetArgs()))
	for _, arg := range be.GetArgs() {
		e, err := translateExpr(arg)
		if err != nil {
			return nil, err
		}
		if e == nil {
			// `x AND <nothing>` -- the parser will not produce this, but a nil
			// arg must not silently become a TRUE that widens the filter.
			return nil, fmt.Errorf("%w: boolean operand is empty", errUnsupported)
		}
		args = append(args, e)
	}
	var op BoolOp
	switch be.GetBoolop() {
	case pg_query.BoolExprType_AND_EXPR:
		op = BoolAnd
	case pg_query.BoolExprType_OR_EXPR:
		op = BoolOr
	case pg_query.BoolExprType_NOT_EXPR:
		op = BoolNot
		if len(args) != 1 {
			return nil, fmt.Errorf("%w: NOT takes exactly one operand", errUnsupported)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported boolean operator", errUnsupported)
	}

	// Simplification below is a two-valued algebra: in a WHERE clause, an
	// UNKNOWN result and a FALSE result both drop the row, so "does this
	// expression admit the row" is all a filter needs to know. That is what
	// makes folding a FALSE operand out of an OR sound.

	switch op {
	case BoolAnd:
		// A conjunct that admits nothing admits nothing.
		for _, a := range args {
			if _, ok := a.(*FalseExpr); ok {
				return &FalseExpr{}, nil
			}
		}
		args = dropTrue(args)
		if len(args) == 0 {
			return &TrueExpr{}, nil
		}
	case BoolOr:
		// A disjunct that admits everything admits everything. Getting this
		// wrong is silent rather than loud: dropping the TRUE operand turns
		// `a = 1 OR TRUE` into `a = 1`, which returns too few rows and looks
		// like a correct filter.
		for _, a := range args {
			if _, ok := a.(*TrueExpr); ok {
				return &TrueExpr{}, nil
			}
		}
		args = dropFalse(args)
		if len(args) == 0 {
			return &FalseExpr{}, nil
		}
	case BoolNot:
		switch args[0].(type) {
		case *TrueExpr:
			return &FalseExpr{}, nil
		case *FalseExpr:
			return &TrueExpr{}, nil
		}
	}
	// A connective that ended up with one surviving operand is that operand --
	// but never for NOT. `NOT (a = 1)` is not `a = 1`; collapsing it would
	// return precisely the rows the client excluded, and it would look like a
	// working filter rather than an error.
	if len(args) == 1 && op != BoolNot {
		return args[0], nil
	}
	return &BoolExpr{Op: op, Args: args}, nil
}

// dropTrue removes the operands that admit every row.
func dropTrue(args []Expr) []Expr {
	out := args[:0]
	for _, a := range args {
		if _, ok := a.(*TrueExpr); ok {
			continue
		}
		out = append(out, a)
	}
	return out
}

// dropFalse removes the operands that admit no row.
func dropFalse(args []Expr) []Expr {
	out := args[:0]
	for _, a := range args {
		if _, ok := a.(*FalseExpr); ok {
			continue
		}
		out = append(out, a)
	}
	return out
}

func translateNullTest(nt *pg_query.NullTest) (Expr, error) {
	col, ok := columnRef(nt.GetArg())
	if !ok || col == "*" || strings.HasSuffix(col, ".*") {
		return nil, fmt.Errorf("%w: IS NULL tests a plain column", errUnsupported)
	}
	switch nt.GetNulltesttype() {
	case pg_query.NullTestType_IS_NULL:
		return &NullTestExpr{Col: col}, nil
	case pg_query.NullTestType_IS_NOT_NULL:
		return &NullTestExpr{Col: col, Not: true}, nil
	}
	return nil, fmt.Errorf("%w: only IS NULL and IS NOT NULL are supported", errUnsupported)
}

func translateAExpr(ae *pg_query.A_Expr) (Expr, error) {
	// BETWEEN arrives as its own kind rather than as two comparisons. It is
	// desugared here rather than given a node, because that is what it means --
	// PostgreSQL defines it as `>= AND <=` -- and a node would carry a second
	// representation of a shape the tree already expresses.
	if ae.GetKind() == pg_query.A_Expr_Kind_AEXPR_BETWEEN {
		return translateBetween(ae)
	}
	if ae.GetKind() == pg_query.A_Expr_Kind_AEXPR_LIKE {
		return translateLike(ae)
	}
	// IN and NOT IN are one parser kind distinguished by the operator: `IN`
	// arrives with `=`, NOT IN with `<>`. Both are desugared into the
	// comparisons they mean, which is what PostgreSQL defines them as, so the
	// tree, the planner and the filter each keep one representation of them.
	if ae.GetKind() == pg_query.A_Expr_Kind_AEXPR_IN {
		return translateInList(ae)
	}
	if ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
		return nil, fmt.Errorf("%w: unsupported operator expression", errUnsupported)
	}
	names := ae.GetName()
	if len(names) != 1 || names[0].GetString_() == nil {
		return nil, fmt.Errorf("%w: operator is not a simple name", errUnsupported)
	}
	name := names[0].GetString_().GetSval()
	lexpr, rexpr := ae.GetLexpr(), ae.GetRexpr()

	// The column may be on either side. Reading only the left would reject
	// `5 = id`, which is the same question asked the other way round.
	lcol, lIsCol := plainColumn(lexpr)
	rcol, rIsCol := plainColumn(rexpr)
	if name == "<>" || name == "!=" {
		switch {
		case lIsCol && rIsCol:
			return &ColCmpExpr{Left: lcol, Right: rcol, Op: OpEq, Not: true}, nil
		case rIsCol:
			ref, err := valueRefOrNull(lexpr, rcol)
			if err != nil {
				return nil, err
			}
			return newNe(rcol, ref)
		case lIsCol:
			ref, err := valueRefOrNull(rexpr, lcol)
			if err != nil {
				return nil, err
			}
			return newNe(lcol, ref)
		}
		return nil, fmt.Errorf("%w: a comparison needs one column and one value", errUnsupported)
	}
	if name == "~" || name == "!~" {
		// The regular expression match. The column may be on either side, as
		// with a comparison.
		switch {
		case lIsCol && rIsCol:
			return nil, fmt.Errorf("%w: a regular expression match needs one column and one pattern", errUnsupported)
		case rIsCol:
			ref, err := valueRefOrNull(lexpr, rcol)
			if err != nil {
				return nil, err
			}
			return newRegex(rcol, ref, name == "!~")
		case lIsCol:
			ref, err := valueRefOrNull(rexpr, lcol)
			if err != nil {
				return nil, err
			}
			return newRegex(lcol, ref, name == "!~")
		}
		return nil, fmt.Errorf("%w: a regular expression match needs one column and one pattern", errUnsupported)
	}
	op, ok := cmpOpFor(name)
	if !ok {
		return nil, fmt.Errorf("%w: comparison operator %q is not supported", errUnsupported, name)
	}
	switch {
	case lIsCol && rIsCol:
		return &ColCmpExpr{Left: lcol, Right: rcol, Op: op}, nil
	case rIsCol:
		// Value on the left. Invert so the tree always reads column-first.
		op, _ = op.invert()
		ref, err := valueRefOrNull(lexpr, rcol)
		if err != nil {
			return nil, err
		}
		return newCmp(rcol, op, ref)
	case lIsCol:
		ref, err := valueRefOrNull(rexpr, lcol)
		if err != nil {
			return nil, err
		}
		return newCmp(lcol, op, ref)
	}
	return nil, fmt.Errorf("%w: a comparison needs one column and one value", errUnsupported)
}

// newNe builds an inequality, collapsing `col <> NULL` the same way newCmp
// collapses a comparison against NULL: the result is UNKNOWN, and a filter
// that admits no row is the honest form of that.
func newNe(col string, ref ValRef) (Expr, error) {
	if ref.Null {
		return &FalseExpr{}, nil
	}
	return &NeExpr{Col: col, Val: ref}, nil
}

// newRegex builds a regular expression match, collapsing `col ~ NULL` the
// same way newCmp collapses a comparison against NULL: the result is UNKNOWN,
// and a filter that admits no row is the honest form of that.
func newRegex(col string, ref ValRef, not bool) (Expr, error) {
	if ref.Null {
		return &FalseExpr{}, nil
	}
	return &RegexExpr{Col: col, Val: ref, Not: not}, nil
}

// translateInList reads `column IN (...)` and `column NOT IN (...)` into the
// OR of equalities and the AND of inequalities they mean. A NULL item in the
// list makes `IN` contribute nothing where it does not match (UNKNOWN is
// false in a WHERE clause) and makes NOT IN never true at all, so the whole
// term collapses to FALSE before it can return a row PostgreSQL would not.
// Each item may be a literal or a parameter; a subquery in the list is a
// different node and never reaches here.
func translateInList(ae *pg_query.A_Expr) (Expr, error) {
	col, ok := plainColumn(ae.GetLexpr())
	if !ok {
		return nil, fmt.Errorf("%w: IN needs a plain column on the left", errUnsupported)
	}
	list := ae.GetRexpr().GetList()
	if list == nil || len(list.GetItems()) == 0 {
		return nil, fmt.Errorf("%w: IN needs at least one value", errUnsupported)
	}
	not := false
	if names := ae.GetName(); len(names) == 1 && names[0].GetString_() != nil {
		not = names[0].GetString_().GetSval() == "<>"
	}
	args := make([]Expr, 0, len(list.GetItems()))
	for _, item := range list.GetItems() {
		ref, err := valueRefOrNull(item, col)
		if err != nil {
			return nil, err
		}
		if ref.Null {
			if not {
				return &FalseExpr{}, nil
			}
			args = append(args, &FalseExpr{})
			continue
		}
		if not {
			n, err := newNe(col, ref)
			if err != nil {
				return nil, err
			}
			args = append(args, n)
			continue
		}
		args = append(args, &CmpExpr{Col: col, Op: OpEq, Val: ref})
	}
	if not {
		// NOT IN never yields a NULL operand here: a NULL item returned FALSE
		// above, and newNe collapses an inequality against NULL for the same
		// reason -- UNKNOWN is not a row the filter admits.
		if len(args) == 1 {
			return args[0], nil
		}
		return &BoolExpr{Op: BoolAnd, Args: args}, nil
	}
	// `x IN (a b)` means `x = a OR x = b`, with the UNKNOWN operands a NULL
	// item contributes dropped: in a WHERE clause UNKNOWN and FALSE both
	// reject the row, which is what dropping them does.
	args = dropFalse(args)
	switch len(args) {
	case 0:
		return &FalseExpr{}, nil
	case 1:
		return args[0], nil
	}
	return &BoolExpr{Op: BoolOr, Args: args}, nil
}

// newCmp builds a comparison, collapsing the cases that carry no information for
// the planner.
func newCmp(col string, op CmpOp, ref ValRef) (Expr, error) {
	// `col <op> NULL` is never true, and is not the same as no filter. In SQL
	// the result is UNKNOWN, which in a WHERE clause filters the row out. A
	// plan that dropped the term would return the row, so every operator
	// collapses here rather than only `=`.
	if ref.Null {
		return &FalseExpr{}, nil
	}
	return &CmpExpr{Col: col, Op: op, Val: ref}, nil
}

// FalseExpr matches no row. It is the plan-level form of `col = NULL` and
// `col != NULL`, both of which are UNKNOWN rather than false in SQL, and both
// of which therefore filter the row out.
type FalseExpr struct{}

func (*FalseExpr) exprNode() {}

func translateBetween(ae *pg_query.A_Expr) (Expr, error) {
	col, ok := plainColumn(ae.GetLexpr())
	if !ok {
		return nil, fmt.Errorf("%w: BETWEEN needs a plain column on the left", errUnsupported)
	}
	items := ae.GetRexpr().GetList().GetItems()
	if len(items) != 2 {
		return nil, fmt.Errorf("%w: BETWEEN needs exactly two bounds", errUnsupported)
	}
	// `BETWEEN` is inclusive at both ends, so the low bound is `>=` and the
	// high bound is `<=`. Getting this backwards would drop the endpoints,
	// which is the kind of error that survives a spot check.
	lo, err := valueRefOrNull(items[0], col)
	if err != nil {
		return nil, err
	}
	hi, err := valueRefOrNull(items[1], col)
	if err != nil {
		return nil, err
	}
	if lo.Null || hi.Null {
		// One unknown bound makes the whole term UNKNOWN, so the row is out.
		return &FalseExpr{}, nil
	}
	return &BoolExpr{Op: BoolAnd, Args: []Expr{
		&CmpExpr{Col: col, Op: OpGe, Val: lo},
		&CmpExpr{Col: col, Op: OpLe, Val: hi},
	}}, nil
}

func translateLike(ae *pg_query.A_Expr) (Expr, error) {
	col, ok := plainColumn(ae.GetLexpr())
	if !ok {
		return nil, fmt.Errorf("%w: LIKE needs a plain column on the left", errUnsupported)
	}
	not := false
	if names := ae.GetName(); len(names) == 1 && names[0].GetString_() != nil {
		switch names[0].GetString_().GetSval() {
		case "~~":
		case "!~~":
			// The parser reports NOT LIKE as this expression kind with the
			// opposite operator; reading it as LIKE would silently return
			// exactly the rows the client excluded.
			not = true
		default:
			return nil, fmt.Errorf("%w: only LIKE and NOT LIKE are supported", errUnsupported)
		}
	}
	// The pattern is taken as a value reference rather than a string so a
	// parameter is representable. Its shape (prefix or general) is decided at
	// compile time, where the bytes are known for both a literal and a bound
	// parameter.
	pat, err := valueRefOrNull(ae.GetRexpr(), col)
	if err != nil {
		return nil, err
	}
	if pat.Null {
		// `col LIKE NULL` is UNKNOWN, so no row qualifies.
		return &FalseExpr{}, nil
	}
	return &LikeExpr{Col: col, Val: pat, Not: not}, nil
}

// likePrefixPattern reports whether a LIKE pattern matches only a prefix of
// the value -- its sole wildcard an unescaped trailing `%`, no `_` anywhere --
// and returns that prefix with the trailing `%` and any escape backslashes
// removed. Such a pattern compiles to a HasPrefix test the planner may also
// turn into a key range; every other shape returns ok=false and compiles to
// the general matcher instead. A backslash escapes the next byte in both
// directions, so `pg\_temp%` prefixes literally on `pg_temp`.
func likePrefixPattern(pattern []byte) (prefix []byte, ok bool) {
	var b []byte
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			if i+1 >= len(pattern) {
				// A trailing backslash is a literal backslash.
				b = append(b, '\\')
				continue
			}
			i++
			b = append(b, pattern[i])
		case '_':
			return nil, false
		case '%':
			if i != len(pattern)-1 {
				return nil, false
			}
			return b, true
		default:
			b = append(b, pattern[i])
		}
	}
	// No wildcard at all: the pattern matches exactly, which is a prefix of
	// itself and therefore answerable by HasPrefix.
	return b, true
}

// likeMatch reports whether a value matches a LIKE pattern: `%` matches any
// run of bytes (including none), `_` matches exactly one, a backslash escapes
// the byte after it, and everything else matches itself. A trailing backslash
// is a literal backslash, as PostgreSQL reads one.
//
// The recursion tries every split point a `%` allows, so a pattern of n
// wildcards over a value of m bytes costs O(n*m) in the usual case and can
// degenerate worse on adversarial input like `a%a%a%a%b` against a long run of
// `a`. That is bounded by the statement's own pattern, is the cost of
// answering the shape at all, and is what keeps the answer correct: a
// non-prefix pattern is answered by the matcher rather than refused.
func likeMatch(s, pat []byte) bool {
	if len(pat) == 0 {
		return len(s) == 0
	}
	switch pat[0] {
	case '%':
		// Consecutive percent signs are one wildcard.
		for len(pat) > 1 && pat[1] == '%' {
			pat = pat[1:]
		}
		for i := 0; i <= len(s); i++ {
			if likeMatch(s[i:], pat[1:]) {
				return true
			}
		}
		return false
	case '_':
		return len(s) > 0 && likeMatch(s[1:], pat[1:])
	case '\\':
		if len(pat) == 1 {
			return len(s) > 0 && s[0] == '\\' && likeMatch(s[1:], pat[1:])
		}
		return len(s) > 0 && s[0] == pat[1] && likeMatch(s[1:], pat[2:])
	default:
		return len(s) > 0 && s[0] == pat[0] && likeMatch(s[1:], pat[1:])
	}
}

// plainColumn reports the column a node names, and false for anything else --
// including a star, which `columnRef` accepts for projections but which cannot
// be compared to a value. A qualified star ("t.*") is a star too.
func plainColumn(node *pg_query.Node) (string, bool) {
	col, ok := columnRef(node)
	if !ok || col == "*" || strings.HasSuffix(col, ".*") {
		return "", false
	}
	return col, true
}

func cmpOpFor(name string) (CmpOp, bool) {
	switch name {
	case "=":
		return OpEq, true
	case "<":
		return OpLt, true
	case "<=":
		return OpLe, true
	case ">":
		return OpGt, true
	case ">=":
		return OpGe, true
	}
	return 0, false
}

// walkExpr calls fn for every node in the tree, parents before children.
//
// Parents first is what lets a consumer accumulate state -- collecting the
// columns a predicate reads, or stopping at the first node it can turn into a
// range -- without recursing itself. A child is not visited once fn returns
// false, so returning false prunes the subtree.
func walkExpr(e Expr, fn func(Expr) bool) bool {
	if e == nil {
		return true
	}
	if !fn(e) {
		return false
	}
	if b, ok := e.(*BoolExpr); ok {
		for _, arg := range b.Args {
			if !walkExpr(arg, fn) {
				return false
			}
		}
	}
	return true
}

// exprColumns collects the columns a predicate reads, in visit order and without
// duplicates. The planner needs the set to validate every reference against the
// schema, and needs it before execution so a typo is an error rather than a
// filter that silently matches nothing.
func exprColumns(e Expr) []string {
	var out []string
	seen := map[string]bool{}
	walkExpr(e, func(n Expr) bool {
		var col string
		switch t := n.(type) {
		case *CmpExpr:
			col = t.Col
		case *NeExpr:
			col = t.Col
		case *NullTestExpr:
			col = t.Col
		case *LikeExpr:
			col = t.Col
		case *BoolColExpr:
			col = t.Col
		case *RegexExpr:
			col = t.Col
		case *ColCmpExpr:
			// Both sides are read, so both are reported: a reference to
			// either one has to resolve against the schema.
			for _, c := range []string{t.Left, t.Right} {
				if c != "" && !seen[c] {
					seen[c] = true
					out = append(out, c)
				}
			}
			return true
		default:
			return true
		}
		if col != "" && !seen[col] {
			seen[col] = true
			out = append(out, col)
		}
		return true
	})
	return out
}

// mapExprCols returns e with every column name rewritten by fn, as a fresh
// tree so callers can hold both forms (the single-table planner strips a
// matching qualifier off its predicate; the join planner resolves every name
// to the merged row). A terminal that carries no column passes through; the
// nodes that carry one are rebuilt so the rewrite never aliases its input.
func mapExprCols(e Expr, fn func(string) (string, error)) (Expr, error) {
	switch t := e.(type) {
	case nil:
		return nil, nil
	case *TrueExpr, *FalseExpr:
		return e, nil
	case *CmpExpr:
		col, err := fn(t.Col)
		if err != nil {
			return nil, err
		}
		return &CmpExpr{Col: col, Op: t.Op, Val: t.Val}, nil
	case *NullTestExpr:
		col, err := fn(t.Col)
		if err != nil {
			return nil, err
		}
		return &NullTestExpr{Col: col, Not: t.Not}, nil
	case *NeExpr:
		col, err := fn(t.Col)
		if err != nil {
			return nil, err
		}
		return &NeExpr{Col: col, Val: t.Val}, nil
	case *LikeExpr:
		col, err := fn(t.Col)
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Col: col, Val: t.Val, Not: t.Not}, nil
	case *BoolColExpr:
		col, err := fn(t.Col)
		if err != nil {
			return nil, err
		}
		return &BoolColExpr{Col: col}, nil
	case *RegexExpr:
		col, err := fn(t.Col)
		if err != nil {
			return nil, err
		}
		return &RegexExpr{Col: col, Val: t.Val, Not: t.Not}, nil
	case *ColCmpExpr:
		left, err := fn(t.Left)
		if err != nil {
			return nil, err
		}
		right, err := fn(t.Right)
		if err != nil {
			return nil, err
		}
		return &ColCmpExpr{Left: left, Right: right, Op: t.Op, Not: t.Not}, nil
	case *BoolExpr:
		args := make([]Expr, 0, len(t.Args))
		for _, a := range t.Args {
			m, err := mapExprCols(a, fn)
			if err != nil {
				return nil, err
			}
			args = append(args, m)
		}
		return &BoolExpr{Op: t.Op, Args: args}, nil
	default:
		return e, nil
	}
}

// exprHasLiteralOnly reports whether every value in the tree is a literal, with
// no parameter references. A parameterized tree cannot be resolved to a key
// range at plan time, because the bound is not known until Bind.
func exprHasLiteralOnly(e Expr) bool {
	ok := true
	walkExpr(e, func(n Expr) bool {
		switch t := n.(type) {
		case *CmpExpr:
			if t.Val.Param != 0 {
				ok = false
				return false
			}
		case *NeExpr:
			if t.Val.Param != 0 {
				ok = false
				return false
			}
		case *LikeExpr:
			if t.Val.Param != 0 {
				ok = false
				return false
			}
		}
		return true
	})
	return ok
}
