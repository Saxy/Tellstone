/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: selectexpr.go
Description: Translation and evaluation of a FROM-less SELECT -- a target list
of constant and session-value expressions with no relation behind it. It is the
path a client's startup probe takes (SELECT 1, SELECT version()), so it must
answer without a catalog. Only the shapes a probe actually uses are supported;
anything that needs a relation is refused at translation rather than guessed at.
*/
package sql

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// selectExprOf maps one ResTarget of a FROM-less SELECT onto a selectExpr. It
// accepts a constant literal, a supported scalar function, a
// transaction-dependent value function, and integer/float arithmetic over those
// (a client occasionally sends SELECT 1 + 1). Everything else is refused.
func selectExprOf(rt *pg_query.ResTarget) (*selectExpr, error) {
	if len(rt.GetIndirection()) > 0 {
		return nil, fmt.Errorf("%w: indirection in a FROM-less SELECT target is not supported", errUnsupported)
	}
	return exprNodeOf(rt.GetVal(), rt.GetName())
}

// exprNodeOf narrows one expression node. name is the output alias the client
// wrote, if any; when empty a default label is derived from the expression.
func exprNodeOf(node *pg_query.Node, name string) (*selectExpr, error) {
	switch {
	case node.GetAConst() != nil:
		return constExpr(node.GetAConst(), name)
	case node.GetFuncCall() != nil:
		return funcExpr(node.GetFuncCall(), name)
	case node.GetSqlvalueFunction() != nil:
		return valueFuncExpr(node.GetSqlvalueFunction(), name)
	case node.GetTypeCast() != nil:
		// A cast over a supported expression keeps the inner value; the cast
		// only matters for output type, which the inner expression already
		// carries for the literals a probe uses.
		return exprNodeOf(node.GetTypeCast().GetArg(), name)
	case node.GetAExpr() != nil:
		return arithExpr(node.GetAExpr(), name)
	default:
		return nil, fmt.Errorf("%w: unsupported FROM-less SELECT expression", errUnsupported)
	}
}

// constExpr reads a literal constant. NULL is represented as a selectExpr whose
// Literal is empty and Null is set by the caller's evaluation, so it needs no
// separate kind.
func constExpr(c *pg_query.A_Const, name string) (*selectExpr, error) {
	switch {
	case c.GetIsnull():
		return &selectExpr{Name: name, Kind: selectExprLiteral, Literal: "", OID: oidText, Null: true}, nil
	case c.GetIval() != nil:
		return &selectExpr{Name: name, Kind: selectExprLiteral,
			Literal: strconv.FormatInt(int64(c.GetIval().GetIval()), 10), OID: oidInt4}, nil
	case c.GetFval() != nil:
		return &selectExpr{Name: name, Kind: selectExprLiteral,
			Literal: c.GetFval().GetFval(), OID: oidFloat8}, nil
	case c.GetBoolval() != nil:
		v := "f"
		if c.GetBoolval().GetBoolval() {
			v = "t"
		}
		return &selectExpr{Name: name, Kind: selectExprLiteral, Literal: v, OID: oidBool}, nil
	case c.GetSval() != nil:
		ex := &selectExpr{Name: name, Kind: selectExprLiteral,
			Literal: c.GetSval().GetSval(), OID: oidText}
		if ex.Name == "" {
			ex.Name = "?column?"
		}
		return ex, nil
	default:
		return nil, fmt.Errorf("%w: unsupported constant in FROM-less SELECT", errUnsupported)
	}
}

// aConstText renders a constant node as the text form a GUC stores. It covers
// the literal shapes a SET value uses (string, integer, float, boolean) and
// reports an error for anything else rather than inventing a value.
func aConstText(c *pg_query.A_Const) (string, error) {
	switch {
	case c.GetIval() != nil:
		return strconv.FormatInt(int64(c.GetIval().GetIval()), 10), nil
	case c.GetFval() != nil:
		return c.GetFval().GetFval(), nil
	case c.GetBoolval() != nil:
		if c.GetBoolval().GetBoolval() {
			return "on", nil
		}
		return "off", nil
	case c.GetSval() != nil:
		return c.GetSval().GetSval(), nil
	case c.GetIsnull():
		return "", nil
	default:
		return "", fmt.Errorf("%w: unsupported SET value literal", errUnsupported)
	}
}

// funcExpr reads a scalar function call. Only the functions a startup probe
// and the metadata queries call are recognized; an unknown name, or an argument
// shape no supported function takes, is refused so an unimplemented function
// never returns a fabricated value.
func funcExpr(fc *pg_query.FuncCall, name string) (*selectExpr, error) {
	if len(fc.GetFuncname()) != 1 {
		return nil, fmt.Errorf("%w: schema-qualified function calls are not supported", errUnsupported)
	}
	fname := strings.ToLower(fc.GetFuncname()[0].GetString_().GetSval())
	ex := &selectExpr{Name: name, Kind: selectExprFunc, Func: fname, OID: oidText}
	if fname == "current_schemas" {
		// current_schemas(include) is the one supported function with an
		// argument: the boolean that decides whether the implicitly-searched
		// pg_catalog is part of the result.
		inc, err := boolArg(fc, fname)
		if err != nil {
			return nil, err
		}
		ex.FuncArg = inc
		ex.OID = oidNameArray
	} else {
		if len(fc.GetArgs()) != 0 {
			return nil, fmt.Errorf("%w: function %s() with arguments is not supported", errUnsupported, fname)
		}
		switch fname {
		case "version", "current_database", "current_schema",
			"current_user", "session_user", "pg_backend_pid":
		case "now":
			ex.Kind = selectExprNow
			ex.OID = oidTimestamptz
		default:
			return nil, fmt.Errorf("%w: function %s() is not supported", errUnsupported, fname)
		}
	}
	if ex.Name == "" {
		ex.Name = fname
	}
	return ex, nil
}

// boolArg reads the single boolean literal a supported one-argument function
// is called with. A missing argument or anything that is not a boolean
// constant is refused rather than guessed at.
func boolArg(fc *pg_query.FuncCall, fname string) (bool, error) {
	if len(fc.GetArgs()) != 1 {
		return false, fmt.Errorf("%w: function %s() requires one boolean argument", errUnsupported, fname)
	}
	if b := fc.GetArgs()[0].GetAConst().GetBoolval(); b != nil {
		return b.GetBoolval(), nil
	}
	return false, fmt.Errorf("%w: function %s() argument must be a boolean literal", errUnsupported, fname)
}

// valueFuncExpr reads a SQL value function: the keyword forms PostgreSQL parses
// into SQLValueFunction rather than a FuncCall (current_timestamp, current_user
// and friends). The transaction/statement time ones change per statement; the
// identity and catalog ones are session constants.
func valueFuncExpr(sv *pg_query.SQLValueFunction, name string) (*selectExpr, error) {
	switch sv.GetOp() {
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP,
		pg_query.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP_N:
		return &selectExpr{Name: labelOr(name, "current_timestamp"), Kind: selectExprNow, OID: oidTimestamptz}, nil
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_DATE:
		return &selectExpr{Name: labelOr(name, "current_date"), Kind: selectExprNow, OID: oidText}, nil
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_USER,
		pg_query.SQLValueFunctionOp_SVFOP_USER,
		pg_query.SQLValueFunctionOp_SVFOP_SESSION_USER:
		return &selectExpr{Name: labelOr(name, "current_user"), Kind: selectExprFunc, Func: "current_user", OID: oidText}, nil
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_CATALOG:
		return &selectExpr{Name: labelOr(name, "current_catalog"), Kind: selectExprFunc, Func: "current_database", OID: oidText}, nil
	case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_SCHEMA:
		return &selectExpr{Name: labelOr(name, "current_schema"), Kind: selectExprFunc, Func: "current_schema", OID: oidText}, nil
	default:
		return nil, fmt.Errorf("%w: SQL value function is not supported in a FROM-less SELECT", errUnsupported)
	}
}

// arithExpr reads a two-operand arithmetic expression over integer or float
// literals. It is evaluated at translation because both operands are constants,
// which keeps execution a pure render and lets the result carry the right OID.
func arithExpr(ae *pg_query.A_Expr, name string) (*selectExpr, error) {
	if ae.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
		return nil, fmt.Errorf("%w: only arithmetic operators are supported in a FROM-less SELECT", errUnsupported)
	}
	if len(ae.GetName()) != 1 {
		return nil, fmt.Errorf("%w: unsupported operator", errUnsupported)
	}
	op := ae.GetName()[0].GetString_().GetSval()
	l, lInt, err := constNumber(ae.GetLexpr())
	if err != nil {
		return nil, err
	}
	r, rInt, err := constNumber(ae.GetRexpr())
	if err != nil {
		return nil, err
	}
	res, isFloat, err := applyArith(op, l, lInt, r, rInt)
	if err != nil {
		return nil, err
	}
	oid := int32(oidInt4)
	var lit string
	if isFloat {
		oid = oidFloat8
		lit = strconv.FormatFloat(res, 'g', -1, 64)
	} else {
		lit = strconv.FormatInt(int64(res), 10)
	}
	return &selectExpr{Name: labelOr(name, "?column?"), Kind: selectExprLiteral, Literal: lit, OID: oid}, nil
}

// constNumber reads an integer or float literal as a float64 plus a flag saying
// whether it was integral, so arithmetic can preserve integer typing.
func constNumber(node *pg_query.Node) (val float64, integral bool, err error) {
	c := node.GetAConst()
	if c == nil {
		return 0, false, fmt.Errorf("%w: arithmetic is supported only over numeric literals", errUnsupported)
	}
	if c.GetIval() != nil {
		return float64(c.GetIval().GetIval()), true, nil
	}
	if c.GetFval() != nil {
		f, perr := strconv.ParseFloat(c.GetFval().GetFval(), 64)
		if perr != nil {
			return 0, false, fmt.Errorf("%w: bad float literal", errUnsupported)
		}
		return f, false, nil
	}
	return 0, false, fmt.Errorf("%w: arithmetic is supported only over numeric literals", errUnsupported)
}

// applyArith evaluates one supported operator over two numeric literals,
// returning the result and whether it must be rendered as a float. Integer
// operands stay integer for +, - and *; integer division truncates toward zero
// and stays integer only when both operands are integers. Division by zero is
// the standard error.
func applyArith(op string, l float64, lInt bool, r float64, rInt bool) (float64, bool, error) {
	switch op {
	case "+":
		return l + r, !(lInt && rInt), nil
	case "-":
		return l - r, !(lInt && rInt), nil
	case "*":
		return l * r, !(lInt && rInt), nil
	case "/":
		if r == 0 {
			return 0, false, &pgError{code: errDivisionByZero, msg: "division by zero"}
		}
		if lInt && rInt {
			neg := (l < 0 && r > 0) || (l > 0 && r < 0)
			res := math.Trunc(math.Abs(l)/math.Abs(r))
			if neg {
				res = -res
			}
			return res, false, nil
		}
		return l / r, true, nil
	default:
		return 0, false, fmt.Errorf("%w: operator %q is not supported", errUnsupported, op)
	}
}

// labelOr returns name when the client supplied an alias, otherwise def.
func labelOr(name, def string) string {
	if name != "" {
		return name
	}
	return def
}

// evalSelectExprs renders a FROM-less SELECT's output row. now is the statement
// timestamp, supplied by execution so current_timestamp is stable across the
// columns of one statement.
func evalSelectExprs(exprs []*selectExpr, c *pgConn, now time.Time) ([][]byte, error) {
	row := make([][]byte, len(exprs))
	for i, ex := range exprs {
		cell, err := evalSelectExpr(ex, c, now)
		if err != nil {
			return nil, err
		}
		row[i] = cell
	}
	return row, nil
}

// evalSelectExpr produces one column's text-format cell. A nil return is SQL
// NULL, which the wire layer emits as a -1 length.
func evalSelectExpr(ex *selectExpr, c *pgConn, now time.Time) ([]byte, error) {
	switch ex.Kind {
	case selectExprLiteral:
		if ex.Null {
			return nil, nil
		}
		return []byte(ex.Literal), nil
	case selectExprNow:
		return []byte(now.UTC().Format(timestampText)), nil
	case selectExprFunc:
		return sessionFuncValue(ex, c)
	default:
		return nil, fmt.Errorf("%w: unsupported SELECT expression", errUnsupported)
	}
}

// sessionFuncValue renders a supported function from session state.
func sessionFuncValue(ex *selectExpr, c *pgConn) ([]byte, error) {
	switch ex.Func {
	case "version":
		return []byte(serverVersionString), nil
	case "current_database":
		return []byte(DefaultDB), nil
	case "current_schema":
		if c != nil {
			path := c.showValue("search_path")
			for _, entry := range splitSearchPath(path) {
				if entry == "\"$user\"" {
					continue
				}
				if strings.EqualFold(entry, schemaPGCatalog) || strings.EqualFold(entry, schemaInfo) {
					continue
				}
				return []byte(entry), nil
			}
		}
		return []byte(schemaPublic), nil
	case "current_user", "session_user":
		u := c.user
		if u == "" {
			u = "default"
		}
		return []byte(u), nil
	case "pg_backend_pid":
		return []byte(strconv.FormatInt(int64(c.srv.backendPID()), 10)), nil
	case "current_schemas":
		return currentSchemasValue(c, ex.FuncArg), nil
	default:
		return nil, fmt.Errorf("%w: function %s() is not supported", errUnsupported, ex.Func)
	}
}

// currentSchemasValue renders current_schemas(include): the schemas the
// session's search_path resolves to, in path order, with pg_catalog listed
// first when the argument asks for the implicit entries (and kept when the
// path itself lists it, which is what the golden server answers). An entry
// resolves only if this engine's pg_namespace declares the schema, so $user --
// no schema ever names the role -- and a path naming an unknown schema drop
// out exactly as they do on a fresh database. The result is resolved, not
// copied out of the GUC text.
func currentSchemasValue(c *pgConn, includeImplicit bool) []byte {
	out := make([]string, 0, 4)
	if includeImplicit {
		out = append(out, schemaPGCatalog)
	}
	for _, entry := range splitSearchPath(c.showValue("search_path")) {
		if !schemaKnown(entry) || slices.Contains(out, entry) {
			continue
		}
		out = append(out, entry)
	}
	return []byte("{" + strings.Join(out, ",") + "}")
}

// schemaKnown reports whether name is one of the schemas this engine's
// pg_namespace declares (pgcat.go) -- the only schemas a search_path entry
// can resolve to.
func schemaKnown(name string) bool {
	switch name {
	case schemaPGCatalog, schemaPublic, schemaInfo:
		return true
	}
	return false
}

// splitSearchPath splits a search_path value into its entries, honouring the
// double quotes PostgreSQL allows around an entry (the quotes come off, as
// they are not part of the schema name). The simple shapes clients set are
// what this engine sees; an unbalanced quote ends the entry where it falls
// rather than failing the statement.
func splitSearchPath(v string) []string {
	var (
		out    []string
		cur    strings.Builder
		quoted bool
	)
	for _, r := range v {
		switch {
		case r == '"':
			quoted = !quoted
		case r == ',' && !quoted:
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	return append(out, strings.TrimSpace(cur.String()))
}

// serverVersionString is the value SELECT version() returns and the version
// string a client parses to decide which introspection query to send. It names
// a real PostgreSQL release so client feature-detection takes a known path.
const serverVersionString = "PostgreSQL 14.10 (Tellstone)"
