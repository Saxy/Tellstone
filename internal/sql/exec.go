package sql

import (
	"fmt"

	"github.com/Saxy/Tellstone/internal/rbac"
)

// execOutcome is the materialized result of one statement execution.
type execOutcome struct {
	tag        string
	row        [][]byte // SELECT: column cells (wire-encoded, nil = NULL); nil = empty result
	selectRows bool
}

// execute runs a translated plan against the shared store with the bound
// parameters. RBAC enforcement mirrors the binary frontend: the session's
// command bit and key prefixes are consulted on every data statement.
func (s *Server) execute(c *pgConn, plan *Plan, params []paramVal) (*execOutcome, error) {
	switch plan.Kind {
	case StmtBegin:
		c.inTxn = true
		return &execOutcome{tag: "BEGIN"}, nil
	case StmtCommit:
		c.inTxn = false
		return &execOutcome{tag: "COMMIT"}, nil
	case StmtRollback:
		c.inTxn = false
		return &execOutcome{tag: "ROLLBACK"}, nil
	}

	if c.auth != nil && c.auth.session != nil {
		key, err := plan.Key.resolve(params)
		if err != nil {
			return nil, err
		}
		cmd, name := rbac.CmdGet, "GET"
		switch plan.Kind {
		case StmtInsert, StmtUpdate:
			cmd, name = rbac.CmdSet, "SET"
		case StmtDelete:
			cmd, name = rbac.CmdDel, "DEL"
		}
		if !c.auth.session.IsAllowed(cmd, key) {
			if s.policy != nil {
				s.policy.LogDenied(c.user, c.remoteAddr, name, string(key))
			}
			return nil, &pgError{code: errInsufficientPrivilege, msg: "permission denied for table " + tableName}
		}
		c.auth.session.CountCommand()
	}

	switch plan.Kind {
	case StmtSelect:
		return s.execSelect(c, plan, params)
	case StmtInsert:
		return s.execInsert(plan, params)
	case StmtUpdate:
		return s.execUpdate(plan, params)
	case StmtDelete:
		return s.execDelete(plan, params)
	default:
		return nil, &pgError{code: errFeatureNotSupported, msg: "unhandled statement kind"}
	}
}

func (s *Server) execSelect(c *pgConn, plan *Plan, params []paramVal) (*execOutcome, error) {
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	val, found, serr := s.store.GetErr(string(key))
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if !found {
		return &execOutcome{tag: "SELECT 0", selectRows: true}, nil
	}
	row := make([][]byte, len(plan.Cols))
	for i, col := range plan.Cols {
		switch col {
		case colKey:
			row[i] = key
		case colValue:
			row[i] = encodeByteaText(val)
		}
	}
	return &execOutcome{tag: "SELECT 1", row: row, selectRows: true}, nil
}

func (s *Server) execInsert(plan *Plan, params []paramVal) (*execOutcome, error) {
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	val, err := plan.Val.resolveValue(params)
	if err != nil {
		return nil, err
	}
	// Real primary-key semantics: a plain INSERT on an existing key is a
	// duplicate-key violation, not a silent overwrite. ON CONFLICT opts in to
	// the upsert the store performs.
	if !plan.OnConflict {
		_, found, serr := s.store.GetErr(string(key))
		if serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		if found {
			return nil, &pgError{code: errDuplicateKey, msg: `duplicate key value violates unique constraint "tellstone"`}
		}
	}
	if serr := s.store.Set(string(key), val, 0); serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	return &execOutcome{tag: "INSERT 0 1"}, nil
}

func (s *Server) execUpdate(plan *Plan, params []paramVal) (*execOutcome, error) {
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	_, found, serr := s.store.GetErr(string(key))
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if !found {
		return &execOutcome{tag: "UPDATE 0"}, nil
	}
	val, err := plan.Val.resolveValue(params)
	if err != nil {
		return nil, err
	}
	if serr = s.store.Set(string(key), val, 0); serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	return &execOutcome{tag: "UPDATE 1"}, nil
}

func (s *Server) execDelete(plan *Plan, params []paramVal) (*execOutcome, error) {
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	ok, serr := s.store.Delete(string(key))
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if ok {
		return &execOutcome{tag: "DELETE 1"}, nil
	}
	return &execOutcome{tag: "DELETE 0"}, nil
}

// resolve binds a parameter or returns the literal. key-column references
// reject nulls the way a NOT NULL primary key does.
func (v ValRef) resolve(params []paramVal) ([]byte, error) {
	if v.Param > 0 {
		i := v.Param - 1
		if i >= len(params) {
			return nil, &pgError{
				code: errProtocolViolation,
				msg:  fmt.Sprintf("bind message supplies %d parameters but prepared statement requires %d", len(params), v.Param),
			}
		}
		if params[i].null {
			return nil, &pgError{code: errSyntax, msg: `null value in column "key" violates not-null constraint`}
		}
		return params[i].val, nil
	}
	return v.Literal, nil
}

// resolveValue binds the value column, honoring the bytea text encoding of
// extended-protocol text-format parameters and of SQL bytea literals shaped
// "\x...". Binary-format parameters pass through untouched.
func (v ValRef) resolveValue(params []paramVal) ([]byte, error) {
	if v.Param > 0 {
		i := v.Param - 1
		if i >= len(params) {
			return nil, &pgError{
				code: errProtocolViolation,
				msg:  fmt.Sprintf("bind message supplies %d parameters but prepared statement requires %d", len(params), v.Param),
			}
		}
		if params[i].null {
			// A NULL bytea value stores an empty payload; Get distinguishes
			// presence via its ok flag, so the row round-trips correctly.
			return nil, nil
		}
		if params[i].format == resultFormatText {
			return decodeByteaText(params[i].val)
		}
		return params[i].val, nil
	}
	// SQL literal: decode bytea hex syntax when present, otherwise this is a
	// plain text payload kept verbatim.
	return decodeByteaText(v.Literal)
}
