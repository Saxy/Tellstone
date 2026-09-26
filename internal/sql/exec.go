/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: exec.go
Description: Runs a translated plan against the shared store and emits the
CommandComplete tag a data statement reports. This is where RBAC is enforced
per statement, where conditional writes keep INSERT and UPDATE atomic against
concurrent clients, and where transaction blocks, NULL handling and the audit
trail are decided.
*/
package sql

import (
	"fmt"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

// execOutcome is the materialized result of one statement execution.
type execOutcome struct {
	tag        string
	row        [][]byte // SELECT: column cells (wire-encoded, nil = NULL); nil = empty result
	selectRows bool
}

// isWrite reports whether a statement mutates data. Transaction statements are
// excluded: they carry no key and touch no data.
func (k StmtKind) isWrite() bool {
	switch k {
	case StmtInsert, StmtUpdate, StmtDelete:
		return true
	default:
		return false
	}
}

// command maps a statement onto the RBAC command it needs, plus the lowercase
// token the binary frontend records in ACL LOG and the audit trail.
func (k StmtKind) command() (uint16, string) {
	switch k {
	case StmtInsert, StmtUpdate:
		return rbac.CmdSet, "set"
	case StmtDelete:
		return rbac.CmdDel, "del"
	default:
		return rbac.CmdGet, "get"
	}
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

	// The store exposes single-key operations with no enclosing transaction,
	// so a write issued inside a transaction block would already be durable by
	// the time ROLLBACK ran. Refusing the write is the only way to keep the
	// block's promise: a ROLLBACK then genuinely has nothing to undo.
	if c.inTxn && plan.Kind.isWrite() {
		return nil, &pgError{
			code: errFeatureNotSupported,
			msg:  "data statements are not supported inside a transaction block",
		}
	}

	if c.auth != nil && c.auth.session != nil {
		key, err := plan.Key.resolve(params)
		if err != nil {
			return nil, err
		}
		cmd, name := plan.Kind.command()
		if !c.auth.session.IsAllowed(cmd, key) {
			if s.policy != nil {
				s.policy.LogDenied(c.user, c.remoteAddr, name, string(key))
			}
			s.auditDenied(c, name, key)
			return nil, &pgError{code: errInsufficientPrivilege, msg: "permission denied for table " + tableName}
		}
		c.auth.session.CountCommand()
		s.auditCommand(c, name, key)
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

// auditCommand records one authorized statement with the same field set and
// event the binary frontend uses, so both protocols land in one audit trail.
func (s *Server) auditCommand(c *pgConn, command string, key []byte) {
	if s.audit == nil {
		return
	}
	s.audit.Record(audit.EventCommand, "command dispatched",
		log.String("command", command),
		log.String("key", string(key)),
		log.String("user", c.user),
		log.String("remote_addr", c.remoteAddr),
		log.String("protocol", "sql"),
	)
}

// auditDenied records one RBAC rejection, mirroring Ctx.deny in the command
// layer so a denial is visible in the audit trail whichever protocol issued it.
func (s *Server) auditDenied(c *pgConn, command string, key []byte) {
	if s.audit == nil {
		return
	}
	s.audit.Record(audit.EventACLDeny, "command denied by rbac policy",
		log.String("user", c.user),
		log.String("command", command),
		log.String("key", string(key)),
		log.String("remote_addr", c.remoteAddr),
		log.String("protocol", "sql"),
	)
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
	switch plan.Conflict {
	case ConflictDoUpdate:
		// ON CONFLICT DO UPDATE SET value = excluded.value replaces the whole
		// proposed row, so an unconditional write is exactly the semantics:
		// it creates the row when absent and overwrites it when present.
		if serr := s.store.Set(string(key), val, 0); serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	case ConflictDoNothing:
		// The existing row must survive, so the write may only create. A
		// conflicting key is a successful no-op, reported as zero rows.
		applied, serr := s.store.SetIfAbsent(string(key), val, 0)
		if serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		if !applied {
			return &execOutcome{tag: "INSERT 0 0"}, nil
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	default:
		// Real primary-key semantics: a plain INSERT creates the row and must
		// not overwrite one that already exists. The create-if-absent
		// precondition and the write are one atomic store operation, so two
		// concurrent inserts of the same key cannot both report success.
		applied, serr := s.store.SetIfAbsent(string(key), val, 0)
		if serr != nil {
			return nil, &pgError{code: errIoError, msg: serr.Error()}
		}
		if !applied {
			return nil, &pgError{code: errDuplicateKey, msg: `duplicate key value violates unique constraint "tellstone"`}
		}
		return &execOutcome{tag: "INSERT 0 1"}, nil
	}
}

func (s *Server) execUpdate(plan *Plan, params []paramVal) (*execOutcome, error) {
	key, err := plan.Key.resolve(params)
	if err != nil {
		return nil, err
	}
	val, err := plan.Val.resolveValue(params)
	if err != nil {
		return nil, err
	}
	// UPDATE may only modify a row that exists. Folding the existence check into
	// the write keeps the affected-row count honest: a row deleted
	// concurrently reports zero rows instead of being silently resurrected.
	applied, serr := s.store.SetIfPresent(string(key), val, 0)
	if serr != nil {
		return nil, &pgError{code: errIoError, msg: serr.Error()}
	}
	if !applied {
		return &execOutcome{tag: "UPDATE 0"}, nil
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
			return nil, &pgError{code: errNotNullViolation, msg: fmt.Sprintf("null value in column %q violates not-null constraint", colKey)}
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
			// The store has no way to represent a NULL distinct from an empty
			// payload: presence is the only signal Get returns. Storing NULL as
			// an empty value would make the row read back as '' instead of NULL,
			// so the column is declared NOT NULL and a NULL is rejected here.
			return nil, &pgError{code: errNotNullViolation, msg: fmt.Sprintf("null value in column %q violates not-null constraint", colValue)}
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
