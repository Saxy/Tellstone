/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: update_exec.go
Description: Multi-row UPDATE execution (Phase 12).
*/
package sql

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// execUpdateMulti updates all rows matched by the plan, returning UPDATE n.
func (s *Server) execUpdateMulti(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema == nil || plan.Phys == nil {
		return &execOutcome{tag: "UPDATE 0"}, nil
	}
	if plan.Phys.EmptyRange {
		return &execOutcome{tag: "UPDATE 0"}, nil
	}
	phys := plan.Phys
	sch := plan.Schema
	proj := []int{sch.PrimaryKey}
	filt, err := s.compileFilter(phys.Filter, sch, params)
	if err != nil {
		return nil, err
	}
	if filt != nil && filt.alwaysFalse() {
		return &execOutcome{tag: "UPDATE 0"}, nil
	}
	cells, err := plan.buildCells(params, false)
	if err != nil {
		return nil, err
	}
	n := int64(0)
	err = newScan(st, phys, sch, proj, filt).run(func(c *Chunk) error {
		off := 0
		for i := uint32(0); i < c.Rows; i++ {
			j := uint32(0)
			if c.isNull(i, j) {
				continue
			}
			l := binary.BigEndian.Uint32(c.data[off:])
			off += 4
			pkb := c.data[off : off+int(l)]
			off += int(l)
			id, ok := pkBytesToRowID(pkb, sch)
			if !ok {
				continue
			}
			if err := updateRow(st, sch, id, cells); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNotNull) {
			return nil, &pgError{code: errNotNullViolation, msg: err.Error()}
		}
		if errors.Is(err, ErrBadSchema) {
			return nil, &pgError{code: errSyntax, msg: err.Error()}
		}
		return nil, &pgError{code: errDataCorrupt, msg: err.Error()}
	}
	return &execOutcome{tag: fmt.Sprintf("UPDATE %d", n)}, nil
}
