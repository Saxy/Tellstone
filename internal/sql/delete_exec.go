/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: delete_exec.go
Description: Multi-row DELETE execution (Phase 12).
*/
package sql

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Saxy/Tellstone/internal/keyspace"
)

// execDeleteMulti deletes all rows matched by the plan, returning DELETE n.
func (s *Server) execDeleteMulti(st Store, plan *Plan, params []paramVal) (*execOutcome, error) {
	if plan.Schema == nil || plan.Phys == nil {
		return &execOutcome{tag: "DELETE 0"}, nil
	}
	if plan.Phys.EmptyRange {
		return &execOutcome{tag: "DELETE 0"}, nil
	}
	phys := plan.Phys
	sch := plan.Schema
	proj := []int{sch.PrimaryKey}
	filt, err := s.compileFilter(phys.Filter, sch, params)
	if err != nil {
		return nil, err
	}
	if filt != nil && filt.alwaysFalse() {
		return &execOutcome{tag: "DELETE 0"}, nil
	}
	n := int64(0)
	err = newScan(st, phys, sch, proj, filt).run(func(c *Chunk) error {
		off := 0
		for i := uint32(0); i < c.Rows; i++ {
			// skip over non-pk columns if any projected (we only project pk)
			j := uint32(0) // pk is only col
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
			if _, err := deleteRowGroup(st, sch, id); err != nil {
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
	return &execOutcome{tag: fmt.Sprintf("DELETE %d", n)}, nil
}

// pkBytesToRowID turns a projected primary-key value into the row id used in
// keys. It must agree with canonicalRowID, which produced the row id when the
// row was written: an integer primary key is the sign-flipped hex form, a text
// primary key is the text itself, and every other type is the canonical wire
// text of the stored value. Deriving it any other way, such as from the raw
// stored bytes, addresses a different key, so a scan-driven UPDATE or DELETE
// would match nothing. The stored bytes are the primary-key column's value as
// the scan read it, which is the same encoding canonicalRowID fed to wireCell.
func pkBytesToRowID(pkb []byte, sch *Schema) (string, bool) {
	if sch == nil {
		return "", false
	}
	pkType := sch.Columns[sch.PrimaryKey].Type
	switch pkType {
	case TypeInt, TypeBigInt:
		v, err := DecodeValue(pkType, pkb)
		if err != nil {
			return "", false
		}
		n, ok := v.(int64)
		if !ok {
			return "", false
		}
		return keyspace.EncodeIntRowID(n), true
	case TypeVarchar:
		return string(pkb), true
	default:
		canon, err := wireCell(pkType, pkb)
		if err != nil {
			return "", false
		}
		return string(canon), true
	}
}
