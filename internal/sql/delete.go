/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: delete.go
Description: Multi-row DELETE execution (Phase 12).

DeleteExec consumes a row stream and deletes the whole row for each matched row.
The affected count is the number of matched rows.
*/
package sql

// deleteRowGroup deletes a row, reporting whether it was present. In the packed
// layout the row is one key, so this is a single delete.
func deleteRowGroup(st Store, sch *Schema, rowID string) (bool, error) {
	return st.Delete(rowKey(sch, rowID))
}
