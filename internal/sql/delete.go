/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: delete.go
Description: Multi-row DELETE execution (Phase 12).

DeleteExec consumes a row stream and deletes the entire row group for
each matched row. The affected count is the number of matched rows.
*/
package sql

// deleteRowGroup deletes every column key of the row, reporting whether any key
// was present. The key set comes from the schema, not from a scan of the row
// prefix: a range read may be served best-effort and miss a key, and a missed
// column would survive the delete as a live key, keeping the row readable and
// the table non-empty for a later DROP.
func deleteRowGroup(st Store, sch *Schema, rowID string) (bool, error) {
	deleted := false
	for i := range sch.Columns {
		ok, err := st.Delete(ColumnKey(sch.DB, sch.Table, rowID, sch.Columns[i].Name))
		if err != nil {
			return deleted, err
		}
		if ok {
			deleted = true
		}
	}
	return deleted, nil
}
