/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: pgcat_relations.go
Description: The definitions of the virtual catalog relations. Each relation
declares its columns and how it derives its rows from the live catalog.
*/
package sql

// The column lists and row builders of the virtual relations. A relation
// without a builder yields no rows: it is present so that a client which
// names it gets an empty answer rather than an error, which is what ADR-016
// decision D7 allows for the catalogs outside the clients' scope.

// relPGNamespace describes the schemas a client can see.
func relPGNamespace() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("nspname", TypeVarchar),
		cc("nspowner", TypeInt),
		ccn("nspacl", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_namespace", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			owner := catInt(10)
			return [][]rowValue{
				catRow(cols, map[string]rowValue{
					"oid": catInt(oidPGCatalogNamespace), "nspname": catText(schemaPGCatalog), "nspowner": owner,
				}),
				catRow(cols, map[string]rowValue{
					"oid": catInt(oidPublicNamespace), "nspname": catText(schemaPublic), "nspowner": owner,
				}),
				catRow(cols, map[string]rowValue{
					"oid": catInt(oidInfoNamespace), "nspname": catText(schemaInfo), "nspowner": owner,
				}),
			}, nil
		},
	}
}

// relPGClass describes the relations a client can see.
func relPGClass() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("relname", TypeVarchar),
		cc("relnamespace", TypeInt),
		cc("reltype", TypeInt),
		cc("reloftype", TypeInt),
		cc("relowner", TypeInt),
		cc("relam", TypeInt),
		cc("relfilenode", TypeInt),
		cc("reltablespace", TypeInt),
		cc("relpages", TypeInt),
		cc("reltuples", TypeFloat),
		cc("relallvisible", TypeInt),
		cc("reltoastrelid", TypeInt),
		cc("relhasindex", TypeBool),
		cc("relisshared", TypeBool),
		cc("relpersistence", TypeVarchar),
		cc("relkind", TypeVarchar),
		cc("relnatts", TypeInt),
		cc("relchecks", TypeInt),
		cc("relhasrules", TypeBool),
		cc("relhastriggers", TypeBool),
		cc("relhassubclass", TypeBool),
		cc("relrowsecurity", TypeBool),
		cc("relforcerowsecurity", TypeBool),
		cc("relispopulated", TypeBool),
		cc("relreplident", TypeVarchar),
		cc("relispartition", TypeBool),
		cc("relrewrite", TypeInt),
		cc("relfrozenxid", TypeInt),
		cc("relminmxid", TypeInt),
		ccn("relacl", TypeVarchar),
		ccn("reloptions", TypeVarchar),
		ccn("relpartbound", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_class", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			var out [][]rowValue
			base := func(oid int32, name string, ns int32, kind string, natts int) map[string]rowValue {
				return map[string]rowValue{
					"oid": catInt(int64(oid)), "relname": catText(name), "relnamespace": catInt(int64(ns)),
					"reltype": catInt(0), "reloftype": catInt(0), "relowner": catInt(10), "relam": catInt(2),
					"relfilenode": catInt(int64(oid)), "reltablespace": catInt(0), "relpages": catInt(0),
					"reltuples": catFloat(-1), "relallvisible": catInt(0), "reltoastrelid": catInt(0),
					"relhasindex": catBool(false), "relisshared": catBool(false), "relpersistence": catText("p"),
					"relkind": catText(kind), "relnatts": catInt(int64(natts)), "relchecks": catInt(0),
					"relhasrules": catBool(false), "relhastriggers": catBool(false), "relhassubclass": catBool(false),
					"relrowsecurity": catBool(false), "relforcerowsecurity": catBool(false),
					"relispopulated": catBool(true), "relreplident": catText("d"), "relispartition": catBool(false),
					"relrewrite": catInt(0), "relfrozenxid": catInt(0), "relminmxid": catInt(0),
				}
			}
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			for _, t := range tables {
				m := base(objectOID(t.Table), t.Table, oidPublicNamespace, "r", len(t.Columns))
				out = append(out, catRow(cols, m))
			}
			for _, r := range catalogRelations {
				ns := int32(oidPGCatalogNamespace)
				if r.schema == schemaInfo {
					ns = oidInfoNamespace
				}
				m := base(r.oid, r.name, ns, "r", len(r.columns))
				out = append(out, catRow(cols, m))
			}
			return out, nil
		},
	}
}

// relPGAttribute describes the columns of the engine's tables.
func relPGAttribute() *catalogRelation {
	cols := []catalogColumn{
		cc("attrelid", TypeInt),
		cc("attname", TypeVarchar),
		cc("atttypid", TypeInt),
		cc("attlen", TypeInt),
		cc("attnum", TypeInt),
		cc("atttypmod", TypeInt),
		cc("attbyval", TypeBool),
		cc("attalign", TypeVarchar),
		cc("attstorage", TypeVarchar),
		cc("attnotnull", TypeBool),
		cc("atthasdef", TypeBool),
		cc("atthasmissing", TypeBool),
		cc("attidentity", TypeVarchar),
		cc("attgenerated", TypeVarchar),
		cc("attisdropped", TypeBool),
		cc("attislocal", TypeBool),
		cc("attinhcount", TypeInt),
		cc("attcollation", TypeInt),
		ccn("attacl", TypeVarchar),
		ccn("attoptions", TypeVarchar),
		ccn("attfdwoptions", TypeVarchar),
		ccn("attmissingval", TypeVarchar),
		cc("attstattarget", TypeInt),
		cc("attndims", TypeInt),
		cc("attcacheoff", TypeInt),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_attribute", columns: cols, key: []string{"attrelid", "attnum"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				relid := objectOID(t.Table)
				for i, col := range t.Columns {
					toid, _, tlen, byval := pgTypeOf(col.Type)
					storage := "p"
					if col.Type == TypeVarchar || col.Type == TypeBytes || col.Type == TypeJSONB {
						storage = "x"
					}
					m := map[string]rowValue{
						"attrelid": catInt(int64(relid)), "attname": catText(col.Name),
						"atttypid": catInt(int64(toid)), "attlen": catInt(int64(tlen)),
						"attnum": catInt(int64(i + 1)), "atttypmod": catInt(-1),
						"attbyval": catBool(byval), "attalign": catText("i"), "attstorage": catText(storage),
						"attnotnull": catBool(!col.Nullable), "atthasdef": catBool(col.Default != nil),
						"atthasmissing": catBool(false), "attidentity": catText(""), "attgenerated": catText(""),
						"attisdropped": catBool(false), "attislocal": catBool(true), "attinhcount": catInt(0),
						"attcollation": catInt(0), "attstattarget": catInt(-1), "attndims": catInt(0),
						"attcacheoff": catInt(-1),
					}
					out = append(out, catRow(cols, m))
				}
			}
			return out, nil
		},
	}
}

// relPGType describes the types a client can name.
func relPGType() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("typname", TypeVarchar),
		cc("typnamespace", TypeInt),
		cc("typowner", TypeInt),
		cc("typlen", TypeInt),
		cc("typbyval", TypeBool),
		cc("typtype", TypeVarchar),
		cc("typcategory", TypeVarchar),
		cc("typispreferred", TypeBool),
		cc("typisdefined", TypeBool),
		cc("typdelim", TypeVarchar),
		cc("typrelid", TypeInt),
		cc("typelem", TypeInt),
		cc("typarray", TypeInt),
		cc("typinput", TypeVarchar),
		cc("typoutput", TypeVarchar),
		cc("typnotnull", TypeBool),
		cc("typbasetype", TypeInt),
		cc("typtypmod", TypeInt),
		cc("typndims", TypeInt),
		cc("typcollation", TypeInt),
		ccn("typdefaultbin", TypeVarchar),
		ccn("typdefault", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_type", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			var out [][]rowValue
			seen := map[int32]bool{}
			for _, t := range standardTypes {
				seen[t.oid] = true
				out = append(out, catRow(cols, map[string]rowValue{
					"oid": catInt(int64(t.oid)), "typname": catText(t.name), "typnamespace": catInt(oidPGCatalogNamespace),
					"typowner": catInt(10), "typlen": catInt(int64(t.len)), "typbyval": catBool(t.byval),
					"typtype": catText(t.typType), "typcategory": catText(t.category), "typispreferred": catBool(false),
					"typisdefined": catBool(true), "typdelim": catText(","), "typrelid": catInt(0),
					"typelem": catInt(int64(t.elem)), "typarray": catInt(int64(t.array)), "typinput": catText("-"),
					"typoutput": catText("-"), "typnotnull": catBool(false), "typbasetype": catInt(0),
					"typtypmod": catInt(-1), "typndims": catInt(0), "typcollation": catInt(0),
				}))
			}
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			for _, t := range tables {
				for _, col := range t.Columns {
					toid, _, tlen, byval := pgTypeOf(col.Type)
					if seen[toid] {
						continue
					}
					seen[toid] = true
					out = append(out, catRow(cols, map[string]rowValue{
						"oid": catInt(int64(toid)), "typname": catText(typeNameOf(col.Type)), "typnamespace": catInt(oidPGCatalogNamespace),
						"typowner": catInt(10), "typlen": catInt(int64(tlen)), "typbyval": catBool(byval),
						"typtype": catText("b"), "typcategory": catText(categoryOf(col.Type)), "typispreferred": catBool(false),
						"typisdefined": catBool(true), "typdelim": catText(","), "typrelid": catInt(0),
						"typelem": catInt(0), "typarray": catInt(0), "typinput": catText("-"),
						"typoutput": catText("-"), "typnotnull": catBool(false), "typbasetype": catInt(0),
						"typtypmod": catInt(-1), "typndims": catInt(0), "typcollation": catInt(0),
					}))
				}
			}
			return out, nil
		},
	}
}

// typeNameOf names the PostgreSQL type a Tellstone column type maps to.
func typeNameOf(t ColumnType) string {
	_, name, _, _ := pgTypeOf(t)
	return name
}

// categoryOf names the type category a Tellstone column type maps to.
func categoryOf(t ColumnType) string {
	switch t {
	case TypeInt, TypeBigInt, TypeFloat:
		return "N"
	case TypeBool:
		return "B"
	case TypeTimestamp:
		return "D"
	case TypeJSONB, TypeBytes:
		return "U"
	default:
		return "S"
	}
}

// relPGAttrdef describes the column defaults of the engine's tables.
func relPGAttrdef() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("adrelid", TypeInt),
		cc("adnum", TypeInt),
		ccn("adbin", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_attrdef", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				relid := objectOID(t.Table)
				for i, col := range t.Columns {
					if col.Default == nil {
						continue
					}
					out = append(out, catRow(cols, map[string]rowValue{
						"oid":     catInt(int64(objectOID("attrdef:" + t.Table + ":" + col.Name))),
						"adrelid": catInt(int64(relid)), "adnum": catInt(int64(i + 1)),
						"adbin": catText(defaultText(col)),
					}))
				}
			}
			return out, nil
		},
	}
}

// defaultText renders a column's default as the text a client reads.
func defaultText(col Column) string {
	if col.Default == nil {
		return ""
	}
	v, err := wireCell(col.Type, col.Default)
	if err != nil {
		return ""
	}
	return string(v)
}

// relPGIndex describes the indexes of the engine's tables. Every table has
// exactly one index: its primary key.
func relPGIndex() *catalogRelation {
	cols := []catalogColumn{
		cc("indexrelid", TypeInt),
		cc("indrelid", TypeInt),
		cc("indnatts", TypeInt),
		cc("indnkeyatts", TypeInt),
		cc("indisunique", TypeBool),
		cc("indisprimary", TypeBool),
		cc("indisexclusion", TypeBool),
		cc("indimmediate", TypeBool),
		cc("indisclustered", TypeBool),
		cc("indisvalid", TypeBool),
		cc("indcheckxmin", TypeBool),
		cc("indisready", TypeBool),
		cc("indislive", TypeBool),
		cc("indisreplident", TypeBool),
		cc("indkey", TypeVarchar),
		cc("indcollation", TypeVarchar),
		cc("indclass", TypeVarchar),
		cc("indoption", TypeVarchar),
		ccn("indexprs", TypeVarchar),
		ccn("indpred", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_index", columns: cols, key: []string{"indexrelid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				out = append(out, catRow(cols, map[string]rowValue{
					"indexrelid": catInt(int64(objectOID("index:" + t.Table))),
					"indrelid":   catInt(int64(objectOID(t.Table))),
					"indnatts":   catInt(1), "indnkeyatts": catInt(1), "indisunique": catBool(true),
					"indisprimary": catBool(true), "indisexclusion": catBool(false), "indimmediate": catBool(true),
					"indisclustered": catBool(false), "indisvalid": catBool(true), "indcheckxmin": catBool(false),
					"indisready": catBool(true), "indislive": catBool(true), "indisreplident": catBool(false),
					"indkey": catText("1"), "indcollation": catText("0"), "indclass": catText("0"),
					"indoption": catText("0"),
				}))
			}
			return out, nil
		},
	}
}

// relPGConstraint describes the constraints of the engine's tables. Every table
// has exactly one constraint: its primary key.
func relPGConstraint() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("conname", TypeVarchar),
		cc("connamespace", TypeInt),
		cc("contype", TypeVarchar),
		cc("condeferrable", TypeBool),
		cc("condeferred", TypeBool),
		cc("convalidated", TypeBool),
		cc("conrelid", TypeInt),
		cc("contypid", TypeInt),
		cc("conindid", TypeInt),
		cc("conparentid", TypeInt),
		cc("confrelid", TypeInt),
		cc("confupdtype", TypeVarchar),
		cc("confdeltype", TypeVarchar),
		cc("confmatchtype", TypeVarchar),
		cc("conislocal", TypeBool),
		cc("coninhcount", TypeInt),
		cc("connoinherit", TypeBool),
		cc("conkey", TypeVarchar),
		ccn("confkey", TypeVarchar),
		ccn("conpfeqop", TypeVarchar),
		ccn("conppeqop", TypeVarchar),
		ccn("conffeqop", TypeVarchar),
		ccn("conexclop", TypeVarchar),
		ccn("conbin", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_constraint", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				out = append(out, catRow(cols, map[string]rowValue{
					"oid":     catInt(int64(objectOID("constraint:" + t.Table))),
					"conname": catText(t.Table + "_pkey"), "connamespace": catInt(oidPublicNamespace),
					"contype": catText("p"), "condeferrable": catBool(false), "condeferred": catBool(false),
					"convalidated": catBool(true), "conrelid": catInt(int64(objectOID(t.Table))),
					"contypid": catInt(0), "conindid": catInt(int64(objectOID("index:" + t.Table))),
					"conparentid": catInt(0), "confrelid": catInt(0), "confupdtype": catText("a"),
					"confdeltype": catText("a"), "confmatchtype": catText("s"), "conislocal": catBool(true),
					"coninhcount": catInt(0), "connoinherit": catBool(false), "conkey": catText("1"),
				}))
			}
			return out, nil
		},
	}
}

// relPGDatabase describes the database a client is connected to.
func relPGDatabase() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("datname", TypeVarchar),
		cc("datdba", TypeInt),
		cc("encoding", TypeInt),
		cc("datlocprovider", TypeVarchar),
		cc("datistemplate", TypeBool),
		cc("datallowconn", TypeBool),
		cc("datconnlimit", TypeInt),
		cc("datfrozenxid", TypeInt),
		cc("datminmxid", TypeInt),
		cc("dattablespace", TypeInt),
		cc("datcollate", TypeVarchar),
		cc("datctype", TypeVarchar),
		ccn("daticulocale", TypeVarchar),
		ccn("datcollversion", TypeVarchar),
		ccn("datacl", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_database", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			return [][]rowValue{catRow(cols, map[string]rowValue{
				"oid": catInt(oidPublicDatabase), "datname": catText(c.db), "datdba": catInt(10),
				"encoding": catInt(6), "datlocprovider": catText("c"), "datistemplate": catBool(false),
				"datallowconn": catBool(true), "datconnlimit": catInt(-1), "datfrozenxid": catInt(0),
				"datminmxid": catInt(0), "dattablespace": catInt(1663), "datcollate": catText("C"),
				"datctype": catText("C"),
			})}, nil
		},
	}
}

// relPGDescription describes the comments on objects. Tellstone has no object
// comments, so the relation is empty.
func relPGDescription() *catalogRelation {
	cols := []catalogColumn{
		cc("objoid", TypeInt),
		cc("classoid", TypeInt),
		cc("objsubid", TypeInt),
		cc("description", TypeVarchar),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_description", columns: cols, key: []string{"objoid", "classoid", "objsubid"}}
}

// relPGRoles describes the roles a client can see. The session's user is the
// only role Tellstone knows.
func relPGRoles() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("rolname", TypeVarchar),
		cc("rolsuper", TypeBool),
		cc("rolinherit", TypeBool),
		cc("rolcreaterole", TypeBool),
		cc("rolcreatedb", TypeBool),
		cc("rolcanlogin", TypeBool),
		cc("rolreplication", TypeBool),
		cc("rolbypassrls", TypeBool),
		cc("rolconnlimit", TypeInt),
		ccn("rolpassword", TypeVarchar),
		ccn("rolvaliduntil", TypeVarchar),
		ccn("rolconfig", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_roles", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			return [][]rowValue{catRow(cols, map[string]rowValue{
				"oid": catInt(10), "rolname": catText(c.user), "rolsuper": catBool(true),
				"rolinherit": catBool(true), "rolcreaterole": catBool(true), "rolcreatedb": catBool(true),
				"rolcanlogin": catBool(true), "rolreplication": catBool(true), "rolbypassrls": catBool(true),
				"rolconnlimit": catInt(-1),
			})}, nil
		},
	}
}

// relPGAm describes the access methods. Only the heap method exists.
func relPGAm() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("amname", TypeVarchar),
		cc("amhandler", TypeVarchar),
		cc("amtype", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_am", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			return [][]rowValue{catRow(cols, map[string]rowValue{
				"oid": catInt(2), "amname": catText("heap"), "amhandler": catText("-"), "amtype": catText("t"),
			})}, nil
		},
	}
}

// relPGTablespace describes the tablespaces.
func relPGTablespace() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("spcname", TypeVarchar),
		cc("spcowner", TypeInt),
		ccn("spcacl", TypeVarchar),
		ccn("spcoptions", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_tablespace", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			return [][]rowValue{
				catRow(cols, map[string]rowValue{"oid": catInt(1663), "spcname": catText("pg_default"), "spcowner": catInt(10)}),
				catRow(cols, map[string]rowValue{"oid": catInt(1664), "spcname": catText("pg_global"), "spcowner": catInt(10)}),
			}, nil
		},
	}
}

// relPGSettings describes the run-time parameters a client asks about.
func relPGSettings() *catalogRelation {
	cols := []catalogColumn{
		cc("name", TypeVarchar),
		cc("setting", TypeVarchar),
		ccn("unit", TypeVarchar),
		cc("category", TypeVarchar),
		cc("short_desc", TypeVarchar),
		ccn("extra_desc", TypeVarchar),
		cc("context", TypeVarchar),
		cc("vartype", TypeVarchar),
		cc("source", TypeVarchar),
		ccn("min_val", TypeVarchar),
		ccn("max_val", TypeVarchar),
		ccn("enumvals", TypeVarchar),
		cc("boot_val", TypeVarchar),
		cc("reset_val", TypeVarchar),
		ccn("sourcefile", TypeVarchar),
		ccn("sourceline", TypeInt),
		cc("pending_restart", TypeBool),
	}
	settings := []struct{ name, value, desc string }{
		{"server_version", "18.0 (Tellstone)", "Shows the server version."},
		{"server_version_num", "180000", "Shows the server version as an integer."},
		{"server_encoding", "UTF8", "Shows the server's character encoding."},
		{"client_encoding", "UTF8", "Shows the client's character encoding."},
		{"DateStyle", "ISO, MDY", "Sets the date and time output format."},
		{"TimeZone", "UTC", "Sets the time zone for displaying and interpreting time stamps."},
		{"IntervalStyle", "postgres", "Sets the display format for interval values."},
		{"integer_datetimes", "on", "Shows whether date/time values are integer based."},
		{"standard_conforming_strings", "on", "Causes '...' strings to treat backslashes literally."},
		{"extra_float_digits", "1", "Sets the number of digits displayed for floating-point values."},
		{"is_superuser", "on", "Shows whether the current user is a superuser."},
		{"session_authorization", "postgres", "Shows the session's authorization identifier."},
		{"search_path", "\"$user\", public", "Sets the schema search order for names that are not schema-qualified."},
		{"max_index_keys", "32", "Shows the maximum number of index columns."},
		{"max_identifier_length", "63", "Shows the maximum identifier length."},
		{"block_size", "8192", "Shows the size of disk blocks in bytes."},
		{"segment_size", "131072", "Shows the size of a data file segment in bytes."},
		{"wal_block_size", "8192", "Shows the size of write-ahead log blocks in bytes."},
		{"wal_segment_size", "16777216", "Shows the size of write-ahead log segments in bytes."},
		{"default_transaction_isolation", "read committed", "Sets the isolation level of the current transaction."},
		{"transaction_isolation", "read committed", "Reports the isolation level of the current transaction."},
		{"transaction_read_only", "off", "Shows the transaction's read-only status."},
		{"default_transaction_read_only", "off", "Sets the default read-only status of new transactions."},
		{"statement_timeout", "0", "Sets the maximum allowed duration of any statement."},
		{"lock_timeout", "0", "Sets the maximum allowed duration of any wait for a lock."},
		{"idle_in_transaction_session_timeout", "0", "Sets the maximum allowed idle time in a transaction."},
		{"application_name", "", "Sets the application name to be reported in statistics and logs."},
		{"bytea_output", "hex", "Sets the output format for bytea."},
		{"lc_messages", "C", "Sets the message locale."},
		{"lc_monetary", "C", "Sets the monetary formatting locale."},
		{"lc_numeric", "C", "Sets the numeric formatting locale."},
		{"lc_time", "C", "Sets the time formatting locale."},
		{"max_connections", "100", "Sets the maximum number of concurrent connections."},
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_settings", columns: cols, key: []string{"name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			var out [][]rowValue
			for _, s := range settings {
				m := map[string]rowValue{
					"name": catText(s.name), "setting": catText(s.value), "category": catText("Preset Options"),
					"short_desc": catText(s.desc), "context": catText("user"), "vartype": catText("string"),
					"source": catText("default"), "boot_val": catText(s.value), "reset_val": catText(s.value),
					"pending_restart": catBool(false),
				}
				if s.name == "session_authorization" {
					m["setting"] = catText(c.user)
					m["boot_val"] = catText(c.user)
					m["reset_val"] = catText(c.user)
				}
				if s.name == "is_superuser" {
					m["setting"] = catBool(true)
					m["boot_val"] = catBool(true)
					m["reset_val"] = catBool(true)
				}
				out = append(out, catRow(cols, m))
			}
			return out, nil
		},
	}
}

// relPGProc describes the stored procedures. Tellstone has none.
func relPGProc() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("proname", TypeVarchar),
		cc("pronamespace", TypeInt),
		cc("proowner", TypeInt),
		cc("prolang", TypeInt),
		cc("procost", TypeFloat),
		cc("prorows", TypeFloat),
		cc("provariadic", TypeInt),
		cc("prokind", TypeVarchar),
		cc("prosecdef", TypeBool),
		cc("proleakproof", TypeBool),
		cc("proisstrict", TypeBool),
		cc("proretset", TypeBool),
		cc("provolatile", TypeVarchar),
		cc("proparallel", TypeVarchar),
		cc("pronargs", TypeInt),
		cc("pronargdefaults", TypeInt),
		cc("prorettype", TypeInt),
		cc("proargtypes", TypeVarchar),
		ccn("prosrc", TypeVarchar),
		ccn("probin", TypeVarchar),
		ccn("proconfig", TypeVarchar),
		ccn("proacl", TypeVarchar),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_proc", columns: cols, key: []string{"oid"}}
}

// relPGInherits describes the inheritance links. Tellstone's tables inherit
// nothing.
func relPGInherits() *catalogRelation {
	cols := []catalogColumn{
		cc("inhrelid", TypeInt),
		cc("inhparent", TypeInt),
		cc("inhseqno", TypeInt),
		cc("inhdetachpending", TypeBool),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_inherits", columns: cols, key: []string{"inhrelid", "inhseqno"}}
}

// relPGCollation describes the collations. The default collation is the only
// one Tellstone has.
func relPGCollation() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("collname", TypeVarchar),
		cc("collnamespace", TypeInt),
		cc("collowner", TypeInt),
		cc("collencoding", TypeInt),
		cc("collcollate", TypeVarchar),
		cc("collctype", TypeVarchar),
		ccn("collversion", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_collation", columns: cols, key: []string{"oid"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			return [][]rowValue{catRow(cols, map[string]rowValue{
				"oid": catInt(100), "collname": catText("default"), "collnamespace": catInt(oidPGCatalogNamespace),
				"collowner": catInt(10), "collencoding": catInt(-1), "collcollate": catText("C"),
				"collctype": catText("C"),
			})}, nil
		},
	}
}

// relPGEnum describes the enum types. Tellstone has none.
func relPGEnum() *catalogRelation {
	cols := []catalogColumn{
		cc("oid", TypeInt),
		cc("enumtypid", TypeInt),
		cc("enumsortorder", TypeFloat),
		cc("enumlabel", TypeVarchar),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_enum", columns: cols, key: []string{"oid", "enumsortorder"}}
}

// relPGRange describes the range types. Tellstone has none.
func relPGRange() *catalogRelation {
	cols := []catalogColumn{
		cc("rngtypid", TypeInt),
		cc("rngsubtype", TypeInt),
		cc("rngmultitypid", TypeInt),
		cc("rngcollation", TypeInt),
		cc("rngsubopc", TypeInt),
		cc("rngcanonical", TypeInt),
		cc("rngsubdiff", TypeInt),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_range", columns: cols, key: []string{"rngtypid"}}
}

// relPGTables describes the tables in the view pg_tables offers.
func relPGTables() *catalogRelation {
	cols := []catalogColumn{
		cc("schemaname", TypeVarchar),
		cc("tablename", TypeVarchar),
		cc("tableowner", TypeVarchar),
		ccn("tablespace", TypeVarchar),
		cc("hasindexes", TypeBool),
		cc("hasrules", TypeBool),
		cc("hastriggers", TypeBool),
		cc("rowsecurity", TypeBool),
	}
	return &catalogRelation{
		schema: schemaPGCatalog, name: "pg_tables", columns: cols, key: []string{"schemaname", "tablename"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				out = append(out, catRow(cols, map[string]rowValue{
					"schemaname": catText(schemaPublic), "tablename": catText(t.Table),
					"tableowner": catText(c.user), "hasindexes": catBool(true), "hasrules": catBool(false),
					"hastriggers": catBool(false), "rowsecurity": catBool(false),
				}))
			}
			return out, nil
		},
	}
}

// relPGViews describes the views in the view pg_views offers. Tellstone has none.
func relPGViews() *catalogRelation {
	cols := []catalogColumn{
		cc("schemaname", TypeVarchar),
		cc("viewname", TypeVarchar),
		cc("viewowner", TypeVarchar),
		cc("definition", TypeVarchar),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_views", columns: cols, key: []string{"schemaname", "viewname"}}
}

// relPGMatviews describes the materialized views. Tellstone has none.
func relPGMatviews() *catalogRelation {
	cols := []catalogColumn{
		cc("schemaname", TypeVarchar),
		cc("matviewname", TypeVarchar),
		cc("matviewowner", TypeVarchar),
		ccn("tablespace", TypeVarchar),
		cc("hasindexes", TypeBool),
		cc("ispopulated", TypeBool),
		cc("definition", TypeVarchar),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_matviews", columns: cols, key: []string{"schemaname", "matviewname"}}
}

// relPGSequences describes the sequences. Tellstone has none.
func relPGSequences() *catalogRelation {
	cols := []catalogColumn{
		cc("schemaname", TypeVarchar),
		cc("sequencename", TypeVarchar),
		cc("sequenceowner", TypeVarchar),
		cc("data_type", TypeVarchar),
		cc("start_value", TypeBigInt),
		cc("min_value", TypeBigInt),
		cc("max_value", TypeBigInt),
		cc("increment_by", TypeBigInt),
		cc("cycle", TypeBool),
		cc("cache_size", TypeBigInt),
		ccn("last_value", TypeBigInt),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_sequences", columns: cols, key: []string{"schemaname", "sequencename"}}
}

// relPGStatUserTables describes the per-table statistics. They are not
// collected, so the relation is empty.
func relPGStatUserTables() *catalogRelation {
	cols := []catalogColumn{
		cc("relid", TypeInt),
		cc("schemaname", TypeVarchar),
		cc("relname", TypeVarchar),
		cc("seq_scan", TypeBigInt),
		cc("seq_tup_read", TypeBigInt),
		cc("idx_scan", TypeBigInt),
		cc("idx_tup_fetch", TypeBigInt),
		cc("n_tup_ins", TypeBigInt),
		cc("n_tup_upd", TypeBigInt),
		cc("n_tup_del", TypeBigInt),
		cc("n_tup_hot_upd", TypeBigInt),
		cc("n_live_tup", TypeBigInt),
		cc("n_dead_tup", TypeBigInt),
		ccn("last_vacuum", TypeVarchar),
		ccn("last_autovacuum", TypeVarchar),
		ccn("last_analyze", TypeVarchar),
		ccn("last_autoanalyze", TypeVarchar),
	}
	return &catalogRelation{schema: schemaPGCatalog, name: "pg_stat_user_tables", columns: cols, key: []string{"relid"}}
}

// relInfoSchemata describes the schemas in the SQL-standard view.
func relInfoSchemata() *catalogRelation {
	cols := []catalogColumn{
		cc("catalog_name", TypeVarchar),
		cc("schema_name", TypeVarchar),
		cc("schema_owner", TypeVarchar),
		ccn("default_character_set_catalog", TypeVarchar),
		ccn("default_character_set_schema", TypeVarchar),
		ccn("default_character_set_name", TypeVarchar),
		ccn("sql_path", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaInfo, name: "schemata", columns: cols, key: []string{"schema_name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			var out [][]rowValue
			for _, s := range []string{schemaPublic, schemaPGCatalog, schemaInfo} {
				out = append(out, catRow(cols, map[string]rowValue{
					"catalog_name": catText(c.db), "schema_name": catText(s), "schema_owner": catText(c.user),
				}))
			}
			return out, nil
		},
	}
}

// relInfoTables describes the tables in the SQL-standard view.
func relInfoTables() *catalogRelation {
	cols := []catalogColumn{
		cc("table_catalog", TypeVarchar),
		cc("table_schema", TypeVarchar),
		cc("table_name", TypeVarchar),
		cc("table_type", TypeVarchar),
		ccn("self_referencing_column_name", TypeVarchar),
		ccn("reference_generation", TypeVarchar),
		ccn("user_defined_type_catalog", TypeVarchar),
		ccn("user_defined_type_schema", TypeVarchar),
		ccn("user_defined_type_name", TypeVarchar),
		cc("is_insertable_into", TypeVarchar),
		cc("is_typed", TypeVarchar),
		ccn("commit_action", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaInfo, name: "tables", columns: cols, key: []string{"table_schema", "table_name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			var out [][]rowValue
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			for _, t := range tables {
				out = append(out, catRow(cols, map[string]rowValue{
					"table_catalog": catText(c.db), "table_schema": catText(schemaPublic),
					"table_name": catText(t.Table), "table_type": catText("BASE TABLE"),
					"is_insertable_into": catText("YES"), "is_typed": catText("NO"),
				}))
			}
			for _, r := range catalogRelations {
				ns := schemaPGCatalog
				if r.schema == schemaInfo {
					ns = schemaInfo
				}
				out = append(out, catRow(cols, map[string]rowValue{
					"table_catalog": catText(c.db), "table_schema": catText(ns),
					"table_name": catText(r.name), "table_type": catText("SYSTEM TABLE"),
					"is_insertable_into": catText("NO"), "is_typed": catText("NO"),
				}))
			}
			return out, nil
		},
	}
}

// relInfoColumns describes the columns in the SQL-standard view.
func relInfoColumns() *catalogRelation {
	cols := []catalogColumn{
		cc("table_catalog", TypeVarchar),
		cc("table_schema", TypeVarchar),
		cc("table_name", TypeVarchar),
		cc("column_name", TypeVarchar),
		cc("ordinal_position", TypeInt),
		ccn("column_default", TypeVarchar),
		cc("is_nullable", TypeVarchar),
		cc("data_type", TypeVarchar),
		ccn("character_maximum_length", TypeInt),
		ccn("character_octet_length", TypeInt),
		ccn("numeric_precision", TypeInt),
		ccn("numeric_precision_radix", TypeInt),
		ccn("numeric_scale", TypeInt),
		ccn("datetime_precision", TypeInt),
		ccn("interval_type", TypeVarchar),
		ccn("interval_precision", TypeInt),
		ccn("character_set_catalog", TypeVarchar),
		ccn("character_set_schema", TypeVarchar),
		ccn("character_set_name", TypeVarchar),
		ccn("collation_catalog", TypeVarchar),
		ccn("collation_schema", TypeVarchar),
		ccn("collation_name", TypeVarchar),
		ccn("domain_catalog", TypeVarchar),
		ccn("domain_schema", TypeVarchar),
		ccn("domain_name", TypeVarchar),
		cc("udt_catalog", TypeVarchar),
		cc("udt_schema", TypeVarchar),
		cc("udt_name", TypeVarchar),
		ccn("scope_catalog", TypeVarchar),
		ccn("scope_schema", TypeVarchar),
		ccn("scope_name", TypeVarchar),
		ccn("maximum_cardinality", TypeInt),
		cc("dtd_identifier", TypeVarchar),
		cc("is_self_referencing", TypeBool),
		cc("is_identity", TypeVarchar),
		ccn("identity_generation", TypeVarchar),
		ccn("identity_start", TypeVarchar),
		ccn("identity_increment", TypeVarchar),
		ccn("identity_maximum", TypeVarchar),
		ccn("identity_minimum", TypeVarchar),
		cc("identity_cycle", TypeVarchar),
		cc("is_generated", TypeVarchar),
		ccn("generation_expression", TypeVarchar),
		cc("is_updatable", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaInfo, name: "columns", columns: cols, key: []string{"table_schema", "table_name", "column_name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				for i, col := range t.Columns {
					m := map[string]rowValue{
						"table_catalog": catText(c.db), "table_schema": catText(schemaPublic),
						"table_name": catText(t.Table), "column_name": catText(col.Name),
						"ordinal_position": catInt(int64(i + 1)), "is_nullable": catText(nullableWord(col.Nullable)),
						"data_type": catText(sqlTypeOf(col.Type)), "udt_catalog": catText(schemaPGCatalog),
						"udt_schema": catText(schemaPGCatalog), "udt_name": catText(typeNameOf(col.Type)),
						"dtd_identifier": catText(itoa(i + 1)), "is_self_referencing": catBool(false),
						"is_identity": catText("NO"), "identity_cycle": catText("NO"),
						"is_generated": catText("NEVER"), "is_updatable": catText("YES"),
					}
					if col.Default != nil {
						m["column_default"] = catText(defaultText(col))
					}
					if col.Nullable {
						m["is_nullable"] = catText("YES")
					}
					out = append(out, catRow(cols, m))
				}
			}
			return out, nil
		},
	}
}

// nullableWord renders a nullability flag the way information_schema does.
func nullableWord(nullable bool) string {
	if nullable {
		return "YES"
	}
	return "NO"
}

// itoa renders an integer without pulling in the strconv dependency at the
// call site.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// relInfoViews describes the views in the SQL-standard view. Tellstone has none.
func relInfoViews() *catalogRelation {
	cols := []catalogColumn{
		cc("table_catalog", TypeVarchar),
		cc("table_schema", TypeVarchar),
		cc("table_name", TypeVarchar),
		cc("view_definition", TypeVarchar),
		cc("check_option", TypeVarchar),
		cc("is_updatable", TypeVarchar),
		cc("is_insertable_into", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "views", columns: cols, key: []string{"table_schema", "table_name"}}
}

// relInfoTableConstraints describes the table constraints in the SQL-standard
// view.
func relInfoTableConstraints() *catalogRelation {
	cols := []catalogColumn{
		cc("constraint_catalog", TypeVarchar),
		cc("constraint_schema", TypeVarchar),
		cc("constraint_name", TypeVarchar),
		cc("table_catalog", TypeVarchar),
		cc("table_schema", TypeVarchar),
		cc("table_name", TypeVarchar),
		cc("constraint_type", TypeVarchar),
		cc("is_deferrable", TypeVarchar),
		cc("initially_deferred", TypeVarchar),
		cc("enforced", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaInfo, name: "table_constraints", columns: cols, key: []string{"constraint_schema", "constraint_name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				out = append(out, catRow(cols, map[string]rowValue{
					"constraint_catalog": catText(c.db), "constraint_schema": catText(schemaPublic),
					"constraint_name": catText(t.Table + "_pkey"), "table_catalog": catText(c.db),
					"table_schema": catText(schemaPublic), "table_name": catText(t.Table),
					"constraint_type": catText("PRIMARY KEY"), "is_deferrable": catText("NO"),
					"initially_deferred": catText("NO"), "enforced": catText("YES"),
				}))
			}
			return out, nil
		},
	}
}

// relInfoKeyColumnUsage describes the key columns in the SQL-standard view.
func relInfoKeyColumnUsage() *catalogRelation {
	cols := []catalogColumn{
		cc("constraint_catalog", TypeVarchar),
		cc("constraint_schema", TypeVarchar),
		cc("constraint_name", TypeVarchar),
		cc("table_catalog", TypeVarchar),
		cc("table_schema", TypeVarchar),
		cc("table_name", TypeVarchar),
		cc("column_name", TypeVarchar),
		cc("ordinal_position", TypeInt),
		ccn("position_in_unique_constraint", TypeInt),
	}
	return &catalogRelation{
		schema: schemaInfo, name: "key_column_usage", columns: cols, key: []string{"constraint_schema", "constraint_name", "column_name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			tables, err := c.userTables()
			if err != nil {
				return nil, err
			}
			var out [][]rowValue
			for _, t := range tables {
				if len(t.Columns) == 0 {
					continue
				}
				out = append(out, catRow(cols, map[string]rowValue{
					"constraint_catalog": catText(c.db), "constraint_schema": catText(schemaPublic),
					"constraint_name": catText(t.Table + "_pkey"), "table_catalog": catText(c.db),
					"table_schema": catText(schemaPublic), "table_name": catText(t.Table),
					"column_name": catText(t.Columns[0].Name), "ordinal_position": catInt(1),
				}))
			}
			return out, nil
		},
	}
}

// relInfoReferentialConstraints describes the foreign keys in the SQL-standard
// view. Tellstone's tables have none.
func relInfoReferentialConstraints() *catalogRelation {
	cols := []catalogColumn{
		cc("constraint_catalog", TypeVarchar),
		cc("constraint_schema", TypeVarchar),
		cc("constraint_name", TypeVarchar),
		cc("unique_constraint_catalog", TypeVarchar),
		cc("unique_constraint_schema", TypeVarchar),
		cc("unique_constraint_name", TypeVarchar),
		cc("match_option", TypeVarchar),
		cc("update_rule", TypeVarchar),
		cc("delete_rule", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "referential_constraints", columns: cols, key: []string{"constraint_schema", "constraint_name"}}
}

// relInfoCheckConstraints describes the check constraints in the SQL-standard
// view. Tellstone's tables have none.
func relInfoCheckConstraints() *catalogRelation {
	cols := []catalogColumn{
		cc("constraint_catalog", TypeVarchar),
		cc("constraint_schema", TypeVarchar),
		cc("constraint_name", TypeVarchar),
		cc("check_clause", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "check_constraints", columns: cols, key: []string{"constraint_schema", "constraint_name"}}
}

// relInfoRoutines describes the routines in the SQL-standard view. Tellstone
// has none.
func relInfoRoutines() *catalogRelation {
	cols := []catalogColumn{
		cc("specific_catalog", TypeVarchar),
		cc("specific_schema", TypeVarchar),
		cc("specific_name", TypeVarchar),
		cc("routine_catalog", TypeVarchar),
		cc("routine_schema", TypeVarchar),
		cc("routine_name", TypeVarchar),
		cc("routine_type", TypeVarchar),
		cc("data_type", TypeVarchar),
		cc("type_udt_name", TypeVarchar),
		cc("routine_body", TypeVarchar),
		ccn("routine_definition", TypeVarchar),
		cc("external_language", TypeVarchar),
		ccn("external_name", TypeVarchar),
		cc("parameter_mode", TypeVarchar),
		cc("is_deterministic", TypeVarchar),
		cc("security_type", TypeVarchar),
		cc("sql_security", TypeVarchar),
		cc("is_nullable", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "routines", columns: cols, key: []string{"specific_schema", "specific_name"}}
}

// relInfoParameters describes the routine parameters in the SQL-standard view.
func relInfoParameters() *catalogRelation {
	cols := []catalogColumn{
		cc("specific_catalog", TypeVarchar),
		cc("specific_schema", TypeVarchar),
		cc("specific_name", TypeVarchar),
		cc("ordinal_position", TypeInt),
		cc("parameter_mode", TypeVarchar),
		cc("is_nullable", TypeVarchar),
		cc("data_type", TypeVarchar),
		cc("type_udt_name", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "parameters", columns: cols, key: []string{"specific_schema", "specific_name", "ordinal_position"}}
}

// relInfoSequences describes the sequences in the SQL-standard view.
func relInfoSequences() *catalogRelation {
	cols := []catalogColumn{
		cc("sequence_catalog", TypeVarchar),
		cc("sequence_schema", TypeVarchar),
		cc("sequence_name", TypeVarchar),
		cc("data_type", TypeVarchar),
		cc("start_value", TypeBigInt),
		cc("minimum_value", TypeBigInt),
		cc("maximum_value", TypeBigInt),
		cc("increment", TypeBigInt),
		cc("cycle_option", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "sequences", columns: cols, key: []string{"sequence_schema", "sequence_name"}}
}

// relInfoDomains describes the domains in the SQL-standard view.
func relInfoDomains() *catalogRelation {
	cols := []catalogColumn{
		cc("domain_catalog", TypeVarchar),
		cc("domain_schema", TypeVarchar),
		cc("domain_name", TypeVarchar),
		cc("data_type", TypeVarchar),
		cc("type_udt_name", TypeVarchar),
		cc("is_nullable", TypeVarchar),
		ccn("domain_default", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "domains", columns: cols, key: []string{"domain_schema", "domain_name"}}
}

// relInfoCharacterSets describes the character sets in the SQL-standard view.
func relInfoCharacterSets() *catalogRelation {
	cols := []catalogColumn{
		cc("character_set_catalog", TypeVarchar),
		cc("character_set_schema", TypeVarchar),
		cc("character_set_name", TypeVarchar),
		cc("character_repertoire", TypeVarchar),
		cc("form_of_use", TypeVarchar),
		cc("default_collate_name", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "character_sets", columns: cols, key: []string{"character_set_schema", "character_set_name"}}
}

// relInfoTablePrivileges describes the table privileges in the SQL-standard
// view. Tellstone's access control is its own, and the view is empty.
func relInfoTablePrivileges() *catalogRelation {
	cols := []catalogColumn{
		cc("grantor", TypeVarchar),
		cc("grantee", TypeVarchar),
		cc("table_catalog", TypeVarchar),
		cc("table_schema", TypeVarchar),
		cc("table_name", TypeVarchar),
		cc("privilege_type", TypeVarchar),
		cc("is_grantable", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "table_privileges", columns: cols, key: []string{"table_schema", "table_name", "privilege_type"}}
}

// relInfoColumnPrivileges describes the column privileges in the SQL-standard
// view.
func relInfoColumnPrivileges() *catalogRelation {
	cols := []catalogColumn{
		cc("grantor", TypeVarchar),
		cc("grantee", TypeVarchar),
		cc("table_catalog", TypeVarchar),
		cc("table_schema", TypeVarchar),
		cc("table_name", TypeVarchar),
		cc("column_name", TypeVarchar),
		cc("privilege_type", TypeVarchar),
		cc("is_grantable", TypeVarchar),
	}
	return &catalogRelation{schema: schemaInfo, name: "column_privileges", columns: cols, key: []string{"table_schema", "table_name", "column_name", "privilege_type"}}
}

// relInfoEnabledRoles describes the roles a session can use.
func relInfoEnabledRoles() *catalogRelation {
	cols := []catalogColumn{
		cc("role_oid", TypeInt),
		cc("role_name", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaInfo, name: "enabled_roles", columns: cols, key: []string{"role_name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			return [][]rowValue{catRow(cols, map[string]rowValue{
				"role_oid": catInt(10), "role_name": catText(c.user),
			})}, nil
		},
	}
}

// relInfoApplicableRoles describes the roles a session can assume.
func relInfoApplicableRoles() *catalogRelation {
	cols := []catalogColumn{
		cc("role_oid", TypeInt),
		cc("role_name", TypeVarchar),
	}
	return &catalogRelation{
		schema: schemaInfo, name: "applicable_roles", columns: cols, key: []string{"role_name"},
		rows: func(c *catalogContext) ([][]rowValue, error) {
			return [][]rowValue{catRow(cols, map[string]rowValue{
				"role_oid": catInt(10), "role_name": catText(c.user),
			})}, nil
		},
	}
}
