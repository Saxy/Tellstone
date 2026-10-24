# Golden client transcripts

Each `.jsonl` file is a newline-delimited list of SQL statements, exactly as a
PostgreSQL client sent them (Query or Parse payloads, JSON-encoded strings).
`TestTranscriptsReplay` (`internal/sql/transcript_test.go`) replays every
statement against an empty schema and fails on any error other than 42P01
(undefined table -- the replay schema has no user tables, so a captured
`select * from instances` is expected to miss there). The files therefore pin
the SQL surface ADR-016 decision 4 scopes the engine to: a statement here is a
requirement, a statement absent here is not (pending a re-record).

- `datagrip-introspection.jsonl`: a **live capture** of DataGrip 2026.1.2
  (stock PostgreSQL data source) connecting through `pgrecord`, running its
  init sequence, its empty-query validation round-trip, the schema browser and
  a data preview. Statements that error on Tellstone are kept: they are the
  requirements that drove `current_schemas(bool)` and the extended-protocol
  empty-query answer.
- `pgjdbc-startup.jsonl`: still a reconstruction from the pgJDBC source
  (REL 42.7.3) -- session GUCs, isolation level, version, type metadata. No
  live pgJDBC session has been recorded yet.

## Provenance and maintenance

A live capture supersedes a reconstruction on contact. To re-record, put
`pgrecord` between the client and a local-cluster node (`--trace` logs every
frame, which is how protocol bugs are diagnosed):

    go run ./scripts/tools/pgrecord \
        --listen 127.0.0.1:55433 --upstream 127.0.0.1:47101 --trace \
        --out internal/sql/testdata/transcripts/datagrip-introspection.jsonl

Then point the client at the `--listen` address and use it normally.
Re-recording on a client upgrade is the intended way to discover new surface;
see ADR-016 decision 4.
