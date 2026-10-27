/*
Package sql
Tellstone PostgreSQL Wire Frontend
File: transcript_test.go
Description: The introspection transcript replay harness (ADR-016 Stage B).

Each file under testdata/transcripts is a JSONL list of the SQL statements a
real client sent during connect and introspection, captured with
scripts/tools/pgrecord. The harness replays them against a fresh server, in one
session and in order, so session state one statement sets is visible to the
next, exactly as on a real connection. Every statement must be accepted. A
statement that fails only because the table it names does not exist in the
fresh replay schema (SQLSTATE 42P01) is tolerated; any other failure -- an
unsupported construct or a syntax error -- fails the test and names the
statement, which is what makes the harness the driver of the SQL surface rather
than a guess at it.

Two transcripts are seeded. datagrip-introspection is a live capture: DataGrip
2026.1.2 was pointed at pgrecord in front of a local-cluster node and used
normally -- connect, empty-query validation round-trip, schema browser, data
preview -- and every Parse/Query payload was recorded, including the init
sequence and the statements that error (they are the requirements). pgjdbc-
startup is still a reconstruction from the driver's source (REL 42.7.3); no
live pgJDBC session has been recorded against it yet. A live capture supersedes
a reconstruction on contact, and re-recording on a client upgrade is the
intended way to grow the surface; see ADR-016 decision 4 and the transcripts
README.
*/
package sql

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// transcriptDir holds the recorded sessions.
const transcriptDir = "testdata/transcripts"

// loadTranscript reads one JSONL transcript into its list of SQL statements,
// one per line, in order.
func loadTranscript(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open transcript %s: %v", path, err)
	}
	defer f.Close()
	var stmts []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var q string
		if err := json.Unmarshal([]byte(line), &q); err != nil {
			t.Fatalf("%s: bad JSONL line %q: %v", path, line, err)
		}
		stmts = append(stmts, q)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read transcript %s: %v", path, err)
	}
	return stmts
}

// TestTranscriptsReplay replays every golden transcript. It is the assertion
// that the SQL surface a real client needs is accepted: every statement must
// run, and the only tolerated failure is the not-yet-implemented catalog's
// 42P01.
func TestTranscriptsReplay(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(transcriptDir, "*.jsonl"))
	if err != nil {
		t.Fatalf("glob transcripts: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no transcripts under %s", transcriptDir)
	}
	for _, path := range files {
		path := path
		name := strings.TrimSuffix(filepath.Base(path), ".jsonl")
		t.Run(name, func(t *testing.T) {
			stmts := loadTranscript(t, path)
			srv, _ := newTestServer(t, srvOpts{})
			cl := dialServer(t, srv.Addr())
			cl.startupTrust("default")
			for i, q := range stmts {
				replayStatement(t, cl, i, q)
			}
		})
	}
}

// replayStatement sends one transcript statement over the simple-query
// protocol and reports any failure that is not the tolerated catalog gap.
func replayStatement(t *testing.T, cl *tclient, idx int, q string) {
	t.Helper()
	frames := cl.query(q)
	code, msg, ok := findError(frames)
	if !ok {
		return
	}
	if code == errUndefinedTable {
		// The statement names a pg_catalog relation Stage C will add. Its SQL
		// shape translated and planned fine, which is what Stage B checks.
		return
	}
	t.Fatalf("statement #%d failed: %s %s\n  SQL: %s", idx, code, msg, q)
}
