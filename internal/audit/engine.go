/*
Package audit
Tellstone Cloud-Native In-Memory Database
File: engine.go
Description: Concrete audit log engine. Writes structured JSON to an io.Writer,
event-type-gated via eventSet, independent of the operational log level. A server
running at --log-level fatal still emits a full audit trail when --enable-audit
is set.

Every JSON line carries "level": "AUDIT" so log aggregators can distinguish audit
entries from operational log lines (INFO/WARN/ERROR/FATAL) without any custom parsing.

Authors:

	Maximilian Hagen
*/
package audit

import (
	"encoding/json"
	"io"
	"time"

	"github.com/Saxy/Tellstone/internal/log"
)

// auditLevel is the fixed severity label emitted in every JSON audit line.
const auditLevel = "AUDIT"

// LogEngine is the concrete audit logger. When enabled is false (--enable-audit
// not set), Record() returns immediately with a single bool comparison — no
// writer, no encoder allocated. When enabled, Record() checks the event against
// the filter and writes one JSON line to the writer. History is retrieved from
// the log files directly — no in-memory buffer is maintained.
type LogEngine struct {
	enabled bool
	filter  *eventSet
	writer  io.Writer
	enc     *json.Encoder
}

// NewLogEngine creates an audit engine. When enabled is false, the engine is
// a lightweight no-op: no writer opened, no encoder created, Record() is a
// single bool check. When enabled is true, structured JSON is written to
// a writer. Pass nil for the writer to discard output (useful for tests).
func NewLogEngine(enabled bool, filter *eventSet, writer io.Writer) *LogEngine {
	if !enabled {
		return &LogEngine{enabled: false}
	}
	var enc *json.Encoder
	if writer != nil {
		enc = json.NewEncoder(writer)
	}
	return &LogEngine{
		enabled: true,
		filter:  filter,
		writer:  writer,
		enc:     enc,
	}
}

// Record writes one audit event. When the engine is not enabled, this is a
// single bool comparison — zero overhead. When enabled, the event is checked
// against the filter; filtered-out events return immediately. Passing events
// write one JSON line with "level": "AUDIT".
func (e *LogEngine) Record(event EventType, msg string, fields ...log.Field) {
	if !e.enabled || !e.filter.has(event) {
		return
	}
	entry := map[string]any{
		"time":  time.Now().Format(time.RFC3339Nano),
		"level": auditLevel,
		"event": event,
		"msg":   msg,
	}
	for _, f := range fields {
		switch f.Type {
		case log.TypeString:
			entry[f.Key] = f.StrVal
		case log.TypeInt:
			entry[f.Key] = f.IntVal
		case log.TypeBool:
			entry[f.Key] = f.BoolVal
		case log.TypeFloat:
			entry[f.Key] = f.FloatVal
		case log.TypeUint:
			entry[f.Key] = f.UintVal
		}
	}
	_ = e.enc.Encode(entry)
}

// Close closes the underlying writer if it implements io.Closer.
// Returns nil when the engine is not enabled or the writer does not
// implement io.Closer (e.g. os.Stdout).
func (e *LogEngine) Close() error {
	if !e.enabled {
		return nil
	}
	if closer, ok := e.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
