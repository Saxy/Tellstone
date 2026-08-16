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
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Saxy/Tellstone/internal/crypto"
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
	closeFn func() error
	mu      sync.Mutex
	// firstErr is the first failed Encode, retained so Close can report it.
	// Once set, later records are dropped instead of overwriting the original
	// cause, which would hide the root failure from the operator.
	firstErr error
}

var envelopeFileName = "audit.env"

// NewLogEngine creates an audit engine. When enabled is false, the engine is
// a lightweight no-op: no writer opened, no encoder created, Record() is a
// single bool check. When enabled, the destination is chosen from the audit
// log path: "stdout" writes JSON to os.Stdout, any other value is treated as
// a directory whose audit files are created and rotated automatically. The
// path is only inspected when enabled, so a disabled engine pays nothing.
//
// In envelope mode (envelopeEnabled) the audit log uses a DEK of its own —
// never the operator's KEK, never a shard's DEK — wrapped by the KEK and
// stored as an envelope file beside the audit records, mirroring how each
// shard seals its data. A changed KEK is rejected (fail-closed) instead of
// silently generating a fresh DEK and bricking the audit history. A stdout
// destination is never persisted, so no envelope is created and records stay
// plaintext.
func NewLogEngine(
	enabled bool,
	filter *eventSet,
	auditLogPath string,
	logger log.Logger,
	envelopeEnabled bool,
	key []byte,
	engine *crypto.Engine) (*LogEngine, error) {
	if !enabled {
		return &LogEngine{enabled: false}, nil
	}
	if auditLogPath == "" || auditLogPath == "stdout" {
		return &LogEngine{
			enabled: true,
			filter:  filter,
			writer:  os.Stdout,
			enc:     json.NewEncoder(os.Stdout),
		}, nil
	}
	if envelopeEnabled {
		env, err := crypto.NewEnvelope(key, logger)
		if err != nil {
			return nil, fmt.Errorf("audit: envelope init: %w", err)
		}
		dek, err := env.Load(auditLogPath, envelopeFileName)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("audit: load envelope: %w", err)
			}
			if err = env.GenerateDEK(); err != nil {
				return nil, fmt.Errorf("audit: generate DEK: %w", err)
			}
			if err = env.Store(auditLogPath, envelopeFileName); err != nil {
				return nil, fmt.Errorf("audit: store envelope: %w", err)
			}
			dek = env.DEK()
		}
		engine, err = crypto.NewEngine(dek, logger)
		if err != nil {
			return nil, fmt.Errorf("audit: data engine init: %w", err)
		}
	}
	// Compute the file header metadata. The keyMode must match the record
	// format the reader will encounter:
	//   - KeyModeSimple  → plaintext JSON lines, no engine needed.
	//   - KeyModeEnvelope → length-prefixed sealed records, engine required.
	// The envelope flag only controls which key sealed the records (DEK vs
	// KEK), not whether they are sealed at all.
	var keyMode byte
	var fingerprint [16]byte
	if engine != nil && engine.Enabled() {
		keyMode = KeyModeEnvelope
		fingerprint = engine.KeyFingerprint()
	} else if len(key) > 0 {
		keyMode = KeyModeSimple
		fingerprint = crypto.FingerprintBytes(key)
	}
	f, err := newFile(auditLogPath, engine, logger, keyMode, fingerprint)
	if err != nil {
		return nil, err
	}
	return &LogEngine{
		enabled: true,
		filter:  filter,
		writer:  f,
		enc:     json.NewEncoder(f),
		closeFn: f.Close,
	}, nil
}

// Record writes one audit event. When the engine is not enabled, this is a
// single bool comparison — zero overhead. When enabled, the event is checked
// against the filter; filtered-out events return immediately. Passing events
// write one JSON line with "level": "AUDIT". A failing write is retained as
// the engine's first error and reported by Close; subsequent records are
// dropped rather than masking it.
func (e *LogEngine) Record(event EventType, msg string, fields ...log.Field) {
	if !e.enabled || !e.filter.has(event) {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	// A broken sink keeps failing; stop attempting writes once the first
	// failure is recorded so Close reports the original error, not the last one.
	if e.firstErr != nil {
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
	if err := e.enc.Encode(entry); err != nil && e.firstErr == nil {
		e.firstErr = err
	}
}

// Close closes the file backing a non-stdout audit log and reports the first
// write failure, if any. Returns nil when the engine is not enabled — the
// disabled engine never records, so it can have no write error. The first
// failed write outranks a close error: it happened first and is the root
// cause the operator needs. os.Stdout is never closed — it is owned by the
// process. The mutex serializes Close against any in-flight Record, so the
// underlying file is never closed mid-write.
func (e *LogEngine) Close() error {
	if !e.enabled {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var closeErr error
	if e.closeFn != nil {
		closeErr = e.closeFn()
	}
	if e.firstErr != nil {
		return e.firstErr
	}
	return closeErr
}
