/*
Package logger
Tellstone Cloud-Native In-Memory Database
File: log.go
Description: Public logging contract — the Logger interface, levels, and structured fields.
The internal/log package re-exports these types so internal packages and server code keep
using their existing log.Level / log.Logger identifiers. Defining the contract here (a
non-internal package) lets public packages like client accept and build loggers without
leaking internal imports to external consumers.

Authors:

	Maximilian Hagen
*/
package logger

// Level identifies the severity of a log message.
type Level uint8

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

// ParseLogLevel maps a string level name to a Level, defaulting to Info.
func ParseLogLevel(lvl string) Level {
	switch lvl {
	case "debug", "DEBUG":
		return LevelDebug
	case "info", "INFO":
		return LevelInfo
	case "warn", "WARN", "warning":
		return LevelWarn
	case "error", "ERROR":
		return LevelError
	case "fatal", "FATAL":
		return LevelFatal
	default:
		return LevelInfo
	}
}

// FieldType discriminates the typed value carried by a Field.
type FieldType uint8

const (
	TypeString FieldType = iota
	TypeInt
	TypeBool
	TypeFloat
	TypeUint
)

// Field is a structured key/value pair attached to a log message. The typed
// accessors below build one without boxing, keeping the hot path allocation-free.
type Field struct {
	Key      string
	StrVal   string
	IntVal   int
	UintVal  uint64
	BoolVal  bool
	FloatVal float64
	Type     FieldType
}

func String(key, val string) Field  { return Field{Key: key, StrVal: val, Type: TypeString} }
func Int(key string, val int) Field { return Field{Key: key, IntVal: val, Type: TypeInt} }
func Uint(key string, val uint32) Field {
	return Field{Key: key, UintVal: uint64(val), Type: TypeUint}
}
func Uint64(key string, val uint64) Field { return Field{Key: key, UintVal: val, Type: TypeUint} }
func Int64(key string, val int64) Field   { return Field{Key: key, IntVal: int(val), Type: TypeInt} }
func Float(key string, val float64) Field { return Field{Key: key, FloatVal: val, Type: TypeFloat} }
func Bool(key string, val bool) Field     { return Field{Key: key, BoolVal: val, Type: TypeBool} }

// Logger is the logging surface used across the codebase. Implementations must
// be safe for concurrent use.
type Logger interface {
	Enabled(level Level) bool
	Log(level Level, msg string, fields ...Field)
}

// NewNoOpLogger returns a Logger that discards every message. It is the default
// for optional logging: zero allocation, zero output.
func NewNoOpLogger() Logger {
	return &noOpLogger{}
}

type noOpLogger struct{}

func (n *noOpLogger) Enabled(level Level) bool                     { return false }
func (n *noOpLogger) Log(level Level, msg string, fields ...Field) {}
