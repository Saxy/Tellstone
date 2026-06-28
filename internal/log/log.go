/*
Package log
Tellstone Cloud-Native In-Memory Database
File: log.go
Description: Lightweight logging abstraction with log levels and a Logger interface.

Authors:

	Maximilian Hagen
*/
/*
Package log
Tellstone Cloud-Native In-Memory Database
File: log.go
Description: Lightweight logging abstraction defining log levels and a Logger interface.

Authors:

	Maximilian Hagen
*/
package log

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

type FieldType uint8

const (
	TypeString FieldType = iota
	TypeInt
	TypeBool
	TypeFloat
	TypeUint
)

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
func Int(key string, val int) Field { return Field{Key: key, IntVal: int(int64(val)), Type: TypeInt} }
func Uint(key string, val uint32) Field {
	return Field{Key: key, UintVal: uint64(val), Type: TypeUint}
}
func Uint64(key string, val uint64) Field { return Field{Key: key, UintVal: val, Type: TypeUint} }
func Int64(key string, val int64) Field   { return Field{Key: key, IntVal: int(val), Type: TypeInt} }
func Float(key string, val float64) Field { return Field{Key: key, FloatVal: val, Type: TypeFloat} }
func Bool(key string, val bool) Field     { return Field{Key: key, BoolVal: val, Type: TypeBool} }

type Logger interface {
	Enabled(level Level) bool
	Log(level Level, msg string, fields ...Field)
}
