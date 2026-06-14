package log

import "context"

type Level uint8

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

type FieldType uint8

const (
	TypeString FieldType = iota
	TypeInt
	TypeBool
	TypeFloat
)

type Field struct {
	Key      string
	StrVal   string
	IntVal   int
	BoolVal  bool
	FloatVal float64
	Type     FieldType
}

func String(key, val string) Field      { return Field{Key: key, StrVal: val, Type: TypeString} }
func Int(key string, val int) Field     { return Field{Key: key, IntVal: int(int64(val)), Type: TypeInt} }
func Int64(key string, val int64) Field { return Field{Key: key, IntVal: int(val), Type: TypeInt} }
func Bool(key string, val bool) Field   { return Field{Key: key, BoolVal: val, Type: TypeBool} }

type Logger interface {
	Enabled(level Level) bool
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
	Fatal(msg string)
	WithFields(fields ...Field) Logger
	WithContext(ctx context.Context) Logger
	Log(level Level, msg string, fields ...Field)
}
