/*
Package log
Tellstone Cloud-Native In-Memory Database
File: log.go
Description: Re-exports the public logging contract from the logger package so internal
code keeps using the log.Level / log.Logger / log.Field identifiers it already imports.
The contract lives in the non-internal logger package so public packages (client) can
build and accept loggers without importing an internal package.

Authors:

	Maximilian Hagen
*/
package log

import (
	"github.com/Saxy/Tellstone/logger"
)

type Level = logger.Level
type FieldType = logger.FieldType
type Field = logger.Field
type Logger = logger.Logger

const (
	LevelDebug = logger.LevelDebug
	LevelInfo  = logger.LevelInfo
	LevelWarn  = logger.LevelWarn
	LevelError = logger.LevelError
	LevelFatal = logger.LevelFatal
)

const (
	TypeString = logger.TypeString
	TypeInt    = logger.TypeInt
	TypeBool   = logger.TypeBool
	TypeFloat  = logger.TypeFloat
	TypeUint   = logger.TypeUint
)

// ParseLogLevel maps a string level name to a Level, defaulting to Info.
func ParseLogLevel(lvl string) Level { return logger.ParseLogLevel(lvl) }

func String(key, val string) Field  { return logger.String(key, val) }
func Int(key string, val int) Field { return logger.Int(key, val) }
func Uint(key string, val uint32) Field {
	return logger.Uint(key, val)
}
func Uint64(key string, val uint64) Field { return logger.Uint64(key, val) }
func Int64(key string, val int64) Field   { return logger.Int64(key, val) }
func Float(key string, val float64) Field { return logger.Float(key, val) }
func Bool(key string, val bool) Field     { return logger.Bool(key, val) }

// NewNoOpLogger returns a Logger that discards every message.
func NewNoOpLogger() Logger { return logger.NewNoOpLogger() }
