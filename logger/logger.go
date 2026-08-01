/*
Package logger
Tellstone Cloud-Native In-Memory Database
File: logger.go
Description: Adapter that bridges the logging contract to Go's structured slog logger (JSON output).

Authors:

	Maximilian Hagen
*/
package logger

import (
	"context"
	"log/slog"
	"os"
)

type SlogAdapter struct {
	slogLogger *slog.Logger
}

// NewSlogLogger builds a Logger backed by slog's JSON handler on stdout.
// Logging is disabled by default in every other component; callers opt in.
func NewSlogLogger(level Level) Logger {
	opts := &slog.HandlerOptions{
		Level: translateLevelToSlog(level),
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return &SlogAdapter{slogLogger: slog.New(handler)}
}

func (s *SlogAdapter) Enabled(level Level) bool {
	return s.slogLogger.Enabled(context.Background(), translateLevelToSlog(level))
}

func (s *SlogAdapter) Log(level Level, msg string, fields ...Field) {
	attrs := make([]any, len(fields))
	for i, f := range fields {
		switch f.Type {
		case TypeString:
			attrs[i] = slog.String(f.Key, f.StrVal)
		case TypeInt:
			attrs[i] = slog.Int(f.Key, f.IntVal)
		case TypeBool:
			attrs[i] = slog.Bool(f.Key, f.BoolVal)
		case TypeFloat:
			attrs[i] = slog.Float64(f.Key, f.FloatVal)
		case TypeUint:
			attrs[i] = slog.Uint64(f.Key, f.UintVal)
		}
	}
	s.slogLogger.Log(context.Background(), translateLevelToSlog(level), msg, attrs...)
}

func translateLevelToSlog(l Level) slog.Level {
	switch l {
	case LevelDebug:
		return slog.LevelDebug
	case LevelInfo:
		return slog.LevelInfo
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelError
	}
}
