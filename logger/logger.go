package logger

import (
	"context"
	"log/slog"
	"os"

	"github.com/Saxy/Tellstone/internal/log"
)

type SlogAdapter struct {
	slogLogger *slog.Logger
}

func NewSlogLogger(level log.Level) log.Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: translateLevelToSlog(level),
	}
	handler = slog.NewJSONHandler(os.Stdout, opts)
	return &SlogAdapter{slogLogger: slog.New(handler)}
}

func (s *SlogAdapter) Enabled(level log.Level) bool {
	return s.slogLogger.Enabled(context.Background(), translateLevelToSlog(level))
}

func (s *SlogAdapter) Log(level log.Level, msg string, fields ...log.Field) {
	attrs := make([]any, len(fields))
	for i, f := range fields {
		switch f.Type {
		case log.TypeString:
			attrs[i] = slog.String(f.Key, f.StrVal)
		case log.TypeInt:
			attrs[i] = slog.Int(f.Key, f.IntVal)
		case log.TypeBool:
			attrs[i] = slog.Bool(f.Key, f.BoolVal)
		case log.TypeFloat:
			attrs[i] = slog.Float64(f.Key, f.FloatVal)
		case log.TypeUint32:
			attrs[i] = slog.Uint64(f.Key, uint64(f.UintVal))
		}
	}
	s.slogLogger.Log(context.Background(), translateLevelToSlog(level), msg, attrs...)
}

func translateLevelToSlog(l log.Level) slog.Level {
	switch l {
	case log.LevelDebug:
		return slog.LevelDebug
	case log.LevelInfo:
		return slog.LevelInfo
	case log.LevelWarn:
		return slog.LevelWarn
	case log.LevelError:
		return slog.LevelError
	default:
		return slog.LevelError
	}
}
