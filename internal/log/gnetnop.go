package log

import "fmt"

type GnetAdapter struct {
	logger Logger
}

func NewGnetAdapter(l Logger) GnetAdapter { return GnetAdapter{logger: l} }

func (a GnetAdapter) Debugf(format string, args ...any) {
	if a.logger.Enabled(LevelDebug) {
		a.logger.Log(LevelDebug, fmt.Sprintf(format, args...))
	}
}

func (a GnetAdapter) Infof(format string, args ...any) {
	if a.logger.Enabled(LevelInfo) {
		a.logger.Log(LevelInfo, fmt.Sprintf(format, args...))
	}
}

func (a GnetAdapter) Warnf(format string, args ...any) {
	if a.logger.Enabled(LevelWarn) {
		a.logger.Log(LevelWarn, fmt.Sprintf(format, args...))
	}
}

func (a GnetAdapter) Errorf(format string, args ...any) {
	if a.logger.Enabled(LevelError) {
		a.logger.Log(LevelError, fmt.Sprintf(format, args...))
	}
}

func (a GnetAdapter) Fatalf(format string, args ...any) {
	if a.logger.Enabled(LevelFatal) {
		a.logger.Log(LevelFatal, fmt.Sprintf(format, args...))
	}
}
