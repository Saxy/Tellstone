package log

import "context"

func NewNoOpLogger() Logger {
	return &noOpLogger{}
}

type noOpLogger struct{}

func (n *noOpLogger) Enabled(level Level) bool                     { return false }
func (n *noOpLogger) Debug(msg string)                             {}
func (n *noOpLogger) Info(msg string)                              {}
func (n *noOpLogger) Warn(msg string)                              {}
func (n *noOpLogger) Error(msg string)                             {}
func (n *noOpLogger) Fatal(msg string)                             {}
func (n *noOpLogger) Log(level Level, msg string, fields ...Field) {}
func (n *noOpLogger) WithFields(fields ...Field) Logger            { return n }
func (n *noOpLogger) WithContext(ctx context.Context) Logger       { return n }
