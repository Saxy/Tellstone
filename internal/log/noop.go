/*
Package log
Tellstone Cloud-Native In-Memory Database
File: noop.go
Description: No‑op implementation of the Logger interface that discards all logs.

Authors:

	Maximilian Hagen
*/
package log

func NewNoOpLogger() Logger {
	return &noOpLogger{}
}

type noOpLogger struct{}

func (n *noOpLogger) Enabled(level Level) bool                     { return false }
func (n *noOpLogger) Log(level Level, msg string, fields ...Field) {}
