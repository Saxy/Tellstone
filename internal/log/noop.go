package log

func NewNoOpLogger() Logger {
	return &noOpLogger{}
}

type noOpLogger struct{}

func (n *noOpLogger) Enabled(level Level) bool                     { return false }
func (n *noOpLogger) Log(level Level, msg string, fields ...Field) {}
