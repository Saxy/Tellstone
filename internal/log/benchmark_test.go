package log

import "testing"

func BenchmarkNoOpLoggerEnabled(b *testing.B) {
    logger := NewNoOpLogger()
    for i := 0; i < b.N; i++ {
        _ = logger.Enabled(LevelInfo)
    }
}

func BenchmarkNoOpLoggerLog(b *testing.B) {
    logger := NewNoOpLogger()
    f1 := String("key", "value")
    f2 := Int("num", 42)
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        logger.Log(LevelInfo, "benchmark message", f1, f2)
    }
}
