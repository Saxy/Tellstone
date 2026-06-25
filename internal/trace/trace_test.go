package trace

import (
    "context"
    "testing"
)

func TestNoOpTracer(t *testing.T) {
    var nt NoOpTracer
    span := nt.StartSpan(context.Background(), "test")
    if span == nil {
        t.Fatalf("StartSpan returned nil")
    }
    if span.IsRecording() {
        t.Fatalf("NoOpSpan should not be recording")
    }
    // Ensure methods do not panic
    span.SetAttribute("key", "value")
    span.SetError(nil)
    span.End()
}

func TestInitTracer(t *testing.T) {
    tp, err := InitTracer("testservice", "http://localhost:4317", 1.0)
    if err != nil {
        t.Fatalf("InitTracer error: %v", err)
    }
    if tp == nil {
        t.Fatalf("InitTracer returned nil provider")
    }
    if OTelInstance == nil {
        t.Fatalf("OTelInstance not set after InitTracer")
    }
    // Start and end a span using the global tracer instance
    ctx, span := OTelInstance.Start(context.Background(), "spantest")
    if span == nil {
        t.Fatalf("OTelInstance.Start returned nil span")
    }
    span.End()
    _ = ctx // suppress unused variable warning
    // Shut down the provider to clean up resources
    if err := tp.Shutdown(context.Background()); err != nil {
        t.Fatalf("Provider shutdown error: %v", err)
    }
}
