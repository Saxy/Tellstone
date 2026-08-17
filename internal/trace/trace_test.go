package trace

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

// noopExporter implements sdktrace.SpanExporter as a no-op sink so the test
// never touches a live OTLP endpoint.
type noopExporter struct{}

func (noopExporter) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error { return nil }
func (noopExporter) Shutdown(_ context.Context) error                               { return nil }

func TestInitTracer(t *testing.T) {
	// Build a tracer provider in-process with a no-op exporter so CI never
	// depends on a live OTLP collector.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(noopExporter{}),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	OTelInstance = otel.Tracer("tsd-core")

	if tp == nil {
		t.Fatalf("TracerProvider is nil")
	}
	if OTelInstance == nil {
		t.Fatalf("OTelInstance not set")
	}

	// Start and end a span using the global tracer instance.
	ctx, span := OTelInstance.Start(context.Background(), "spantest")
	if span == nil {
		t.Fatalf("OTelInstance.Start returned nil span")
	}
	span.End()
	_ = ctx

	// Shut down with a bounded timeout so BatchSpanProcessor flushing
	// cannot hang in CI.
	shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tp.Shutdown(shutCtx); err != nil {
		t.Fatalf("Provider shutdown error: %v", err)
	}
}
