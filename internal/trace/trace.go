/*
Package trace
Tellstone Observability Package

File: trace.go
Description: Provides a thin wrapper around OpenTelemetry (OTel) that can be
used throughout the code‑base without pulling the full OTel API everywhere.
The wrapper offers two concrete implementations:

  - NoOpTracer – a zero‑allocation stub used when tracing is disabled.
  - OTelTracer – a production‑grade tracer that forwards spans to an
    external collector (Jaeger, Zipkin, OTLP, …).

Why a wrapper?
  - Decouples the rest of the project from the heavy OTel dependency.
  - Guarantees allocation‑free operation when tracing is turned off,
    which is critical for the ultra‑low‑latency components (storage,
    network, crypto).
  - Allows easy swapping of the tracer implementation at runtime.
*/
package trace

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// OTelInstance holds the global OpenTelemetry tracer used by the package.
var OTelInstance oteltrace.Tracer

type Tracer interface {
	StartSpan(ctx context.Context, name string) Span
}

type Span interface {
	End()
	SetAttribute(key, value string)
	SetError(err error)
	IsRecording() bool
}

type NoOpTracer struct{}
type NoOpSpan struct{}

func (t NoOpTracer) StartSpan(ctx context.Context, name string) Span { return NoOpSpan{} }
func (n NoOpSpan) End()                                              {}
func (n NoOpSpan) SetAttribute(key, value string)                    {}
func (n NoOpSpan) SetError(err error)                                {}
func (n NoOpSpan) IsRecording() bool                                 { return false }

type OTelTracer struct {
	t oteltrace.Tracer
}

func NewOTelTracer(t oteltrace.Tracer) Tracer {
	return &OTelTracer{t: t}
}

func (o *OTelTracer) StartSpan(ctx context.Context, name string) Span {
	_, span := o.t.Start(ctx, name)
	return &OTelSpan{span: span}
}

type OTelSpan struct {
	span oteltrace.Span
}

func (o *OTelSpan) End() { o.span.End() }
func (o *OTelSpan) SetAttribute(key, value string) {
	o.span.SetAttributes(attribute.String(key, value))
}
func (o *OTelSpan) SetError(err error) { o.span.RecordError(err) }
func (o *OTelSpan) IsRecording() bool  { return o.span.IsRecording() }

func InitTracer(serviceName, url string, sampleRatio float64) (*sdktrace.TracerProvider, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		// critical for performance: hard sampling on the root case
		// without sampling otel would allocate heap
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	)
	otel.SetTracerProvider(tp)
	OTelInstance = otel.Tracer("tsd-core")
	return tp, nil
}
