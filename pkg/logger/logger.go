package logger

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// TracingHandler wraps slog.Handler to inject OpenTelemetry trace_id and span_id
type TracingHandler struct {
	slog.Handler
}

// Handle extracts trace information from context before delegating to inner handler
func (h *TracingHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		span := trace.SpanFromContext(ctx)
		if span.SpanContext().IsValid() {
			r.AddAttrs(
				slog.String("trace_id", span.SpanContext().TraceID().String()),
				slog.String("span_id", span.SpanContext().SpanID().String()),
			)
		}
	}
	return h.Handler.Handle(ctx, r)
}

// New creates a production structured JSON logger with tracing capabilities
func New(serviceName string, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}

	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	jsonHandler := slog.NewJSONHandler(w, opts)
	tracingHandler := &TracingHandler{Handler: jsonHandler}

	return slog.New(tracingHandler).With(
		slog.String("service", serviceName),
	)
}
