package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InitTracer initializes OpenTelemetry OTLP gRPC exporter to Jaeger with W3C propagation
func InitTracer(ctx context.Context, serviceName string, otlpEndpoint string, logger *slog.Logger) (func(context.Context) error, error) {
	if otlpEndpoint == "" {
		otlpEndpoint = "localhost:4317"
	}

	// 1. Setup global W3C TraceContext Propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// 2. Setup OTLP gRPC client with short timeout
	dialCtx, dialCancel := context.WithTimeout(ctx, 3*time.Second)
	defer dialCancel()

	exporter, err := otlptracegrpc.New(
		dialCtx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()),
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	)
	if err != nil {
		if logger != nil {
			logger.Warn("Jaeger/OTLP endpoint unreachable, falling back to Noop tracer provider",
				"endpoint", otlpEndpoint,
				"error", err,
			)
		}
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(ctx context.Context) error { return nil }, nil
	}

	// 3. Create Resource with service name
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		res = resource.Default()
	}

	// 4. Register BatchSpanProcessor with configurable ratio-based sampler (anti-trace-flooding)
	sampler := sdktrace.Sampler(sdktrace.AlwaysSample())
	if ratioStr := os.Getenv("OTEL_SAMPLING_RATIO"); ratioStr != "" {
		if ratio, err := strconv.ParseFloat(ratioStr, 64); err == nil && ratio >= 0 && ratio <= 1.0 {
			sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
		}
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(500*time.Millisecond)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)

	if logger != nil {
		logger.Info("OpenTelemetry tracer initialized with Jaeger OTLP",
			"service", serviceName,
			"endpoint", otlpEndpoint,
		)
	}

	return tp.Shutdown, nil
}

// Tracer returns a named tracer from the global OpenTelemetry provider
func Tracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}
