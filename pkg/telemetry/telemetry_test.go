package telemetry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	"go.opentelemetry.io/otel/trace"
)

func TestW3CPropagation_HTTP(t *testing.T) {
	_, _ = telemetry.InitTracer(context.Background(), "test-service", "localhost:4317", nil)

	tracer := telemetry.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-http-span")
	defer span.End()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/test", nil)
	telemetry.InjectHTTP(ctx, req)

	traceparent := req.Header.Get("traceparent")
	if traceparent == "" {
		t.Fatalf("expected traceparent header to be injected into HTTP request")
	}

	extractedCtx := telemetry.ExtractHTTP(req)
	extractedSpan := trace.SpanFromContext(extractedCtx)
	if !extractedSpan.SpanContext().IsValid() {
		t.Fatalf("expected valid span context extracted from HTTP request")
	}
}

func TestW3CPropagation_Kafka(t *testing.T) {
	_, _ = telemetry.InitTracer(context.Background(), "test-kafka", "localhost:4317", nil)

	tracer := telemetry.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-kafka-span")
	defer span.End()

	msg := &kafka.Message{Topic: "test.topic", Key: []byte("k"), Value: []byte("v")}
	telemetry.InjectKafka(ctx, msg)

	if len(msg.Headers) == 0 {
		t.Fatalf("expected Kafka message headers to have injected traceparent")
	}

	extractedCtx := telemetry.ExtractKafka(ctx, msg)
	extractedSpan := trace.SpanFromContext(extractedCtx)
	if !extractedSpan.SpanContext().IsValid() {
		t.Fatalf("expected valid span context extracted from Kafka headers")
	}
}

func TestW3CPropagation_GRPC(t *testing.T) {
	_, _ = telemetry.InitTracer(context.Background(), "test-grpc", "localhost:4317", nil)

	tracer := telemetry.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-grpc-span")
	defer span.End()

	outgoingCtx := telemetry.InjectGRPC(ctx)
	incomingCtx := telemetry.ExtractGRPC(outgoingCtx)

	extractedSpan := trace.SpanFromContext(incomingCtx)
	if !extractedSpan.SpanContext().IsValid() {
		t.Fatalf("expected valid span context extracted from gRPC metadata")
	}
}

func TestW3CPropagation_Map(t *testing.T) {
	_, _ = telemetry.InitTracer(context.Background(), "test-map", "localhost:4317", nil)

	tracer := telemetry.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-map-span")
	defer span.End()

	mapData := telemetry.InjectMap(ctx)
	if len(mapData) == 0 {
		t.Fatalf("expected map data to contain traceparent")
	}

	extractedCtx := telemetry.ExtractMap(context.Background(), mapData)
	extractedSpan := trace.SpanFromContext(extractedCtx)
	if !extractedSpan.SpanContext().IsValid() {
		t.Fatalf("expected valid span context extracted from map")
	}
}

func TestMetricsMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	instrumented := telemetry.MetricsMiddleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	rec := httptest.NewRecorder()

	instrumented.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}
}
