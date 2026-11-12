package telemetry

import (
	"context"
	"net/http"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/metadata"
)

// -----------------------------------------------------------------------------
// 1. HTTP Propagation Helpers
// -----------------------------------------------------------------------------

func InjectHTTP(ctx context.Context, req *http.Request) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
}

func ExtractHTTP(req *http.Request) context.Context {
	return otel.GetTextMapPropagator().Extract(req.Context(), propagation.HeaderCarrier(req.Header))
}

// -----------------------------------------------------------------------------
// 2. gRPC Propagation Helpers
// -----------------------------------------------------------------------------

type grpcMetadataCarrier metadata.MD

func (c grpcMetadataCarrier) Get(key string) string {
	values := metadata.MD(c).Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (c grpcMetadataCarrier) Set(key, value string) {
	metadata.MD(c).Set(key, value)
}

func (c grpcMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectGRPC injects trace context into outgoing gRPC metadata
func InjectGRPC(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	otel.GetTextMapPropagator().Inject(ctx, grpcMetadataCarrier(md))
	return metadata.NewOutgoingContext(ctx, md)
}

// ExtractGRPC extracts trace context from incoming gRPC metadata
func ExtractGRPC(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, grpcMetadataCarrier(md))
}

// -----------------------------------------------------------------------------
// 3. Kafka Propagation Helpers
// -----------------------------------------------------------------------------

type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key, value string) {
	if c.headers == nil {
		return
	}
	// Replace if exists
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	// Append if not exists
	*c.headers = append(*c.headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

func (c kafkaHeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectKafka injects trace context into a Kafka message's headers
func InjectKafka(ctx context.Context, msg *kafka.Message) {
	if msg.Headers == nil {
		msg.Headers = make([]kafka.Header, 0, 4)
	}
	carrier := kafkaHeaderCarrier{headers: &msg.Headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractKafka extracts trace context from a Kafka message's headers
func ExtractKafka(ctx context.Context, msg *kafka.Message) context.Context {
	if msg.Headers == nil {
		return ctx
	}
	carrier := kafkaHeaderCarrier{headers: &msg.Headers}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// -----------------------------------------------------------------------------
// 4. Map Propagation Helpers (e.g. for JSON database outbox records)
// -----------------------------------------------------------------------------

// InjectMap serializes W3C trace context into a string map suitable for JSON payloads
func InjectMap(ctx context.Context) map[string]string {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return map[string]string(carrier)
}

// ExtractMap deserializes W3C trace context from a string map
func ExtractMap(ctx context.Context, m map[string]string) context.Context {
	if m == nil {
		return ctx
	}
	carrier := propagation.MapCarrier(m)
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
