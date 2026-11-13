package telemetry

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTP Metrics
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed, partitioned by method, path, and status",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Histogram of latencies for HTTP requests",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"method", "path"},
	)

	// Ledger Metrics
	TransactionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_transactions_total",
			Help: "Total number of financial transactions processed in ledger",
		},
		[]string{"status"},
	)

	// Fraud Engine Metrics
	FraudEvaluationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fraud_evaluations_total",
			Help: "Total number of fraud engine evaluations partitioned by action",
		},
		[]string{"action"},
	)

	// Notification & DLQ Metrics
	NotificationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_dispatched_total",
			Help: "Total number of external webhook notifications attempted",
		},
		[]string{"status"},
	)

	NotificationRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "notification_retries_total",
			Help: "Total number of webhook retry backoff attempts",
		},
	)

	NotificationDLQTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "notification_dlq_total",
			Help: "Total number of poison messages routed to Dead Letter Queue (DLQ)",
		},
	)
)

// responseWriterInterceptor intercepts StatusCode for Prometheus metrics
type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriterInterceptor) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// MetricsMiddleware wraps an http.Handler with Prometheus request duration and counter metrics
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		interceptor := &responseWriterInterceptor{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(interceptor, r)

		duration := time.Since(start).Seconds()
		statusStr := strconv.Itoa(interceptor.statusCode)

		// Guard against High Cardinality Metric Bomb: Use route pattern (e.g. /api/v1/accounts/{account_number}/balance)
		// rather than dynamic raw URL path containing specific account numbers.
		pathPattern := r.Pattern
		if pathPattern == "" {
			pathPattern = r.URL.Path
		}

		HTTPRequestsTotal.WithLabelValues(r.Method, pathPattern, statusStr).Inc()
		HTTPRequestDuration.WithLabelValues(r.Method, pathPattern).Observe(duration)
	})
}

// NewMetricsServer returns an HTTP server dedicated to serving /metrics and /healthz
func NewMetricsServer(port string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}
