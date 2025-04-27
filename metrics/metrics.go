package metrics

import (
	"bytes"
	"net/http"

	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTP request duration
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	// Total requests
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// MongoDB operation duration
	mongoOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mongo_operation_duration_seconds",
			Help:    "Duration of MongoDB operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "database", "collection"},
	)

	// MongoDB operation errors
	mongoOperationErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mongo_operation_errors_total",
			Help: "Total number of MongoDB operation errors",
		},
		[]string{"operation", "database", "collection"},
	)
)

// RecordHTTPRequest records HTTP request metrics
func RecordHTTPRequest(method, path, status string, duration float64) {
	httpRequestDuration.WithLabelValues(method, path, status).Observe(duration)
	httpRequestsTotal.WithLabelValues(method, path, status).Inc()
}

// RecordMongoOperation records MongoDB operation metrics
func RecordMongoOperation(operation, database, collection string, duration float64, err error) {
	mongoOperationDuration.WithLabelValues(operation, database, collection).Observe(duration)
	if err != nil {
		mongoOperationErrors.WithLabelValues(operation, database, collection).Inc()
	}
}

// Handler returns a Fiber handler for Prometheus metrics
func Handler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Create a buffer to capture the metrics output
		buf := &bytes.Buffer{}

		// Create a custom response writer
		rw := &responseWriter{
			header: make(http.Header),
			buf:    buf,
		}

		// Create a dummy http.Request for promhttp
		req, err := http.NewRequest("GET", "/metrics", nil)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString("Failed to create request")
		}

		// Serve the metrics
		promhttp.Handler().ServeHTTP(rw, req)

		// Set the content type
		c.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		// Send the metrics
		return c.Send(rw.buf.Bytes())
	}
}

// responseWriter implements http.ResponseWriter for Fiber
type responseWriter struct {
	header http.Header
	buf    *bytes.Buffer
}

func (rw *responseWriter) Header() http.Header {
	return rw.header
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	return rw.buf.Write(b)
}

func (rw *responseWriter) WriteHeader(statusCode int) {
	// Status code is handled by Fiber
} 