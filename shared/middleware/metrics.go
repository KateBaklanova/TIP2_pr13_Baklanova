package middleware

import (
	"kate/shared/metrics"
	"net/http"
	"strconv"
	"time"
)

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		metrics.HttpInFlightRequests.Inc()
		defer metrics.HttpInFlightRequests.Dec()

		route := metrics.NormalizeRoute(r.URL.Path)

		start := time.Now()

		wrapped := &statusRecorder{ResponseWriter: w, statusCode: 200}
		next.ServeHTTP(wrapped, r)

		duration := time.Since(start).Seconds()

		metrics.HttpRequestsTotal.WithLabelValues(
			r.Method, route, strconv.Itoa(wrapped.statusCode),
		).Inc()

		metrics.HttpRequestDuration.WithLabelValues(r.Method, route).Observe(duration)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}
