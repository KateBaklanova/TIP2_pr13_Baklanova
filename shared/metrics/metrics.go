package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// счётчик запросов
	HttpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "route", "status"},
	)

	// гистограмма длительности запросов
	HttpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 1, 3},
		},
		[]string{"method", "route"},
	)

	// текущее число активных запросов
	HttpInFlightRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_in_flight_requests",
			Help: "Current number of in-flight HTTP requests",
		},
	)
)

// ID на шаблон
func NormalizeRoute(path string) string {
	if len(path) > len("/v1/tasks/") && path[:len("/v1/tasks/")] == "/v1/tasks/" {
		return "/v1/tasks/:id"
	}
	return path
}
