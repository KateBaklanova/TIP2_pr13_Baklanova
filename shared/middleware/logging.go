package middleware

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriterWrapper) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func LoggingMiddleware(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}
			reqID := GetRequestID(r.Context())

			next.ServeHTTP(wrapped, r)

			logger.Info("request completed",
				zap.String("request_id", reqID),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", wrapped.statusCode),
				zap.Float64("duration_ms", float64(time.Since(start).Nanoseconds())/1e6),
				zap.String("remote_ip", r.RemoteAddr),
				zap.String("user_agent", r.UserAgent()),
			)
		})
	}
}
