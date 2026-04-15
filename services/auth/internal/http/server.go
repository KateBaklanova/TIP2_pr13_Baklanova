package http

import (
	"kate/services/auth/internal/http/handler"
	"kate/services/auth/internal/service"
	"kate/shared/middleware"
	"net/http"

	"go.uber.org/zap"
)

func StartServer(port string, authSvc *service.AuthService, logger *zap.Logger) {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/auth/login", handler.LoginHandler(logger, authSvc))
	mux.HandleFunc("/v1/auth/verify", handler.VerifyHandler(logger, authSvc))

	handlerWithMiddleware := middleware.RequestIDMiddleware(
		middleware.LoggingMiddleware(logger)(mux),
	)

	logger.Info("Auth HTTP server starting", zap.String("port", port))
	if err := http.ListenAndServe(":"+port, handlerWithMiddleware); err != nil {
		logger.Fatal("HTTP server failed", zap.Error(err))
	}
}
