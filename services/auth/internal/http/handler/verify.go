package handler

import (
	"encoding/json"
	"kate/services/auth/internal/service"
	"kate/shared/middleware"
	"net/http"
	"strings"

	"go.uber.org/zap"
)

type verifyResponse struct {
	Valid   bool   `json:"valid"`
	Subject string `json:"subject,omitempty"`
	Error   string `json:"error,omitempty"`
}

func VerifyHandler(logger *zap.Logger, svc *service.AuthService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := middleware.GetRequestID(r.Context())

		if r.Method != http.MethodGet {
			logger.Warn("method not allowed", zap.String("request_id", reqID))
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			logger.Info("missing authorization header", zap.String("request_id", reqID))
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(verifyResponse{Valid: false, Error: "unauthorized"})
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			logger.Info("invalid authorization format", zap.String("request_id", reqID))
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(verifyResponse{Valid: false, Error: "unauthorized"})
			return
		}

		token := parts[1]
		valid, subject := svc.VerifyToken(token)

		if valid {
			logger.Info("token verified",
				zap.String("request_id", reqID),
				zap.String("subject", subject),
			)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(verifyResponse{Valid: true, Subject: subject})
		} else {
			logger.Info("invalid token", zap.String("request_id", reqID))
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(verifyResponse{Valid: false, Error: "unauthorized"})
		}
	}
}
