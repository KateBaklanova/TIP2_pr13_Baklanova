package handler

import (
	"encoding/json"
	"kate/services/auth/internal/service"
	"kate/shared/middleware"
	"net/http"

	"go.uber.org/zap"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
}

func LoginHandler(logger *zap.Logger, svc *service.AuthService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := middleware.GetRequestID(r.Context())

		if r.Method != http.MethodPost {
			logger.Warn("method not allowed", zap.String("request_id", reqID))
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req loginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			logger.Warn("invalid login body", zap.String("request_id", reqID), zap.Error(err))
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// Проверка пароля (для демо)
		if req.Password != "secret" {
			logger.Info("login failed: invalid password",
				zap.String("request_id", reqID),
				zap.String("username", req.Username))
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Определяем subject
		subject := req.Username
		if subject == "" {
			subject = "anonymous"
		}

		// Генерируем токен с subject внутри
		token := "demo-token-" + reqID + ":" + subject

		logger.Info("login successful",
			zap.String("request_id", reqID),
			zap.String("username", req.Username),
			zap.String("subject", subject))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(loginResponse{Token: token})
	}
}
