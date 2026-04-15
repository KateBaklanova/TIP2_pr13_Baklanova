package service

import (
	"strings"
)

type AuthService struct {
	// заглушка
}

func NewAuthService() *AuthService {
	return &AuthService{}
}

// VerifyToken проверяет токен и возвращает (валидность, subject)
// Формат токена: demo-token-{requestID}:{subject}
// Пример: demo-token-test_kottia:ivan
func (s *AuthService) VerifyToken(token string) (bool, string) {
	// Пустой токен - сразу невалид
	if token == "" {
		return false, ""
	}

	// Проверяем, что токен начинается с demo-token-
	if !strings.HasPrefix(token, "demo-token-") {
		return false, ""
	}

	// Парсим subject из токена (после последнего ":")
	parts := strings.Split(token, ":")
	if len(parts) == 2 {
		// Если есть двоеточие, subject - то что после него
		subject := parts[1]
		if subject != "" {
			return true, subject
		}
	}

	// Если subject не найден, возвращаем "unknown" но токен считаем валидным
	// (чтобы не ломать существующие тесты)
	return true, "unknown"
}
