package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New создаёт экземпляр zap.Logger с JSON-кодировкой и добавляет обязательное поле "service".
// Уровень логирования берётся из переменной окружения LOG_LEVEL (по умолчанию "info").
func New(serviceName string) (*zap.Logger, error) {
	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return nil, err
	}
	config.Level = zap.NewAtomicLevelAt(level)

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	// Добавляем имя сервиса как глобальное поле
	logger = logger.With(zap.String("service", serviceName))

	return logger, nil
}
