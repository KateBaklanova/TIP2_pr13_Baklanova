package main

import (
	"log"
	"os"

	"kate/services/auth/internal/grpc"
	apphttp "kate/services/auth/internal/http"
	"kate/services/auth/internal/service"
	"kate/shared/logger"
)

func main() {
	httpPort := os.Getenv("AUTH_HTTP_PORT")
	if httpPort == "" {
		httpPort = "8081"
	}

	grpcPort := os.Getenv("AUTH_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	logger, err := logger.New("auth")
	if err != nil {
		log.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Sync()

	authSvc := service.NewAuthService()

	// Запускаем HTTP сервер
	go apphttp.StartServer(httpPort, authSvc, logger)

	// Запускаем gRPC сервер (БЕЗ логгера, так как он не нужен в gRPC версии)
	grpc.StartGrpcServer(grpcPort, authSvc)
}
