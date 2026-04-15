package main

import (
	"kate/services/tasks/internal/http"
	"kate/shared/logger"
	"log"
	"os"
)

func main() {
	port := os.Getenv("TASKS_PORT")
	if port == "" {
		port = "8082"
	}

	authGrpcAddr := os.Getenv("AUTH_GRPC_ADDR")
	if authGrpcAddr == "" {
		authGrpcAddr = "localhost:50051"
	}

	logger, err := logger.New("tasks")
	if err != nil {
		log.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Sync()

	http.StartServer(port, authGrpcAddr, logger)
}
