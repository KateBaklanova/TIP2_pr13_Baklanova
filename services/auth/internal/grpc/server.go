package grpc

import (
	"context"
	"kate/proto_gen/auth"
	"kate/services/auth/internal/service"
	"log"
	"net"

	"google.golang.org/grpc"
)

type GrpcServer struct {
	auth.UnimplementedAuthServiceServer
	authSvc *service.AuthService
}

func (s *GrpcServer) Verify(ctx context.Context, req *auth.VerifyRequest) (*auth.VerifyResponse, error) {
	valid, subject := s.authSvc.VerifyToken(req.Token)

	// ГАРАНТИРУЕМ, ЧТО SUBJECT НЕ ПУСТОЙ
	if valid && subject == "" {
		subject = "unknown" // fallback
	}

	log.Printf("gRPC Verify: token=%s, valid=%v, subject=%s", req.Token, valid, subject)

	return &auth.VerifyResponse{
		Valid:   valid,
		Subject: subject, // теперь точно не пустой
	}, nil
}

func StartGrpcServer(port string, authSvc *service.AuthService) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	auth.RegisterAuthServiceServer(s, &GrpcServer{authSvc: authSvc})

	log.Printf("Auth gRPC server starting on port %s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
