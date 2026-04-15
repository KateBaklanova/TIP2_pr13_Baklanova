package client

import (
	"context"
	"kate/proto_gen/auth"
	"kate/shared/middleware"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type AuthGrpcClient struct {
	conn   *grpc.ClientConn
	client auth.AuthServiceClient
	logger *zap.Logger
}

func NewAuthGrpcClient(addr string, logger *zap.Logger) (*AuthGrpcClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &AuthGrpcClient{
		conn:   conn,
		client: auth.NewAuthServiceClient(conn),
		logger: logger,
	}, nil
}

func (c *AuthGrpcClient) Close() {
	c.conn.Close()
}

func (c *AuthGrpcClient) VerifyToken(ctx context.Context, token string) (bool, string, error) {
	reqID := middleware.GetRequestID(ctx)

	// Передаём request-id в метаданных
	md := metadata.Pairs(middleware.HeaderRequestID, reqID)
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp, err := c.client.Verify(ctx, &auth.VerifyRequest{Token: token})
	if err != nil {
		c.logger.Error("auth verify failed",
			zap.String("request_id", reqID),
			zap.Error(err),
			zap.String("component", "auth_client"),
		)
		return false, "", err
	}
	c.logger.Info("auth verify success",
		zap.String("request_id", reqID),
		zap.String("subject", resp.Subject),
	)
	return resp.Valid, resp.Subject, nil
}
