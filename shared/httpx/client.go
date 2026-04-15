package httpx

import (
	"context"
	"kate/shared/middleware"
	"net/http"
	"time"
)

type Client struct {
	client  *http.Client
	baseURL string
}

func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		client: &http.Client{
			Timeout: timeout,
		},
		baseURL: baseURL,
	}
}

func (c *Client) BaseURL() string {
	return c.baseURL
}

func (c *Client) DoWithRequestID(ctx context.Context, req *http.Request) (*http.Response, error) {
	req = req.WithContext(ctx)
	if reqID := middleware.GetRequestID(ctx); reqID != "" {
		req.Header.Set(middleware.HeaderRequestID, reqID)
	}
	return c.client.Do(req)
}
