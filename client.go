package nrpc

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type RPCClient struct {
	cfg Config
	nc  *nats.Conn
}

func NewRPCClient(nc *nats.Conn, cfg Config) *RPCClient {
	return &RPCClient{cfg: cfg, nc: nc}
}

type RequestOptions struct {
	Timeout time.Duration
}

func (c *RPCClient) SendRequest(ctx context.Context) ([]byte, error) {
	return nil, nil
}

func (c *RPCClient) PublishMessage(ctx context.Context, data []byte) error {
	return nil
}
