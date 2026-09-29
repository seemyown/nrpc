package nrpc

import (
	"context"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

// Client is an autonomous RPC / pub client (no Server required).
type Client struct {
	nc     *nats.Conn
	codec  Codec
	logger Logger
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithClientCodec sets the client codec.
func WithClientCodec(c Codec) ClientOption {
	return func(cl *Client) {
		if c != nil {
			cl.codec = c
		}
	}
}

// WithClientLogger sets the client logger.
func WithClientLogger(l Logger) ClientOption {
	return func(cl *Client) {
		if l != nil {
			cl.logger = l
		}
	}
}

// NewClient creates a Client bound to an existing NATS connection.
func NewClient(nc *nats.Conn, opts ...ClientOption) *Client {
	c := &Client{
		nc:     nc,
		codec:  NewJSONCodec(),
		logger: NoopLogger(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Request performs an RPC call and decodes the response body into response.
func (c *Client) Request(ctx context.Context, subject string, request any, response any) error {
	if c.nc == nil {
		return fmt.Errorf("nats connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var body []byte
	var err error
	if request != nil {
		body, err = c.codec.Encode(request)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}

	reqID := newRequestID()
	env := Request{
		ID:      reqID,
		Headers: map[string]string{HeaderRequestID: reqID},
		Body:    body,
	}
	data, err := c.codec.Encode(env)
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set(HeaderRequestID, reqID)

	reply, err := c.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return err
	}

	var resp Response
	if err := c.codec.Decode(reply.Data, &resp); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if resp.Error != nil {
		if resp.Error.Status == StatusOK {
			resp.Error.Status = resp.Status
		}
		if resp.Error.Status == StatusOK {
			resp.Error.Status = StatusInternal
		}
		return resp.Error
	}
	if resp.Status != StatusOK {
		return StatusError(resp.Status, "RPC_ERROR", "rpc status "+strconv.Itoa(int(resp.Status)))
	}

	if response != nil && len(resp.Body) > 0 {
		if err := c.codec.Decode(resp.Body, response); err != nil {
			return fmt.Errorf("decode response body: %w", err)
		}
	}
	return nil
}

// Publish sends a pub/sub message (no reply expected).
func (c *Client) Publish(ctx context.Context, subject string, payload any) error {
	if c.nc == nil {
		return fmt.Errorf("nats connection is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	var data []byte
	var err error
	if payload != nil {
		data, err = c.codec.Encode(payload)
		if err != nil {
			return fmt.Errorf("encode payload: %w", err)
		}
	}

	reqID := newRequestID()
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set(HeaderRequestID, reqID)
	return c.nc.PublishMsg(msg)
}
