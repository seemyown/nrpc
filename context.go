package nrpc

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
)

// Context is the handler/middleware abstraction (Fiber-style interface).
type Context interface {
	Msg() *nats.Msg

	Subject() string
	Body() []byte
	Bind(v any) error

	Param(key string, defaultValue ...string) string

	Header(key string, defaultValue ...string) string
	SetHeader(key string, value string)

	JSON(v any) error
	Send(data []byte) error
	String(value string) error

	RequestID() string

	Context() context.Context

	Set(key string, value any)
	Get(key string, defaultValue ...any) any

	Next() error

	// Status sets the RPC response status (optional helper for handlers).
	Status(status Status) Context
}

type defaultCtx struct {
	msg       *nats.Msg
	subject   string
	body      []byte
	headers   map[string]string
	params    map[string]string
	requestID string
	ctx       context.Context
	codec     Codec
	reply     bool // RPC expects reply

	respStatus  Status
	respHeaders map[string]string
	respBody    []byte
	respErr     *Error
	responded   bool

	store map[string]any
	mu    sync.RWMutex

	index int
	chain []Handler
}

func newDefaultCtx(
	ctx context.Context,
	msg *nats.Msg,
	subject string,
	body []byte,
	headers map[string]string,
	params map[string]string,
	requestID string,
	codec Codec,
	reply bool,
	chain []Handler,
) *defaultCtx {
	if headers == nil {
		headers = make(map[string]string)
	}
	if params == nil {
		params = make(map[string]string)
	}
	return &defaultCtx{
		msg:         msg,
		subject:     subject,
		body:        body,
		headers:     headers,
		params:      params,
		requestID:   requestID,
		ctx:         ctx,
		codec:       codec,
		reply:       reply,
		respStatus:  StatusOK,
		respHeaders: make(map[string]string),
		store:       make(map[string]any),
		index:       -1,
		chain:       chain,
	}
}

func (c *defaultCtx) Msg() *nats.Msg { return c.msg }

func (c *defaultCtx) Subject() string { return c.subject }

func (c *defaultCtx) Body() []byte { return c.body }

func (c *defaultCtx) Bind(v any) error {
	if len(c.body) == 0 {
		return StatusError(StatusInvalid, "EMPTY_BODY", "empty body")
	}
	if err := c.codec.Decode(c.body, v); err != nil {
		return StatusError(StatusInvalid, "BIND_ERROR", err.Error())
	}
	return nil
}

func (c *defaultCtx) Param(key string, defaultValue ...string) string {
	if v, ok := c.params[key]; ok {
		return v
	}
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return ""
}

func (c *defaultCtx) Header(key string, defaultValue ...string) string {
	if v, ok := c.headers[key]; ok {
		return v
	}
	// also check response-set? no — request headers only for Header()
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return ""
}

func (c *defaultCtx) SetHeader(key string, value string) {
	if c.respHeaders == nil {
		c.respHeaders = make(map[string]string)
	}
	c.respHeaders[key] = value
}

func (c *defaultCtx) JSON(v any) error {
	data, err := c.codec.Encode(v)
	if err != nil {
		return err
	}
	return c.Send(data)
}

func (c *defaultCtx) Send(data []byte) error {
	c.respBody = data
	c.responded = true
	if c.respStatus == StatusOK && c.respErr == nil {
		c.respStatus = StatusOK
	}
	return nil
}

func (c *defaultCtx) String(value string) error {
	return c.Send([]byte(value))
}

func (c *defaultCtx) RequestID() string { return c.requestID }

func (c *defaultCtx) Context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

func (c *defaultCtx) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = value
}

func (c *defaultCtx) Get(key string, defaultValue ...any) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.store[key]; ok {
		return v
	}
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return nil
}

func (c *defaultCtx) Next() error {
	c.index++
	if c.index >= len(c.chain) {
		return nil
	}
	return c.chain[c.index](c)
}

func (c *defaultCtx) Status(status Status) Context {
	c.respStatus = status
	return c
}

func (c *defaultCtx) buildResponse() *Response {
	resp := &Response{
		ID:      c.requestID,
		Status:  c.respStatus,
		Headers: c.respHeaders,
		Body:    c.respBody,
		Error:   c.respErr,
	}
	if resp.Headers == nil {
		resp.Headers = make(map[string]string)
	}
	return resp
}

func (c *defaultCtx) applyError(err error) {
	if err == nil {
		return
	}
	e := toRPCError(err)
	c.respErr = e
	c.respStatus = e.Status
	if c.respStatus == StatusOK {
		c.respStatus = StatusInternal
		e.Status = StatusInternal
	}
}
