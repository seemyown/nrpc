package nrpc

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
)

type Ctx interface {
	Subject() string
	ReplySubject() string
	Queue() string
	Route() *Route
	App() *App

	Request() *nats.Msg
	Response() *nats.Msg
	Body() []byte
	BodyString() string

	BindJSON(any) error
	JSON(any) error
	ReplyJSON(any) error
	Send([]byte) error
	SendString(string) error
	Replied() bool

	Headers() nats.Header
	ResponseHeaders() nats.Header
	GetHeader(string) []string
	Get(string) string
	SetHeader(string, ...string)
	AddHeader(string, ...string)

	Locals(key any, value ...any) any
	Next() error
	GetContext() context.Context
	SetContext(context.Context)
}

type ctx struct {
	context.Context

	app      *App
	route    *Route
	request  *nats.Msg
	response *nats.Msg
	replied  bool

	index    int
	handlers []Handler
	locals   map[any]any
}

func (c *ctx) Subject() string {
	if c.request == nil {
		return ""
	}
	return c.request.Subject
}

func (c *ctx) ReplySubject() string {
	if c.request == nil {
		return ""
	}
	return c.request.Reply
}

func (c *ctx) Queue() string {
	if c.route == nil {
		return ""
	}
	return c.route.effectiveQueue()
}

func (c *ctx) Route() *Route {
	return c.route
}

func (c *ctx) App() *App {
	return c.app
}

func (c *ctx) Request() *nats.Msg {
	return c.request
}

func (c *ctx) Response() *nats.Msg {
	return c.response
}

func (c *ctx) Body() []byte {
	if c.request == nil {
		return nil
	}
	return c.request.Data
}

func (c *ctx) BodyString() string {
	return string(c.Body())
}

func (c *ctx) BindJSON(v any) error {
	if len(c.Body()) == 0 {
		return NewRPCError(RPCInvalidParamsCode, errors.New("empty message"))
	}
	if err := c.app.cfg.JSONDecoder(c.Body(), v); err != nil {
		return NewRPCError(RPCInvalidParamsCode, err)
	}
	return nil
}

func (c *ctx) JSON(v any) error {
	return c.ReplyJSON(v)
}

func (c *ctx) ReplyJSON(v any) error {
	data, err := c.app.cfg.JSONEncoder(v)
	if err != nil {
		return NewRPCError(RPCInternalErrorCode, err)
	}
	return c.Send(data)
}

func (c *ctx) Send(data []byte) error {
	if c.replied {
		return NewRPCError(RPCReplyAlreadySentCode, errors.New("reply already sent"))
	}
	if c.ReplySubject() == "" {
		return NewRPCError(RPCReplyUnsupportedError, errors.New("no reply subject"))
	}
	if c.app == nil || c.app.nc == nil {
		return ErrNoConnection
	}

	msg := c.response
	if msg == nil {
		msg = newResponseMsg(c.request)
	}
	msg.Subject = c.ReplySubject()
	msg.Data = data
	msg.Reply = ""

	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}

	if err := c.app.nc.PublishMsg(msg); err != nil {
		return err
	}

	c.response = msg
	c.replied = true
	return nil
}

func (c *ctx) SendString(s string) error {
	return c.Send([]byte(s))
}

func (c *ctx) Replied() bool {
	return c.replied
}

func (c *ctx) Headers() nats.Header {
	if c.request == nil || c.request.Header == nil {
		return nats.Header{}
	}
	return c.request.Header
}

func (c *ctx) ResponseHeaders() nats.Header {
	c.ensureResponseHeader()
	return c.response.Header
}

func (c *ctx) GetHeader(key string) []string {
	return c.Headers()[key]
}

func (c *ctx) Get(key string) string {
	return c.Headers().Get(key)
}

func (c *ctx) SetHeader(key string, value ...string) {
	c.ensureResponseHeader()
	c.response.Header.Del(key)
	for _, v := range value {
		c.response.Header.Add(key, v)
	}
}

func (c *ctx) AddHeader(key string, value ...string) {
	c.ensureResponseHeader()
	for _, v := range value {
		c.response.Header.Add(key, v)
	}
}

func (c *ctx) Locals(key any, value ...any) any {
	if c.locals == nil {
		c.locals = make(map[any]any)
	}
	if len(value) > 0 {
		c.locals[key] = value[0]
	}
	return c.locals[key]
}

func (c *ctx) Next() error {
	c.index++
	if c.index >= len(c.handlers) {
		return nil
	}
	return c.handlers[c.index](c)
}

func (c *ctx) GetContext() context.Context {
	if c.Context == nil {
		return context.Background()
	}
	return c.Context
}

func (c *ctx) SetContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.Context = ctx
}

func (c *ctx) ensureResponseHeader() {
	if c.response == nil {
		c.response = newResponseMsg(c.request)
	}
	if c.response.Header == nil {
		c.response.Header = make(nats.Header)
	}
}

func newResponseMsg(request *nats.Msg) *nats.Msg {
	msg := &nats.Msg{Header: make(nats.Header)}
	if request == nil {
		return msg
	}
	msg.Subject = request.Reply
	return msg
}
