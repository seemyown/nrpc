package nrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type MessageOption func(*nats.Msg)

func WithHeader(key string, values ...string) MessageOption {
	return func(msg *nats.Msg) {
		if msg.Header == nil {
			msg.Header = make(nats.Header)
		}
		msg.Header.Del(key)
		for _, value := range values {
			msg.Header.Add(key, value)
		}
	}
}

func WithHeaders(headers nats.Header) MessageOption {
	return func(msg *nats.Msg) {
		if len(headers) == 0 {
			return
		}
		if msg.Header == nil {
			msg.Header = make(nats.Header, len(headers))
		}
		for key, values := range headers {
			msg.Header.Del(key)
			for _, value := range values {
				msg.Header.Add(key, value)
			}
		}
	}
}

func WithReply(reply string) MessageOption {
	return func(msg *nats.Msg) {
		msg.Reply = reply
	}
}

func (a *App) Publish(subject string, payload any, opts ...MessageOption) error {
	if a.nc == nil {
		return ErrNoConnection
	}

	msg, err := a.newMessage(subject, payload, opts...)
	if err != nil {
		return err
	}

	return a.nc.PublishMsg(msg)
}

func (a *App) Request(ctx context.Context, subject string, payload any, out any, opts ...MessageOption) error {
	if a.nc == nil {
		return ErrNoConnection
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if _, ok := ctx.Deadline(); !ok && a.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.cfg.Timeout)
		defer cancel()
	}

	msg, err := a.newMessage(subject, payload, opts...)
	if err != nil {
		return err
	}

	reply, err := a.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}

	switch v := out.(type) {
	case *[]byte:
		*v = append((*v)[:0], reply.Data...)
		return nil
	case *string:
		*v = string(reply.Data)
		return nil
	default:
		if len(reply.Data) == 0 {
			return NewRPCError(RPCInvalidParamsCode, fmt.Errorf("empty response from %q", subject))
		}
		return a.cfg.JSONDecoder(reply.Data, out)
	}
}

func (a *App) RequestTimeout(timeout time.Duration, subject string, payload any, out any, opts ...MessageOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.Request(ctx, subject, payload, out, opts...)
}

func (a *App) newMessage(subject string, payload any, opts ...MessageOption) (*nats.Msg, error) {
	data, err := a.encodePayload(payload)
	if err != nil {
		return nil, err
	}

	msg := &nats.Msg{
		Subject: normalizeSubject(subject),
		Data:    data,
		Header:  make(nats.Header),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(msg)
		}
	}
	return msg, nil
}

func (a *App) encodePayload(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case nil:
		return nil, nil
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case json.RawMessage:
		return v, nil
	default:
		data, err := a.cfg.JSONEncoder(v)
		if err != nil {
			return nil, NewRPCError(RPCInvalidParamsCode, err)
		}
		return data, nil
	}
}
