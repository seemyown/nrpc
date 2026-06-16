package nrpc

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestGroupRouteAndMiddlewareChain(t *testing.T) {
	app := NewWithConn(nil, Config{
		NoQueue: true,
		Timeout: 500 * time.Millisecond,
		ErrorHandler: func(_ Ctx, err error) error {
			return err
		},
	})

	var order []string
	app.Use(func(c Ctx) error {
		order = append(order, "global:before")
		c.Locals("global", true)
		err := c.Next()
		order = append(order, "global:after")
		return err
	})

	group := app.Group("internal.routing", func(c Ctx) error {
		order = append(order, "group:before")
		err := c.Next()
		order = append(order, "group:after")
		return err
	})

	route := group.Queue("workers").RPC(".update.", func(c Ctx) error {
		order = append(order, "handler")

		var in struct {
			Name string `json:"name"`
		}
		if err := c.BindJSON(&in); err != nil {
			return err
		}
		if in.Name != "axon" {
			t.Fatalf("unexpected payload: %q", in.Name)
		}
		if c.Locals("global") != true {
			t.Fatal("global middleware did not set local value")
		}
		if c.Subject() != "internal.routing.update" {
			t.Fatalf("unexpected subject: %q", c.Subject())
		}
		if c.Queue() != "workers" {
			t.Fatalf("unexpected queue: %q", c.Queue())
		}
		if c.Get("X-Test") != "1" {
			t.Fatalf("unexpected header value: %q", c.Get("X-Test"))
		}
		return nil
	})

	if route.Subject() != "internal.routing.update" {
		t.Fatalf("unexpected route subject: %q", route.Subject())
	}
	if route.QueueName() != "workers" {
		t.Fatalf("unexpected route queue: %q", route.QueueName())
	}

	msg := &nats.Msg{
		Subject: "internal.routing.update",
		Data:    []byte(`{"name":"axon"}`),
		Header:  nats.Header{"X-Test": []string{"1"}},
	}
	if err := app.dispatch(route, msg); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	want := []string{"global:before", "group:before", "handler", "group:after", "global:after"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("unexpected middleware order: got %v want %v", order, want)
	}
}

func TestNextStopsChain(t *testing.T) {
	app := NewWithConn(nil, Config{NoQueue: true})
	var called bool

	route := app.Consume("events.created",
		func(_ Ctx) error {
			return nil
		},
		func(_ Ctx) error {
			called = true
			return nil
		},
	)

	if err := app.dispatch(route, &nats.Msg{Subject: "events.created"}); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if called {
		t.Fatal("handler after middleware without Next was called")
	}
}

func TestRecoverConvertsPanicToRPCError(t *testing.T) {
	app := NewWithConn(nil, Config{
		NoQueue: true,
		ErrorHandler: func(_ Ctx, err error) error {
			return err
		},
	})

	route := app.Consume("panic", Recover(), func(_ Ctx) error {
		panic("boom")
	})

	err := app.dispatch(route, &nats.Msg{Subject: "panic"})
	if err == nil {
		t.Fatal("expected panic to be converted to error")
	}

	var rpcErr RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("expected RPCError, got %T", err)
	}
	if rpcErr.Code != RPCInternalErrorCode {
		t.Fatalf("unexpected error code: %s", rpcErr.Code)
	}
}

func TestSetHeaderWritesResponseOnly(t *testing.T) {
	app := NewWithConn(nil, Config{NoQueue: true})
	req := &nats.Msg{
		Subject: "subject",
		Reply:   "reply",
		Header:  nats.Header{"X-Value": []string{"request"}},
	}
	c := &ctx{
		app:      app,
		request:  req,
		response: newResponseMsg(req),
	}

	c.SetHeader("X-Value", "response")

	if got := req.Header.Get("X-Value"); got != "request" {
		t.Fatalf("request header changed: %q", got)
	}
	if got := c.ResponseHeaders().Get("X-Value"); got != "response" {
		t.Fatalf("response header not set: %q", got)
	}
}

func TestDefaultQueueUsesAppName(t *testing.T) {
	app := NewWithConn(nil, Config{AppName: "event-handler"})
	route := app.RPC("internal.ping", func(_ Ctx) error { return nil })

	if route.QueueName() != "event-handler" {
		t.Fatalf("unexpected default queue: %q", route.QueueName())
	}
}
