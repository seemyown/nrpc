package nrpc_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/seemyown/nrpc"
	"github.com/seemyown/nrpc/middleware"
)

func TestDuplicateRoute(t *testing.T) {
	app := nrpc.New()
	if err := app.Handle("user.get", func(c nrpc.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := app.Handle("user.get", func(c nrpc.Context) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "route already registered") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestDuplicateByNATSSubject(t *testing.T) {
	app := nrpc.New()
	if err := app.Handle("user.:id", func(c nrpc.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := app.Handle("user.*", func(c nrpc.Context) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "route already registered") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestGroupNesting(t *testing.T) {
	app := nrpc.New()
	api := app.Group("api")
	users := api.Group("users")
	if err := users.Handle("get", func(c nrpc.Context) error {
		return c.JSON(map[string]string{"ok": "1"})
	}); err != nil {
		t.Fatal(err)
	}
	routes := app.Routes()
	if len(routes) != 1 || routes[0].Subject != "api.users.get" {
		t.Fatalf("routes: %+v", routes)
	}
}

func TestParamAndSubject(t *testing.T) {
	app := nrpc.New()
	_ = app.Handle("user.:id", func(c nrpc.Context) error {
		if c.Param("id") != "42" {
			t.Fatalf("param: %q", c.Param("id"))
		}
		if c.Subject() != "user.42" {
			t.Fatalf("subject: %q", c.Subject())
		}
		return c.JSON(map[string]string{"id": c.Param("id")})
	})

	resp, err := app.Test(nrpc.NewTestRequest("user.42", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status() != nrpc.StatusOK {
		t.Fatalf("status: %v err=%v", resp.Status(), resp.Error())
	}
}

func TestMiddlewareOrder(t *testing.T) {
	var order []string
	app := nrpc.New()
	app.Use(func(c nrpc.Context) error {
		order = append(order, "A")
		return c.Next()
	})
	_ = app.Handle("test", func(c nrpc.Context) error {
		order = append(order, "H")
		return nil
	}, func(c nrpc.Context) error {
		order = append(order, "B")
		return c.Next()
	})

	_, err := app.Test(nrpc.NewTestRequest("test", nil))
	if err != nil {
		t.Fatal(err)
	}
	want := "A B H"
	got := strings.Join(order, " ")
	if got != want {
		t.Fatalf("order: got %q want %q", got, want)
	}
}

func TestBindAndJSON(t *testing.T) {
	type req struct {
		Name string `json:"name"`
	}
	type resp struct {
		Hello string `json:"hello"`
	}
	app := nrpc.New()
	_ = app.Handle("echo", func(c nrpc.Context) error {
		var r req
		if err := c.Bind(&r); err != nil {
			return err
		}
		return c.JSON(resp{Hello: r.Name})
	})

	body := []byte(`{"name":"world"}`)
	tr, err := app.Test(nrpc.NewTestRequest("echo", body))
	if err != nil {
		t.Fatal(err)
	}
	if tr.Status() != nrpc.StatusOK {
		t.Fatalf("status %v %v", tr.Status(), tr.Error())
	}
	var out resp
	if err := nrpc.NewJSONCodec().Decode(tr.Body(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Hello != "world" {
		t.Fatalf("got %+v", out)
	}
}

func TestHandlerErrorStatus(t *testing.T) {
	app := nrpc.New()
	_ = app.Handle("missing", func(c nrpc.Context) error {
		return nrpc.StatusError(nrpc.StatusNotFound, "USER_NOT_FOUND", "user not found")
	})
	resp, err := app.Test(nrpc.NewTestRequest("missing", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status() != nrpc.StatusNotFound {
		t.Fatalf("status %v", resp.Status())
	}
	if resp.Error() == nil || resp.Error().Code != "USER_NOT_FOUND" {
		t.Fatalf("error %+v", resp.Error())
	}
}

func TestRecoverPanic(t *testing.T) {
	app := nrpc.New()
	app.Use(nrpc.Recover())
	_ = app.Handle("panic", func(c nrpc.Context) error {
		panic("boom")
	})
	resp, err := app.Test(nrpc.NewTestRequest("panic", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status() != nrpc.StatusInternal {
		t.Fatalf("status %v err=%v", resp.Status(), resp.Error())
	}
}

func TestRequestIDGenerated(t *testing.T) {
	app := nrpc.New()
	app.Use(middleware.RequestID())
	var seen string
	_ = app.Handle("id", func(c nrpc.Context) error {
		seen = c.RequestID()
		return nil
	})
	resp, err := app.Test(nrpc.NewTestRequest("id", nil))
	if err != nil {
		t.Fatal(err)
	}
	if seen == "" || resp.ID() == "" {
		t.Fatal("expected request id")
	}
}

func TestRequestIDPropagated(t *testing.T) {
	app := nrpc.New()
	var seen string
	_ = app.Handle("id", func(c nrpc.Context) error {
		seen = c.RequestID()
		return nil
	})
	req := nrpc.NewTestRequest("id", nil).WithHeader(nrpc.HeaderRequestID, "fixed-id")
	_, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if seen != "fixed-id" {
		t.Fatalf("got %q", seen)
	}
}

func TestSubscribeQueueOption(t *testing.T) {
	app := nrpc.New()
	err := app.Subscribe("events.user.created", func(c nrpc.Context) error {
		return nil
	}, nrpc.WithQueue("users"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientServerIntegration(t *testing.T) {
	ns := runTestServer(t)
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	app := nrpc.New()
	app.Use(nrpc.Recover())
	_ = app.Handle("user.get", func(c nrpc.Context) error {
		var in map[string]int
		if err := c.Bind(&in); err != nil {
			return err
		}
		return c.JSON(map[string]any{"id": in["id"], "name": "Ada"})
	})
	if err := app.Listen(nc); err != nil {
		t.Fatal(err)
	}
	defer app.Shutdown()

	client := nrpc.NewClient(nc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out map[string]any
	if err := client.Request(ctx, "user.get", map[string]int{"id": 42}, &out); err != nil {
		t.Fatal(err)
	}
	if out["name"] != "Ada" {
		t.Fatalf("got %+v", out)
	}
}

func TestClientTimeout(t *testing.T) {
	ns := runTestServer(t)
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	app := nrpc.New()
	_ = app.Handle("slow", func(c nrpc.Context) error {
		time.Sleep(500 * time.Millisecond)
		return c.JSON(map[string]string{"ok": "1"})
	})
	if err := app.Listen(nc); err != nil {
		t.Fatal(err)
	}
	defer app.Shutdown()

	client := nrpc.NewClient(nc)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = client.Request(ctx, "slow", map[string]string{}, &map[string]any{})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) &&
		!strings.Contains(err.Error(), "timeout") &&
		!strings.Contains(err.Error(), "deadline") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestGracefulShutdownWaitsForHandler(t *testing.T) {
	ns := runTestServer(t)
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	started := make(chan struct{})
	release := make(chan struct{})

	app := nrpc.New(nrpc.WithShutdownTimeout(2 * time.Second))
	_ = app.Handle("slow.job", func(c nrpc.Context) error {
		close(started)
		<-release
		return c.JSON(map[string]string{"ok": "1"})
	})
	if err := app.Listen(nc); err != nil {
		t.Fatal(err)
	}

	client := nrpc.NewClient(nc)
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var out map[string]string
		errCh <- client.Request(ctx, "slow.job", map[string]string{}, &out)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	done := make(chan error, 1)
	go func() {
		done <- app.Shutdown()
	}()

	select {
	case <-done:
		t.Fatal("shutdown returned before handler finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete")
	}

	if err := <-errCh; err != nil {
		t.Fatalf("request: %v", err)
	}
	if app.IsRunning() {
		t.Fatal("expected server stopped")
	}
}

func TestGracefulShutdownTimeout(t *testing.T) {
	ns := runTestServer(t)
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	started := make(chan struct{})
	app := nrpc.New(nrpc.WithShutdownTimeout(100 * time.Millisecond))
	_ = app.Handle("hang", func(c nrpc.Context) error {
		close(started)
		time.Sleep(2 * time.Second)
		return nil
	})
	if err := app.Listen(nc); err != nil {
		t.Fatal(err)
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = nrpc.NewClient(nc).Request(ctx, "hang", nil, nil)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	err = app.Shutdown()
	if err == nil {
		t.Fatal("expected shutdown timeout error")
	}
	if !strings.Contains(err.Error(), "deadline") && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestPublishSubscribe(t *testing.T) {
	ns := runTestServer(t)
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	done := make(chan string, 1)
	app := nrpc.New()
	_ = app.Subscribe("events.user.created", func(c nrpc.Context) error {
		var ev map[string]string
		if err := c.Bind(&ev); err != nil {
			return err
		}
		done <- ev["name"]
		return nil
	}, nrpc.WithQueue("users"))
	if err := app.Listen(nc); err != nil {
		t.Fatal(err)
	}
	defer app.Shutdown()

	client := nrpc.NewClient(nc)
	if err := client.Publish(context.Background(), "events.user.created", map[string]string{"name": "bob"}); err != nil {
		t.Fatal(err)
	}

	select {
	case name := <-done:
		if name != "bob" {
			t.Fatalf("got %q", name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}
