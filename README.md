# nrpc

Fiber-like NATS framework for RPC handlers and queue consumers.

```go
app, err := nrpc.New(nrpc.Config{
	AppName: "event-handler",
	NatsURL: "nats://localhost:4222",
})
if err != nil {
	return err
}

app.Use(nrpc.Recover())

internal := app.Group("internal.routing")

internal.RPC("update", func(ctx nrpc.Ctx) error {
	var in UpdateRequest
	if err := ctx.BindJSON(&in); err != nil {
		return err
	}

	out, err := service.Update(ctx.GetContext(), in)
	if err != nil {
		return err
	}

	return ctx.JSON(nrpc.OK(out))
})

app.Queue("event-handler").Consume("events.route.created", func(ctx nrpc.Ctx) error {
	var event RouteCreated
	if err := ctx.BindJSON(&event); err != nil {
		return err
	}

	return service.HandleRouteCreated(ctx.GetContext(), event)
})

return app.Listen(ctx)
```

Client-side helpers use the same encoder and decoder:

```go
var out UpdateResponse
err := app.Request(ctx, "internal.routing.update", in, &out)

err = app.Publish("events.route.created", event, nrpc.WithHeader("X-Trace-ID", traceID))
```
