# nrpc

[English](README.md) | [Русский](README.ru.md)

A thin Go framework for building RPC and pub/sub services on top of [NATS](https://nats.io).

`nrpc` hides low-level `nats.go` plumbing and gives you an ergonomic API: routes, middleware, groups, a `Context` interface, typed errors, and an autonomous client — without turning NATS into HTTP.

```go
app := nrpc.New()

app.Use(nrpc.Recover())

app.Handle("user.get", GetUser)
app.Handle("user.:id", GetUserByID)

app.Subscribe("events.user.created", OnUserCreated, nrpc.WithQueue("users"))

if err := app.Run(ctx, nc); err != nil {
    log.Fatal(err)
}
```

## Features

- **RPC** request/reply with a minimal envelope protocol
- **Pub/Sub** subscriptions, including NATS queue groups
- **Middleware** chain with `Next()` (global, group, and route-scoped)
- **Route groups** with subject prefixes (`api.users.get`)
- **Subject params** — `user.:id` → `c.Param("id")`
- **Autonomous client** — works without a server instance
- **Graceful shutdown** — drain subscriptions, finish in-flight handlers, optional timeout
- **In-process testing** via `app.Test` (no NATS required)
- **Pluggable codec** (JSON by default)
- **Custom RPC status codes** (not HTTP)

## Install

```bash
go get github.com/seemyown/nrpc
```

Requires Go 1.22+ and a NATS connection owned by your application.

## Quick start

### Server

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    "github.com/nats-io/nats.go"
    "github.com/seemyown/nrpc"
)

func main() {
    nc, err := nats.Connect(nats.DefaultURL)
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    app := nrpc.New(
        nrpc.WithName("user-service"),
    )
    app.Use(nrpc.Recover())

    app.Handle("user.get", func(c nrpc.Context) error {
        var req struct {
            ID int `json:"id"`
        }
        if err := c.Bind(&req); err != nil {
            return err
        }
        return c.JSON(map[string]any{
            "id":   req.ID,
            "name": "Ada",
        })
    })

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    if err := app.Run(ctx, nc); err != nil {
        log.Fatal(err)
    }
}
```

`nrpc` does **not** manage the NATS connection. You connect, pass `*nats.Conn` into `Listen` / `Run`, and close it yourself.

### Client

The client is standalone — no `nrpc.Server` required:

```go
client := nrpc.NewClient(nc)

ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

var user map[string]any
err := client.Request(ctx, "user.get", map[string]int{"id": 42}, &user)
```

Publish events:

```go
err := client.Publish(ctx, "events.user.created", map[string]string{
    "name": "Ada",
})
```

## Routing

Register RPC handlers with `Handle`:

```go
app.Handle("user.get", GetUser)
app.Handle("user.create", CreateUser, Auth())
```

### Groups

```go
api := app.Group("api")
users := api.Group("users")

users.Handle("get", GetUser)       // api.users.get
users.Handle("create", CreateUser) // api.users.create
```

### Subject parameters & wildcards

Named params use `:name` and map to a single NATS token (`*`):

```go
app.Handle("user.:id", func(c nrpc.Context) error {
    id := c.Param("id")       // "42"
    subject := c.Subject()    // "user.42"
    return c.JSON(map[string]string{"id": id})
})
```

Raw NATS wildcards are supported as well:

```go
app.Handle("events.>", EventsHandler)
app.Handle("metrics.*", MetricsHandler)
```

Duplicate routes that resolve to the same NATS subject (e.g. `user.:id` and `user.*`) return an error unless `WithAllowRouteOverride(true)` is set.

## Middleware

```go
app.Use(nrpc.Recover())
app.Use(Auth())

app.Handle("user.delete", DeleteUser, AdminOnly())
```

Execution order: **global → group → route → handler**.

```go
func Auth() nrpc.Middleware {
    return func(c nrpc.Context) error {
        if c.Header("Authorization") == "" {
            return nrpc.ErrUnauthorized
        }
        return c.Next()
    }
}
```

Built-ins:

| Middleware | Package | Purpose |
|---|---|---|
| `nrpc.Recover()` | `nrpc` | Panic → `StatusInternal` |
| `middleware.Logger()` | `nrpc/middleware` | Request logging via server logger |
| `middleware.RequestID()` | `nrpc/middleware` | Echo request id into response headers |

## Pub/Sub

```go
app.Subscribe("events.user.created", func(c nrpc.Context) error {
    var event UserCreated
    if err := c.Bind(&event); err != nil {
        return err
    }
    return service.Handle(event)
}, nrpc.WithQueue("user-service"))
```

Queue groups use `QueueSubscribe` so only one instance in the group processes each message — suitable for horizontal scaling.

Subscriber response bodies are ignored; errors go to the error handler / logger.

## Context

Handlers receive an interface (implementation stays internal):

```go
type Context interface {
    Msg() *nats.Msg
    Subject() string
    Body() []byte
    Bind(v any) error
    Param(key string, defaultValue ...string) string
    Header(key string, defaultValue ...string) string
    SetHeader(key, value string)
    JSON(v any) error
    Send(data []byte) error
    String(value string) error
    RequestID() string
    Context() context.Context
    Set(key string, value any)
    Get(key string, defaultValue ...any) any
    Next() error
    Status(status Status) Context
}
```

## Errors & status codes

RPC statuses are **not** HTTP codes:

| Constant | Value |
|---|---|
| `StatusOK` | `0` |
| `StatusInvalid` | `1` |
| `StatusUnauthenticated` | `2` |
| `StatusForbidden` | `3` |
| `StatusNotFound` | `4` |
| `StatusInternal` | `5` |

```go
return nrpc.StatusError(nrpc.StatusNotFound, "USER_NOT_FOUND", "user not found")

return nrpc.NewError("SOMETHING_BROKE", "details here") // StatusInternal

return nrpc.ErrUnauthorized
```

Unhandled errors become `StatusInternal`. Panics caught by `Recover()` do the same.

## Protocol

Messages use a small envelope encoded by a `Codec` (JSON by default):

```text
Request  { id, headers, body }
Response { id, status, headers, body, error }
```

`X-Request-ID` is generated when missing and mirrored onto NATS headers for observability. Trace headers such as `traceparent` / `tracestate` can be passed through as regular headers.

## Lifecycle & graceful shutdown

```go
app := nrpc.New(
    nrpc.WithShutdownTimeout(15 * time.Second), // default 30s; 0 = wait forever
)

// Recommended: signal → cancel → Run performs graceful Shutdown
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

if err := app.Run(ctx, nc); err != nil {
    log.Fatal(err)
}
```

Or manually:

```go
_ = app.Listen(nc)
// ...
err := app.Shutdown()                 // uses WithShutdownTimeout
err := app.ShutdownContext(ctx)       // custom deadline
```

Shutdown sequence:

1. Stop accepting new messages (`IsRunning() == false`)
2. Drain NATS subscriptions
3. Wait for in-flight handlers (RPC replies are still sent)
4. On timeout — cancel remaining request contexts and return an error

`Shutdown` does **not** close `*nats.Conn`. Close the connection after shutdown if you own it:

```go
_ = app.Shutdown()
nc.Close()
```

Limit concurrency with:

```go
app := nrpc.New(nrpc.WithConcurrency(100))
```

## Testing

Test handlers without a live NATS server:

```go
app := nrpc.New()
app.Handle("user.get", GetUser)

resp, err := app.Test(nrpc.NewTestRequest(
    "user.get",
    []byte(`{"id":10}`),
))

resp.Status()
resp.Body()
resp.Headers()
resp.Error()
```

## Configuration

### Server options

```go
nrpc.New(
    nrpc.WithName("user-service"),
    nrpc.WithLogger(logger),
    nrpc.WithCodec(nrpc.NewJSONCodec()),
    nrpc.WithErrorHandler(func(c nrpc.Context, err error) { /* ... */ }),
    nrpc.WithConcurrency(64),
    nrpc.WithShutdownTimeout(15 * time.Second),
    nrpc.WithAllowRouteOverride(false),
    nrpc.WithHooks(nrpc.Hooks{
        BeforeRequest: func(c nrpc.Context) {},
        AfterRequest:  func(c nrpc.Context, err error) {},
        OnError:       func(c nrpc.Context, err error) {},
    }),
)
```

### Client options

```go
nrpc.NewClient(nc,
    nrpc.WithClientCodec(nrpc.NewJSONCodec()),
    nrpc.WithClientLogger(logger),
)
```

## Design principles

| NATS owns | nrpc owns |
|---|---|
| Transport & delivery | Routing & groups |
| Subscriptions & queue groups | Middleware pipeline |
| Connection lifecycle | Request/response protocol |
| | Context abstraction & errors |
| | Serialization via `Codec` |

`nrpc` intentionally stays out of service discovery, retries, circuit breakers, JetStream, codegen, and DI.

## License

MIT
