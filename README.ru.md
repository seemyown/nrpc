# nrpc

[English](README.md) | [Русский](README.ru.md)

Тонкий Go-фреймворк для RPC и pub/sub поверх [NATS](https://nats.io).

`nrpc` скрывает низкоуровневую работу с `nats.go` и даёт удобный API: маршруты, middleware, группы, интерфейс `Context`, типизированные ошибки и автономный клиент — без превращения NATS в HTTP.

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

## Возможности

- **RPC** request/reply с минимальным envelope-протоколом
- **Pub/Sub** подписки, включая NATS queue groups
- **Middleware** с `Next()` (глобальные, групповые и на route)
- **Группы маршрутов** с префиксами subject (`api.users.get`)
- **Параметры в subject** — `user.:id` → `c.Param("id")`
- **Автономный клиент** — работает без экземпляра сервера
- **Graceful shutdown** — drain подписок, ожидание активных handlers, опциональный timeout
- **Тесты in-process** через `app.Test` (без NATS)
- **Подключаемый codec** (по умолчанию JSON)
- **Свои RPC status codes** (не HTTP)

## Установка

```bash
go get github.com/seemyown/nrpc
```

Нужен Go 1.22+ и NATS-соединение, которым владеет ваше приложение.

## Быстрый старт

### Сервер

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

`nrpc` **не** управляет NATS-соединением. Вы сами подключаетесь, передаёте `*nats.Conn` в `Listen` / `Run` и сами закрываете соединение.

### Клиент

Клиент автономен — `nrpc.Server` не нужен:

```go
client := nrpc.NewClient(nc)

ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

var user map[string]any
err := client.Request(ctx, "user.get", map[string]int{"id": 42}, &user)
```

Публикация событий:

```go
err := client.Publish(ctx, "events.user.created", map[string]string{
    "name": "Ada",
})
```

## Маршрутизация

RPC-обработчики регистрируются через `Handle`:

```go
app.Handle("user.get", GetUser)
app.Handle("user.create", CreateUser, Auth())
```

### Группы

```go
api := app.Group("api")
users := api.Group("users")

users.Handle("get", GetUser)       // api.users.get
users.Handle("create", CreateUser) // api.users.create
```

### Параметры subject и wildcards

Именованные параметры — `:name`, на стороне NATS это один токен (`*`):

```go
app.Handle("user.:id", func(c nrpc.Context) error {
    id := c.Param("id")       // "42"
    subject := c.Subject()    // "user.42"
    return c.JSON(map[string]string{"id": id})
})
```

Сырые NATS wildcards тоже поддерживаются:

```go
app.Handle("events.>", EventsHandler)
app.Handle("metrics.*", MetricsHandler)
```

Дубликаты, которые сходятся в один NATS subject (например `user.:id` и `user.*`), возвращают ошибку, если не включён `WithAllowRouteOverride(true)`.

## Middleware

```go
app.Use(nrpc.Recover())
app.Use(Auth())

app.Handle("user.delete", DeleteUser, AdminOnly())
```

Порядок выполнения: **global → group → route → handler**.

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

Встроенные:

| Middleware | Пакет | Назначение |
|---|---|---|
| `nrpc.Recover()` | `nrpc` | Panic → `StatusInternal` |
| `middleware.Logger()` | `nrpc/middleware` | Лог запросов через logger сервера |
| `middleware.RequestID()` | `nrpc/middleware` | Прокидывает request id в response headers |

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

Queue groups используют `QueueSubscribe`: каждое сообщение обрабатывает только один инстанс в группе — удобно для горизонтального масштабирования.

Ответ subscriber игнорируется; ошибки уходят в error handler / logger.

## Context

Handlers получают интерфейс (реализация скрыта):

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
    SetContext(ctx context.Context)
    Set(key string, value any)
    Get(key string, defaultValue ...any) any
    Next() error
    Status(status Status) Context
}
```

Подмена Go context из middleware (как в Fiber):

```go
app.Use(func(c nrpc.Context) error {
    ctx := context.WithValue(c.Context(), userKey{}, user)
    c.SetContext(ctx)
    return c.Next()
})
```

## Ошибки и статусы

RPC-статусы — **не** HTTP-коды:

| Константа | Значение |
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

Необработанные ошибки превращаются в `StatusInternal`. То же делает `Recover()` при panic.

## Протокол

Сообщения используют небольшой envelope, который кодирует `Codec` (по умолчанию JSON):

```text
Request  { id, headers, body }
Response { id, status, headers, body, error }
```

`X-Request-ID` генерируется, если его нет, и дублируется в NATS headers. Trace-заголовки вроде `traceparent` / `tracestate` можно передавать как обычные headers.

## Lifecycle и graceful shutdown

```go
app := nrpc.New(
    nrpc.WithShutdownTimeout(15 * time.Second), // по умолчанию 30s; 0 = ждать бесконечно
)

// Рекомендуемый путь: сигнал → cancel → Run делает graceful Shutdown
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

if err := app.Run(ctx, nc); err != nil {
    log.Fatal(err)
}
```

Или вручную:

```go
_ = app.Listen(nc)
// ...
err := app.Shutdown()                 // использует WithShutdownTimeout
err := app.ShutdownContext(ctx)       // свой deadline
```

Последовательность Shutdown:

1. Прекратить приём новых сообщений (`IsRunning() == false`)
2. Drain NATS-подписок
3. Дождаться завершения активных handlers (RPC-ответы всё ещё отправляются)
4. При timeout — отменить оставшиеся request context и вернуть ошибку

`Shutdown` **не** закрывает `*nats.Conn`. Соединение закрывайте сами после остановки:

```go
_ = app.Shutdown()
nc.Close()
```

Ограничение конкурентности:

```go
app := nrpc.New(nrpc.WithConcurrency(100))
```

## Тестирование

Handlers можно тестировать без живого NATS:

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

## Конфигурация

### Опции сервера

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

### Опции клиента

```go
nrpc.NewClient(nc,
    nrpc.WithClientCodec(nrpc.NewJSONCodec()),
    nrpc.WithClientLogger(logger),
)
```

## Принципы дизайна

| Зона ответственности NATS | Зона ответственности nrpc |
|---|---|
| Транспорт и доставка | Routing и группы |
| Subscriptions и queue groups | Middleware pipeline |
| Lifecycle соединения | Request/response протокол |
| | Абстракция Context и ошибки |
| | Сериализация через `Codec` |

`nrpc` сознательно не занимается service discovery, retries, circuit breakers, JetStream, codegen и DI.

## Лицензия

MIT
