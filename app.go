package nrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

const version = "0.0.1"

type Handler func(ctx Ctx) error
type Middleware = Handler

type App struct {
	cfg     Config
	nc      *nats.Conn
	ownConn bool

	mu          sync.RWMutex
	routes      []*Route
	middlewares []Handler
	subs        []*nats.Subscription
	started     bool
}

func New(config ...Config) (*App, error) {
	cfg := defaultConfig()
	if len(config) > 0 {
		cfg = mergeConfig(cfg, config[0])
	}

	if cfg.NatsConn != nil {
		return newWithConn(cfg.NatsConn, cfg, false), nil
	}

	opts := append([]nats.Option(nil), cfg.NatsOptions...)
	if cfg.AppName != "" {
		opts = append([]nats.Option{nats.Name(cfg.AppName)}, opts...)
	}

	nc, err := nats.Connect(cfg.NatsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nrpc: could not connect to NATS: %w", err)
	}

	return newWithConn(nc, cfg, true), nil
}

func NewApp(cfg *Config) (*App, error) {
	if cfg == nil {
		return New()
	}
	return New(*cfg)
}

func NewWithConn(nc *nats.Conn, config ...Config) *App {
	cfg := defaultConfig()
	if len(config) > 0 {
		cfg = mergeConfig(cfg, config[0])
	}
	cfg.NatsConn = nc
	return newWithConn(nc, cfg, false)
}

func newWithConn(nc *nats.Conn, cfg Config, ownConn bool) *App {
	return &App{
		cfg:         cfg,
		nc:          nc,
		ownConn:     ownConn,
		routes:      make([]*Route, 0),
		middlewares: make([]Handler, 0),
		subs:        make([]*nats.Subscription, 0),
	}
}

func (a *App) Version() string {
	return version
}

func (a *App) Agent() string {
	if a.cfg.AppName == "" {
		return "go-nrpc/" + a.Version()
	}
	return a.cfg.AppName
}

func (a *App) Nats() *nats.Conn {
	return a.nc
}

func (a *App) Use(handlers ...Handler) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.middlewares = append(a.middlewares, compactHandlers(handlers)...)
}

func (a *App) Group(prefix string, handlers ...Handler) *Group {
	return newGroup(a, prefix, "", compactHandlers(handlers)...)
}

func (a *App) Queue(queue string, handlers ...Handler) *Group {
	return newGroup(a, "", queue, compactHandlers(handlers)...)
}

func (a *App) Consume(subject string, handlers ...Handler) *Route {
	return a.addRoute(RouteConsume, subject, "", compactHandlers(handlers)...)
}

func (a *App) RPC(subject string, handlers ...Handler) *Route {
	return a.addRoute(RouteRPC, subject, "", compactHandlers(handlers)...)
}

func (a *App) Call(subject string, handlers ...Handler) *Route {
	return a.RPC(subject, handlers...)
}

func (a *App) Routes() []*Route {
	a.mu.RLock()
	defer a.mu.RUnlock()

	routes := make([]*Route, len(a.routes))
	copy(routes, a.routes)
	return routes
}

func (a *App) Serve() error {
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return ErrAppStarted
	}
	if a.nc == nil {
		a.mu.Unlock()
		return ErrNoConnection
	}

	routes := make([]*Route, len(a.routes))
	copy(routes, a.routes)
	a.started = true
	a.mu.Unlock()

	subs := make([]*nats.Subscription, 0, len(routes))
	for _, route := range routes {
		if len(route.handlers) == 0 {
			_ = unsubscribeAll(subs)
			a.markStopped()
			return fmt.Errorf("nrpc: route %q has no handlers", route.subject)
		}

		sub, err := a.subscribe(route)
		if err != nil {
			_ = unsubscribeAll(subs)
			a.markStopped()
			return err
		}
		subs = append(subs, sub)
	}

	if a.cfg.FlushTimeout > 0 {
		if err := a.nc.FlushTimeout(a.cfg.FlushTimeout); err != nil {
			_ = unsubscribeAll(subs)
			a.markStopped()
			return fmt.Errorf("nrpc: flush subscriptions: %w", err)
		}
	}

	a.mu.Lock()
	a.subs = append(a.subs, subs...)
	a.mu.Unlock()

	return nil
}

func (a *App) Listen(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := a.Serve(); err != nil {
		return err
	}
	<-ctx.Done()
	return a.Shutdown(context.Background())
}

func (a *App) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	a.mu.Lock()
	subs := make([]*nats.Subscription, len(a.subs))
	copy(subs, a.subs)
	a.subs = nil
	a.started = false
	a.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		err := unsubscribeAll(subs)
		if a.nc != nil && (a.ownConn || a.cfg.DrainConnection) {
			if drainErr := a.nc.Drain(); drainErr != nil && err == nil {
				err = drainErr
			}
		}
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if a.nc != nil && (a.ownConn || a.cfg.DrainConnection) {
			a.nc.Close()
		}
		return ctx.Err()
	}
}

func (a *App) Close() {
	_ = a.Shutdown(context.Background())
}

func (a *App) Client() *RPCClient {
	return &RPCClient{
		cfg: a.cfg,
		nc:  a.nc,
	}
}

func (a *App) addRoute(kind RouteKind, subject, queue string, handlers ...Handler) *Route {
	a.mu.Lock()
	defer a.mu.Unlock()

	chain := make([]Handler, 0, len(a.middlewares)+len(handlers))
	chain = append(chain, a.middlewares...)
	chain = append(chain, handlers...)

	route := &Route{
		app:      a,
		kind:     kind,
		subject:  normalizeSubject(subject),
		queue:    queue,
		timeout:  a.cfg.Timeout,
		handlers: chain,
	}
	a.routes = append(a.routes, route)
	return route
}

func (a *App) subscribe(route *Route) (*nats.Subscription, error) {
	handler := func(msg *nats.Msg) {
		_ = a.dispatch(route, msg)
	}

	queue := route.effectiveQueue()
	if queue == "" {
		sub, err := a.nc.Subscribe(route.subject, handler)
		if err != nil {
			return nil, fmt.Errorf("nrpc: subscribe %q: %w", route.subject, err)
		}
		return sub, nil
	}

	sub, err := a.nc.QueueSubscribe(route.subject, queue, handler)
	if err != nil {
		return nil, fmt.Errorf("nrpc: queue subscribe %q[%s]: %w", route.subject, queue, err)
	}
	return sub, nil
}

func (a *App) dispatch(route *Route, msg *nats.Msg) error {
	timeout := route.timeout
	if timeout <= 0 {
		timeout = a.cfg.Timeout
	}

	parent := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		parent, cancel = context.WithTimeout(parent, timeout)
	} else {
		parent, cancel = context.WithCancel(parent)
	}
	defer cancel()

	c := &ctx{
		Context:  parent,
		app:      a,
		route:    route,
		request:  msg,
		response: newResponseMsg(msg),
		index:    -1,
		handlers: route.handlers,
		locals:   make(map[any]any),
	}

	if err := c.Next(); err != nil {
		return a.cfg.ErrorHandler(c, err)
	}

	return nil
}

func (a *App) markStopped() {
	a.mu.Lock()
	a.started = false
	a.mu.Unlock()
}

func unsubscribeAll(subs []*nats.Subscription) error {
	var result error
	for _, sub := range subs {
		if sub == nil {
			continue
		}
		if err := sub.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func compactHandlers(handlers []Handler) []Handler {
	out := handlers[:0]
	for _, h := range handlers {
		if h != nil {
			out = append(out, h)
		}
	}
	return out
}
