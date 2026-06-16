package nrpc

import (
	"strings"
	"time"
)

type RouteKind string

const (
	RouteRPC     RouteKind = "rpc"
	RouteConsume RouteKind = "consume"
)

type Router interface {
	Use(...Handler)
	Group(prefix string, handlers ...Handler) *Group
	Queue(queue string, handlers ...Handler) *Group
	Consume(subject string, handlers ...Handler) *Route
	RPC(subject string, handlers ...Handler) *Route
	Call(subject string, handlers ...Handler) *Route
}

type Route struct {
	app      *App
	kind     RouteKind
	subject  string
	queue    string
	noQueue  bool
	timeout  time.Duration
	name     string
	handlers []Handler
}

func (r *Route) Kind() RouteKind {
	return r.kind
}

func (r *Route) Subject() string {
	return r.subject
}

func (r *Route) QueueName() string {
	return r.effectiveQueue()
}

func (r *Route) Name(name string) *Route {
	r.name = name
	return r
}

func (r *Route) RouteName() string {
	return r.name
}

func (r *Route) Timeout(timeout time.Duration) *Route {
	r.timeout = timeout
	return r
}

func (r *Route) Queue(queue string) *Route {
	r.queue = queue
	r.noQueue = false
	return r
}

func (r *Route) NoQueue() *Route {
	r.queue = ""
	r.noQueue = true
	return r
}

func (r *Route) effectiveQueue() string {
	if r.noQueue || r.app == nil {
		return ""
	}
	if r.queue != "" {
		return r.queue
	}
	if r.app.cfg.NoQueue {
		return ""
	}
	return r.app.cfg.QueueName
}

type Group struct {
	app      *App
	prefix   string
	queue    string
	noQueue  bool
	handlers []Handler
}

func newGroup(app *App, prefix, queue string, handlers ...Handler) *Group {
	return &Group{
		app:      app,
		prefix:   normalizeSubject(prefix),
		queue:    queue,
		handlers: compactHandlers(handlers),
	}
}

func (g *Group) Use(handlers ...Handler) {
	g.handlers = append(g.handlers, compactHandlers(handlers)...)
}

func (g *Group) Group(prefix string, handlers ...Handler) *Group {
	chain := make([]Handler, 0, len(g.handlers)+len(handlers))
	chain = append(chain, g.handlers...)
	chain = append(chain, compactHandlers(handlers)...)

	return &Group{
		app:      g.app,
		prefix:   joinSubject(g.prefix, prefix),
		queue:    g.queue,
		noQueue:  g.noQueue,
		handlers: chain,
	}
}

func (g *Group) Queue(queue string, handlers ...Handler) *Group {
	chain := make([]Handler, 0, len(g.handlers)+len(handlers))
	chain = append(chain, g.handlers...)
	chain = append(chain, compactHandlers(handlers)...)

	return &Group{
		app:      g.app,
		prefix:   g.prefix,
		queue:    queue,
		noQueue:  false,
		handlers: chain,
	}
}

func (g *Group) NoQueue() *Group {
	return &Group{
		app:      g.app,
		prefix:   g.prefix,
		noQueue:  true,
		handlers: append([]Handler(nil), g.handlers...),
	}
}

func (g *Group) Consume(subject string, handlers ...Handler) *Route {
	return g.addRoute(RouteConsume, subject, handlers...)
}

func (g *Group) RPC(subject string, handlers ...Handler) *Route {
	return g.addRoute(RouteRPC, subject, handlers...)
}

func (g *Group) Call(subject string, handlers ...Handler) *Route {
	return g.RPC(subject, handlers...)
}

func (g *Group) addRoute(kind RouteKind, subject string, handlers ...Handler) *Route {
	chain := make([]Handler, 0, len(g.handlers)+len(handlers))
	chain = append(chain, g.handlers...)
	chain = append(chain, compactHandlers(handlers)...)

	route := g.app.addRoute(kind, joinSubject(g.prefix, subject), g.queue, chain...)
	if g.noQueue {
		route.NoQueue()
	}
	return route
}

func normalizeSubject(subject string) string {
	return strings.Trim(subject, ".")
}

func joinSubject(prefix, subject string) string {
	prefix = normalizeSubject(prefix)
	subject = normalizeSubject(subject)
	if prefix == "" {
		return subject
	}
	if subject == "" {
		return prefix
	}
	return prefix + "." + subject
}
