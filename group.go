package nrpc

// Group scopes routes under a subject prefix with optional middleware.
type Group struct {
	server *Server
	prefix string
	mws    []Middleware
}

// Use appends middleware to the group.
func (g *Group) Use(mw ...Middleware) {
	g.mws = append(g.mws, mw...)
}

// Group creates a nested group.
func (g *Group) Group(prefix string) *Group {
	return &Group{
		server: g.server,
		prefix: JoinSubject(g.prefix, prefix),
		mws:    append([]Middleware{}, g.mws...),
	}
}

// Handle registers an RPC route under the group prefix.
func (g *Group) Handle(subject string, handler Handler, mw ...Middleware) error {
	full := JoinSubject(g.prefix, subject)
	mws := append(append([]Middleware{}, g.mws...), mw...)
	return g.server.handle(full, handler, mws...)
}

// Subscribe registers a pub/sub handler under the group prefix.
func (g *Group) Subscribe(subject string, handler Handler, opts ...SubscribeOption) error {
	full := JoinSubject(g.prefix, subject)
	o := subscribeOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	mws := append(append([]Middleware{}, g.mws...), o.middleware...)
	return g.server.subscribe(full, handler, o.queue, mws...)
}
