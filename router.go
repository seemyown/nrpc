package nrpc

import (
	"fmt"
	"sync"

	"github.com/seemyown/nrpc/internal/subject"
)

type router struct {
	mu     sync.RWMutex
	routes map[string]*Route // keyed by NATS subject
	order  []string
	subs   []*SubRoute
}

func newRouter() *router {
	return &router{
		routes: make(map[string]*Route),
	}
}

func (r *router) addRoute(pattern string, handler Handler, mws []Middleware, allowOverride bool) (*Route, error) {
	p, err := subject.Compile(pattern)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.routes[p.NATS]; ok {
		if !allowOverride {
			return nil, fmt.Errorf("route already registered: %s", existing.Subject)
		}
	} else {
		r.order = append(r.order, p.NATS)
	}
	rt := &Route{
		Subject:     pattern,
		NATSSubject: p.NATS,
		Handler:     handler,
		Middleware:  append([]Middleware{}, mws...),
		pattern:     p,
	}
	r.routes[p.NATS] = rt
	return rt, nil
}

func (r *router) addSub(pattern string, handler Handler, mws []Middleware, queue string) (*SubRoute, error) {
	p, err := subject.Compile(pattern)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sr := &SubRoute{
		Subject:     pattern,
		NATSSubject: p.NATS,
		Handler:     handler,
		Middleware:  append([]Middleware{}, mws...),
		Queue:       queue,
		pattern:     p,
	}
	r.subs = append(r.subs, sr)
	return sr, nil
}

func (r *router) list() []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Route, 0, len(r.order))
	for _, key := range r.order {
		if rt, ok := r.routes[key]; ok {
			out = append(out, Route{
				Subject:     rt.Subject,
				NATSSubject: rt.NATSSubject,
				Handler:     rt.Handler,
				Middleware:  append([]Middleware{}, rt.Middleware...),
			})
		}
	}
	return out
}

func (r *router) matchRPC(msgSubject string) (*Route, map[string]string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, key := range r.order {
		rt := r.routes[key]
		if params, ok := rt.pattern.Match(msgSubject); ok {
			return rt, params
		}
	}
	return nil, nil
}

func (r *router) allSubs() []*SubRoute {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*SubRoute, len(r.subs))
	copy(out, r.subs)
	return out
}

func (r *router) allRoutes() []*Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Route, 0, len(r.order))
	for _, key := range r.order {
		out = append(out, r.routes[key])
	}
	return out
}
