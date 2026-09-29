package nrpc

import "github.com/seemyown/nrpc/internal/subject"

// Route describes a registered RPC route.
type Route struct {
	Subject     string // pattern, e.g. user.:id
	NATSSubject string // e.g. user.*
	Handler     Handler
	Middleware  []Middleware
	pattern     *subject.Pattern
}

// SubRoute describes a registered pub/sub subscription.
type SubRoute struct {
	Subject     string
	NATSSubject string
	Handler     Handler
	Middleware  []Middleware
	Queue       string
	pattern     *subject.Pattern
}
