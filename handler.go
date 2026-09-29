package nrpc

// Handler processes an RPC or pub/sub message.
type Handler func(c Context) error

// Middleware is a Fiber-style middleware function.
type Middleware func(c Context) error
