// Package nrpc provides a Fiber-like framework for handling NATS RPC requests
// and queue messages.
//
// Handlers and middleware share the same signature. Middleware continues the
// chain by calling Ctx.Next.
package nrpc
