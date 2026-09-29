package middleware

import (
	"time"

	"github.com/seemyown/nrpc"
)

// Logger returns middleware that logs request subject, request id, and duration.
func Logger() nrpc.Middleware {
	return func(c nrpc.Context) error {
		start := time.Now()
		err := c.Next()
		// Prefer server logger if stored; otherwise silent aside from storage.
		if l, ok := c.Get("nrpc.logger").(nrpc.Logger); ok && l != nil {
			if err != nil {
				l.Error("request", "subject", c.Subject(), "request_id", c.RequestID(), "duration", time.Since(start), "err", err)
			} else {
				l.Info("request", "subject", c.Subject(), "request_id", c.RequestID(), "duration", time.Since(start))
			}
		}
		return err
	}
}

// RequestID ensures X-Request-ID is present (server also generates one).
func RequestID() nrpc.Middleware {
	return func(c nrpc.Context) error {
		if c.RequestID() == "" {
			// Server always sets request id; nothing to do for interface-only Context.
		}
		c.SetHeader(nrpc.HeaderRequestID, c.RequestID())
		return c.Next()
	}
}

// Recover is an alias for nrpc.Recover in the middleware package.
func Recover() nrpc.Middleware {
	return nrpc.Recover()
}
