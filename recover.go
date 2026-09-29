package nrpc

import (
	"fmt"
	"runtime/debug"
)

// Recover returns middleware that recovers from panics.
// For RPC, the panic becomes StatusInternal; for subscribers the error is returned to the pipeline.
func Recover() Middleware {
	return func(c Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				err = StatusError(StatusInternal, "INTERNAL_SERVER_ERROR", fmt.Sprintf("panic: %v", r))
				if dc, ok := c.(*defaultCtx); ok {
					dc.applyError(err)
				}
				_ = stack
			}
		}()
		return c.Next()
	}
}
