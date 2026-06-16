package nrpc

import (
	"context"
	"fmt"
)

func Recover() Handler {
	return func(c Ctx) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = NewRPCError(RPCInternalErrorCode, fmt.Errorf("panic: %v", recovered))
			}
		}()
		return c.Next()
	}
}

func WithContextValue(key, value any) Handler {
	return func(c Ctx) error {
		c.SetContext(context.WithValue(c.GetContext(), key, value))
		return c.Next()
	}
}
