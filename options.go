package nrpc

import "time"

// ErrorHandler handles errors from middleware, handlers, subscribers, and recovery.
type ErrorHandler func(c Context, err error)

// Option configures a Server.
type Option func(*Server)

// WithName sets a human-readable server name.
func WithName(name string) Option {
	return func(s *Server) {
		s.name = name
	}
}

// WithLogger sets the server logger.
func WithLogger(l Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithCodec sets the server codec.
func WithCodec(c Codec) Option {
	return func(s *Server) {
		if c != nil {
			s.codec = c
		}
	}
}

// WithErrorHandler sets the global error handler.
func WithErrorHandler(h ErrorHandler) Option {
	return func(s *Server) {
		s.errorHandler = h
	}
}

// WithConcurrency limits concurrent in-flight handlers. n <= 0 means unlimited.
func WithConcurrency(n int) Option {
	return func(s *Server) {
		s.concurrency = n
	}
}

// WithAllowRouteOverride allows replacing an existing route with the same NATS subject.
func WithAllowRouteOverride(allow bool) Option {
	return func(s *Server) {
		s.allowOverride = allow
	}
}

// WithHooks registers lifecycle hooks for metrics and similar.
func WithHooks(h Hooks) Option {
	return func(s *Server) {
		s.hooks = h
	}
}

// WithShutdownTimeout sets how long Shutdown / Run wait for in-flight handlers.
// The default is 30s. Pass 0 to wait indefinitely.
func WithShutdownTimeout(d time.Duration) Option {
	return func(s *Server) {
		s.shutdownTimeout = d
	}
}
