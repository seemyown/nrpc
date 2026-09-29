package nrpc

type subscribeOptions struct {
	queue      string
	middleware []Middleware
}

// SubscribeOption configures a subscription.
type SubscribeOption func(*subscribeOptions)

// WithQueue enables a NATS queue group for horizontal scaling.
func WithQueue(queue string) SubscribeOption {
	return func(o *subscribeOptions) {
		o.queue = queue
	}
}

// WithSubscribeMiddleware attaches middleware only to this subscription.
func WithSubscribeMiddleware(mw ...Middleware) SubscribeOption {
	return func(o *subscribeOptions) {
		o.middleware = append(o.middleware, mw...)
	}
}
