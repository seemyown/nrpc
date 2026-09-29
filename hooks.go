package nrpc

// Hooks are lifecycle callbacks for observability (metrics, etc.).
type Hooks struct {
	BeforeRequest func(c Context)
	AfterRequest  func(c Context, err error)
	OnError       func(c Context, err error)
}
