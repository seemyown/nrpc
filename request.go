package nrpc

// HeaderRequestID is the correlation / request id header.
const HeaderRequestID = "X-Request-ID"

// HeaderStatus is mirrored on NATS headers for observability.
const HeaderStatus = "X-NRPC-Status"

// Request is the RPC request envelope.
type Request struct {
	ID      string
	Headers map[string]string
	Body    []byte
}

// TestRequest is used with Server.Test.
type TestRequest struct {
	Subject string
	Body    []byte
	Headers map[string]string
	ID      string
}

// NewTestRequest builds a test request for in-process handler testing.
func NewTestRequest(subject string, body []byte) *TestRequest {
	return &TestRequest{
		Subject: subject,
		Body:    body,
		Headers: make(map[string]string),
	}
}

// WithHeader sets a header on the test request.
func (r *TestRequest) WithHeader(key, value string) *TestRequest {
	if r.Headers == nil {
		r.Headers = make(map[string]string)
	}
	r.Headers[key] = value
	return r
}

// WithID sets the request id.
func (r *TestRequest) WithID(id string) *TestRequest {
	r.ID = id
	return r
}
