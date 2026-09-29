package nrpc

// Response is the RPC response envelope.
type Response struct {
	ID      string
	Status  Status
	Headers map[string]string
	Body    []byte
	Error   *Error
}

// TestResponse is returned by Server.Test.
type TestResponse struct {
	status  Status
	body    []byte
	headers map[string]string
	err     *Error
	id      string
}

func newTestResponse(resp *Response) *TestResponse {
	if resp == nil {
		return &TestResponse{status: StatusInternal, headers: map[string]string{}}
	}
	h := resp.Headers
	if h == nil {
		h = map[string]string{}
	}
	return &TestResponse{
		status:  resp.Status,
		body:    resp.Body,
		headers: h,
		err:     resp.Error,
		id:      resp.ID,
	}
}

func (r *TestResponse) Status() Status             { return r.status }
func (r *TestResponse) Body() []byte               { return r.body }
func (r *TestResponse) Headers() map[string]string { return r.headers }
func (r *TestResponse) Error() *Error              { return r.err }
func (r *TestResponse) ID() string                 { return r.id }
func (r *TestResponse) Header(key string) string   { return r.headers[key] }
