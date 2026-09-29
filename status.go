package nrpc

// Status is an RPC protocol status code (not HTTP).
type Status int

const (
	StatusOK              Status = 0
	StatusInvalid         Status = 1
	StatusUnauthenticated Status = 2
	StatusForbidden       Status = 3
	StatusNotFound        Status = 4
	StatusInternal        Status = 5
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusInvalid:
		return "INVALID"
	case StatusUnauthenticated:
		return "UNAUTHENTICATED"
	case StatusForbidden:
		return "FORBIDDEN"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusInternal:
		return "INTERNAL"
	default:
		return "UNKNOWN"
	}
}
