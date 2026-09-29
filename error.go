package nrpc

import (
	"errors"
	"fmt"
)

// Error is the standard RPC application error.
type Error struct {
	Status  Status `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewError creates an Error with StatusInternal by default.
func NewError(code, message string) *Error {
	return &Error{
		Status:  StatusInternal,
		Code:    code,
		Message: message,
	}
}

// StatusError creates an Error with an explicit RPC status.
func StatusError(status Status, code, message string) *Error {
	return &Error{
		Status:  status,
		Code:    code,
		Message: message,
	}
}

// WithDetails attaches details to the error.
func (e *Error) WithDetails(details any) *Error {
	if e == nil {
		return nil
	}
	cp := *e
	cp.Details = details
	return &cp
}

// Common sentinel errors.
var (
	ErrUnauthorized = StatusError(StatusUnauthenticated, "UNAUTHORIZED", "unauthorized")
	ErrForbidden    = StatusError(StatusForbidden, "FORBIDDEN", "forbidden")
	ErrNotFound     = StatusError(StatusNotFound, "NOT_FOUND", "not found")
	ErrInvalid      = StatusError(StatusInvalid, "INVALID", "invalid request")
	ErrInternal     = StatusError(StatusInternal, "INTERNAL_SERVER_ERROR", "internal server error")
)

// AsError extracts *Error from err, if present.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// toRPCError converts any error into a protocol Error.
func toRPCError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := AsError(err); ok {
		cp := *e
		if cp.Status == StatusOK {
			cp.Status = StatusInternal
		}
		return &cp
	}
	return StatusError(StatusInternal, "INTERNAL_SERVER_ERROR", err.Error())
}
