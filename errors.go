package nrpc

import (
	"errors"
	"fmt"
)

type RPCErrorCode string

const (
	RPCSuccessCode            RPCErrorCode = "SUCCESS"
	RPCInvalidParamsCode      RPCErrorCode = "INVALID_PARAMS"
	RPCReplyUnsupportedError  RPCErrorCode = "REPLY_UNSUPPORTED_ERROR"
	RPCReplyAlreadySentCode   RPCErrorCode = "REPLY_ALREADY_SENT"
	RPCMethodNotFoundCode     RPCErrorCode = "METHOD_NOT_FOUND"
	RPCAuthorizationErrorCode RPCErrorCode = "AUTHORIZATION_ERROR"
	RPCInternalErrorCode      RPCErrorCode = "INTERNAL_ERROR"
)

var (
	ErrAppStarted   = errors.New("nrpc: app already started")
	ErrNoConnection = errors.New("nrpc: nats connection is nil")
)

type ErrorHandler func(Ctx, error) error

type RPCError struct {
	Code    RPCErrorCode `json:"code"`
	Message string       `json:"message"`
	Err     error        `json:"-"`
}

func (e RPCError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("rpc error [%s]", e.Code)
	}
	return fmt.Sprintf("rpc error [%s]: %s", e.Code, e.Message)
}

func (e RPCError) Unwrap() error {
	return e.Err
}

func NewRPCError(code RPCErrorCode, err error) RPCError {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return RPCError{
		Code:    code,
		Message: msg,
		Err:     err,
	}
}

func DefaultErrorHandler(c Ctx, err error) error {
	if err == nil {
		return nil
	}

	if c == nil || c.Replied() || c.ReplySubject() == "" {
		return err
	}

	var rpcErr RPCError
	if !errors.As(err, &rpcErr) {
		rpcErr = NewRPCError(RPCInternalErrorCode, err)
	}

	return c.JSON(rpcErr)
}
