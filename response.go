package nrpc

import "errors"

type Response struct {
	Code  RPCErrorCode `json:"code"`
	Data  any          `json:"data,omitempty"`
	Error *RPCError    `json:"error,omitempty"`
}

func OK(data any) Response {
	return Response{
		Code: RPCSuccessCode,
		Data: data,
	}
}

func Fail(err error) Response {
	if err == nil {
		return Response{Code: RPCInternalErrorCode}
	}
	rpcErr, ok := errors.AsType[RPCError](err)
	if !ok {
		rpcErr = NewRPCError(RPCInternalErrorCode, err)
	}

	return Response{
		Code:  rpcErr.Code,
		Error: &rpcErr,
	}
}
