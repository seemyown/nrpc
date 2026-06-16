package nrpc

type Decoder func([]byte, any) error
type Encoder func(any) ([]byte, error)
