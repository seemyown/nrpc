package nrpc

import (
	"time"

	"github.com/bytedance/sonic"
	"github.com/nats-io/nats.go"
)

type Config struct {
	NatsURL     string
	NatsConn    *nats.Conn
	NatsOptions []nats.Option

	AppName   string
	QueueName string
	NoQueue   bool

	JSONEncoder  Encoder
	JSONDecoder  Decoder
	ErrorHandler ErrorHandler

	Timeout         time.Duration
	FlushTimeout    time.Duration
	DrainConnection bool
}

var DefaultConfig = defaultConfig()

func defaultConfig() Config {
	return Config{
		NatsURL:      nats.DefaultURL,
		AppName:      "nrpc",
		QueueName:    "nrpc",
		JSONDecoder:  sonic.Unmarshal,
		JSONEncoder:  sonic.Marshal,
		ErrorHandler: DefaultErrorHandler,
		Timeout:      10 * time.Second,
		FlushTimeout: 5 * time.Second,
	}
}

func mergeConfig(base, override Config) Config {
	if override.NatsURL != "" {
		base.NatsURL = override.NatsURL
	}
	if override.NatsConn != nil {
		base.NatsConn = override.NatsConn
	}
	if override.NatsOptions != nil {
		base.NatsOptions = override.NatsOptions
	}
	if override.AppName != "" {
		base.AppName = override.AppName
		if override.QueueName == "" && !override.NoQueue {
			base.QueueName = override.AppName
		}
	}
	if override.QueueName != "" {
		base.QueueName = override.QueueName
	}
	if override.NoQueue {
		base.NoQueue = true
		base.QueueName = ""
	}
	if override.JSONEncoder != nil {
		base.JSONEncoder = override.JSONEncoder
	}
	if override.JSONDecoder != nil {
		base.JSONDecoder = override.JSONDecoder
	}
	if override.ErrorHandler != nil {
		base.ErrorHandler = override.ErrorHandler
	}
	if override.Timeout != 0 {
		base.Timeout = override.Timeout
	}
	if override.FlushTimeout != 0 {
		base.FlushTimeout = override.FlushTimeout
	}
	if override.DrainConnection {
		base.DrainConnection = true
	}

	return base
}
