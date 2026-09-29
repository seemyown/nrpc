package nrpc_test

import (
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
)

func runTestServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := test.DefaultTestOptions
	opts.Port = -1
	return test.RunServer(&opts)
}
