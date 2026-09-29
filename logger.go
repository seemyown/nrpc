package nrpc

// Logger is a minimal structured-ish logger abstraction.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// NoopLogger returns a logger that discards all messages.
func NoopLogger() Logger {
	return noopLogger{}
}
