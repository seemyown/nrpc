package nrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const defaultShutdownTimeout = 30 * time.Second

// Run calls Listen and blocks until ctx is done, then performs a graceful Shutdown.
func (s *Server) Run(ctx context.Context, nc *nats.Conn) error {
	if err := s.Listen(nc); err != nil {
		return err
	}
	<-ctx.Done()
	return s.Shutdown()
}

// Shutdown gracefully stops the server using the configured shutdown timeout
// (default 30s, or unlimited when WithShutdownTimeout(0) is set).
//
// Steps:
//  1. stop accepting new messages
//  2. drain NATS subscriptions
//  3. wait for in-flight handlers (they may still send RPC replies)
//  4. cancel remaining request contexts on timeout
//
// It does not close the NATS connection — the application owns *nats.Conn.
func (s *Server) Shutdown() error {
	timeout := s.shutdownTimeout
	if timeout < 0 {
		timeout = defaultShutdownTimeout
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	}
	return s.ShutdownContext(ctx)
}

// ShutdownContext gracefully stops the server, bounded by ctx.
func (s *Server) ShutdownContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if !s.running.Load() {
		s.mu.Unlock()
		return nil
	}
	// Stop accepting new work; keep baseCtx alive so in-flight handlers can finish & reply.
	s.running.Store(false)
	subs := s.subs
	s.subs = nil
	cancel := s.cancel
	s.mu.Unlock()

	s.logger.Info("nrpc shutting down", "name", s.name)

	for _, sub := range subs {
		if err := sub.Drain(); err != nil {
			s.logger.Error("drain subscription", "err", err)
			_ = sub.Unsubscribe()
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if cancel != nil {
			cancel()
		}
		s.logger.Info("nrpc shutdown complete", "name", s.name)
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		s.logger.Error("nrpc shutdown timed out", "name", s.name, "err", ctx.Err())
		// Best-effort wait a moment after cancel so handlers can unwind.
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
		}
		return fmt.Errorf("graceful shutdown: %w", ctx.Err())
	}
}

// IsRunning reports whether the server is serving.
func (s *Server) IsRunning() bool {
	return s.running.Load()
}
