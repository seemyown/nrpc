package nrpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// Server is the main nrpc application entry point.
type Server struct {
	name            string
	logger          Logger
	codec           Codec
	errorHandler    ErrorHandler
	concurrency     int
	allowOverride   bool
	shutdownTimeout time.Duration
	hooks           Hooks

	router *router
	global []Middleware

	nc      *nats.Conn
	subs    []*nats.Subscription
	sem     chan struct{}
	wg      sync.WaitGroup
	running atomic.Bool
	mu      sync.Mutex
	baseCtx context.Context
	cancel  context.CancelFunc
}

// New creates a new Server.
func New(opts ...Option) *Server {
	s := &Server{
		logger:          NoopLogger(),
		codec:           NewJSONCodec(),
		router:          newRouter(),
		shutdownTimeout: defaultShutdownTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Use registers global middleware.
func (s *Server) Use(mw ...Middleware) {
	s.global = append(s.global, mw...)
}

// Handle registers an RPC route.
func (s *Server) Handle(subject string, handler Handler, mw ...Middleware) error {
	return s.handle(subject, handler, mw...)
}

func (s *Server) handle(subject string, handler Handler, mw ...Middleware) error {
	_, err := s.router.addRoute(subject, handler, mw, s.allowOverride)
	return err
}

// Subscribe registers a pub/sub handler.
func (s *Server) Subscribe(subject string, handler Handler, opts ...SubscribeOption) error {
	o := subscribeOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return s.subscribe(subject, handler, o.queue, o.middleware...)
}

func (s *Server) subscribe(subject string, handler Handler, queue string, mw ...Middleware) error {
	_, err := s.router.addSub(subject, handler, mw, queue)
	return err
}

// Group creates a route group with a subject prefix.
func (s *Server) Group(prefix string) *Group {
	return &Group{server: s, prefix: prefix}
}

// Routes returns registered RPC routes.
func (s *Server) Routes() []Route {
	return s.router.list()
}

// Listen registers NATS subscriptions and starts serving.
func (s *Server) Listen(nc *nats.Conn) error {
	if nc == nil {
		return fmt.Errorf("nats connection is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running.Load() {
		return fmt.Errorf("server already running")
	}

	s.nc = nc
	s.baseCtx, s.cancel = context.WithCancel(context.Background())
	if s.concurrency > 0 {
		s.sem = make(chan struct{}, s.concurrency)
	}

	var subs []*nats.Subscription

	for _, rt := range s.router.allRoutes() {
		rt := rt
		sub, err := nc.Subscribe(rt.NATSSubject, func(msg *nats.Msg) {
			s.dispatchRPC(rt, msg)
		})
		if err != nil {
			s.unsubscribeAll(subs)
			return fmt.Errorf("subscribe %s: %w", rt.NATSSubject, err)
		}
		subs = append(subs, sub)
	}

	for _, sr := range s.router.allSubs() {
		sr := sr
		var (
			sub *nats.Subscription
			err error
		)
		cb := func(msg *nats.Msg) {
			s.dispatchSub(sr, msg)
		}
		if sr.Queue != "" {
			sub, err = nc.QueueSubscribe(sr.NATSSubject, sr.Queue, cb)
		} else {
			sub, err = nc.Subscribe(sr.NATSSubject, cb)
		}
		if err != nil {
			s.unsubscribeAll(subs)
			return fmt.Errorf("subscribe %s: %w", sr.NATSSubject, err)
		}
		subs = append(subs, sub)
	}

	s.subs = subs
	s.running.Store(true)
	s.logger.Info("nrpc listening", "name", s.name, "routes", len(s.router.allRoutes()), "subs", len(s.router.allSubs()))
	return nil
}

func (s *Server) unsubscribeAll(subs []*nats.Subscription) {
	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}
}

func (s *Server) acquire() bool {
	if s.sem == nil {
		return true
	}
	select {
	case s.sem <- struct{}{}:
		return true
	case <-s.baseCtx.Done():
		return false
	}
}

func (s *Server) release() {
	if s.sem != nil {
		<-s.sem
	}
}

func (s *Server) dispatchRPC(rt *Route, msg *nats.Msg) {
	if !s.running.Load() {
		return
	}
	if !s.acquire() {
		return
	}
	s.wg.Add(1)
	defer func() {
		s.release()
		s.wg.Done()
	}()

	ctx, cancel := context.WithCancel(s.baseCtx)
	defer cancel()

	params, _ := rt.pattern.Match(msg.Subject)
	_, reqID, headers, body := s.decodeIncoming(msg)

	chain := s.buildChain(rt.Middleware, rt.Handler)
	c := newDefaultCtx(ctx, msg, msg.Subject, body, headers, params, reqID, s.codec, true, chain)

	s.runPipeline(c)

	if msg.Reply == "" {
		return
	}
	// Allow in-flight requests to reply during graceful shutdown.
	// Skip only when the request context was cancelled (forced timeout).
	if c.Context().Err() != nil {
		return
	}
	resp := c.buildResponse()
	data, err := s.codec.Encode(resp)
	if err != nil {
		s.logger.Error("encode response", "err", err)
		return
	}
	out := &nats.Msg{
		Subject: msg.Reply,
		Data:    data,
		Header:  nats.Header{},
	}
	out.Header.Set(HeaderRequestID, resp.ID)
	out.Header.Set(HeaderStatus, strconv.Itoa(int(resp.Status)))
	if err := s.nc.PublishMsg(out); err != nil {
		s.logger.Error("publish response", "err", err)
	}
}

func (s *Server) dispatchSub(sr *SubRoute, msg *nats.Msg) {
	if !s.running.Load() {
		return
	}
	if !s.acquire() {
		return
	}
	s.wg.Add(1)
	defer func() {
		s.release()
		s.wg.Done()
	}()

	ctx, cancel := context.WithCancel(s.baseCtx)
	defer cancel()

	params, _ := sr.pattern.Match(msg.Subject)
	_, reqID, headers, body := s.decodeIncomingPub(msg)

	chain := s.buildChain(sr.Middleware, sr.Handler)
	c := newDefaultCtx(ctx, msg, msg.Subject, body, headers, params, reqID, s.codec, false, chain)
	s.runPipeline(c)
}

func (s *Server) decodeIncoming(msg *nats.Msg) (Request, string, map[string]string, []byte) {
	var req Request
	headers := map[string]string{}
	if msg.Header != nil {
		for k, vals := range msg.Header {
			if len(vals) > 0 {
				headers[k] = vals[0]
			}
		}
	}
	if len(msg.Data) > 0 {
		if err := s.codec.Decode(msg.Data, &req); err != nil {
			req.Body = msg.Data
		}
	}
	for k, v := range req.Headers {
		headers[k] = v
	}
	reqID := headers[HeaderRequestID]
	if reqID == "" {
		reqID = req.ID
	}
	if reqID == "" {
		reqID = newRequestID()
	}
	headers[HeaderRequestID] = reqID
	req.ID = reqID
	return req, reqID, headers, req.Body
}

func (s *Server) decodeIncomingPub(msg *nats.Msg) (Request, string, map[string]string, []byte) {
	headers := map[string]string{}
	if msg.Header != nil {
		for k, vals := range msg.Header {
			if len(vals) > 0 {
				headers[k] = vals[0]
			}
		}
	}
	reqID := headers[HeaderRequestID]
	if reqID == "" {
		reqID = newRequestID()
	}
	headers[HeaderRequestID] = reqID
	return Request{ID: reqID, Headers: headers, Body: msg.Data}, reqID, headers, msg.Data
}

func (s *Server) buildChain(routeMW []Middleware, handler Handler) []Handler {
	n := len(s.global) + len(routeMW) + 1
	chain := make([]Handler, 0, n)
	for _, mw := range s.global {
		mw := mw
		chain = append(chain, Handler(mw))
	}
	for _, mw := range routeMW {
		mw := mw
		chain = append(chain, Handler(mw))
	}
	chain = append(chain, handler)
	return chain
}

func (s *Server) runPipeline(c *defaultCtx) {
	c.Set("nrpc.logger", s.logger)

	if s.hooks.BeforeRequest != nil {
		s.hooks.BeforeRequest(c)
	}

	err := c.Next()
	if err != nil {
		c.applyError(err)
		s.handleError(c, err)
	}

	if s.hooks.AfterRequest != nil {
		s.hooks.AfterRequest(c, err)
	}
}

func (s *Server) handleError(c Context, err error) {
	s.logger.Error("handler error", "err", err, "subject", c.Subject(), "request_id", c.RequestID())
	if s.hooks.OnError != nil {
		s.hooks.OnError(c, err)
	}
	if s.errorHandler != nil {
		s.errorHandler(c, err)
	}
}

// Test runs the RPC pipeline in-process without NATS.
func (s *Server) Test(req *TestRequest) (*TestResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil test request")
	}
	rt, params := s.router.matchRPC(req.Subject)
	if rt == nil {
		return newTestResponse(&Response{
			Status: StatusNotFound,
			Error:  StatusError(StatusNotFound, "NOT_FOUND", "route not found: "+req.Subject),
		}), nil
	}

	headers := map[string]string{}
	for k, v := range req.Headers {
		headers[k] = v
	}
	reqID := req.ID
	if reqID == "" {
		reqID = headers[HeaderRequestID]
	}
	if reqID == "" {
		reqID = newRequestID()
	}
	headers[HeaderRequestID] = reqID

	chain := s.buildChain(rt.Middleware, rt.Handler)
	c := newDefaultCtx(context.Background(), nil, req.Subject, req.Body, headers, params, reqID, s.codec, true, chain)
	s.runPipeline(c)
	return newTestResponse(c.buildResponse()), nil
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", atomic.AddUint64(&fallbackID, 1))
	}
	return hex.EncodeToString(b[:])
}

var fallbackID uint64
