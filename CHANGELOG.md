# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-09-29

### Added

- `Context.SetContext(ctx context.Context)` — replace the request `context.Context` from middleware or handlers (Fiber-compatible ergonomics)
- CI: run tests on pull requests and pushes to `main`
- Release workflow: create a GitHub Release on merge to `main` (auto patch-bump from the latest tag, or a version provided via `workflow_dispatch`)

## [0.1.0] - 2026-09-29

### Added

- Initial MVP release of `nrpc`
- Server: `New`, `Handle`, `Group`, `Use`, `Subscribe`, `Listen`, `Run`, `Shutdown` / `ShutdownContext`
- Autonomous Client: `NewClient`, `Request`, `Publish`
- Context interface: `Bind`, `JSON`/`Send`/`String`, headers, params (`:name`), storage, `Next`
- Subject patterns: named params, `*`, `>`
- Middleware pipeline (global / group / route), `Recover`, `middleware.Logger`, `middleware.RequestID`
- Queue subscriptions via `WithQueue`
- Custom RPC status codes (`StatusOK`…`StatusInternal`)
- JSON codec and request/response envelope protocol
- Graceful shutdown with drain, in-flight wait, and `WithShutdownTimeout`
- In-process testing via `Server.Test` / `NewTestRequest`
- Hooks: `BeforeRequest`, `AfterRequest`, `OnError`
- Concurrency limiter: `WithConcurrency`

[0.1.1]: https://github.com/seemyown/nrpc/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/seemyown/nrpc/releases/tag/0.1.0
