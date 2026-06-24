# AetherServe V0.1 implementation plan

## Purpose

Deliver a testable, local Go implementation of the AetherServe cluster-level
gateway. It routes streaming requests to mock workers; it does not execute a
model or implement GPU/KV-cache internals.

## Progress

- [x] Inspect the supplied design and identify incomplete placeholder sections.
- [x] Define the external API, worker protocol, routing algorithms, failures,
  and benchmark methodology.
- [x] Cross-check the design documents and record material decisions.
- [x] Bootstrap Go, Protobuf, configuration, and developer tooling. `make tools`
  pins all local binaries under `.tools`; checked-in bindings are generated with
  source-relative paths by `make proto`.
- [x] Milestone 1: real HTTP/SSE/gRPC one-worker vertical slice and cancellation.
- [x] Milestone 2: registration, heartbeats, immutable snapshots, staleness,
  recovery, multiple workers, and round-robin routing.
- [x] Milestone 3: deterministic tokenizer/prefixes, all four routing policies,
  configuration selection, and structured decision traces.
- [x] Milestone 4: token reservation, tenant bucket, pre-token retry only,
  post-token terminal errors, deadline/cancellation, and bounded forwarding.
- [x] Milestone 5: live Prometheus exposition, structured JSON logs, Grafana
  dashboard, seeded load generation, benchmark assets, and result export.
- [x] Milestone 6: README, local/Docker configurations, C++ protocol contract,
  full test/race/vet/build verification.

## Orientation

- `docs/*.md` is the normative V0.1 specification. `system-design.md` is the
  high-level overview; its supporting documents provide exact wire behavior.
- Router HTTP traffic is separate from router control-plane gRPC traffic.
  Workers expose the `Generate` server-streaming RPC and call the router's
  `RegisterWorker`/`Heartbeat` RPCs.
- Worker state is intentionally approximate. Registry snapshots are copied
  under a lock and routing runs without that lock.

## Decision log index

The authoritative decision entries are in `docs/decision-log.md`. In short:

1. Module path is `github.com/aetherserve/aetherserve`; Go 1.24 is required.
2. V0.1 is a strict, streaming-only OpenAI-style subset.
3. The token bucket is a rate charge; the in-flight reservation is released
   exactly once on every terminal request path.
4. Only one pre-first-token retry is permitted; retry keeps the original
   admission charge and reservation.
5. Mock metrics and latency are never represented as GPU measurements.

## Validation and recovery

Each milestone must pass focused unit/integration tests before the next begins.
The final commands are `go fmt ./...`, `go test ./...`, `go test -race ./...`,
`go vet ./...`, and `go build ./...`. Configuration and generated protocol code
are additive; reverting a deployment means stopping router/workers and running
the prior binaries because V0.1 stores no persistent state or migration data.

## Discoveries

- The initial repository had no module, test tooling, CI configuration, or Git
  metadata.
- `docs/system-design.md` repeated the architecture diagram in several sections
  in place of prose. This document is repaired below without changing scope.
- The first remote Buf generation attempt was rejected because it would export
  protocol metadata. The final workflow uses only approved repository-local
  tools: `protoc` 29.3, `protoc-gen-go` v1.36.10, and
  `protoc-gen-go-grpc` v1.5.1. It does not use Buf or a remote generator.

## Final validation

All commands passed on the local Go 1.24.5 toolchain:

- `make tools` and `make proto` (then an absolute-path scan of `api/gen`).
- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- `go build ./...`
- `go test -run TestHTTPToGRPCToSSE -v ./integration`
- `make benchmark`

## Cross-repository MiniInfer integration

### Purpose and outcome

Connect the existing HTTP/SSE gateway to the real C++ MiniInfer worker through
the canonical `aetherserve.v1.InferenceWorker` gRPC schema. The user-visible
outcome is a reproducible local HTTP → Go router → C++ EngineCore → SSE flow,
including registration, heartbeats, cancellation, stale-worker exclusion, and
the documented one-pre-token retry policy.

### Progress

- [x] Inspect both repositories, configs, contracts, generated bindings, and tests.
- [x] Verify the two checked-in `worker.proto` copies are byte-identical.
- [x] Implement canonical MiniInfer worker/control-plane compatibility.
- [x] Harden router timestamp/retry/error handling for a real C++ worker.
- [x] Add cross-repository test harness and integration documentation.
- [x] Run independent, race, sanitizer-compatible, and system validation.

### Decisions and recovery

- The canonical proto remains unchanged; protocol version is the existing `"1.0"` value.
- MiniInfer owns `Generate`; it calls the router for `RegisterWorker` and `Heartbeat`.
- Canonical SHA-256 prefix strings replace MiniInfer's internal FNV values only
  at the cache-protocol adapter boundary; scheduler, admission, and KV policies
  remain intact.
- The integration uses loopback insecure gRPC and no new services or dependencies.
- Roll back by reverting only the integration adapter/configuration changes and
  running the previous standalone router and worker binaries; no persistent data
  or wire-schema migration is involved.

### Validation

Run Go test/race/vet/build; configure MiniInfer with the locally installed
gRPC/Protobuf packages, build, and run CTest; compare proto copies; then run
`MINIINFER_ROOT=/absolute/path/to/MiniInfer scripts/e2e-miniinfer.sh` from this
repository. The harness must cover success, cancellation, staleness, pre-token
retry, and post-token failure without unbounded waits.

All integration validation passed locally: Go test/race/vet/build; MiniInfer's
configured 23-test CTest suite; ASan, UBSan, and TSan-compatible 17-test core
suites; byte-for-byte proto comparison; and the five-scenario cross-process
harness using the real C++ worker.
