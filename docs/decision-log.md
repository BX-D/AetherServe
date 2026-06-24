# AetherServe V0.1 decision log

## D-001: Go module and protocol package

The module is `github.com/aetherserve/aetherserve`; the Protobuf package is
`aetherserve.v1`. Go 1.24 is the supported local baseline. This stable external
path prevents a later MiniInfer integration from depending on a local module
name.

## D-002: Strict streaming API subset

V0.1 accepts only documented text chat-completion fields and requires
`stream: true`. Unsupported OpenAI features are rejected rather than silently
ignored. This makes mock-worker behavior truthful and prevents clients from
assuming unimplemented semantics.

## D-003: Tenant and request identity

There is no authentication. Tenant identity comes from `X-Aether-Tenant-ID`,
defaulting to `public`. A valid caller `X-Request-ID` is preserved; otherwise
the router generates UUIDv4 from `crypto/rand`.

## D-004: Control-plane direction

`Generate` is served by each worker. `RegisterWorker` and `Heartbeat` are
served by the router. They remain methods on `InferenceWorker` to preserve the
requested stable method names; each endpoint embeds unimplemented methods for
the methods belonging to the other role.

## D-005: Prefixes and mock tokens

Canonical message text is role/content lines tokenized by Unicode whitespace.
Every 16-token cumulative prefix is SHA-256 hashed. Mock output is seeded from
request ID and worker seed. This is deterministic, inspectable metadata, not a
model tokenizer or physical cache index.

## D-006: Admission accounting

The global budget reserves `estimated_input_tokens + max_tokens` and is released
once on all terminal paths. Tenant buckets are consumed once at initial
admission and never refunded, which preserves their rate-limiting meaning.
Retries keep both the charge and the original reservation.

## D-007: Retry and stream commitment

The HTTP response is not committed until a valid first chunk. A retryable
failure before that point may make exactly one alternate-worker attempt. A
post-token failure never retries and is reported by a terminal SSE error event.

## D-008: Time-unit routing formulas

Metric-based policies reject incomplete state rather than treating absent rates
as zero. Queue and prefill work are normalized to seconds; Predicted TTFT adds
a documented decode-scheduling interference duration. This avoids arbitrary
unitless weighted scores.

## D-009: Benchmark honesty

Results label themselves `wall_clock_mock`, `deterministic_simulation`, or
`analytical_prediction`. No result is presented as a real GPU, Transformer, or
physical-KV-cache result.

## D-010: Generated protocol workflow

Source `.proto` and generated Go bindings are committed. Normal build/test does
not need a compiler. The explicit protocol-generation target uses the pinned,
repository-local `protoc` and Go plugins described in D-011; no remote
code-generation service is used.

## D-011: Local protocol toolchain

Protocol generation is local-only. `.tools/versions.env` pins `protoc` 29.3,
`protoc-gen-go` v1.36.10, and `protoc-gen-go-grpc` v1.5.1. `make tools`
downloads those executables under `.tools/`; `make proto` invokes local
`protoc` with source-relative output. Buf configuration was removed so no remote
code-generation service can receive source or protocol metadata.
