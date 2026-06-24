# AetherServe V0.1 API contract

This document is normative for external HTTP/SSE and internal Protobuf traffic.
All timestamps are UTC. V0.1 supports one configured logical model and does not
authenticate callers.

## HTTP API

### `POST /v1/chat/completions`

The endpoint accepts `Content-Type: application/json` and only a streaming
request. Unknown JSON fields are rejected.

```json
{
  "model": "mock-llm",
  "messages": [
    {"role": "system", "content": "You are concise."},
    {"role": "user", "content": "Hello"}
  ],
  "stream": true,
  "max_tokens": 64
}
```

| Field | Required | Rules |
| --- | --- | --- |
| `model` | yes | Non-empty and exactly the configured model. |
| `messages` | yes | Non-empty ordered array of `{role,content}`. Roles are `system`, `user`, or `assistant`; content is a non-empty UTF-8 string. |
| `stream` | yes | Must be JSON `true`; non-streaming is unsupported. |
| `max_tokens` | no | Positive integer. Defaults to `router.default_max_output_tokens`; may not exceed `router.max_output_tokens`. |

The router rejects tools/functions, images/audio, `n`, `temperature`, `top_p`,
`stop`, `logprobs`, `response_format`, `user`, and every other unknown field.
The canonical message sequence is `role + "\\n" + content + "\\n"` for each
message; its whitespace-delimited token estimate must fit the configured input
and context limits.

`X-Request-ID` may supply a 1–128 character printable request ID with no control
characters. Otherwise the router emits a random UUIDv4. It is echoed in
`X-Request-ID` and every error/decision record. `X-Aether-Tenant-ID` identifies
the tenant; omitted means `public`. Both are opaque untrusted strings used only
for tracing/admission in V0.1.

`X-Aether-Timeout-Ms` is an optional positive base-10 integer. It is clamped to
the configured minimum and maximum request timeout; omission uses the router
default. The effective deadline is propagated to every worker attempt. A client
disconnect cancels the request immediately.

Before SSE starts, errors use:

```json
{"error":{"message":"...","type":"invalid_request_error","code":"invalid_request","request_id":"..."}}
```

| Status | Type/code | Meaning |
| --- | --- | --- |
| 400 | `invalid_request_error` / `invalid_request` | JSON/schema/model/token/deadline validation failed. |
| 429 | `rate_limit_error` / `admission_rejected` | Tenant bucket or global in-flight budget rejected the request. |
| 502 | `upstream_error` / `worker_unavailable` | A retryable worker failure exhausted the one pre-token retry. |
| 503 | `service_unavailable` / `no_healthy_workers` | No eligible healthy worker exists. |
| 504 | `timeout_error` / `deadline_exceeded` | Deadline expired before stream commitment. |
| 500 | `internal_error` / `internal_error` | Router fault. |

### SSE response

Headers are `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control:
no-cache`, `Connection: keep-alive`, `X-Accel-Buffering: no`, and
`X-Request-ID`. The router does not commit these headers until it has a valid
first worker chunk; this makes pre-token retries and normal HTTP errors possible.

Each non-terminal worker token produces:

```text
event: token
data: {"id":"chatcmpl-<request-id>","object":"chat.completion.chunk","created":1735689600,"model":"mock-llm","choices":[{"index":0,"delta":{"content":"token text"},"finish_reason":null}]}

```

The terminal worker chunk produces `event: completion` with the same envelope,
an empty `delta`, and finish reason `stop`, `length`, `cancelled`, or `error`.
Success then emits exactly:

```text
data: [DONE]

```

After commitment, a worker/protocol/deadline/slow-client failure produces:

```text
event: error
data: {"error":{"message":"...","type":"upstream_error","code":"worker_stream_failed","request_id":"..."}}

data: [DONE]

```

There is no transparent retry after a token event. Client disconnects emit
nothing because the connection is gone; the downstream context is canceled.

### Health and metrics

`GET /livez` returns `200 {"status":"live"}` while the process is running.
`GET /readyz` returns `200 {"status":"ready"}` only after configuration is
valid and at least one non-stale healthy worker exists; otherwise it returns
`503`. `GET /metrics` is a Prometheus text endpoint.

## Internal gRPC API

The source contract is `api/proto/aetherserve/v1/worker.proto`, package
`aetherserve.v1`, service `InferenceWorker`:

```proto
rpc Generate(GenerateRequest) returns (stream GenerateChunk);
rpc RegisterWorker(RegisterWorkerRequest) returns (RegisterWorkerResponse);
rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
```

`Generate` is served by a worker data endpoint. `RegisterWorker` and `Heartbeat`
are served by the router control endpoint. All RPCs require
`protocol_version = "1.0"` in V0.1.

| Message | Required semantic content |
| --- | --- |
| `GenerateRequest` | Protocol/request/attempt/worker/tenant IDs, model, normalized messages, estimated input and expected/max output tokens, deadline, and request prefix fingerprints. |
| `GenerateChunk` | Protocol/request/attempt/worker IDs, monotonic token index, text, final flag, finish reason, and worker timestamp. |
| `RegisterWorkerRequest` | Protocol version, worker ID, data address, model, registration timestamp, and initial state. |
| `HeartbeatRequest` | Protocol version, worker ID, worker timestamp, and complete state. |
| `WorkerState` | Monotonic status version, healthy/draining/unhealthy status, waiting request/work counts, running request/work counts, KV capacity/usage, prefill/decode rates, decode scheduling quantum, cache-prefix metadata, and observation time. |

`waiting_token_count` and `running_work_tokens` are **prefill-token-equivalent
work units**: prompt tokens plus output tokens converted by
`prefill_tokens_per_second / decode_tokens_per_second`, rounded up. This makes
queue delay calculations dimensional.

`request_id` identifies the one client request for its entire lifetime.
`attempt_id` identifies one execution attempt; the retry receives a new attempt
ID while retaining the request ID and admission reservation. Worker IDs and
addresses are supplied only by registered workers and are validated by the
router.

A non-final `GenerateChunk` has `final = false`, one non-empty `token_text`,
the expected next token index, and `FINISH_REASON_UNSPECIFIED`. A normal worker
completion has `final = true`, empty token text, the next token index, a
non-unspecified finish reason, and returns gRPC `OK`. A worker failure,
cancellation, deadline, or queue failure returns a non-OK gRPC status without a
synthetic successful final chunk; the gateway maps that post-commit status to
SSE `error` and `[DONE]`.

## Compatibility rules for MiniInfer

- Implement the checked-in proto source with a proto3 runtime; never copy Go
  structs or Go-specific serialization assumptions.
- Preserve field numbers and enum values. New V0.1-compatible fields must be
  optional/additive; removed or repurposed fields require a new major version.
- A different major protocol version is rejected at registration, heartbeat, and
  generation. A future minor version must ignore unknown fields and retain all
  current required semantics.
- Timestamps are UTC `google.protobuf.Timestamp`; rates are positive tokens per
  second; status versions strictly increase; token indexes start at zero and
  increase by one for every non-final token.
- MiniInfer must honor gRPC cancellation/deadline, bound cache metadata, and
  never duplicate or reorder a chunk. It must report an error through gRPC when
  generation cannot complete.
