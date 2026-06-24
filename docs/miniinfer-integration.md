# AetherServe and MiniInfer local integration

This guide runs a real local serving path:

```text
HTTP client → AetherServe HTTP/SSE gateway → Generate gRPC → MiniInfer C++ WorkerService
            → EngineCore deterministic mock executor → gRPC chunks → SSE
```

MiniInfer executes simulated tokens only. It does not load a model, use CUDA,
allocate GPU memory, or perform real GPU inference. The integration validates
serving, routing, scheduling, streaming, cancellation, failure semantics, and
worker health.

## Prerequisites and build

Use Go 1.24, CMake 3.25+, and locally installed gRPC/Protobuf packages. Do not
enable MiniInfer's FetchContent gRPC fallback for this workflow.

```bash
cd /path/to/AetherServe
go test ./...
go test -race ./...
go vet ./...
go build ./...

cd /path/to/MiniInfer
cmake -S . -B build \
  -DMINIINFER_BUILD_SERVICE=ON \
  -DCMAKE_PREFIX_PATH="/opt/homebrew/opt/grpc;/opt/homebrew/opt/protobuf"
cmake --build build -j
ctest --test-dir build --output-on-failure

AETHERSERVE_ROOT=/path/to/AetherServe scripts/check-proto-sync.sh
```

## Manual startup

The checked-in local defaults use these ports:

| Process | Endpoint | Address |
| --- | --- | --- |
| AetherServe router | HTTP/SSE | `127.0.0.1:8080` |
| AetherServe router | control gRPC | `127.0.0.1:9090` |
| MiniInfer worker | data gRPC / advertised endpoint | `127.0.0.1:50051` |

Start MiniInfer first; its bounded reconnect loop waits for the router. Then
start AetherServe:

```bash
cd /path/to/MiniInfer
./build/miniinfer_worker -config configs/service.cfg

cd /path/to/AetherServe
go run ./cmd/router -config configs/router.yaml
```

Once `GET /readyz` returns `200`, send a streaming request:

```bash
curl --no-buffer http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-aether-tenant-id: demo' \
  -d '{"model":"mock-llm","stream":true,"max_tokens":4,"messages":[{"role":"user","content":"Hello MiniInfer"}]}'
```

The response contains four `event: token` frames, an `event: completion` frame
with `finish_reason: "length"`, and one `data: [DONE]` marker. MiniInfer sends
token indexes over gRPC; the OpenAI-compatible SSE envelope intentionally does
not expose those implementation indexes.

## Reproducible system test

Run all required scenarios with one command:

```bash
cd /path/to/AetherServe
MINIINFER_ROOT=/path/to/MiniInfer scripts/e2e-miniinfer.sh
```

The script uses loopback ports `18080`, `19090`, `15051`, and `15052` by
default; `AETHERSERVE_E2E_*_ADDRESS` environment variables override them. It
builds the router and worker, captures per-process logs, polls bounded
readiness checks, and cleans up its child processes through `trap`.

It verifies the happy path, a client disconnect, heartbeat expiration after an
ungraceful worker stop, exactly one pre-token retry using attempt `-a2`, and no
retry after a token. Set `KEEP_E2E_LOGS=1` to retain successful logs.

## Failure injection and troubleshooting

`MiniInfer/configs/service.cfg` supports these deterministic settings:

- `failure_fail_before_first_count`: fail that many admitted requests before a
  token; AetherServe may try one alternate worker.
- `failure_fail_after_tokens`: fail after the stated number of emitted tokens;
  AetherServe emits SSE `error` and `[DONE]` without retrying.
- `failure_slowdown_multiplier`: slow EngineCore's simulated execution, useful
  for cancellation testing.

If readiness remains `503`, inspect the worker's advertised address, model,
protocol version (`1.0`), and router control address. If the worker later
becomes unavailable, either its heartbeat stopped or it reported `DRAINING` on
graceful shutdown; AetherServe intentionally excludes it from new routing.
