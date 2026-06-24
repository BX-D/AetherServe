# AetherServe V0.1

AetherServe is a Go cluster-level LLM serving gateway and global request router.
It accepts a strict, streaming-only OpenAI-style chat-completions request,
selects a registered worker using token-aware policies, and forwards server
streamed output as SSE.

It is deliberately **not** a model runtime: mock workers simulate tokens,
queueing, rates, and prefix-cache metadata. This repository does not implement
Transformer computation, CUDA kernels, PagedAttention, continuous batching,
physical KV-cache allocation, tensor parallelism, real GPU latency, production
authentication, or billing.

## Architecture

```text
client -- POST /v1/chat/completions --> router -- Generate gRPC --> mock worker
                                              ^
                                              +-- RegisterWorker/Heartbeat -- worker
```

The router owns HTTP validation, admission, health, immutable snapshots,
routing, retries before the first token, cancellation, structured logs, and
metrics. Workers own a bounded queue, simulated execution, and state reports.

## Local configuration and run

Copy or edit the checked-in `configs/router.yaml` and two worker examples, then run:

```sh
go run ./cmd/router -config configs/router.yaml
go run ./cmd/mock-worker -config configs/mock-worker-1.yaml
go run ./cmd/mock-worker -config configs/mock-worker-2.yaml
```

Then request a stream:

```sh
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-aether-tenant-id: demo' \
  -d '{"model":"mock-llm","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"Hello AetherServe"}]}'
```

Health is at `/livez` and `/readyz`; Prometheus exposition is at `/metrics`.
Import `grafana/aetherserve-dashboard.json` into Grafana and point it at the
router's Prometheus scrape target.

For containers, use the Docker-specific worker-advertisement configuration:

```sh
docker compose up --build
```

## Configuration

Router configuration controls addresses, model name, token/context limits,
deadlines, stale-worker timeout, SSE buffering, routing policy, and admission.
Worker configuration controls its data/control addresses, queue capacity,
prefill/decode rates, bounded cache metadata, and deterministic failure
injection. `advertised_address` is optional for local development and required
when a worker binds a wildcard Docker address: it is the routable address sent
to the router during registration. All fields are validated at startup; unknown
YAML keys are rejected.

`routing.policy` is one of `round_robin`, `least_waiting_tokens`,
`prefix_affinity`, or `predicted_ttft`. See
[docs/routing-algorithms.md](docs/routing-algorithms.md) for exact formulas.

## Development and verification

```sh
make tools      # installs pinned protoc/plugins under .tools only
make proto      # regenerates api/gen from api/proto locally
make fmt
make test
make race
make vet
make build
```

`.tools/versions.env` pins `protoc` 29.3, `protoc-gen-go` v1.36.10, and
`protoc-gen-go-grpc` v1.5.1. The installation/generation scripts use local
tools only and source-relative output, so generated files contain no
machine-specific absolute paths. Neither command uses a remote code-generation
service or uploads source/protocol metadata.

Normal Go builds use checked-in bindings. The protocol source is language-neutral:
a C++ MiniInfer worker should implement `aetherserve.v1.InferenceWorker`,
serve `Generate`, and call the router's `RegisterWorker`/`Heartbeat` methods.
It must obey the version, deadline, cancellation, state-version, and token-order
rules in [docs/api-contract.md](docs/api-contract.md).

For the real local C++ worker path, see
[docs/miniinfer-integration.md](docs/miniinfer-integration.md). It builds both
repositories with local gRPC/Protobuf packages and runs the complete
registration, streaming, cancellation, stale-worker, and retry harness with:

```sh
MINIINFER_ROOT=/absolute/path/to/MiniInfer scripts/e2e-miniinfer.sh
```

## Benchmarks

`cmd/loadgen` writes JSON for `wall_clock_mock` or
`deterministic_simulation`. The latter is useful for reproducible routing
comparisons; neither is real GPU inference. Run:

```sh
scripts/benchmark-policies.sh
```

The full workload, metadata, variance, and reporting rules are in
[docs/benchmark-plan.md](docs/benchmark-plan.md). Preserve the generated
configuration and seed alongside every result.

## Troubleshooting and limitations

- A `503 no_healthy_workers` response means workers have not registered, their
  model/addresses are invalid, or all heartbeats are stale.
- A `502` before the first event means the one allowed worker retry was
  exhausted. A named SSE `error` after a token is intentionally not retried.
- If clients read slowly, the bounded SSE buffer/write deadline cancels the
  downstream stream to prevent unbounded memory.
- Router restart loses all registry and admission state. Workers automatically
  re-register; clients must retry interrupted streams.
- This is a local V0.1 serving-control implementation, not a production
  replacement for a model-serving platform.
