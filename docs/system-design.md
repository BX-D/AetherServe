# AetherServe system design

**Status:** V0.1 implementation specification  
**Primary language:** Go 1.24  
**Module:** `github.com/aetherserve/aetherserve`

## Overview

AetherServe is a single-region, cluster-level gateway for placement and
streaming control of LLM inference requests. It exposes a streaming
OpenAI-style HTTP endpoint, admits requests using token costs, selects one mock
inference worker, and forwards its output through SSE. Worker state is
approximate and held in memory by one router.

```text
Client --HTTP/SSE--> Router --Generate gRPC--> Mock/C++ compatible worker
                          ^
                          +-- RegisterWorker / Heartbeat gRPC -- Worker
```

The router owns HTTP validation, deadline/cancellation propagation, admission,
registry snapshots, routing decisions, retries before output, observability,
and reproducible benchmarks. A worker owns its bounded local queue, mock token
generation, cache metadata, and state reporting.

## Scope and non-goals

V0.1 supports one logical model, one router, multiple workers, server-streamed
generation, registration/heartbeats, four routing policies, admission control,
and mock-worker evaluation. It deliberately does **not** implement Transformer
compute, CUDA, physical KV allocation, continuous batching, tensor parallelism,
NCCL, persistent state, consensus, multi-region routing, Kubernetes scheduling,
production authentication, billing, or a C++ worker.

Mock workers simulate timing and prefix-cache metadata only. Their results are
not GPU performance measurements and must never be described as such.

## Request lifecycle

1. The gateway validates a strict streaming request and derives request,
   tenant, deadline, token estimate, and prefix fingerprints.
2. Admission reserves the request's worst-case in-flight token cost and charges
   its tenant bucket.
3. The registry provides an immutable healthy-worker snapshot. A policy returns
   a structured, deterministic decision.
4. The proxy opens `Generate`, validates ordered chunks, and commits the SSE
   response on the first valid chunk.
5. Before that commitment, a retryable worker failure may select one other
   worker. Once output has been sent, failures terminate the existing stream.
6. A single cleanup guard releases the global reservation and cancels all
   associated work on completion, failure, client cancellation, or shutdown.

## External API and health

The only inference endpoint is `POST /v1/chat/completions`; it is streaming
only. `/livez` reports process liveness, `/readyz` requires valid startup plus
at least one healthy worker, and `/metrics` exposes Prometheus metrics. Exact
schemas, SSE framing, errors, request IDs, tenants, and deadlines are specified
in [api-contract.md](api-contract.md).

## Internal protocol and worker state

`aetherserve.v1.InferenceWorker.Generate` is a router-to-worker server-stream
RPC. `RegisterWorker` and `Heartbeat` use the same versioned service definition
but are called by workers against the router control-plane listener. The API is
protobuf-only and language-neutral so MiniInfer can implement it in C++ later.
See [api-contract.md](api-contract.md) for field and compatibility rules.

Registry state contains the worker identity, endpoint, model, registration and
heartbeat time, monotonically increasing status version, status, queue work,
rates, KV metadata, and bounded cache-prefix metadata. A snapshot is sorted and
copied before policy evaluation. A worker becomes ineligible after its
heartbeat is stale and recovers after a newer valid heartbeat.

## Routing and admission

Round Robin, Least Waiting Tokens, Prefix Affinity, and Predicted TTFT share a
narrow policy interface and produce explainable decision traces. Prefixes are
cumulative SHA-256 fingerprints of canonical 16-token boundaries. Every policy
is deterministic for the request, snapshot, configuration, and round-robin
cursor state. The exact eligibility rules, time-unit formulas, ties, and
missing-metric behavior are in [routing-algorithms.md](routing-algorithms.md).

Admission has two independent accounting systems: a global in-flight reservation
released exactly once, and a non-refundable tenant rate bucket charged once at
initial admission. There is no router queue: an admitted request is dispatched
immediately or rejected.

## Reliability and observability

All request contexts carry the effective deadline to the worker. Client
disconnect, server shutdown, slow-client timeout, and deadline expiration
cancel downstream generation. Retries occur at most once and only before the
first client token. The complete behavior, cleanup, metrics, and logs for every
failure class are in [failure-semantics.md](failure-semantics.md).

Metrics cover request outcomes, latency, decisions, worker state, cache
metadata, admission, retries, and cancellation. JSON structured logs contain
IDs and decision costs but never prompts. A Grafana dashboard and reproducible
benchmark workflow are specified in [benchmark-plan.md](benchmark-plan.md).

## Repository layout

```text
cmd/                 router, mock-worker, and load generator binaries
api/proto/           language-neutral source protocol
api/gen/             checked-in Go Protobuf bindings
internal/            gateway, registry, routing, admission, mock-worker, etc.
configs/             validated local example configuration
benchmark/           traces, result schema, and reports
docs/                normative contracts and operator documentation
integration/         loopback HTTP + real gRPC integration tests
```

## Success criteria

V0.1 is complete when a real HTTP client can receive ordered SSE from multiple
registered mock workers, cancellation stops generation, stale workers cannot be
routed, all four policies are reproducible and explained, admission/retry
invariants hold, benchmarks distinguish wall-clock mock results from simulation,
and the protocol can be implemented by a C++ worker without Go types.
