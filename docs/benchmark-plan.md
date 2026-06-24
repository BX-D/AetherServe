# Benchmark plan

AetherServe benchmarks evaluate gateway control and routing behavior. Mock-worker
latencies are not GPU, model, CUDA, or physical-KV-cache measurements.

## Reproducible inputs

The load generator accepts JSONL traces. Each line has `arrival_ms`, `tenant_id`,
`messages`, `max_tokens`, and optional `fault`. Generated traces record workload
name, seed, configuration hash, and generator version. Every run records router
and worker YAML, policy, seed, machine OS/architecture/Go version, start time,
warm-up period, measurement period, repetitions, and result kind.

Use a fixed 30-second warm-up, 120-second measurement period, and five
repetitions by default. Report each metric's mean, minimum, maximum, and 95%
normal-approximation confidence interval; retain all per-run JSON outputs so
variance can be recomputed.

## Workloads and comparisons

Run each policy under:

1. Uniform requests with no shared prefixes.
2. High prefix sharing.
3. One hot prefix with skewed popularity.
4. Mixed short/long prompts.
5. Mixed short/long outputs.
6. Bursty arrivals.
7. Heterogeneous worker rates.
8. Slowdown injected during traffic.
9. Failure before first token.
10. Failure during streaming.

At minimum compare Round Robin against Least Waiting Tokens, Least Waiting Tokens
against Prefix Affinity, and Prefix Affinity against Predicted TTFT. Run each
comparison with the same traces, worker starting state, configurations, and
seeds. Do not hard-code or discard unfavorable results.

## Measurements and output

The load generator emits JSON and CSV per run plus a Markdown report. Required
metrics are request throughput, output tokens/sec, TTFT, end-to-end latency,
P50/P95/P99 latency, queue delay, rejection/retry/midstream-failure rates,
cancellation cleanup latency, per-worker load, prefix-affinity match rate,
reported cache hit rate, predicted-TTFT error, and routing-decision latency.

Every value has one of these explicit classifications:

| Kind | Source | Interpretation |
| --- | --- | --- |
| `wall_clock_mock` | Real local HTTP/gRPC execution against mock workers. | Measures this gateway plus synthetic worker timing. |
| `deterministic_simulation` | Virtual-time seeded trace replay of routing inputs. | Compares deterministic placement behavior, not socket/CPU time. |
| `analytical_prediction` | Formula from routing decision trace. | Tests model calibration, not observed latency. |

The report includes policy/configuration tables, latency/load graphs from the
Grafana dashboard or exported CSV, variance, failure behavior, and limitations.
A result is comparable only when configuration hash, workload trace hash, seed,
run kind, and environment metadata match.

## Operational benchmark commands

`cmd/loadgen` supports `-mode wall_clock_mock` and `-mode deterministic_simulation`,
`-policy`, `-seed`, `-workload`, `-requests`, `-trace`, and `-output`. The
deterministic mode executes the selected routing policy against a seeded virtual
worker snapshot, while wall-clock mode sends real HTTP requests. The repository ships
example YAML and a shell script that runs the three required policy comparisons.
The script exits non-zero on request errors but still writes partial results with
their failure counts.
