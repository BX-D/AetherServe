# Routing algorithms

Every decision receives an immutable, worker-ID-sorted registry snapshot,
canonical request token estimate/prefixes, routing configuration, and (for
Round Robin) a cursor state. It returns a selected worker or no selection plus a
`RoutingDecision` trace. Policies never mutate a registry snapshot.

## Common eligibility and trace

A worker is eligible only when it is registered for the request model, has a
non-empty data address, reports `HEALTHY`, has a non-stale heartbeat, uses
protocol major 1, has non-regressing valid state, has KV use no larger than
capacity, and is not excluded by a retry. Metric policies additionally require
positive prefill/decode rates and a positive decode scheduling quantum. Missing
or invalid metrics make a worker ineligible for a metric policy with a recorded
reason; they are not interpreted as zero.

Every candidate trace has worker ID, eligibility/rejection reason, state version,
waiting/running work, matched prefix tokens, queue delay, uncached prefill,
prefill delay, overload penalty, predicted TTFT, score, and tie-break result.
The decision contains request ID, policy, selected ID, candidate traces,
worker-version map, cursor/seed where applicable, and evaluation duration.

All time values are seconds. For worker `w`:

```text
queue_delay(w)       = (waiting_token_count + running_work_tokens) / prefill_rate
matched_tokens(w)    = largest request prefix token_count in worker cache
uncached_prefill(w)  = max(estimated_input_tokens - matched_tokens, 0)
prefill_delay(w)     = uncached_prefill / prefill_rate
overload_penalty(w)  = running_request_count * decode_quantum / decode_rate
predicted_ttft(w)    = queue_delay + prefill_delay + overload_penalty
```

`waiting_token_count` and `running_work_tokens` are normalized prefill-equivalent
units defined in the API contract. Division never occurs with zero: invalid rates
were excluded before calculation. Cached prefix metadata is a bounded set; the
longest match is the maximum `token_count` whose cumulative SHA-256 value occurs
in both the request and worker sets. No match is zero.

## Policies

| Policy | Selection | Ties | Complexity | Strength / weakness |
| --- | --- | --- | --- | --- |
| Round Robin | Advance atomic cursor and select `eligible[cursor % n]`. | Sorted worker IDs define the ring. | O(W) filtering. | Fair simple distribution; ignores queues and cache. |
| Least Waiting Tokens | Minimum `waiting_token_count`. | Worker ID. | O(W). | Good when reported queue work dominates; ignores a running request, rates, and locality. |
| Prefix Affinity | Minimum `queue_delay + prefill_delay`. | More matched tokens, then worker ID. | O(W + P) with O(1) fingerprint lookup. | Balances saved prefill against queueing; does not model decoder interference. |
| Predicted TTFT | Minimum `predicted_ttft`. | More matched tokens, then worker ID. | O(W + P). | Most complete explainable estimate; sensitive to approximate worker metrics. |

Round Robin's cursor is part of its deterministic input. It begins at configured
`routing.round_robin_seed`, is atomically incremented once per decision, and is
recorded in the trace. The remaining policies are stateless. Worker ID lexical
order is the final universal tie-breaker, so equal inputs always choose the same
worker.

Stale state is excluded rather than penalized. Empty cache metadata means zero
match; missing cache metadata in a malformed heartbeat is rejected. A cache
capacity fully used is still eligible because it can serve known prefixes, but a
worker reporting usage above capacity is malformed and excluded.

Prefix Affinity is intentionally the same time-unit model as TTFT without the
running-decode interference term. This makes its difference inspectable rather
than hiding it behind arbitrary unitless coefficients. It performs well with
reused prompts and moderate queue imbalance; it can choose poorly when decoding
contention dominates. Predicted TTFT should perform better for heterogeneous
rates and mixed decode lengths, but only when heartbeat data is fresh.

