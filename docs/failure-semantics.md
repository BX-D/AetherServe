# Failure semantics

The router treats `request_id` as the client request identity and `attempt_id` as
one selected-worker execution. The first attempt is `<request_id>-a1` and a
single allowed retry is `<request_id>-a2`. All terminal paths invoke one cleanup
guard that cancels attempt contexts, closes buffered forwarding, decrements
active work, and releases the global reservation exactly once.

## Rules that apply everywhere

- Retry only a retryable upstream connection, gRPC status, timeout, or protocol
  error before a valid token has been written. At most one retry is allowed and
  it excludes the failed worker when another eligible worker exists. If no
  alternate worker exists, the original upstream failure is returned as `502`;
  it is not rewritten as a no-healthy-worker response.
- The stream is committed on its first valid chunk. After commitment no retry is
  allowed; an error event followed by `[DONE]` terminates SSE.
- A tenant bucket is charged once at initial admission and is never refunded.
  Global reservation release is independent and always occurs at finalization.
- Logs include request/attempt/worker IDs, cause, retry decision, and final
  status; they never include prompt content. Metrics use the names described
  below in addition to request/worker labels with bounded cardinality.

## Failure matrix

| Failure | Client result | Retry / selection | Accounting and cleanup | Metrics and logs |
| --- | --- | --- | --- | --- |
| Invalid client request | HTTP 400 JSON error. | None. | No reservation. | `aetherserve_requests_rejected_total{reason="invalid_request"}`; validation log. |
| No healthy workers | HTTP 503 JSON error. | None. | No reservation. | rejection/no-healthy counter; decision trace with all exclusions. |
| Admission rejection | HTTP 429 JSON error. | None. | Failed acquisition leaves no reservation or bucket debit. | rejection counter by global/tenant reason. |
| Worker registration failure | gRPC error to worker; no client effect. | Worker retries registration on next interval. | No registry mutation. | registration-failure counter and worker log. |
| Heartbeat timeout | Existing streams continue; no new routing. | N/A. | Mark snapshot stale; close idle client connection outside lock. | stale-transition gauge/counter and health log. |
| Worker recovery | No direct client effect. | New requests may select worker. | Accept only newer valid heartbeat. | recovery counter and health log. |
| gRPC connect failure before first token | HTTP 502 only after retry exhausts. | One retry on another eligible worker. | Preserve initial reservation/bucket; cancel failed attempt. | retry counter, upstream-failure counter, attempt log. |
| Worker error before first token | HTTP 502 only after retry exhausts. | Same one-retry rule. | Same as above. | Same as above with worker status. |
| Worker timeout before first token | HTTP 504 if client deadline expired, otherwise 502 after retry. | Retry only while effective deadline remains. | Cancel attempt; preserve then final-release reservation. | timeout/retry counters and attempt log. |
| Worker failure after partial output | SSE `error` then `[DONE]`. | Never. | Cancel attempt and release reservation. | `midstream_failures_total` and final request log. |
| Client cancellation | No response can be guaranteed. | Never. | Cancel gRPC, wait bounded worker cleanup, release reservation. | cancellations and cleanup-latency histogram. |
| Client deadline expiration | HTTP 504 before commitment; SSE timeout error after. | Never once deadline elapsed. | Cancel all attempts and release reservation. | deadline counter and request log. |
| Slow client/backpressure | SSE error if writable; otherwise close. | Never after output commitment. | Bounded channel/write deadline cancels worker and releases reservation. | slow-client counter, cleanup latency. |
| Router shutdown | Existing requests receive cancellation/connection close. | None. | Stop readiness/accepting, cancel root context, bounded drain, release every lease. | shutdown active-request gauge and logs. |
| Router restart | Existing streams close. | None. | All in-memory registry/admission state disappears; workers re-register. | startup/recovery log. |
| Duplicate request attempts | Router does not deduplicate client submissions. | One retry only per request ID/attempt sequence. | Each HTTP request has independent lease; an internal retry retains its lease. | request/attempt IDs distinguish events. |
| Duplicate/out-of-order chunks | Pre-token: retryable protocol error; post-token: SSE error. | At most one before token. | Cancel malformed stream and release at terminal path. | protocol-violation/retry counters; worker log. |
| State-version regression | Heartbeat rejected. | Worker stays at last valid state; eventually stale. | Do not overwrite snapshot. | heartbeat-rejected counter and worker log. |
| Malformed heartbeat | Heartbeat rejected. | Same as regression. | Do not mutate registry. | heartbeat-rejected counter and reason. |
| Worker slowdown | Requests remain valid; TTFT may grow. | No automatic migration. | Normal cleanup. | worker rates, TTFT, prediction-error metrics. |
| All workers unavailable during traffic | Existing streams may finish; new/pre-token retry requests fail 503/502. | Retry only if another worker becomes eligible before deadline. | Finalize each request normally. | health, rejection, and failure counters. |

A retryable gRPC code is `Unavailable`, `Unknown`, `Internal`, or
`DeadlineExceeded` when the client deadline has not elapsed. Invalid argument,
permission, and locally canceled errors are not retried. The router validates
chunk IDs, worker ID, and exact expected token index before forwarding; final
chunks have the next expected index and no token text.
