#!/usr/bin/env bash
set -euo pipefail

: "${MINIINFER_ROOT:?Set MINIINFER_ROOT to the MiniInfer repository root.}"

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MINIINFER_ROOT=$(cd "${MINIINFER_ROOT}" && pwd)
MINIINFER_BUILD_DIR=${MINIINFER_BUILD_DIR:-"${MINIINFER_ROOT}/build"}
HTTP_ADDRESS=${AETHERSERVE_E2E_HTTP_ADDRESS:-127.0.0.1:18080}
CONTROL_ADDRESS=${AETHERSERVE_E2E_CONTROL_ADDRESS:-127.0.0.1:19090}
WORKER_ONE_ADDRESS=${AETHERSERVE_E2E_WORKER_ONE_ADDRESS:-127.0.0.1:15051}
WORKER_TWO_ADDRESS=${AETHERSERVE_E2E_WORKER_TWO_ADDRESS:-127.0.0.1:15052}
HTTP_URL="http://${HTTP_ADDRESS}"
TMPDIR_E2E=$(mktemp -d "${TMPDIR:-/tmp}/aetherserve-miniinfer.XXXXXX")
ROUTER_PID=""
WORKER_PIDS=()
SUCCESS=0

stop_pid() {
  local pid=$1
  local index
  [[ -z "${pid}" ]] && return 0
  if kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    for ((index = 0; index < 40; ++index)); do
      if ! kill -0 "${pid}" 2>/dev/null; then
        break
      fi
      sleep 0.05
    done
    kill -0 "${pid}" 2>/dev/null && kill -KILL "${pid}" 2>/dev/null || true
  fi
  wait "${pid}" 2>/dev/null || true
}

cleanup() {
  local pid
  for pid in "${WORKER_PIDS[@]:-}"; do
    stop_pid "${pid}"
  done
  stop_pid "${ROUTER_PID}"
  if [[ "${SUCCESS}" -eq 1 && "${KEEP_E2E_LOGS:-0}" != "1" ]]; then
    rm -rf "${TMPDIR_E2E}"
  else
    echo "e2e logs retained at ${TMPDIR_E2E}" >&2
  fi
}
trap cleanup EXIT INT TERM

fail() {
  echo "e2e-miniinfer: $*" >&2
  exit 1
}

wait_for() {
  local description=$1
  local command=$2
  local attempts=${3:-100}
  local index
  for ((index = 0; index < attempts; ++index)); do
    if eval "${command}"; then
      return 0
    fi
    sleep 0.05
  done
  fail "timed out waiting for ${description}"
}

wait_for_not_ready() {
  local attempts=${1:-100}
  local index
  for ((index = 0; index < attempts; ++index)); do
    if ! curl -fsS "${HTTP_URL}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  fail "router remained ready after worker expiration"
}

stop_stack() {
  local pid
  for pid in "${WORKER_PIDS[@]:-}"; do
    stop_pid "${pid}"
  done
  stop_pid "${ROUTER_PID}"
  ROUTER_PID=""
  WORKER_PIDS=()
}

write_router_config() {
  local policy=$1
  cat >"${TMPDIR_E2E}/router.yaml" <<EOF
http_address: "${HTTP_ADDRESS}"
control_address: "${CONTROL_ADDRESS}"
model: "mock-llm"
default_max_output_tokens: 8
max_output_tokens: 32
max_input_tokens: 2048
max_context_tokens: 4096
request_timeout: "5s"
min_request_timeout: "100ms"
max_request_timeout: "10s"
heartbeat_stale_after: "300ms"
sse_buffer_size: 16
slow_client_timeout: "2s"
shutdown_timeout: "2s"
routing:
  policy: "${policy}"
  round_robin_seed: 0
admission:
  global_inflight_tokens: 65536
  tenant_rate_per_second: 65536
  tenant_burst_tokens: 65536
EOF
}

write_worker_config() {
  local path=$1
  local worker_id=$2
  local address=$3
  local fail_before=$4
  local fail_after=$5
  local slowdown=$6
  cat >"${path}" <<EOF
worker_id=${worker_id}
listen_address=${address}
advertised_address=${address}
router_control_address=${CONTROL_ADDRESS}
model=mock-llm
heartbeat_interval_ms=50
reconnect_backoff_ms=25
output_queue_capacity=32
command_queue_capacity=128
max_tracked_requests=256
slow_consumer_timeout_ms=1000
max_batched_tokens=64
max_sequences=16
prefill_chunk_tokens=16
aging_interval_iterations=8
kv_block_tokens=16
kv_total_blocks=128
decode_scheduling_quantum_tokens=1
scheduling_policy=continuous
enable_prefix_cache=true
enable_preemption=true
seed=1
prefill_tokens_per_second=1000
decode_tokens_per_second=100
failure_fail_before_first_count=${fail_before}
failure_fail_after_tokens=${fail_after}
failure_slowdown_multiplier=${slowdown}
EOF
}

start_worker() {
  local worker_id=$1
  local address=$2
  local fail_before=$3
  local fail_after=$4
  local slowdown=$5
  local config="${TMPDIR_E2E}/${worker_id}.cfg"
  write_worker_config "${config}" "${worker_id}" "${address}" "${fail_before}" "${fail_after}" "${slowdown}"
  "${MINIINFER_BUILD_DIR}/miniinfer_worker" -config "${config}" >"${TMPDIR_E2E}/${worker_id}.log" 2>&1 &
  WORKER_PIDS+=("$!")
}

start_router() {
  "${TMPDIR_E2E}/router" -config "${TMPDIR_E2E}/router.yaml" >"${TMPDIR_E2E}/router.log" 2>&1 &
  ROUTER_PID=$!
}

start_stack() {
  local policy=$1
  shift
  stop_stack
  write_router_config "${policy}"
  while (( $# > 0 )); do
    start_worker "$1" "$2" "$3" "$4" "$5"
    shift 5
  done
  start_router
  wait_for "router readiness" "curl -fsS '${HTTP_URL}/readyz' >/dev/null 2>&1" 160
}

stream_request() {
  local output=$1
  local status
  status=$(curl --no-buffer --silent --show-error -o "${output}" -w '%{http_code}' \
    -H 'content-type: application/json' \
    -H 'x-aether-tenant-id: integration' \
    -d '{"model":"mock-llm","stream":true,"max_tokens":4,"messages":[{"role":"user","content":"one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen"}]}' \
    "${HTTP_URL}/v1/chat/completions") || fail "curl request failed"
  printf '%s' "${status}"
}

assert_success_stream() {
  local output=$1
  [[ $(grep -c '^event: token$' "${output}") -eq 4 ]] || fail "expected four token SSE events"
  [[ $(grep -c '^event: completion$' "${output}") -eq 1 ]] || fail "expected one completion SSE event"
  [[ $(grep -c '^data: \[DONE\]$' "${output}") -eq 1 ]] || fail "expected one [DONE] marker"
  ! grep -q '^event: error$' "${output}" || fail "successful stream contained SSE error"
}

echo "Building AetherServe router and MiniInfer worker"
(cd "${ROOT}" && go build -o "${TMPDIR_E2E}/router" ./cmd/router)
cmake -S "${MINIINFER_ROOT}" -B "${MINIINFER_BUILD_DIR}" \
  -DMINIINFER_BUILD_SERVICE=ON \
  -DCMAKE_PREFIX_PATH="/opt/homebrew/opt/grpc;/opt/homebrew/opt/protobuf"
cmake --build "${MINIINFER_BUILD_DIR}" --target miniinfer_worker -j

echo "[1/5] happy path"
start_stack round_robin worker-a "${WORKER_ONE_ADDRESS}" 0 0 1
[[ $(stream_request "${TMPDIR_E2E}/happy.sse") == 200 ]] || fail "happy path did not return HTTP 200"
assert_success_stream "${TMPDIR_E2E}/happy.sse"

echo "[2/5] client cancellation"
start_stack round_robin worker-a "${WORKER_ONE_ADDRESS}" 0 0 50
curl --no-buffer --silent --show-error \
  -H 'content-type: application/json' \
  -d '{"model":"mock-llm","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"cancel this request"}]}' \
  "${HTTP_URL}/v1/chat/completions" >"${TMPDIR_E2E}/cancel.sse" 2>"${TMPDIR_E2E}/cancel.err" &
CANCEL_PID=$!
wait_for "first cancellation token" "grep -q '^event: token$' '${TMPDIR_E2E}/cancel.sse'" 160
kill "${CANCEL_PID}" 2>/dev/null || true
wait "${CANCEL_PID}" 2>/dev/null || true
[[ $(stream_request "${TMPDIR_E2E}/after-cancel.sse") == 200 ]] || fail "worker did not recover after client cancellation"
assert_success_stream "${TMPDIR_E2E}/after-cancel.sse"

echo "[3/5] heartbeat expiration"
start_stack round_robin worker-a "${WORKER_ONE_ADDRESS}" 0 0 1
kill -KILL "${WORKER_PIDS[0]}" 2>/dev/null || true
wait "${WORKER_PIDS[0]}" 2>/dev/null || true
wait_for_not_ready 160
[[ $(stream_request "${TMPDIR_E2E}/stale.json") == 503 ]] || fail "stale worker was still routed"
grep -q 'no_healthy_workers' "${TMPDIR_E2E}/stale.json" || fail "stale worker response was not no_healthy_workers"

echo "[4/5] pre-token retry"
start_stack round_robin \
  worker-a "${WORKER_ONE_ADDRESS}" 1 0 1 \
  worker-b "${WORKER_TWO_ADDRESS}" 0 0 1
[[ $(stream_request "${TMPDIR_E2E}/retry.sse") == 200 ]] || fail "pre-token retry did not return HTTP 200"
assert_success_stream "${TMPDIR_E2E}/retry.sse"
grep -q '"attempt_id":".*-a2"' "${TMPDIR_E2E}/router.log" || fail "router did not create attempt a2"

echo "[5/5] post-token failure"
start_stack round_robin worker-a "${WORKER_ONE_ADDRESS}" 0 1 1
[[ $(stream_request "${TMPDIR_E2E}/post-token.sse") == 200 ]] || fail "post-token stream did not commit HTTP 200"
[[ $(grep -c '^event: token$' "${TMPDIR_E2E}/post-token.sse") -eq 1 ]] || fail "post-token failure emitted unexpected token count"
grep -q '^event: error$' "${TMPDIR_E2E}/post-token.sse" || fail "post-token failure did not emit SSE error"
[[ $(grep -c '^data: \[DONE\]$' "${TMPDIR_E2E}/post-token.sse") -eq 1 ]] || fail "post-token failure did not emit [DONE]"
! grep -q '"attempt_id":".*-a2"' "${TMPDIR_E2E}/router.log" || fail "router retried after output commitment"

SUCCESS=1
echo "AetherServe ↔ MiniInfer integration scenarios passed"
