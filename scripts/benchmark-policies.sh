#!/usr/bin/env sh
set -eu

out="benchmark/results"
if [ "$#" -gt 0 ]; then
  out="$1"
fi
mkdir -p "$out"
for policy in round_robin least_waiting_tokens prefix_affinity predicted_ttft; do
  go run ./cmd/loadgen -mode deterministic_simulation -workload high_prefix_sharing \
    -policy "$policy" -requests 1000 -seed 42 -output "$out/$policy.json"
done
