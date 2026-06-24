#!/usr/bin/env sh
# Generates checked-in Go bindings from the language-neutral proto source.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tools="$root/.tools"
protoc="$tools/protoc/bin/protoc"

if [ ! -x "$protoc" ] || [ ! -x "$tools/bin/protoc-gen-go" ] || [ ! -x "$tools/bin/protoc-gen-go-grpc" ]; then
  echo "local protocol tools are missing; run: make tools" >&2
  exit 1
fi

PATH="$tools/bin:$PATH" "$protoc" \
  --proto_path="$root/api/proto" \
  --proto_path="$tools/protoc/include" \
  --go_out="$root/api/gen" \
  --go_opt=paths=source_relative \
  --go-grpc_out="$root/api/gen" \
  --go-grpc_opt=paths=source_relative \
  "$root/api/proto/aetherserve/v1/worker.proto"

