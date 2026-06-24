#!/usr/bin/env sh
# Installs the pinned protocol toolchain beneath the repository only.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tools="$root/.tools"
if [ ! -f "$tools/versions.env" ]; then
  echo "missing pinned tool versions: $tools/versions.env" >&2
  exit 1
fi
# shellcheck disable=SC1090
. "$tools/versions.env"
protoc_version="$PROTOC_VERSION"
go_plugin_version="$PROTOC_GEN_GO_VERSION"
grpc_plugin_version="$PROTOC_GEN_GO_GRPC_VERSION"
protoc_root="$tools/protoc"
protoc_bin="$protoc_root/bin/protoc"

mkdir -p "$tools/bin" "$tools/downloads"

if [ ! -x "$protoc_bin" ] || ! "$protoc_bin" --version | grep -q " $protoc_version$"; then
  os=$(uname -s)
  arch=$(uname -m)
  if [ "$os" != "Darwin" ] || [ "$arch" != "arm64" ]; then
    echo "unsupported local protoc platform: $os/$arch (install protoc $protoc_version under $protoc_root)" >&2
    exit 1
  fi
  archive="$tools/downloads/protoc-$protoc_version-osx-aarch_64.zip"
  url="https://github.com/protocolbuffers/protobuf/releases/download/v$protoc_version/protoc-$protoc_version-osx-aarch_64.zip"
  curl --fail --location --retry 3 --output "$archive" "$url"
  rm -rf "$protoc_root"
  mkdir -p "$protoc_root"
  unzip -q "$archive" -d "$protoc_root"
fi

if [ ! -x "$tools/bin/protoc-gen-go" ] || ! "$tools/bin/protoc-gen-go" --version | grep -q " $go_plugin_version$"; then
  GOBIN="$tools/bin" go install "google.golang.org/protobuf/cmd/protoc-gen-go@$go_plugin_version"
fi
if [ ! -x "$tools/bin/protoc-gen-go-grpc" ] || ! "$tools/bin/protoc-gen-go-grpc" --version | grep -q " ${grpc_plugin_version#v}$"; then
  GOBIN="$tools/bin" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$grpc_plugin_version"
fi

"$protoc_bin" --version
"$tools/bin/protoc-gen-go" --version
"$tools/bin/protoc-gen-go-grpc" --version
