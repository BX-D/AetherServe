.PHONY: all fmt test race vet build tools proto benchmark

TOOLS_DIR := $(CURDIR)/.tools
PROTOC := $(TOOLS_DIR)/protoc/bin/protoc

all: fmt test vet build

fmt:
	go fmt ./...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

build:
	go build ./...

tools:
	scripts/install-tools.sh

proto: tools
	scripts/generate-proto.sh

benchmark:
	go run ./cmd/loadgen -mode deterministic_simulation -workload uniform -requests 100 -output benchmark/results.json

