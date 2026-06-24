package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aetherserve/aetherserve/internal/admission"
	"github.com/aetherserve/aetherserve/internal/config"
	"github.com/aetherserve/aetherserve/internal/gateway"
	"github.com/aetherserve/aetherserve/internal/mockworker"
	"github.com/aetherserve/aetherserve/internal/routing"
)

func TestHTTPToGRPCToSSE(t *testing.T) {
	server, ctx := startRouter(t, routing.PredictedTTFT)
	_ = startWorker(t, ctx, server, "worker-a", config.FailureConfig{})
	waitFor(t, 2*time.Second, func() bool { return server.HealthyWorkers() == 1 })

	status, events := stream(t, server, 8)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if events.tokens == 0 || !events.completion || !events.done {
		t.Fatalf("incomplete stream: %#v", events)
	}
}

func TestRetriesOnlyBeforeFirstToken(t *testing.T) {
	server, ctx := startRouter(t, routing.RoundRobin)
	_ = startWorker(t, ctx, server, "worker-a", config.FailureConfig{FailBeforeFirstCount: 1, SlowdownMultiplier: 1})
	_ = startWorker(t, ctx, server, "worker-b", config.FailureConfig{SlowdownMultiplier: 1})
	waitFor(t, 2*time.Second, func() bool { return server.HealthyWorkers() == 2 })

	status, events := stream(t, server, 4)
	if status != http.StatusOK || events.tokens == 0 || !events.done {
		t.Fatalf("retry did not stream successfully: status=%d events=%#v", status, events)
	}
}

func TestPreTokenFailureWithoutAlternateReturnsBadGateway(t *testing.T) {
	server, ctx := startRouter(t, routing.RoundRobin)
	_ = startWorker(t, ctx, server, "worker-a", config.FailureConfig{FailBeforeFirstCount: 1, SlowdownMultiplier: 1})
	waitFor(t, 2*time.Second, func() bool { return server.HealthyWorkers() == 1 })

	status, events := stream(t, server, 4)
	if status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; events=%#v", status, events)
	}
	if events.tokens != 0 || events.done {
		t.Fatalf("pre-token failure committed an SSE response: %#v", events)
	}
}

func TestNoRetryAfterStreamingBegins(t *testing.T) {
	server, ctx := startRouter(t, routing.RoundRobin)
	_ = startWorker(t, ctx, server, "worker-a", config.FailureConfig{FailAfterTokens: 1, SlowdownMultiplier: 1})
	waitFor(t, 2*time.Second, func() bool { return server.HealthyWorkers() == 1 })

	status, events := stream(t, server, 4)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if events.tokens != 1 || !events.error || !events.done {
		t.Fatalf("midstream failure did not terminate existing stream: %#v", events)
	}
}

func TestClientCancellationStopsWorker(t *testing.T) {
	server, ctx := startRouter(t, routing.RoundRobin)
	worker := startWorker(t, ctx, server, "worker-a", config.FailureConfig{SlowdownMultiplier: 100})
	waitFor(t, 2*time.Second, func() bool { return server.HealthyWorkers() == 1 })

	payload := []byte("{\"model\":\"mock-llm\",\"stream\":true,\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"cancel me\"}]}")
	request, err := http.NewRequest(http.MethodPost, "http://"+server.HTTPAddress()+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		if scanner.Text() == "event: token" {
			break
		}
	}
	_ = response.Body.Close()
	waitFor(t, 300*time.Millisecond, func() bool { return worker.ActiveRequests() == 0 })
}

type streamEvents struct {
	tokens     int
	completion bool
	error      bool
	done       bool
}

func stream(t *testing.T, server *gateway.Server, maxTokens int) (int, streamEvents) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"model": "mock-llm", "stream": true, "max_tokens": maxTokens,
		"messages": []map[string]string{{"role": "user", "content": "hello from integration test"}},
	})
	response, err := http.Post("http://"+server.HTTPAddress()+"/v1/chat/completions", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	events := streamEvents{}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		switch scanner.Text() {
		case "event: token":
			events.tokens++
		case "event: completion":
			events.completion = true
		case "event: error":
			events.error = true
		case "data: [DONE]":
			events.done = true
		}
	}
	if err := scanner.Err(); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Fatal(err)
	}
	return response.StatusCode, events
}

func startRouter(t *testing.T, policy routing.Policy) (*gateway.Server, context.Context) {
	t.Helper()
	cfg := config.RouterConfig{
		HTTPAddress: "127.0.0.1:0", ControlAddress: "127.0.0.1:0", Model: "mock-llm",
		DefaultMaxOutputTokens: 8, MaxOutputTokens: 32, MaxInputTokens: 1024, MaxContextTokens: 2048,
		RequestTimeout: config.Duration(2 * time.Second), MinRequestTimeout: config.Duration(time.Millisecond), MaxRequestTimeout: config.Duration(3 * time.Second),
		HeartbeatStaleAfter: config.Duration(500 * time.Millisecond), SSEBufferSize: 4, SlowClientTimeout: config.Duration(time.Second), ShutdownTimeout: config.Duration(2 * time.Second),
		Routing:   config.RoutingConfig{Policy: policy},
		Admission: admission.Config{GlobalInFlightTokens: 100000, TenantRatePerSecond: 100000, TenantBurstTokens: 100000},
	}
	server, err := gateway.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		shutdown, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		if err := server.Close(shutdown); err != nil && err != context.Canceled {
			t.Errorf("close router: %v", err)
		}
	})
	return server, ctx
}

func startWorker(t *testing.T, ctx context.Context, server *gateway.Server, id string, failure config.FailureConfig) *mockworker.Worker {
	t.Helper()
	if failure.SlowdownMultiplier == 0 {
		failure.SlowdownMultiplier = 1
	}
	cfg := config.WorkerConfig{
		ID: id, DataAddress: "127.0.0.1:0", RouterControlAddress: server.ControlAddress(), Model: "mock-llm",
		HeartbeatInterval: config.Duration(20 * time.Millisecond), QueueCapacity: 8, PrefillTokensPerSecond: 10000,
		DecodeTokensPerSecond: 1000, DecodeSchedulingQuantumTokens: 1, CacheCapacityTokens: 4096, CachePrefixLimit: 32,
		Seed: 1, Failure: failure,
	}
	worker, err := mockworker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Stop)
	return worker
}

func waitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("condition not met within %s", timeout))
}
