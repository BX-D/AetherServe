// Command loadgen runs reproducible mock-wall-clock or deterministic simulation workloads.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aetherserve/aetherserve/internal/model"
	"github.com/aetherserve/aetherserve/internal/prefix"
	"github.com/aetherserve/aetherserve/internal/routing"
	"github.com/aetherserve/aetherserve/internal/tokenizer"
)

type workItem struct {
	ArrivalMS int64     `json:"arrival_ms"`
	TenantID  string    `json:"tenant_id"`
	Messages  []message `json:"messages"`
	MaxTokens int       `json:"max_tokens"`
	Fault     string    `json:"fault,omitempty"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type sample struct {
	Request   int     `json:"request"`
	TTFTMS    float64 `json:"ttft_ms"`
	LatencyMS float64 `json:"latency_ms"`
	Status    int     `json:"status"`
	Error     string  `json:"error,omitempty"`
}

type result struct {
	Kind         string    `json:"kind"`
	Workload     string    `json:"workload"`
	Policy       string    `json:"policy"`
	Seed         uint64    `json:"seed"`
	StartedAt    time.Time `json:"started_at"`
	Requests     int       `json:"requests"`
	Completed    int       `json:"completed"`
	Failed       int       `json:"failed"`
	Throughput   float64   `json:"throughput_requests_per_second"`
	TTFTP50MS    float64   `json:"ttft_p50_ms"`
	TTFTP95MS    float64   `json:"ttft_p95_ms"`
	LatencyP50MS float64   `json:"latency_p50_ms"`
	LatencyP95MS float64   `json:"latency_p95_ms"`
	Samples      []sample  `json:"samples"`
}

func main() {
	mode := flag.String("mode", "deterministic_simulation", "wall_clock_mock or deterministic_simulation")
	workload := flag.String("workload", "uniform", "workload name")
	requests := flag.Int("requests", 100, "number of requests")
	seed := flag.Uint64("seed", 1, "deterministic seed")
	policyName := flag.String("policy", "predicted_ttft", "routing policy for deterministic simulation metadata")
	router := flag.String("router", "http://127.0.0.1:8080", "router base URL")
	trace := flag.String("trace", "", "optional JSONL workload trace")
	output := flag.String("output", "benchmark/results.json", "JSON output path")
	csvPath := flag.String("csv", "", "optional CSV sample output path")
	flag.Parse()

	items, err := workloadItems(*workload, *requests, *seed, *trace)
	if err != nil {
		fatal(err)
	}
	policy := routing.Policy(*policyName)
	var simulator *routingSimulator
	if *mode == "deterministic_simulation" {
		simulator, err = newRoutingSimulator(policy, *seed)
		if err != nil {
			fatal(err)
		}
	}
	start := time.Now().UTC()
	run := result{Kind: *mode, Workload: *workload, Policy: string(policy), Seed: *seed, StartedAt: start, Requests: len(items)}
	for index, item := range items {
		var s sample
		switch *mode {
		case "deterministic_simulation":
			s = simulator.run(index, item)
		case "wall_clock_mock":
			s = execute(context.Background(), *router, index, item)
		default:
			fatal(fmt.Errorf("unknown mode %q", *mode))
		}
		run.Samples = append(run.Samples, s)
		if s.Error == "" && s.Status >= 200 && s.Status < 300 {
			run.Completed++
		} else {
			run.Failed++
		}
	}
	elapsed := time.Since(start).Seconds()
	if elapsed > 0 {
		run.Throughput = float64(run.Completed) / elapsed
	}
	run.TTFTP50MS, run.TTFTP95MS = percentiles(run.Samples, func(s sample) float64 { return s.TTFTMS })
	run.LatencyP50MS, run.LatencyP95MS = percentiles(run.Samples, func(s sample) float64 { return s.LatencyMS })
	if err := writeJSON(*output, run); err != nil {
		fatal(err)
	}
	if *csvPath != "" {
		if err := writeCSV(*csvPath, run.Samples); err != nil {
			fatal(err)
		}
	}
}

func workloadItems(kind string, count int, seed uint64, tracePath string) ([]workItem, error) {
	if tracePath != "" {
		file, err := os.Open(tracePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		var items []workItem
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var item workItem
			if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
				return nil, fmt.Errorf("decode trace: %w", err)
			}
			items = append(items, item)
		}
		return items, scanner.Err()
	}
	if count <= 0 {
		return nil, fmt.Errorf("requests must be positive")
	}
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	items := make([]workItem, count)
	for i := range items {
		prompt := "request"
		switch kind {
		case "uniform":
			prompt = fmt.Sprintf("request-%d", i)
		case "high_prefix_sharing":
			prompt = strings.Repeat("shared system prompt and document ", 5)
		case "hot_prefix":
			if random.IntN(10) < 8 {
				prompt = strings.Repeat("hot prefix prompt ", 8)
			} else {
				prompt = fmt.Sprintf("cold-%d", i)
			}
		case "mixed_lengths":
			prompt = strings.Repeat("mixed ", 1+random.IntN(64))
		case "mixed_outputs":
			prompt = "mixed output prompt"
		case "heterogeneous_workers":
			prompt = "heterogeneous worker routing prompt"
		case "slowdown":
			prompt = "slowdown injection prompt"
		case "failure_before_first":
			prompt = "pre-first-token failure prompt"
		case "failure_during_streaming":
			prompt = "midstream failure prompt"
		case "bursty":
			prompt = "bursty shared prompt"
		default:
			return nil, fmt.Errorf("unknown workload %q", kind)
		}
		maxTokens := 8 + random.IntN(24)
		if kind == "mixed_outputs" {
			maxTokens = 1 + random.IntN(64)
		}
		fault := ""
		if kind == "slowdown" {
			fault = "slowdown"
		}
		if kind == "failure_before_first" {
			fault = "before_first"
		}
		if kind == "failure_during_streaming" {
			fault = "midstream"
		}
		items[i] = workItem{ArrivalMS: int64(i * 5), TenantID: "benchmark", MaxTokens: maxTokens, Fault: fault,
			Messages: []message{{Role: "user", Content: fmt.Sprintf("%s %d", prompt, i)}}}
	}
	return items, nil
}

type routingSimulator struct {
	router  *routing.Router
	workers []model.Worker
}

func newRoutingSimulator(policy routing.Policy, seed uint64) (*routingSimulator, error) {
	router, err := routing.New(policy, seed)
	if err != nil {
		return nil, err
	}
	workers := []model.Worker{
		{ID: "sim-a", DataAddress: "sim-a", Model: "mock-llm", ProtocolVersion: model.ProtocolVersion, State: model.WorkerState{StatusVersion: 1, Status: model.WorkerHealthy, KVUsage: model.KVUsage{CapacityTokens: 8192}, PrefillTokensPerSecond: 1200, DecodeTokensPerSecond: 120, DecodeSchedulingQuantumTokens: 1}},
		{ID: "sim-b", DataAddress: "sim-b", Model: "mock-llm", ProtocolVersion: model.ProtocolVersion, State: model.WorkerState{StatusVersion: 1, Status: model.WorkerHealthy, KVUsage: model.KVUsage{CapacityTokens: 8192}, PrefillTokensPerSecond: 800, DecodeTokensPerSecond: 80, DecodeSchedulingQuantumTokens: 1}},
		{ID: "sim-c", DataAddress: "sim-c", Model: "mock-llm", ProtocolVersion: model.ProtocolVersion, State: model.WorkerState{StatusVersion: 1, Status: model.WorkerHealthy, KVUsage: model.KVUsage{CapacityTokens: 8192}, PrefillTokensPerSecond: 500, DecodeTokensPerSecond: 50, DecodeSchedulingQuantumTokens: 1}},
	}
	return &routingSimulator{router: router, workers: workers}, nil
}

func (s *routingSimulator) run(index int, item workItem) sample {
	messages := make([]model.Message, 0, len(item.Messages))
	for _, message := range item.Messages {
		messages = append(messages, model.Message{Role: message.Role, Content: message.Content})
	}
	tokens := tokenizer.Tokens(messages)
	for workerIndex := range s.workers {
		worker := &s.workers[workerIndex]
		worker.State.WaitingTokenCount = uint64((index*(workerIndex+3) + workerIndex*11) % 48)
		worker.State.RunningWorkTokens = uint64((index + workerIndex*5) % 20)
		worker.State.RunningRequestCount = uint32((index + workerIndex) % 3)
		worker.State.StatusVersion++
	}
	request := model.Request{ID: fmt.Sprintf("sim-%d", index), Model: "mock-llm", TenantID: item.TenantID, Messages: messages,
		EstimatedInputToken: tokenizer.Estimate(messages), ExpectedOutputToken: uint64(item.MaxTokens), MaxOutputTokens: uint64(item.MaxTokens), PrefixFingerprints: prefix.Fingerprints(tokens)}
	decision, err := s.router.Select(request, s.workers, nil)
	if err != nil {
		return sample{Request: index, Status: http.StatusServiceUnavailable, Error: err.Error()}
	}
	if item.Fault == "before_first" {
		return sample{Request: index, Status: http.StatusBadGateway, Error: "simulated pre-first-token failure"}
	}
	selected := -1
	var candidate model.CandidateEvaluation
	for i := range s.workers {
		if s.workers[i].ID == decision.SelectedWorker {
			selected = i
			break
		}
	}
	for _, evaluation := range decision.Candidates {
		if evaluation.WorkerID == decision.SelectedWorker {
			candidate = evaluation
			break
		}
	}
	if selected < 0 {
		return sample{Request: index, Status: http.StatusServiceUnavailable, Error: "selected worker disappeared"}
	}
	worker := &s.workers[selected]
	worker.State.CachedPrefixes = appendUniquePrefixes(worker.State.CachedPrefixes, request.PrefixFingerprints)
	ttft := candidate.PredictedTTFT * 1000
	latency := ttft + float64(item.MaxTokens)/worker.State.DecodeTokensPerSecond*1000
	if item.Fault == "midstream" {
		return sample{Request: index, TTFTMS: ttft, LatencyMS: latency, Status: http.StatusOK, Error: "simulated midstream failure"}
	}
	return sample{Request: index, TTFTMS: ttft, LatencyMS: latency, Status: http.StatusOK}
}

func appendUniquePrefixes(current, additions []model.PrefixFingerprint) []model.PrefixFingerprint {
	seen := make(map[string]struct{}, len(current)+len(additions))
	for _, fingerprint := range current {
		seen[fingerprint.SHA256] = struct{}{}
	}
	for _, fingerprint := range additions {
		if _, exists := seen[fingerprint.SHA256]; !exists {
			current = append(current, fingerprint)
			seen[fingerprint.SHA256] = struct{}{}
		}
	}
	if len(current) > 64 {
		return append([]model.PrefixFingerprint(nil), current[len(current)-64:]...)
	}
	return current
}

func execute(parent context.Context, base string, index int, item workItem) sample {
	start := time.Now()
	payload := map[string]any{"model": "mock-llm", "messages": item.Messages, "stream": true, "max_tokens": item.MaxTokens}
	body, _ := json.Marshal(payload)
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return sample{Request: index, Status: 0, Error: err.Error()}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Aether-Tenant-ID", item.TenantID)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return sample{Request: index, Status: 0, Error: err.Error()}
	}
	defer response.Body.Close()
	s := sample{Request: index, Status: response.StatusCode}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		s.Error = response.Status
		return s
	}
	scanner := bufio.NewScanner(response.Body)
	first := time.Time{}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "event: token" && first.IsZero() {
			first = time.Now()
		}
		if line == "data: [DONE]" {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		s.Error = err.Error()
		return s
	}
	if first.IsZero() {
		s.Error = "stream had no token event"
		return s
	}
	s.TTFTMS = float64(first.Sub(start).Microseconds()) / 1000
	s.LatencyMS = float64(time.Since(start).Microseconds()) / 1000
	return s
}

func percentiles(samples []sample, value func(sample) float64) (float64, float64) {
	values := make([]float64, 0, len(samples))
	for _, s := range samples {
		if s.Error == "" && s.Status >= 200 && s.Status < 300 {
			values = append(values, value(s))
		}
	}
	if len(values) == 0 {
		return 0, 0
	}
	sort.Float64s(values)
	at := func(percent float64) float64 {
		index := int(math.Ceil(percent*float64(len(values)))) - 1
		if index < 0 {
			index = 0
		}
		return values[index]
	}
	return at(.50), at(.95)
}

func writeJSON(path string, output result) error {
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(output)
}

func writeCSV(path string, samples []sample) error {
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"request", "ttft_ms", "latency_ms", "status", "error"}); err != nil {
		return err
	}
	for _, s := range samples {
		if err := writer.Write([]string{fmt.Sprint(s.Request), fmt.Sprint(s.TTFTMS), fmt.Sprint(s.LatencyMS), fmt.Sprint(s.Status), s.Error}); err != nil {
			return err
		}
	}
	return nil
}

func filepathDir(path string) string {
	index := strings.LastIndex(path, "/")
	if index < 0 {
		return "."
	}
	return path[:index]
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "loadgen:", err)
	os.Exit(1)
}
