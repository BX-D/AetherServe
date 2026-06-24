package routing

import (
	"testing"

	"github.com/aetherserve/aetherserve/internal/model"
)

func worker(id string, waiting uint64, cached []model.PrefixFingerprint) model.Worker {
	return model.Worker{
		ID: id, DataAddress: id + ":1", Model: "mock-llm", ProtocolVersion: model.ProtocolVersion,
		State: model.WorkerState{StatusVersion: 1, Status: model.WorkerHealthy, WaitingTokenCount: waiting,
			KVUsage: model.KVUsage{CapacityTokens: 100}, PrefillTokensPerSecond: 100, DecodeTokensPerSecond: 50,
			DecodeSchedulingQuantumTokens: 1, CachedPrefixes: cached},
	}
}

func TestPoliciesAreDeterministicAndExcludeStale(t *testing.T) {
	prefix := model.PrefixFingerprint{TokenCount: 16, SHA256: "same"}
	request := model.Request{ID: "request", Model: "mock-llm", EstimatedInputToken: 32, PrefixFingerprints: []model.PrefixFingerprint{prefix}}
	workers := []model.Worker{worker("b", 0, nil), worker("a", 4, []model.PrefixFingerprint{prefix})}
	workers[1].Stale = true
	for _, policy := range []Policy{RoundRobin, LeastWaitingTokens, PrefixAffinity, PredictedTTFT} {
		router, err := New(policy, 0)
		if err != nil {
			t.Fatal(err)
		}
		decision, err := router.Select(request, workers, nil)
		if err != nil {
			t.Fatalf("%s: %v", policy, err)
		}
		if decision.SelectedWorker != "b" {
			t.Fatalf("%s selected %q, want b", policy, decision.SelectedWorker)
		}
		if decision.Candidates[0].RejectionReason != "stale_heartbeat" {
			t.Fatalf("%s did not record stale rejection: %#v", policy, decision.Candidates)
		}
	}
}

func TestPrefixAffinityPrefersSavedPrefillWhenQueueAllows(t *testing.T) {
	prefix := model.PrefixFingerprint{TokenCount: 16, SHA256: "same"}
	request := model.Request{ID: "request", Model: "mock-llm", EstimatedInputToken: 32, PrefixFingerprints: []model.PrefixFingerprint{prefix}}
	router, _ := New(PrefixAffinity, 0)
	decision, err := router.Select(request, []model.Worker{worker("a", 0, []model.PrefixFingerprint{prefix}), worker("b", 0, nil)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedWorker != "a" {
		t.Fatalf("selected %q, want a", decision.SelectedWorker)
	}
}
