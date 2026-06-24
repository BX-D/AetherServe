// Package routing contains deterministic worker selection policies.
package routing

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/aetherserve/aetherserve/internal/model"
	"github.com/aetherserve/aetherserve/internal/prefix"
)

type Policy string

const (
	RoundRobin         Policy = "round_robin"
	LeastWaitingTokens Policy = "least_waiting_tokens"
	PrefixAffinity     Policy = "prefix_affinity"
	PredictedTTFT      Policy = "predicted_ttft"
)

var ErrNoEligibleWorkers = errors.New("no eligible workers")

type Router struct {
	policy Policy
	cursor atomic.Uint64
}

func New(policy Policy, seed uint64) (*Router, error) {
	switch policy {
	case RoundRobin, LeastWaitingTokens, PrefixAffinity, PredictedTTFT:
	default:
		return nil, fmt.Errorf("unknown routing policy %q", policy)
	}
	r := &Router{policy: policy}
	r.cursor.Store(seed)
	return r, nil
}

func (r *Router) Policy() Policy { return r.policy }

func (r *Router) Select(request model.Request, workers []model.Worker, excluded map[string]struct{}) (model.RoutingDecision, error) {
	start := time.Now()
	decision := model.RoutingDecision{
		RequestID: request.ID, Policy: string(r.policy), WorkerVersions: make(map[string]uint64),
	}
	sorted := append([]model.Worker(nil), workers...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	type candidate struct {
		worker model.Worker
		trace  model.CandidateEvaluation
	}
	eligible := make([]candidate, 0, len(sorted))
	for _, worker := range sorted {
		decision.WorkerVersions[worker.ID] = worker.State.StatusVersion
		trace := model.CandidateEvaluation{WorkerID: worker.ID, StateVersion: worker.State.StatusVersion}
		if reason := r.ineligibility(request, worker, excluded); reason != "" {
			trace.RejectionReason = reason
			decision.Candidates = append(decision.Candidates, trace)
			continue
		}
		trace.Eligible = true
		trace.MatchedTokens = prefix.LongestMatch(request.PrefixFingerprints, worker.State.CachedPrefixes)
		trace.QueueDelay = float64(worker.State.WaitingTokenCount+worker.State.RunningWorkTokens) / worker.State.PrefillTokensPerSecond
		input := request.EstimatedInputToken
		if uint64(trace.MatchedTokens) < input {
			trace.UncachedPrefill = input - uint64(trace.MatchedTokens)
		}
		trace.PrefillDelay = float64(trace.UncachedPrefill) / worker.State.PrefillTokensPerSecond
		trace.OverloadPenalty = float64(worker.State.RunningRequestCount*worker.State.DecodeSchedulingQuantumTokens) / worker.State.DecodeTokensPerSecond
		trace.PredictedTTFT = trace.QueueDelay + trace.PrefillDelay + trace.OverloadPenalty
		eligible = append(eligible, candidate{worker: worker, trace: trace})
	}
	if len(eligible) == 0 {
		decision.DecisionTime = time.Since(start)
		return decision, ErrNoEligibleWorkers
	}

	selected := 0
	switch r.policy {
	case RoundRobin:
		cursor := r.cursor.Add(1) - 1
		decision.Cursor = cursor
		selected = int(cursor % uint64(len(eligible)))
		for i := range eligible {
			eligible[i].trace.Score = float64(i)
		}
		eligible[selected].trace.TieBreak = "round_robin_cursor"
	case LeastWaitingTokens:
		for i := range eligible {
			eligible[i].trace.Score = float64(eligible[i].worker.State.WaitingTokenCount)
			if i > 0 && less(eligible[i].trace.Score, eligible[i].trace.MatchedTokens, eligible[i].worker.ID, eligible[selected].trace.Score, eligible[selected].trace.MatchedTokens, eligible[selected].worker.ID, false) {
				selected = i
			}
		}
	case PrefixAffinity:
		for i := range eligible {
			eligible[i].trace.Score = eligible[i].trace.QueueDelay + eligible[i].trace.PrefillDelay
			if i > 0 && less(eligible[i].trace.Score, eligible[i].trace.MatchedTokens, eligible[i].worker.ID, eligible[selected].trace.Score, eligible[selected].trace.MatchedTokens, eligible[selected].worker.ID, true) {
				selected = i
			}
		}
	case PredictedTTFT:
		for i := range eligible {
			eligible[i].trace.Score = eligible[i].trace.PredictedTTFT
			if i > 0 && less(eligible[i].trace.Score, eligible[i].trace.MatchedTokens, eligible[i].worker.ID, eligible[selected].trace.Score, eligible[selected].trace.MatchedTokens, eligible[selected].worker.ID, true) {
				selected = i
			}
		}
	}
	for _, item := range eligible {
		if item.worker.ID == eligible[selected].worker.ID && item.trace.TieBreak == "" {
			item.trace.TieBreak = "minimum_score"
		}
		decision.Candidates = append(decision.Candidates, item.trace)
	}
	decision.SelectedWorker = eligible[selected].worker.ID
	decision.DecisionTime = time.Since(start)
	return decision, nil
}

func (r *Router) ineligibility(request model.Request, worker model.Worker, excluded map[string]struct{}) string {
	if _, blocked := excluded[worker.ID]; blocked {
		return "excluded_attempt_worker"
	}
	if worker.Stale {
		return "stale_heartbeat"
	}
	if worker.ProtocolVersion != model.ProtocolVersion {
		return "protocol_mismatch"
	}
	if worker.DataAddress == "" || worker.Model != request.Model {
		return "incompatible_worker"
	}
	if worker.State.Status != model.WorkerHealthy {
		return "unhealthy"
	}
	if worker.State.KVUsage.CapacityTokens == 0 || worker.State.KVUsage.UsedTokens > worker.State.KVUsage.CapacityTokens {
		return "invalid_kv_metadata"
	}
	if r.policy != RoundRobin && (worker.State.PrefillTokensPerSecond <= 0 || worker.State.DecodeTokensPerSecond <= 0 || worker.State.DecodeSchedulingQuantumTokens == 0) {
		return "missing_metric"
	}
	return ""
}

func less(score float64, matched uint32, id string, bestScore float64, bestMatched uint32, bestID string, preferPrefix bool) bool {
	if math.Abs(score-bestScore) > 1e-12 {
		return score < bestScore
	}
	if preferPrefix && matched != bestMatched {
		return matched > bestMatched
	}
	return id < bestID
}
