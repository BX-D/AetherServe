// Package registry owns mutable worker state and creates immutable snapshots.
package registry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aetherserve/aetherserve/internal/model"
)

var (
	ErrUnknownWorker          = errors.New("unknown worker")
	ErrStateVersionRegression = errors.New("worker state version regression")
)

type Registry struct {
	mu         sync.RWMutex
	workers    map[string]model.Worker
	staleAfter time.Duration
	now        func() time.Time
}

func New(staleAfter time.Duration, now func() time.Time) (*Registry, error) {
	if staleAfter <= 0 {
		return nil, fmt.Errorf("stale timeout must be positive")
	}
	if now == nil {
		now = time.Now
	}
	return &Registry{workers: make(map[string]model.Worker), staleAfter: staleAfter, now: now}, nil
}

func (r *Registry) Register(worker model.Worker) error {
	if err := validate(worker); err != nil {
		return err
	}
	now := r.now().UTC()
	worker.LastHeartbeat = now
	worker.Stale = false
	worker = cloneWorker(worker)
	r.mu.Lock()
	r.workers[worker.ID] = worker
	r.mu.Unlock()
	return nil
}

func (r *Registry) Heartbeat(workerID string, state model.WorkerState) error {
	if err := validateState(state); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	worker, ok := r.workers[workerID]
	if !ok {
		return ErrUnknownWorker
	}
	if state.StatusVersion <= worker.State.StatusVersion {
		return ErrStateVersionRegression
	}
	worker.State = cloneState(state)
	worker.LastHeartbeat = r.now().UTC()
	worker.Stale = false
	r.workers[workerID] = worker
	return nil
}

// Snapshot returns copied, sorted state. It never changes registry entries.
func (r *Registry) Snapshot() []model.Worker {
	now := r.now()
	r.mu.RLock()
	workers := make([]model.Worker, 0, len(r.workers))
	for _, worker := range r.workers {
		copy := cloneWorker(worker)
		copy.Stale = now.Sub(copy.LastHeartbeat) > r.staleAfter
		workers = append(workers, copy)
	}
	r.mu.RUnlock()
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers
}

func (r *Registry) HealthyCount() int {
	count := 0
	for _, worker := range r.Snapshot() {
		if !worker.Stale && worker.State.Status == model.WorkerHealthy {
			count++
		}
	}
	return count
}

func validate(worker model.Worker) error {
	if worker.ID == "" || worker.DataAddress == "" || worker.Model == "" {
		return fmt.Errorf("worker id, data address, and model are required")
	}
	if worker.ProtocolVersion != model.ProtocolVersion {
		return fmt.Errorf("unsupported worker protocol %q", worker.ProtocolVersion)
	}
	return validateState(worker.State)
}

func validateState(state model.WorkerState) error {
	if state.StatusVersion == 0 {
		return fmt.Errorf("worker state version must be positive")
	}
	switch state.Status {
	case model.WorkerHealthy, model.WorkerDraining, model.WorkerUnhealthy:
	default:
		return fmt.Errorf("invalid worker status %q", state.Status)
	}
	if state.KVUsage.CapacityTokens == 0 || state.KVUsage.UsedTokens > state.KVUsage.CapacityTokens {
		return fmt.Errorf("invalid KV usage")
	}
	if state.PrefillTokensPerSecond <= 0 || state.DecodeTokensPerSecond <= 0 {
		return fmt.Errorf("worker rates must be positive")
	}
	if state.DecodeSchedulingQuantumTokens == 0 {
		return fmt.Errorf("decode scheduling quantum must be positive")
	}
	for _, cached := range state.CachedPrefixes {
		if cached.TokenCount == 0 || cached.SHA256 == "" {
			return fmt.Errorf("invalid cached prefix metadata")
		}
	}
	return nil
}

func cloneWorker(worker model.Worker) model.Worker {
	worker.State = cloneState(worker.State)
	return worker
}

func cloneState(state model.WorkerState) model.WorkerState {
	state.CachedPrefixes = append([]model.PrefixFingerprint(nil), state.CachedPrefixes...)
	return state
}
