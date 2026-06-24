package registry

import (
	"errors"
	"testing"
	"time"

	"github.com/aetherserve/aetherserve/internal/model"
)

func validWorker() model.Worker {
	return model.Worker{
		ID: "worker-a", DataAddress: "127.0.0.1:9000", Model: "mock-llm", ProtocolVersion: model.ProtocolVersion,
		State: model.WorkerState{
			StatusVersion: 1, Status: model.WorkerHealthy, KVUsage: model.KVUsage{CapacityTokens: 100},
			PrefillTokensPerSecond: 100, DecodeTokensPerSecond: 50, DecodeSchedulingQuantumTokens: 1,
		},
	}
}

func TestSnapshotStalenessAndRecovery(t *testing.T) {
	now := time.Unix(100, 0)
	reg, err := New(time.Second, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(validWorker()); err != nil {
		t.Fatal(err)
	}
	snapshot := reg.Snapshot()
	if snapshot[0].Stale {
		t.Fatal("fresh worker is stale")
	}
	now = now.Add(2 * time.Second)
	if !reg.Snapshot()[0].Stale {
		t.Fatal("expired worker is eligible")
	}

	state := validWorker().State
	state.StatusVersion = 2
	if err := reg.Heartbeat("worker-a", state); err != nil {
		t.Fatal(err)
	}
	if reg.Snapshot()[0].Stale {
		t.Fatal("valid heartbeat did not recover worker")
	}
}

func TestHeartbeatDoesNotRegressOrMutateSnapshot(t *testing.T) {
	reg, err := New(time.Minute, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(validWorker()); err != nil {
		t.Fatal(err)
	}
	state := validWorker().State
	if err := reg.Heartbeat("worker-a", state); !errors.Is(err, ErrStateVersionRegression) {
		t.Fatalf("heartbeat error = %v, want regression", err)
	}
	snapshot := reg.Snapshot()
	snapshot[0].State.CachedPrefixes = append(snapshot[0].State.CachedPrefixes, model.PrefixFingerprint{TokenCount: 16, SHA256: "x"})
	if len(reg.Snapshot()[0].State.CachedPrefixes) != 0 {
		t.Fatal("snapshot mutation changed registry state")
	}
}
