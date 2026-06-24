package protocol

import (
	"testing"
	"time"

	aetherservev1 "github.com/aetherserve/aetherserve/api/gen/aetherserve/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStateFromProtoRequiresObservedTimestamp(t *testing.T) {
	state := &aetherservev1.WorkerState{
		StatusVersion:                 1,
		Status:                        aetherservev1.WorkerStatus_WORKER_STATUS_HEALTHY,
		KvUsage:                       &aetherservev1.KVUsageMetadata{CapacityTokens: 1},
		PrefillTokensPerSecond:        1,
		DecodeTokensPerSecond:         1,
		DecodeSchedulingQuantumTokens: 1,
	}
	if _, err := StateFromProto(state); err == nil {
		t.Fatal("state without observed_at was accepted")
	}
	state.ObservedAt = timestamppb.New(time.Now())
	if _, err := StateFromProto(state); err != nil {
		t.Fatalf("valid state rejected: %v", err)
	}
}

func TestTimestampFromProtoRequiresValue(t *testing.T) {
	if _, err := TimestampFromProto(nil, "test timestamp"); err == nil {
		t.Fatal("nil timestamp accepted")
	}
}
