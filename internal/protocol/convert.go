// Package protocol converts between the language-neutral generated API and router domain values.
package protocol

import (
	"fmt"
	"time"

	aetherservev1 "github.com/aetherserve/aetherserve/api/gen/aetherserve/v1"
	"github.com/aetherserve/aetherserve/internal/model"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func MessagesToProto(messages []model.Message) []*aetherservev1.ChatMessage {
	result := make([]*aetherservev1.ChatMessage, 0, len(messages))
	for _, message := range messages {
		result = append(result, &aetherservev1.ChatMessage{Role: message.Role, Content: message.Content})
	}
	return result
}

func MessagesFromProto(messages []*aetherservev1.ChatMessage) []model.Message {
	result := make([]model.Message, 0, len(messages))
	for _, message := range messages {
		if message != nil {
			result = append(result, model.Message{Role: message.GetRole(), Content: message.GetContent()})
		}
	}
	return result
}

func PrefixesToProto(prefixes []model.PrefixFingerprint) []*aetherservev1.PrefixFingerprint {
	result := make([]*aetherservev1.PrefixFingerprint, 0, len(prefixes))
	for _, prefix := range prefixes {
		result = append(result, &aetherservev1.PrefixFingerprint{TokenCount: prefix.TokenCount, Sha256: prefix.SHA256})
	}
	return result
}

func PrefixesFromProto(prefixes []*aetherservev1.PrefixFingerprint) []model.PrefixFingerprint {
	result := make([]model.PrefixFingerprint, 0, len(prefixes))
	for _, prefix := range prefixes {
		if prefix != nil {
			result = append(result, model.PrefixFingerprint{TokenCount: prefix.GetTokenCount(), SHA256: prefix.GetSha256()})
		}
	}
	return result
}

func StateToProto(state model.WorkerState) *aetherservev1.WorkerState {
	cached := make([]*aetherservev1.CachedPrefixMetadata, 0, len(state.CachedPrefixes))
	for _, fingerprint := range state.CachedPrefixes {
		cached = append(cached, &aetherservev1.CachedPrefixMetadata{
			Prefix: &aetherservev1.PrefixFingerprint{TokenCount: fingerprint.TokenCount, Sha256: fingerprint.SHA256},
		})
	}
	return &aetherservev1.WorkerState{
		StatusVersion: state.StatusVersion, Status: statusToProto(state.Status),
		WaitingRequestCount: state.WaitingRequestCount, WaitingTokenCount: state.WaitingTokenCount,
		RunningRequestCount: state.RunningRequestCount, RunningWorkTokens: state.RunningWorkTokens,
		KvUsage:                &aetherservev1.KVUsageMetadata{CapacityTokens: state.KVUsage.CapacityTokens, UsedTokens: state.KVUsage.UsedTokens},
		PrefillTokensPerSecond: state.PrefillTokensPerSecond, DecodeTokensPerSecond: state.DecodeTokensPerSecond,
		DecodeSchedulingQuantumTokens: state.DecodeSchedulingQuantumTokens, CachedPrefixes: cached,
		ObservedAt: timestamppb.New(state.ObservedAt),
	}
}

func StateFromProto(state *aetherservev1.WorkerState) (model.WorkerState, error) {
	if state == nil {
		return model.WorkerState{}, fmt.Errorf("missing worker state")
	}
	status, err := statusFromProto(state.GetStatus())
	if err != nil {
		return model.WorkerState{}, err
	}
	observed, err := TimestampFromProto(state.GetObservedAt(), "state observed_at")
	if err != nil {
		return model.WorkerState{}, err
	}
	cached := make([]model.PrefixFingerprint, 0, len(state.GetCachedPrefixes()))
	for _, item := range state.GetCachedPrefixes() {
		if item == nil || item.GetPrefix() == nil {
			return model.WorkerState{}, fmt.Errorf("invalid cached prefix")
		}
		cached = append(cached, model.PrefixFingerprint{TokenCount: item.GetPrefix().GetTokenCount(), SHA256: item.GetPrefix().GetSha256()})
	}
	kv := state.GetKvUsage()
	return model.WorkerState{
		StatusVersion: state.GetStatusVersion(), Status: status, WaitingRequestCount: state.GetWaitingRequestCount(),
		WaitingTokenCount: state.GetWaitingTokenCount(), RunningRequestCount: state.GetRunningRequestCount(),
		RunningWorkTokens: state.GetRunningWorkTokens(), KVUsage: model.KVUsage{CapacityTokens: kv.GetCapacityTokens(), UsedTokens: kv.GetUsedTokens()},
		PrefillTokensPerSecond: state.GetPrefillTokensPerSecond(), DecodeTokensPerSecond: state.GetDecodeTokensPerSecond(),
		DecodeSchedulingQuantumTokens: state.GetDecodeSchedulingQuantumTokens(), CachedPrefixes: cached, ObservedAt: observed,
	}, nil
}

// TimestampFromProto requires a valid timestamp for control-plane fields whose
// semantics depend on an observed or sent time.
func TimestampFromProto(timestamp *timestamppb.Timestamp, field string) (time.Time, error) {
	if timestamp == nil {
		return time.Time{}, fmt.Errorf("missing %s", field)
	}
	if err := timestamp.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("invalid %s: %w", field, err)
	}
	return timestamp.AsTime(), nil
}

func RequestToProto(request model.Request, workerID string) *aetherservev1.GenerateRequest {
	return &aetherservev1.GenerateRequest{
		ProtocolVersion: model.ProtocolVersion, RequestId: request.ID, AttemptId: request.AttemptID, WorkerId: workerID,
		TenantId: request.TenantID, Model: request.Model, Messages: MessagesToProto(request.Messages),
		EstimatedInputTokens: request.EstimatedInputToken, ExpectedOutputTokens: request.ExpectedOutputToken,
		MaxOutputTokens: request.MaxOutputTokens, Deadline: timestamppb.New(request.Deadline),
		PrefixFingerprints: PrefixesToProto(request.PrefixFingerprints),
	}
}

func ChunkFromProto(chunk *aetherservev1.GenerateChunk) (model.Chunk, error) {
	if chunk == nil {
		return model.Chunk{}, fmt.Errorf("missing generation chunk")
	}
	if chunk.GetProtocolVersion() != model.ProtocolVersion {
		return model.Chunk{}, fmt.Errorf("chunk protocol mismatch")
	}
	at := time.Now().UTC()
	if timestamp := chunk.GetWorkerTimestamp(); timestamp != nil {
		if err := timestamp.CheckValid(); err != nil {
			return model.Chunk{}, fmt.Errorf("invalid chunk timestamp: %w", err)
		}
		at = timestamp.AsTime()
	}
	var reason model.FinishReason
	if chunk.GetFinal() {
		parsed, err := finishFromProto(chunk.GetFinishReason())
		if err != nil {
			return model.Chunk{}, err
		}
		reason = parsed
	} else if chunk.GetFinishReason() != aetherservev1.FinishReason_FINISH_REASON_UNSPECIFIED {
		return model.Chunk{}, fmt.Errorf("non-final chunk must use an unspecified finish reason")
	}
	return model.Chunk{RequestID: chunk.GetRequestId(), AttemptID: chunk.GetAttemptId(), WorkerID: chunk.GetWorkerId(),
		TokenIndex: chunk.GetTokenIndex(), TokenText: chunk.GetTokenText(), Final: chunk.GetFinal(), Reason: reason, At: at}, nil
}

func ChunkToProto(chunk model.Chunk) *aetherservev1.GenerateChunk {
	result := &aetherservev1.GenerateChunk{
		ProtocolVersion: model.ProtocolVersion, RequestId: chunk.RequestID, AttemptId: chunk.AttemptID, WorkerId: chunk.WorkerID,
		TokenIndex: chunk.TokenIndex, TokenText: chunk.TokenText, Final: chunk.Final,
		WorkerTimestamp: timestamppb.New(chunk.At),
	}
	if chunk.Final {
		result.FinishReason = finishToProto(chunk.Reason)
	}
	return result
}

func statusToProto(status model.WorkerStatus) aetherservev1.WorkerStatus {
	switch status {
	case model.WorkerHealthy:
		return aetherservev1.WorkerStatus_WORKER_STATUS_HEALTHY
	case model.WorkerDraining:
		return aetherservev1.WorkerStatus_WORKER_STATUS_DRAINING
	default:
		return aetherservev1.WorkerStatus_WORKER_STATUS_UNHEALTHY
	}
}

func statusFromProto(status aetherservev1.WorkerStatus) (model.WorkerStatus, error) {
	switch status {
	case aetherservev1.WorkerStatus_WORKER_STATUS_HEALTHY:
		return model.WorkerHealthy, nil
	case aetherservev1.WorkerStatus_WORKER_STATUS_DRAINING:
		return model.WorkerDraining, nil
	case aetherservev1.WorkerStatus_WORKER_STATUS_UNHEALTHY:
		return model.WorkerUnhealthy, nil
	default:
		return "", fmt.Errorf("invalid worker status %q", status)
	}
}

func finishToProto(reason model.FinishReason) aetherservev1.FinishReason {
	switch reason {
	case model.FinishStop:
		return aetherservev1.FinishReason_FINISH_REASON_STOP
	case model.FinishLength:
		return aetherservev1.FinishReason_FINISH_REASON_LENGTH
	case model.FinishCancelled:
		return aetherservev1.FinishReason_FINISH_REASON_CANCELLED
	default:
		return aetherservev1.FinishReason_FINISH_REASON_ERROR
	}
}

func finishFromProto(reason aetherservev1.FinishReason) (model.FinishReason, error) {
	switch reason {
	case aetherservev1.FinishReason_FINISH_REASON_STOP:
		return model.FinishStop, nil
	case aetherservev1.FinishReason_FINISH_REASON_LENGTH:
		return model.FinishLength, nil
	case aetherservev1.FinishReason_FINISH_REASON_CANCELLED:
		return model.FinishCancelled, nil
	case aetherservev1.FinishReason_FINISH_REASON_ERROR:
		return model.FinishError, nil
	default:
		return "", fmt.Errorf("invalid finish reason %q", reason)
	}
}
