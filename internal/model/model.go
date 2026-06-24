// Package model contains protocol-neutral domain values shared by router packages.
package model

import "time"

const ProtocolVersion = "1.0"

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type PrefixFingerprint struct {
	TokenCount uint32 `json:"token_count"`
	SHA256     string `json:"sha256"`
}

type WorkerStatus string

const (
	WorkerHealthy   WorkerStatus = "healthy"
	WorkerDraining  WorkerStatus = "draining"
	WorkerUnhealthy WorkerStatus = "unhealthy"
)

type KVUsage struct {
	CapacityTokens uint64 `json:"capacity_tokens"`
	UsedTokens     uint64 `json:"used_tokens"`
}

type WorkerState struct {
	StatusVersion                 uint64              `json:"status_version"`
	Status                        WorkerStatus        `json:"status"`
	WaitingRequestCount           uint32              `json:"waiting_request_count"`
	WaitingTokenCount             uint64              `json:"waiting_token_count"`
	RunningRequestCount           uint32              `json:"running_request_count"`
	RunningWorkTokens             uint64              `json:"running_work_tokens"`
	KVUsage                       KVUsage             `json:"kv_usage"`
	PrefillTokensPerSecond        float64             `json:"prefill_tokens_per_second"`
	DecodeTokensPerSecond         float64             `json:"decode_tokens_per_second"`
	DecodeSchedulingQuantumTokens uint32              `json:"decode_scheduling_quantum_tokens"`
	CachedPrefixes                []PrefixFingerprint `json:"cached_prefixes"`
	ObservedAt                    time.Time           `json:"observed_at"`
}

type Worker struct {
	ID              string      `json:"id"`
	DataAddress     string      `json:"data_address"`
	Model           string      `json:"model"`
	ProtocolVersion string      `json:"protocol_version"`
	State           WorkerState `json:"state"`
	LastHeartbeat   time.Time   `json:"last_heartbeat"`
	Stale           bool        `json:"stale"`
}

type Request struct {
	ID                  string              `json:"id"`
	AttemptID           string              `json:"attempt_id"`
	TenantID            string              `json:"tenant_id"`
	Model               string              `json:"model"`
	Messages            []Message           `json:"messages"`
	EstimatedInputToken uint64              `json:"estimated_input_tokens"`
	ExpectedOutputToken uint64              `json:"expected_output_tokens"`
	MaxOutputTokens     uint64              `json:"max_output_tokens"`
	PrefixFingerprints  []PrefixFingerprint `json:"prefix_fingerprints"`
	Deadline            time.Time           `json:"deadline"`
}

type FinishReason string

const (
	FinishStop      FinishReason = "stop"
	FinishLength    FinishReason = "length"
	FinishCancelled FinishReason = "cancelled"
	FinishError     FinishReason = "error"
)

type Chunk struct {
	RequestID  string
	AttemptID  string
	WorkerID   string
	TokenIndex uint32
	TokenText  string
	Final      bool
	Reason     FinishReason
	At         time.Time
}

type CandidateEvaluation struct {
	WorkerID        string  `json:"worker_id"`
	Eligible        bool    `json:"eligible"`
	RejectionReason string  `json:"rejection_reason,omitempty"`
	StateVersion    uint64  `json:"state_version"`
	MatchedTokens   uint32  `json:"matched_tokens"`
	QueueDelay      float64 `json:"queue_delay_seconds"`
	UncachedPrefill uint64  `json:"uncached_prefill_tokens"`
	PrefillDelay    float64 `json:"prefill_delay_seconds"`
	OverloadPenalty float64 `json:"overload_penalty_seconds"`
	PredictedTTFT   float64 `json:"predicted_ttft_seconds"`
	Score           float64 `json:"score_seconds"`
	TieBreak        string  `json:"tie_break,omitempty"`
}

type RoutingDecision struct {
	RequestID      string                `json:"request_id"`
	Policy         string                `json:"policy"`
	SelectedWorker string                `json:"selected_worker,omitempty"`
	Candidates     []CandidateEvaluation `json:"candidates"`
	WorkerVersions map[string]uint64     `json:"worker_versions"`
	Cursor         uint64                `json:"cursor,omitempty"`
	DecisionTime   time.Duration         `json:"decision_time"`
}
