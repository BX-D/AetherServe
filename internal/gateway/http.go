package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
	"unicode"

	aetherservev1 "github.com/aetherserve/aetherserve/api/gen/aetherserve/v1"
	"github.com/aetherserve/aetherserve/internal/admission"
	"github.com/aetherserve/aetherserve/internal/model"
	"github.com/aetherserve/aetherserve/internal/prefix"
	"github.com/aetherserve/aetherserve/internal/protocol"
	"github.com/aetherserve/aetherserve/internal/routing"
	"github.com/aetherserve/aetherserve/internal/tokenizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chatRequest struct {
	Model     string          `json:"model"`
	Messages  []model.Message `json:"messages"`
	Stream    *bool           `json:"stream"`
	MaxTokens *uint64         `json:"max_tokens,omitempty"`
}

type apiError struct {
	Error apiErrorBody `json:"error"`
}

type apiErrorBody struct {
	Message   string `json:"message"`
	Type      string `json:"type"`
	Code      string `json:"code"`
	RequestID string `json:"request_id"`
}

type streamChoice struct {
	Index        int                 `json:"index"`
	Delta        map[string]string   `json:"delta"`
	FinishReason *model.FinishReason `json:"finish_reason"`
}

type streamPayload struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []streamChoice `json:"choices"`
}

type attemptEvent struct {
	chunk model.Chunk
	err   error
}

type generationAttempt struct {
	cancel context.CancelFunc
	events <-chan attemptEvent
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	ready := s.registry.HealthyCount() > 0 && s.root.Err() == nil
	s.ready.Store(ready)
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	s.active.Add(1)
	defer s.active.Done()
	s.metrics.Inc("aetherserve_requests_total")
	started := time.Now()

	request, err := s.parseRequest(w, r)
	if err != nil {
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), request.Deadline)
	defer cancel()
	if s.registry.HealthyCount() == 0 {
		s.metrics.Inc("aetherserve_request_rejections_total")
		writeAPIError(w, http.StatusServiceUnavailable, request.ID, "service_unavailable", "no_healthy_workers", "no eligible healthy workers")
		return
	}
	lease, err := s.admission.Reserve(request.TenantID, request.EstimatedInputToken+request.MaxOutputTokens)
	if err != nil {
		s.metrics.Inc("aetherserve_request_rejections_total")
		if errors.Is(err, admission.ErrGlobalLimit) || errors.Is(err, admission.ErrTenantLimit) {
			writeAPIError(w, http.StatusTooManyRequests, request.ID, "rate_limit_error", "admission_rejected", err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, request.ID, "internal_error", "internal_error", err.Error())
		return
	}
	defer func() {
		lease.Release()
		s.metrics.Set("aetherserve_active_reservations", float64(s.admission.InFlight()))
	}()
	s.metrics.Add("aetherserve_input_tokens_total", request.EstimatedInputToken)
	s.metrics.Set("aetherserve_active_reservations", float64(s.admission.InFlight()))

	excluded := make(map[string]struct{})
	var previousAttemptErr error
	for attemptNumber := 1; attemptNumber <= 2; attemptNumber++ {
		request.AttemptID = fmt.Sprintf("%s-a%d", request.ID, attemptNumber)
		decision, worker, err := s.selectWorker(request, excluded)
		if err != nil {
			if previousAttemptErr != nil {
				s.writePrecommitAttemptError(w, request.ID, ctx, previousAttemptErr)
				return
			}
			s.metrics.Inc("aetherserve_request_rejections_total")
			writeAPIError(w, http.StatusServiceUnavailable, request.ID, "service_unavailable", "no_healthy_workers", "no eligible healthy workers")
			return
		}
		s.metrics.Observe("aetherserve_routing_decision_seconds", decision.DecisionTime.Seconds())
		s.metrics.IncLabel("aetherserve_worker_requests_total", "worker_id", worker.ID)
		s.metrics.SetLabel("aetherserve_worker_waiting_tokens", "worker_id", worker.ID, float64(worker.State.WaitingTokenCount))
		s.metrics.SetLabel("aetherserve_worker_running_requests", "worker_id", worker.ID, float64(worker.State.RunningRequestCount))
		for _, candidate := range decision.Candidates {
			if candidate.WorkerID == worker.ID {
				s.metrics.Observe("aetherserve_predicted_ttft_seconds", candidate.PredictedTTFT)
				s.metrics.Add("aetherserve_prefix_matched_tokens_total", uint64(candidate.MatchedTokens))
				if candidate.MatchedTokens > 0 {
					s.metrics.Inc("aetherserve_reported_prefix_cache_matches_total")
				}
				break
			}
		}
		s.log.Info("routing decision", "request_id", request.ID, "attempt_id", request.AttemptID, "decision", decision)

		current, err := s.openAttempt(ctx, request, worker)
		if err != nil {
			s.clients.Invalidate(worker.DataAddress)
			previousAttemptErr = err
			if s.shouldRetry(ctx, err, attemptNumber) {
				excluded[worker.ID] = struct{}{}
				s.metrics.Inc("aetherserve_request_retries_total")
				continue
			}
			s.writePrecommitAttemptError(w, request.ID, ctx, err)
			return
		}
		first, ok := <-current.events
		if !ok {
			current.cancel()
			err = io.ErrUnexpectedEOF
		} else {
			err = first.err
		}
		if err != nil {
			current.cancel()
			s.clients.Invalidate(worker.DataAddress)
			previousAttemptErr = err
			if s.shouldRetry(ctx, err, attemptNumber) {
				excluded[worker.ID] = struct{}{}
				s.metrics.Inc("aetherserve_request_retries_total")
				continue
			}
			s.writePrecommitAttemptError(w, request.ID, ctx, err)
			return
		}
		if first.chunk.Final && first.chunk.Reason == model.FinishError {
			current.cancel()
			upstreamErr := status.Error(codes.Internal, "worker reported terminal generation error")
			s.clients.Invalidate(worker.DataAddress)
			previousAttemptErr = upstreamErr
			if s.shouldRetry(ctx, upstreamErr, attemptNumber) {
				excluded[worker.ID] = struct{}{}
				s.metrics.Inc("aetherserve_request_retries_total")
				continue
			}
			s.writePrecommitAttemptError(w, request.ID, ctx, upstreamErr)
			return
		}

		s.writeStreamHeaders(w, request.ID)
		if first.chunk.Final {
			s.writeCompletion(w, request, first.chunk)
			s.writeDone(w)
			s.metrics.Observe("aetherserve_end_to_end_seconds", time.Since(started).Seconds())
			current.cancel()
			return
		}
		if err := s.writeToken(w, request, first.chunk); err != nil {
			current.cancel()
			s.metrics.Inc("aetherserve_client_cancellations_total")
			return
		}
		s.metrics.Observe("aetherserve_ttft_seconds", time.Since(started).Seconds())
		s.forwardCommitted(ctx, w, request, current, started)
		return
	}
}

func (s *Server) parseRequest(w http.ResponseWriter, r *http.Request) (model.Request, error) {
	requestID, err := clientRequestID(r.Header.Get("X-Request-ID"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "", "invalid_request_error", "invalid_request", err.Error())
		return model.Request{}, err
	}
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var input chatRequest
	if err := decoder.Decode(&input); err != nil {
		writeAPIError(w, http.StatusBadRequest, requestID, "invalid_request_error", "invalid_request", "invalid JSON request")
		return model.Request{}, err
	}
	if err := ensureEOF(decoder); err != nil {
		writeAPIError(w, http.StatusBadRequest, requestID, "invalid_request_error", "invalid_request", "request must contain one JSON object")
		return model.Request{}, err
	}
	if input.Model != s.config.Model || input.Stream == nil || !*input.Stream || len(input.Messages) == 0 {
		err := errors.New("model, non-empty messages, and stream:true are required")
		writeAPIError(w, http.StatusBadRequest, requestID, "invalid_request_error", "invalid_request", err.Error())
		return model.Request{}, err
	}
	for _, message := range input.Messages {
		if message.Content == "" || (message.Role != "system" && message.Role != "user" && message.Role != "assistant") {
			err := errors.New("messages require a supported role and non-empty text content")
			writeAPIError(w, http.StatusBadRequest, requestID, "invalid_request_error", "invalid_request", err.Error())
			return model.Request{}, err
		}
	}
	maxTokens := s.config.DefaultMaxOutputTokens
	if input.MaxTokens != nil {
		maxTokens = *input.MaxTokens
	}
	if maxTokens == 0 || maxTokens > s.config.MaxOutputTokens {
		err := errors.New("max_tokens is outside the configured limit")
		writeAPIError(w, http.StatusBadRequest, requestID, "invalid_request_error", "invalid_request", err.Error())
		return model.Request{}, err
	}
	estimate := tokenizer.Estimate(input.Messages)
	if estimate > s.config.MaxInputTokens || estimate+maxTokens > s.config.MaxContextTokens {
		err := errors.New("request exceeds configured token limits")
		writeAPIError(w, http.StatusBadRequest, requestID, "invalid_request_error", "invalid_request", err.Error())
		return model.Request{}, err
	}
	timeout, err := s.effectiveTimeout(r.Header.Get("X-Aether-Timeout-Ms"))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, requestID, "invalid_request_error", "invalid_request", err.Error())
		return model.Request{}, err
	}
	tenant := r.Header.Get("X-Aether-Tenant-ID")
	if tenant == "" {
		tenant = "public"
	}
	tokens := tokenizer.Tokens(input.Messages)
	return model.Request{ID: requestID, TenantID: tenant, Model: input.Model, Messages: input.Messages,
		EstimatedInputToken: estimate, ExpectedOutputToken: maxTokens, MaxOutputTokens: maxTokens,
		PrefixFingerprints: prefix.Fingerprints(tokens), Deadline: time.Now().Add(timeout)}, nil
}

func (s *Server) effectiveTimeout(header string) (time.Duration, error) {
	if header == "" {
		return s.config.RequestTimeout.Std(), nil
	}
	milliseconds, err := strconv.ParseInt(header, 10, 64)
	if err != nil || milliseconds <= 0 {
		return 0, errors.New("X-Aether-Timeout-Ms must be a positive integer")
	}
	timeout := time.Duration(milliseconds) * time.Millisecond
	if timeout < s.config.MinRequestTimeout.Std() {
		timeout = s.config.MinRequestTimeout.Std()
	}
	if timeout > s.config.MaxRequestTimeout.Std() {
		timeout = s.config.MaxRequestTimeout.Std()
	}
	return timeout, nil
}

func (s *Server) selectWorker(request model.Request, excluded map[string]struct{}) (model.RoutingDecision, model.Worker, error) {
	workers := s.registry.Snapshot()
	decision, err := s.router.Select(request, workers, excluded)
	if err != nil {
		return decision, model.Worker{}, err
	}
	for _, worker := range workers {
		if worker.ID == decision.SelectedWorker {
			return decision, worker, nil
		}
	}
	return decision, model.Worker{}, routing.ErrNoEligibleWorkers
}

func (s *Server) openAttempt(ctx context.Context, request model.Request, worker model.Worker) (generationAttempt, error) {
	attemptCtx, cancel := context.WithCancel(ctx)
	client, err := s.clients.Client(attemptCtx, worker.DataAddress)
	if err != nil {
		cancel()
		return generationAttempt{}, err
	}
	stream, err := client.Generate(attemptCtx, protocol.RequestToProto(request, worker.ID))
	if err != nil {
		cancel()
		return generationAttempt{}, err
	}
	events := make(chan attemptEvent, s.config.SSEBufferSize)
	go s.receiveAttempt(attemptCtx, request, worker.ID, stream, events, cancel)
	return generationAttempt{cancel: cancel, events: events}, nil
}

type receiveStream interface {
	Recv() (*aetherservev1.GenerateChunk, error)
}

func (s *Server) receiveAttempt(ctx context.Context, request model.Request, workerID string, stream receiveStream, events chan<- attemptEvent, cancel context.CancelFunc) {
	defer close(events)
	defer cancel()
	expected := uint32(0)
	for {
		wireChunk, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.deliverAttempt(ctx, events, attemptEvent{err: io.ErrUnexpectedEOF})
			} else {
				s.deliverAttempt(ctx, events, attemptEvent{err: err})
			}
			return
		}
		chunk, err := protocol.ChunkFromProto(wireChunk)
		if err == nil {
			err = validateChunk(chunk, request, workerID, expected)
		}
		if err != nil {
			s.deliverAttempt(ctx, events, attemptEvent{err: err})
			return
		}
		if !chunk.Final {
			expected++
		}
		if !s.deliverAttempt(ctx, events, attemptEvent{chunk: chunk}) {
			return
		}
		if chunk.Final {
			return
		}
	}
}

func (s *Server) deliverAttempt(ctx context.Context, events chan<- attemptEvent, event attemptEvent) bool {
	timer := time.NewTimer(s.config.SlowClientTimeout.Std())
	defer timer.Stop()
	select {
	case events <- event:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		s.metrics.Inc("aetherserve_slow_clients_total")
		return false
	}
}

func validateChunk(chunk model.Chunk, request model.Request, workerID string, expected uint32) error {
	if chunk.RequestID != request.ID || chunk.AttemptID != request.AttemptID || chunk.WorkerID != workerID {
		return errors.New("worker chunk identifiers do not match attempt")
	}
	if chunk.TokenIndex != expected {
		return fmt.Errorf("out-of-order or duplicate token index %d, expected %d", chunk.TokenIndex, expected)
	}
	if !chunk.Final && chunk.TokenText == "" {
		return errors.New("non-final worker chunk has no token text")
	}
	if chunk.Final && chunk.TokenText != "" {
		return errors.New("final worker chunk must not include token text")
	}
	return nil
}

func (s *Server) forwardCommitted(ctx context.Context, w http.ResponseWriter, request model.Request, current generationAttempt, started time.Time) {
	defer current.cancel()
	for {
		select {
		case event, ok := <-current.events:
			if !ok {
				s.writeStreamError(w, request.ID, "worker stream closed without completion")
				s.writeDone(w)
				s.metrics.Inc("aetherserve_midstream_failures_total")
				return
			}
			if event.err != nil {
				s.writeStreamError(w, request.ID, event.err.Error())
				s.writeDone(w)
				s.metrics.Inc("aetherserve_midstream_failures_total")
				return
			}
			if event.chunk.Final {
				if event.chunk.Reason == model.FinishError {
					s.writeStreamError(w, request.ID, "worker reported terminal generation error")
					s.writeDone(w)
					s.metrics.Inc("aetherserve_midstream_failures_total")
					return
				}
				s.writeCompletion(w, request, event.chunk)
				s.writeDone(w)
				s.metrics.Observe("aetherserve_end_to_end_seconds", time.Since(started).Seconds())
				return
			}
			if err := s.writeToken(w, request, event.chunk); err != nil {
				s.metrics.Inc("aetherserve_client_cancellations_total")
				return
			}
		case <-ctx.Done():
			s.metrics.Inc("aetherserve_client_cancellations_total")
			return
		}
	}
}

func (s *Server) shouldRetry(ctx context.Context, err error, attempt int) bool {
	if attempt >= 2 || ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	code := status.Code(err)
	switch code {
	case codes.Unavailable, codes.Unknown, codes.Internal, codes.DeadlineExceeded:
		return true
	}
	return code == codes.Unknown || status.Code(err) == codes.OK
}

func (s *Server) writePrecommitAttemptError(w http.ResponseWriter, requestID string, ctx context.Context, err error) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		writeAPIError(w, http.StatusGatewayTimeout, requestID, "timeout_error", "deadline_exceeded", "request deadline expired")
		return
	}
	s.metrics.Inc("aetherserve_precommit_worker_failures_total")
	writeAPIError(w, http.StatusBadGateway, requestID, "upstream_error", "worker_unavailable", err.Error())
}

func (s *Server) writeStreamHeaders(w http.ResponseWriter, requestID string) {
	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream; charset=utf-8")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")
	headers.Set("X-Accel-Buffering", "no")
	headers.Set("X-Request-ID", requestID)
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (s *Server) writeToken(w http.ResponseWriter, request model.Request, chunk model.Chunk) error {
	payload := streamPayload{ID: "chatcmpl-" + request.ID, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: request.Model,
		Choices: []streamChoice{{Index: 0, Delta: map[string]string{"content": chunk.TokenText}}}}
	err := s.writeSSE(w, "token", payload)
	if err == nil {
		s.metrics.Inc("aetherserve_output_tokens_total")
	}
	return err
}

func (s *Server) writeCompletion(w http.ResponseWriter, request model.Request, chunk model.Chunk) {
	reason := chunk.Reason
	payload := streamPayload{ID: "chatcmpl-" + request.ID, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: request.Model,
		Choices: []streamChoice{{Index: 0, Delta: map[string]string{}, FinishReason: &reason}}}
	_ = s.writeSSE(w, "completion", payload)
}

func (s *Server) writeStreamError(w http.ResponseWriter, requestID, message string) {
	_ = s.writeSSE(w, "error", apiError{Error: apiErrorBody{Message: message, Type: "upstream_error", Code: "worker_stream_failed", RequestID: requestID}})
}

func (s *Server) writeDone(w http.ResponseWriter) {
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (s *Server) writeSSE(w http.ResponseWriter, event string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(s.config.SlowClientTimeout.Std()))
	defer controller.SetWriteDeadline(time.Time{})
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
		return err
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, statusCode int, requestID, kind, code, message string) {
	if requestID != "" {
		w.Header().Set("X-Request-ID", requestID)
	}
	writeJSON(w, statusCode, apiError{Error: apiErrorBody{Message: message, Type: kind, Code: code, RequestID: requestID}})
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("multiple JSON values")
}

func clientRequestID(raw string) (string, error) {
	if raw == "" {
		bytes := make([]byte, 16)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		bytes[6] = (bytes[6] & 0x0f) | 0x40
		bytes[8] = (bytes[8] & 0x3f) | 0x80
		encoded := hex.EncodeToString(bytes)
		return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
	}
	if len(raw) > 128 {
		return "", errors.New("X-Request-ID exceeds 128 characters")
	}
	for _, r := range raw {
		if unicode.IsControl(r) {
			return "", errors.New("X-Request-ID contains a control character")
		}
	}
	return raw, nil
}
