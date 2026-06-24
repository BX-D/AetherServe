// Package mockworker implements the V0.1 worker protocol with deterministic simulated generation.
package mockworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	aetherservev1 "github.com/aetherserve/aetherserve/api/gen/aetherserve/v1"
	"github.com/aetherserve/aetherserve/internal/config"
	"github.com/aetherserve/aetherserve/internal/model"
	"github.com/aetherserve/aetherserve/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type result struct {
	chunk *model.Chunk
	err   error
	done  bool
}

type job struct {
	ctx     context.Context
	request *aetherservev1.GenerateRequest
	work    uint64
	results chan result
}

type Worker struct {
	aetherservev1.UnimplementedInferenceWorkerServer

	config config.WorkerConfig

	mu                  sync.Mutex
	state               model.WorkerState
	cache               map[string]model.PrefixFingerprint
	cacheOrder          []string
	failBeforeRemaining int
	dataAddress         string

	queue      chan *job
	ctx        context.Context
	cancel     context.CancelFunc
	listener   net.Listener
	grpcServer *grpc.Server
	wg         sync.WaitGroup
	stopOnce   sync.Once
}

func New(cfg config.WorkerConfig) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		config: cfg,
		state: model.WorkerState{
			Status:                        model.WorkerHealthy,
			KVUsage:                       model.KVUsage{CapacityTokens: cfg.CacheCapacityTokens},
			PrefillTokensPerSecond:        cfg.PrefillTokensPerSecond,
			DecodeTokensPerSecond:         cfg.DecodeTokensPerSecond,
			DecodeSchedulingQuantumTokens: cfg.DecodeSchedulingQuantumTokens,
		},
		cache: make(map[string]model.PrefixFingerprint), failBeforeRemaining: cfg.Failure.FailBeforeFirstCount,
		dataAddress: cfg.DataAddress, queue: make(chan *job, cfg.QueueCapacity), ctx: ctx, cancel: cancel,
	}, nil
}

func (w *Worker) Start(parent context.Context) error {
	listener, err := net.Listen("tcp", w.config.DataAddress)
	if err != nil {
		return fmt.Errorf("listen worker: %w", err)
	}
	w.listener = listener
	if w.config.AdvertisedAddress != "" {
		w.dataAddress = w.config.AdvertisedAddress
	} else {
		w.dataAddress = listener.Addr().String()
	}
	w.grpcServer = grpc.NewServer()
	aetherservev1.RegisterInferenceWorkerServer(w.grpcServer, w)

	w.wg.Add(3)
	go func() { defer w.wg.Done(); _ = w.grpcServer.Serve(listener) }()
	go func() { defer w.wg.Done(); w.dispatch() }()
	go func() { defer w.wg.Done(); w.registrationLoop() }()
	go func() {
		select {
		case <-parent.Done():
			w.Stop()
		case <-w.ctx.Done():
		}
	}()
	return nil
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	w.Stop()
	return nil
}

func (w *Worker) Address() string { return w.dataAddress }

// ActiveRequests reports current queued plus running mock work for bounded cleanup tests.
func (w *Worker) ActiveRequests() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state.WaitingRequestCount + w.state.RunningRequestCount
}

func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.cancel()
		if w.grpcServer != nil {
			w.grpcServer.Stop()
		}
		if w.listener != nil {
			_ = w.listener.Close()
		}
		w.wg.Wait()
	})
}

func (w *Worker) Generate(request *aetherservev1.GenerateRequest, stream aetherservev1.InferenceWorker_GenerateServer) error {
	if request.GetProtocolVersion() != model.ProtocolVersion || request.GetWorkerId() != w.config.ID || request.GetModel() != w.config.Model {
		return status.Error(codes.InvalidArgument, "invalid generation request")
	}
	if request.GetRequestId() == "" || request.GetAttemptId() == "" || request.GetMaxOutputTokens() == 0 {
		return status.Error(codes.InvalidArgument, "missing generation identifiers or output limit")
	}
	work := normalizedWork(request.GetEstimatedInputTokens(), request.GetMaxOutputTokens(), w.config.PrefillTokensPerSecond, w.config.DecodeTokensPerSecond)
	current := &job{ctx: stream.Context(), request: request, work: work, results: make(chan result, 1)}
	w.addWaiting(work)
	select {
	case w.queue <- current:
	case <-stream.Context().Done():
		w.removeWaiting(work)
		return status.FromContextError(stream.Context().Err()).Err()
	default:
		w.removeWaiting(work)
		return status.Error(codes.ResourceExhausted, "mock worker queue is full")
	}

	for {
		select {
		case outcome := <-current.results:
			if outcome.err != nil {
				return outcome.err
			}
			if outcome.chunk != nil {
				if err := stream.Send(protocol.ChunkToProto(*outcome.chunk)); err != nil {
					return err
				}
			}
			if outcome.done {
				return nil
			}
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		case <-w.ctx.Done():
			return status.Error(codes.Unavailable, "worker stopping")
		}
	}
}

func (w *Worker) dispatch() {
	for {
		select {
		case current := <-w.queue:
			if current == nil {
				continue
			}
			w.moveToRunning(current.work)
			w.runJob(current)
			w.finishRunning(current.work)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) runJob(current *job) {
	if !w.wait(current.ctx, w.prefillDuration(current.request.GetEstimatedInputTokens())) {
		return
	}
	w.mu.Lock()
	failBefore := w.failBeforeRemaining > 0
	if failBefore {
		w.failBeforeRemaining--
	}
	w.mu.Unlock()
	if failBefore {
		w.send(current, result{err: status.Error(codes.Unavailable, "injected failure before first token")})
		return
	}

	outputs := current.request.GetMaxOutputTokens()
	if outputs > 8 {
		outputs = 8
	}
	for index := uint64(0); index < outputs; index++ {
		if w.config.Failure.FailAfterTokens > 0 && int(index) >= w.config.Failure.FailAfterTokens {
			w.send(current, result{err: status.Error(codes.Unavailable, "injected midstream failure")})
			return
		}
		if !w.wait(current.ctx, w.decodeDuration()) {
			return
		}
		chunk := model.Chunk{RequestID: current.request.GetRequestId(), AttemptID: current.request.GetAttemptId(), WorkerID: w.config.ID,
			TokenIndex: uint32(index), TokenText: w.token(current.request.GetRequestId(), index), At: time.Now().UTC()}
		if !w.send(current, result{chunk: &chunk}) {
			return
		}
	}
	reason := model.FinishStop
	if outputs == current.request.GetMaxOutputTokens() {
		reason = model.FinishLength
	}
	final := model.Chunk{RequestID: current.request.GetRequestId(), AttemptID: current.request.GetAttemptId(), WorkerID: w.config.ID,
		TokenIndex: uint32(outputs), Final: true, Reason: reason, At: time.Now().UTC()}
	w.addPrefixes(protocol.PrefixesFromProto(current.request.GetPrefixFingerprints()))
	w.send(current, result{chunk: &final, done: true})
}

func (w *Worker) send(current *job, outcome result) bool {
	select {
	case current.results <- outcome:
		return true
	case <-current.ctx.Done():
		return false
	case <-w.ctx.Done():
		return false
	}
}

func (w *Worker) registrationLoop() {
	for {
		if err := w.registerAndHeartbeat(); err != nil {
			if !w.wait(w.ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		if w.ctx.Err() != nil {
			return
		}
	}
}

func (w *Worker) registerAndHeartbeat() error {
	dialCtx, cancel := context.WithTimeout(w.ctx, time.Second)
	connection, err := grpc.DialContext(dialCtx, w.config.RouterControlAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	cancel()
	if err != nil {
		return err
	}
	defer connection.Close()
	client := aetherservev1.NewInferenceWorkerClient(connection)
	register, err := client.RegisterWorker(w.ctx, &aetherservev1.RegisterWorkerRequest{
		ProtocolVersion: model.ProtocolVersion, WorkerId: w.config.ID, DataAddress: w.Address(), Model: w.config.Model,
		RegisteredAt: timestamppb.Now(), InitialState: protocol.StateToProto(w.nextState()),
	})
	if err != nil {
		return err
	}
	if !register.GetAccepted() {
		return fmt.Errorf("registration rejected: %s", register.GetMessage())
	}

	ticker := time.NewTicker(w.config.HeartbeatInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			response, err := client.Heartbeat(w.ctx, &aetherservev1.HeartbeatRequest{
				ProtocolVersion: model.ProtocolVersion, WorkerId: w.config.ID, WorkerTimestamp: timestamppb.Now(), State: protocol.StateToProto(w.nextState()),
			})
			if err != nil {
				return err
			}
			if !response.GetAccepted() {
				return fmt.Errorf("heartbeat rejected: %s", response.GetMessage())
			}
		case <-w.ctx.Done():
			return nil
		}
	}
}

func (w *Worker) nextState() model.WorkerState {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.state.StatusVersion++
	w.state.ObservedAt = time.Now().UTC()
	w.state.CachedPrefixes = make([]model.PrefixFingerprint, 0, len(w.cacheOrder))
	for _, key := range w.cacheOrder {
		w.state.CachedPrefixes = append(w.state.CachedPrefixes, w.cache[key])
	}
	return cloneState(w.state)
}

func (w *Worker) addWaiting(work uint64) {
	w.mu.Lock()
	w.state.WaitingRequestCount++
	w.state.WaitingTokenCount += work
	w.mu.Unlock()
}

func (w *Worker) removeWaiting(work uint64) {
	w.mu.Lock()
	if w.state.WaitingRequestCount > 0 {
		w.state.WaitingRequestCount--
	}
	if w.state.WaitingTokenCount >= work {
		w.state.WaitingTokenCount -= work
	} else {
		w.state.WaitingTokenCount = 0
	}
	w.mu.Unlock()
}

func (w *Worker) moveToRunning(work uint64) {
	w.mu.Lock()
	if w.state.WaitingRequestCount > 0 {
		w.state.WaitingRequestCount--
	}
	if w.state.WaitingTokenCount >= work {
		w.state.WaitingTokenCount -= work
	} else {
		w.state.WaitingTokenCount = 0
	}
	w.state.RunningRequestCount++
	w.state.RunningWorkTokens += work
	w.mu.Unlock()
}

func (w *Worker) finishRunning(work uint64) {
	w.mu.Lock()
	if w.state.RunningRequestCount > 0 {
		w.state.RunningRequestCount--
	}
	if w.state.RunningWorkTokens >= work {
		w.state.RunningWorkTokens -= work
	} else {
		w.state.RunningWorkTokens = 0
	}
	w.mu.Unlock()
}

func (w *Worker) addPrefixes(prefixes []model.PrefixFingerprint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, fingerprint := range prefixes {
		if _, exists := w.cache[fingerprint.SHA256]; exists {
			continue
		}
		w.cache[fingerprint.SHA256] = fingerprint
		w.cacheOrder = append(w.cacheOrder, fingerprint.SHA256)
		w.state.KVUsage.UsedTokens += uint64(fingerprint.TokenCount)
		for (len(w.cacheOrder) > w.config.CachePrefixLimit || w.state.KVUsage.UsedTokens > w.state.KVUsage.CapacityTokens) && len(w.cacheOrder) > 0 {
			evicted := w.cacheOrder[0]
			w.cacheOrder = w.cacheOrder[1:]
			old := w.cache[evicted]
			delete(w.cache, evicted)
			if w.state.KVUsage.UsedTokens >= uint64(old.TokenCount) {
				w.state.KVUsage.UsedTokens -= uint64(old.TokenCount)
			} else {
				w.state.KVUsage.UsedTokens = 0
			}
		}
	}
}

func (w *Worker) prefillDuration(tokens uint64) time.Duration {
	return scaledDuration(float64(tokens)/w.config.PrefillTokensPerSecond, w.config.Failure.SlowdownMultiplier)
}

func (w *Worker) decodeDuration() time.Duration {
	return scaledDuration(1/w.config.DecodeTokensPerSecond, w.config.Failure.SlowdownMultiplier)
}

func (w *Worker) wait(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-w.ctx.Done():
		return false
	}
}

func (w *Worker) token(requestID string, index uint64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%d", w.config.Seed, requestID, index)))
	return "mock-" + hex.EncodeToString(sum[:4])
}

func normalizedWork(input, output uint64, prefillRate, decodeRate float64) uint64 {
	return input + uint64(math.Ceil(float64(output)*prefillRate/decodeRate))
}

func scaledDuration(seconds, multiplier float64) time.Duration {
	return time.Duration(seconds * multiplier * float64(time.Second))
}

func cloneState(state model.WorkerState) model.WorkerState {
	state.CachedPrefixes = append([]model.PrefixFingerprint(nil), state.CachedPrefixes...)
	return state
}
