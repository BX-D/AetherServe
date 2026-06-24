// Package gateway exposes the router HTTP and control-plane gRPC servers.
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	aetherservev1 "github.com/aetherserve/aetherserve/api/gen/aetherserve/v1"
	"github.com/aetherserve/aetherserve/internal/admission"
	"github.com/aetherserve/aetherserve/internal/config"
	"github.com/aetherserve/aetherserve/internal/metrics"
	"github.com/aetherserve/aetherserve/internal/model"
	"github.com/aetherserve/aetherserve/internal/observability"
	"github.com/aetherserve/aetherserve/internal/protocol"
	"github.com/aetherserve/aetherserve/internal/registry"
	"github.com/aetherserve/aetherserve/internal/routing"
	"github.com/aetherserve/aetherserve/internal/workerclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	aetherservev1.UnimplementedInferenceWorkerServer

	config    config.RouterConfig
	registry  *registry.Registry
	admission *admission.Controller
	router    *routing.Router
	clients   *workerclient.Pool
	metrics   *metrics.Registry
	log       *slog.Logger

	root       context.Context
	cancelRoot context.CancelFunc
	ready      atomic.Bool
	active     sync.WaitGroup

	httpServer      *http.Server
	controlServer   *grpc.Server
	httpListener    net.Listener
	controlListener net.Listener
	closeOnce       sync.Once
}

func New(cfg config.RouterConfig) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	reg, err := registry.New(cfg.HeartbeatStaleAfter.Std(), time.Now)
	if err != nil {
		return nil, err
	}
	adm, err := admission.New(cfg.Admission, time.Now)
	if err != nil {
		return nil, err
	}
	router, err := routing.New(cfg.Routing.Policy, cfg.Routing.RoundRobinSeed)
	if err != nil {
		return nil, err
	}
	root, cancel := context.WithCancel(context.Background())
	return &Server{
		config: cfg, registry: reg, admission: adm, router: router, clients: workerclient.NewPool(), metrics: metrics.New(),
		log: observability.NewJSON(os.Stderr, slog.LevelInfo), root: root, cancelRoot: cancel,
	}, nil
}

// Start begins both listeners without blocking. Call Close to stop them.
func (s *Server) Start() error {
	httpListener, err := net.Listen("tcp", s.config.HTTPAddress)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	controlListener, err := net.Listen("tcp", s.config.ControlAddress)
	if err != nil {
		_ = httpListener.Close()
		return fmt.Errorf("listen control gRPC: %w", err)
	}
	s.httpListener, s.controlListener = httpListener, controlListener

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("GET /livez", s.handleLive)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", s.metrics.Handler())
	s.httpServer = &http.Server{
		Handler: mux,
		BaseContext: func(net.Listener) context.Context {
			return s.root
		},
	}
	s.controlServer = grpc.NewServer()
	aetherservev1.RegisterInferenceWorkerServer(s.controlServer, s)
	go func() {
		if err := s.httpServer.Serve(httpListener); err != nil && err != http.ErrServerClosed {
			s.log.Error("http server stopped", "error", err)
		}
	}()
	go func() {
		if err := s.controlServer.Serve(controlListener); err != nil {
			s.log.Error("control server stopped", "error", err)
		}
	}()
	return nil
}

func (s *Server) HTTPAddress() string {
	if s.httpListener == nil {
		return ""
	}
	return s.httpListener.Addr().String()
}

func (s *Server) ControlAddress() string {
	if s.controlListener == nil {
		return ""
	}
	return s.controlListener.Addr().String()
}

func (s *Server) Metrics() *metrics.Registry { return s.metrics }

func (s *Server) HealthyWorkers() int { return s.registry.HealthyCount() }

func (s *Server) Close(ctx context.Context) error {
	var result error
	s.closeOnce.Do(func() {
		s.ready.Store(false)
		s.cancelRoot()
		if s.httpServer != nil {
			if err := s.httpServer.Shutdown(ctx); err != nil {
				result = err
			}
		}
		if s.controlServer != nil {
			done := make(chan struct{})
			go func() {
				s.controlServer.GracefulStop()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
				s.controlServer.Stop()
				if result == nil {
					result = ctx.Err()
				}
			}
		}
		if s.httpListener != nil {
			_ = s.httpListener.Close()
		}
		if s.controlListener != nil {
			_ = s.controlListener.Close()
		}
		activeDone := make(chan struct{})
		go func() {
			s.active.Wait()
			close(activeDone)
		}()
		select {
		case <-activeDone:
		case <-ctx.Done():
			if result == nil {
				result = ctx.Err()
			}
		}
		if err := s.clients.Close(); err != nil && result == nil {
			result = err
		}
	})
	return result
}

func (s *Server) RegisterWorker(_ context.Context, request *aetherservev1.RegisterWorkerRequest) (*aetherservev1.RegisterWorkerResponse, error) {
	if request.GetProtocolVersion() != model.ProtocolVersion {
		return nil, status.Error(codes.FailedPrecondition, "unsupported protocol version")
	}
	if _, err := protocol.TimestampFromProto(request.GetRegisteredAt(), "registration timestamp"); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	state, err := protocol.StateFromProto(request.GetInitialState())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	worker := modelWorker(request, state)
	if err := s.registry.Register(worker); err != nil {
		s.metrics.Inc("aetherserve_worker_registration_failures_total")
		return &aetherservev1.RegisterWorkerResponse{Accepted: false, Message: err.Error(), RouterTimestamp: timestamppb.Now()}, nil
	}
	s.ready.Store(s.registry.HealthyCount() > 0)
	s.metrics.Inc("aetherserve_worker_registrations_total")
	s.metrics.Set("aetherserve_healthy_workers", float64(s.registry.HealthyCount()))
	s.log.Info("worker registered", "worker_id", worker.ID, "address", worker.DataAddress)
	return &aetherservev1.RegisterWorkerResponse{Accepted: true, RouterTimestamp: timestamppb.Now()}, nil
}

func (s *Server) Heartbeat(_ context.Context, request *aetherservev1.HeartbeatRequest) (*aetherservev1.HeartbeatResponse, error) {
	if request.GetProtocolVersion() != model.ProtocolVersion {
		return nil, status.Error(codes.FailedPrecondition, "unsupported protocol version")
	}
	if _, err := protocol.TimestampFromProto(request.GetWorkerTimestamp(), "heartbeat timestamp"); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	state, err := protocol.StateFromProto(request.GetState())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.registry.Heartbeat(request.GetWorkerId(), state); err != nil {
		s.metrics.Inc("aetherserve_worker_heartbeat_rejected_total")
		return &aetherservev1.HeartbeatResponse{Accepted: false, Message: err.Error(), RouterTimestamp: timestamppb.Now()}, nil
	}
	s.ready.Store(s.registry.HealthyCount() > 0)
	s.metrics.Set("aetherserve_healthy_workers", float64(s.registry.HealthyCount()))
	return &aetherservev1.HeartbeatResponse{Accepted: true, RouterTimestamp: timestamppb.Now()}, nil
}

func modelWorker(request *aetherservev1.RegisterWorkerRequest, state model.WorkerState) model.Worker {
	return model.Worker{ID: request.GetWorkerId(), DataAddress: request.GetDataAddress(), Model: request.GetModel(),
		ProtocolVersion: request.GetProtocolVersion(), State: state}
}
