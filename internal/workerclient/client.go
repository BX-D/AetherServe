// Package workerclient provides a small concurrency-safe worker gRPC connection pool.
package workerclient

import (
	"context"
	"sync"

	aetherservev1 "github.com/aetherserve/aetherserve/api/gen/aetherserve/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Pool struct {
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
}

func NewPool() *Pool { return &Pool{connections: make(map[string]*grpc.ClientConn)} }

func (p *Pool) Client(ctx context.Context, address string) (aetherservev1.InferenceWorkerClient, error) {
	p.mu.Lock()
	connection := p.connections[address]
	p.mu.Unlock()
	if connection != nil {
		return aetherservev1.NewInferenceWorkerClient(connection), nil
	}

	dialed, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if existing := p.connections[address]; existing != nil {
		p.mu.Unlock()
		_ = dialed.Close()
		return aetherservev1.NewInferenceWorkerClient(existing), nil
	}
	p.connections[address] = dialed
	p.mu.Unlock()
	return aetherservev1.NewInferenceWorkerClient(dialed), nil
}

func (p *Pool) Invalidate(address string) {
	p.mu.Lock()
	connection := p.connections[address]
	delete(p.connections, address)
	p.mu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
}

func (p *Pool) Close() error {
	p.mu.Lock()
	connections := p.connections
	p.connections = make(map[string]*grpc.ClientConn)
	p.mu.Unlock()
	var first error
	for _, connection := range connections {
		if err := connection.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
