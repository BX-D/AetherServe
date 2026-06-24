// Package admission implements token-aware request admission.
package admission

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrGlobalLimit = errors.New("global in-flight token limit")
	ErrTenantLimit = errors.New("tenant token bucket limit")
)

type Config struct {
	GlobalInFlightTokens uint64  `yaml:"global_inflight_tokens"`
	TenantRatePerSecond  float64 `yaml:"tenant_rate_per_second"`
	TenantBurstTokens    float64 `yaml:"tenant_burst_tokens"`
}

type bucket struct {
	tokens float64
	last   time.Time
}

type Controller struct {
	mu      sync.Mutex
	config  Config
	used    uint64
	buckets map[string]bucket
	now     func() time.Time
}

type Lease struct {
	controller *Controller
	cost       uint64
	once       sync.Once
}

func New(config Config, now func() time.Time) (*Controller, error) {
	if config.GlobalInFlightTokens == 0 || config.TenantRatePerSecond <= 0 || config.TenantBurstTokens <= 0 {
		return nil, fmt.Errorf("admission limits must be positive")
	}
	if now == nil {
		now = time.Now
	}
	return &Controller{config: config, buckets: make(map[string]bucket), now: now}, nil
}

func (c *Controller) Reserve(tenant string, cost uint64) (*Lease, error) {
	if cost == 0 {
		return nil, fmt.Errorf("reservation cost must be positive")
	}
	if tenant == "" {
		tenant = "public"
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.buckets[tenant]
	if b.last.IsZero() {
		b.tokens, b.last = c.config.TenantBurstTokens, now
	} else {
		b.tokens = min(c.config.TenantBurstTokens, b.tokens+now.Sub(b.last).Seconds()*c.config.TenantRatePerSecond)
		b.last = now
	}
	if b.tokens < float64(cost) {
		c.buckets[tenant] = b
		return nil, ErrTenantLimit
	}
	if cost > c.config.GlobalInFlightTokens-c.used {
		c.buckets[tenant] = b
		return nil, ErrGlobalLimit
	}
	b.tokens -= float64(cost)
	c.buckets[tenant] = b
	c.used += cost
	return &Lease{controller: c, cost: cost}, nil
}

func (l *Lease) Release() {
	if l == nil || l.controller == nil {
		return
	}
	l.once.Do(func() {
		l.controller.mu.Lock()
		if l.cost > l.controller.used {
			panic("admission reservation underflow")
		}
		l.controller.used -= l.cost
		l.controller.mu.Unlock()
	})
}

func (c *Controller) InFlight() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
