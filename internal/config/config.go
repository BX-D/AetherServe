// Package config loads and validates human-readable AetherServe configuration.
package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/aetherserve/aetherserve/internal/admission"
	"github.com/aetherserve/aetherserve/internal/routing"
	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

type RoutingConfig struct {
	Policy         routing.Policy `yaml:"policy"`
	RoundRobinSeed uint64         `yaml:"round_robin_seed"`
}

type RouterConfig struct {
	HTTPAddress            string           `yaml:"http_address"`
	ControlAddress         string           `yaml:"control_address"`
	Model                  string           `yaml:"model"`
	DefaultMaxOutputTokens uint64           `yaml:"default_max_output_tokens"`
	MaxOutputTokens        uint64           `yaml:"max_output_tokens"`
	MaxInputTokens         uint64           `yaml:"max_input_tokens"`
	MaxContextTokens       uint64           `yaml:"max_context_tokens"`
	RequestTimeout         Duration         `yaml:"request_timeout"`
	MinRequestTimeout      Duration         `yaml:"min_request_timeout"`
	MaxRequestTimeout      Duration         `yaml:"max_request_timeout"`
	HeartbeatStaleAfter    Duration         `yaml:"heartbeat_stale_after"`
	SSEBufferSize          int              `yaml:"sse_buffer_size"`
	SlowClientTimeout      Duration         `yaml:"slow_client_timeout"`
	ShutdownTimeout        Duration         `yaml:"shutdown_timeout"`
	Routing                RoutingConfig    `yaml:"routing"`
	Admission              admission.Config `yaml:"admission"`
}

type FailureConfig struct {
	FailBeforeFirstCount int     `yaml:"fail_before_first_count"`
	FailAfterTokens      int     `yaml:"fail_after_tokens"`
	SlowdownMultiplier   float64 `yaml:"slowdown_multiplier"`
}

type WorkerConfig struct {
	ID                            string        `yaml:"id"`
	DataAddress                   string        `yaml:"data_address"`
	AdvertisedAddress             string        `yaml:"advertised_address"`
	RouterControlAddress          string        `yaml:"router_control_address"`
	Model                         string        `yaml:"model"`
	HeartbeatInterval             Duration      `yaml:"heartbeat_interval"`
	QueueCapacity                 int           `yaml:"queue_capacity"`
	PrefillTokensPerSecond        float64       `yaml:"prefill_tokens_per_second"`
	DecodeTokensPerSecond         float64       `yaml:"decode_tokens_per_second"`
	DecodeSchedulingQuantumTokens uint32        `yaml:"decode_scheduling_quantum_tokens"`
	CacheCapacityTokens           uint64        `yaml:"cache_capacity_tokens"`
	CachePrefixLimit              int           `yaml:"cache_prefix_limit"`
	Seed                          uint64        `yaml:"seed"`
	Failure                       FailureConfig `yaml:"failure"`
}

func LoadRouter(path string) (RouterConfig, error) {
	var config RouterConfig
	if err := load(path, &config); err != nil {
		return RouterConfig{}, err
	}
	return config, config.Validate()
}

func LoadWorker(path string) (WorkerConfig, error) {
	var config WorkerConfig
	if err := load(path, &config); err != nil {
		return WorkerConfig{}, err
	}
	return config, config.Validate()
}

func load(path string, output any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read configuration: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode configuration: %w", err)
	}
	return nil
}

func (c RouterConfig) Validate() error {
	if c.HTTPAddress == "" || c.ControlAddress == "" || c.Model == "" {
		return fmt.Errorf("http_address, control_address, and model are required")
	}
	if c.DefaultMaxOutputTokens == 0 || c.DefaultMaxOutputTokens > c.MaxOutputTokens || c.MaxOutputTokens == 0 {
		return fmt.Errorf("default_max_output_tokens must be positive and no greater than max_output_tokens")
	}
	if c.MaxInputTokens == 0 || c.MaxContextTokens < c.MaxInputTokens {
		return fmt.Errorf("invalid input/context token limits")
	}
	if c.RequestTimeout.Std() <= 0 || c.MinRequestTimeout.Std() <= 0 || c.MaxRequestTimeout.Std() < c.MinRequestTimeout.Std() || c.RequestTimeout.Std() > c.MaxRequestTimeout.Std() {
		return fmt.Errorf("invalid request timeout settings")
	}
	if c.HeartbeatStaleAfter.Std() <= 0 || c.SSEBufferSize <= 0 || c.SlowClientTimeout.Std() <= 0 || c.ShutdownTimeout.Std() <= 0 {
		return fmt.Errorf("heartbeat, SSE buffer, client timeout, and shutdown values must be positive")
	}
	if _, err := routing.New(c.Routing.Policy, c.Routing.RoundRobinSeed); err != nil {
		return err
	}
	if _, err := admission.New(c.Admission, time.Now); err != nil {
		return err
	}
	return nil
}

func (c WorkerConfig) Validate() error {
	if c.ID == "" || c.DataAddress == "" || c.RouterControlAddress == "" || c.Model == "" {
		return fmt.Errorf("worker id, addresses, and model are required")
	}
	if c.HeartbeatInterval.Std() <= 0 || c.QueueCapacity <= 0 || c.CachePrefixLimit <= 0 || c.CacheCapacityTokens == 0 {
		return fmt.Errorf("worker heartbeat, queue, cache limit, and cache capacity must be positive")
	}
	if c.PrefillTokensPerSecond <= 0 || c.DecodeTokensPerSecond <= 0 || c.DecodeSchedulingQuantumTokens == 0 {
		return fmt.Errorf("worker rates and decode scheduling quantum must be positive")
	}
	if c.Failure.FailBeforeFirstCount < 0 || c.Failure.FailAfterTokens < 0 || c.Failure.SlowdownMultiplier < 1 {
		return fmt.Errorf("invalid failure injection configuration")
	}
	return nil
}
