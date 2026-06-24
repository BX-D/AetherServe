package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRouterRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "router.yaml")
	data := []byte("http_address: :1\ncontrol_address: :2\nmodel: m\ndefault_max_output_tokens: 1\nmax_output_tokens: 2\nmax_input_tokens: 2\nmax_context_tokens: 3\nrequest_timeout: 1s\nmin_request_timeout: 1ms\nmax_request_timeout: 2s\nheartbeat_stale_after: 1s\nsse_buffer_size: 1\nslow_client_timeout: 1s\nshutdown_timeout: 1s\nrouting:\n  policy: round_robin\nadmission:\n  global_inflight_tokens: 1\n  tenant_rate_per_second: 1\n  tenant_burst_tokens: 1\nunknown: true\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRouter(path); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestExampleConfigsValidate(t *testing.T) {
	if _, err := LoadRouter("../../configs/router.yaml"); err != nil {
		t.Fatalf("router example: %v", err)
	}
	for _, path := range []string{"../../configs/mock-worker-1.yaml", "../../configs/mock-worker-2.yaml"} {
		if _, err := LoadWorker(path); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	if _, err := LoadRouter("../../configs/docker-router.yaml"); err != nil {
		t.Fatalf("docker router: %v", err)
	}
	for _, path := range []string{"../../configs/docker-mock-worker-1.yaml", "../../configs/docker-mock-worker-2.yaml"} {
		if _, err := LoadWorker(path); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}
