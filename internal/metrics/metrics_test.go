package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrometheusExposition(t *testing.T) {
	registry := New()
	registry.Inc("aetherserve_requests_total")
	registry.Set("aetherserve_active_requests", 2)
	registry.Observe("aetherserve_ttft_seconds", .1)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body := response.Body.String()
	for _, expected := range []string{"aetherserve_requests_total 1", "aetherserve_active_requests 2", "aetherserve_ttft_seconds_count 1"} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics body lacks %q: %s", expected, body)
		}
	}
}
