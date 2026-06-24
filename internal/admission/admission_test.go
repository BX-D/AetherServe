package admission

import (
	"errors"
	"testing"
	"time"
)

func TestReservationReleasesExactlyOnce(t *testing.T) {
	now := time.Unix(0, 0)
	controller, err := New(Config{GlobalInFlightTokens: 10, TenantRatePerSecond: 10, TenantBurstTokens: 10}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	lease, err := controller.Reserve("tenant", 6)
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	lease.Release()
	if got := controller.InFlight(); got != 0 {
		t.Fatalf("in flight = %d, want 0", got)
	}
	if _, err := controller.Reserve("tenant", 5); !errors.Is(err, ErrTenantLimit) {
		t.Fatalf("bucket should remain charged, got %v", err)
	}
	now = now.Add(time.Second)
	if _, err := controller.Reserve("tenant", 5); err != nil {
		t.Fatalf("bucket did not refill: %v", err)
	}
}

func TestGlobalRejectionDoesNotChargeTenant(t *testing.T) {
	controller, err := New(Config{GlobalInFlightTokens: 4, TenantRatePerSecond: 100, TenantBurstTokens: 100}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Reserve("tenant", 5); !errors.Is(err, ErrGlobalLimit) {
		t.Fatalf("got %v", err)
	}
	if _, err := controller.Reserve("tenant", 4); err != nil {
		t.Fatalf("global rejection charged tenant: %v", err)
	}
}
