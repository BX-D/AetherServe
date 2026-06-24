package main

import (
	"testing"

	"github.com/aetherserve/aetherserve/internal/routing"
)

func TestGeneratedWorkloadAndSimulationAreDeterministic(t *testing.T) {
	items, err := workloadItems("high_prefix_sharing", 3, 7, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].Messages[0].Content == "" {
		t.Fatalf("bad workload: %#v", items)
	}
	firstSimulator, err := newRoutingSimulator(routing.PredictedTTFT, 7)
	if err != nil {
		t.Fatal(err)
	}
	secondSimulator, err := newRoutingSimulator(routing.PredictedTTFT, 7)
	if err != nil {
		t.Fatal(err)
	}
	first := firstSimulator.run(0, items[0])
	second := secondSimulator.run(0, items[0])
	if first != second {
		t.Fatalf("simulation drifted: %#v != %#v", first, second)
	}
}
