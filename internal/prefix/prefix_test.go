package prefix

import (
	"testing"

	"github.com/aetherserve/aetherserve/internal/model"
)

func TestFingerprintsAndLongestMatch(t *testing.T) {
	tokens := make([]string, 33)
	for i := range tokens {
		tokens[i] = string(rune('a' + i%26))
	}
	fingerprints := Fingerprints(tokens)
	if len(fingerprints) != 2 || fingerprints[0].TokenCount != 16 || fingerprints[1].TokenCount != 32 {
		t.Fatalf("unexpected fingerprints: %#v", fingerprints)
	}
	if got := LongestMatch(fingerprints, []model.PrefixFingerprint{fingerprints[0]}); got != 16 {
		t.Fatalf("match = %d, want 16", got)
	}
	if got := LongestMatch(fingerprints, nil); got != 0 {
		t.Fatalf("empty match = %d, want 0", got)
	}
}
