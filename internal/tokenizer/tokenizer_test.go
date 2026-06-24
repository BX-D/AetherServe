package tokenizer

import (
	"reflect"
	"testing"

	"github.com/aetherserve/aetherserve/internal/model"
)

func TestCanonicalizeAndEstimate(t *testing.T) {
	messages := []model.Message{{Role: "system", Content: "one  two"}, {Role: "user", Content: "three"}}
	if got, want := Canonicalize(messages), "system\none  two\nuser\nthree\n"; got != want {
		t.Fatalf("canonical form = %q, want %q", got, want)
	}
	if got, want := Tokens(messages), []string{"system", "one", "two", "user", "three"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens = %#v, want %#v", got, want)
	}
	if got := Estimate(messages); got != 5 {
		t.Fatalf("estimate = %d, want 5", got)
	}
}
