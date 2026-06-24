// Package tokenizer supplies the deterministic V0.1 mock tokenizer.
package tokenizer

import (
	"strings"

	"github.com/aetherserve/aetherserve/internal/model"
)

// Canonicalize preserves message order and makes role boundaries unambiguous.
func Canonicalize(messages []model.Message) string {
	var b strings.Builder
	for _, message := range messages {
		b.WriteString(message.Role)
		b.WriteByte('\n')
		b.WriteString(message.Content)
		b.WriteByte('\n')
	}
	return b.String()
}

// Tokens uses Unicode whitespace boundaries. It is intentionally not a model tokenizer.
func Tokens(messages []model.Message) []string {
	return strings.Fields(Canonicalize(messages))
}

func Estimate(messages []model.Message) uint64 {
	return uint64(len(Tokens(messages)))
}
