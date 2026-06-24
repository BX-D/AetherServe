// Package prefix creates and compares bounded cumulative prefix fingerprints.
package prefix

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/aetherserve/aetherserve/internal/model"
)

const BlockTokens = 16

func Fingerprints(tokens []string) []model.PrefixFingerprint {
	result := make([]model.PrefixFingerprint, 0, len(tokens)/BlockTokens)
	for end := BlockTokens; end <= len(tokens); end += BlockTokens {
		sum := sha256.Sum256([]byte(strings.Join(tokens[:end], "\x1f")))
		result = append(result, model.PrefixFingerprint{
			TokenCount: uint32(end),
			SHA256:     hex.EncodeToString(sum[:]),
		})
	}
	return result
}

func LongestMatch(request, cached []model.PrefixFingerprint) uint32 {
	cache := make(map[string]uint32, len(cached))
	for _, fingerprint := range cached {
		if fingerprint.SHA256 != "" && fingerprint.TokenCount > cache[fingerprint.SHA256] {
			cache[fingerprint.SHA256] = fingerprint.TokenCount
		}
	}
	var longest uint32
	for _, fingerprint := range request {
		if cache[fingerprint.SHA256] == fingerprint.TokenCount && fingerprint.TokenCount > longest {
			longest = fingerprint.TokenCount
		}
	}
	return longest
}
