package gateway

import (
	"testing"
	"time"

	"github.com/aetherserve/aetherserve/internal/model"
)

func TestValidateChunkRejectsDuplicateAndOutOfOrder(t *testing.T) {
	request := model.Request{ID: "r", AttemptID: "r-a1"}
	chunk := model.Chunk{RequestID: "r", AttemptID: "r-a1", WorkerID: "w", TokenIndex: 1, TokenText: "x", At: time.Now()}
	if err := validateChunk(chunk, request, "w", 0); err == nil {
		t.Fatal("out-of-order chunk accepted")
	}
	chunk.TokenIndex = 0
	chunk.TokenText = ""
	if err := validateChunk(chunk, request, "w", 0); err == nil {
		t.Fatal("empty token accepted")
	}
	chunk.TokenText = "x"
	if err := validateChunk(chunk, request, "other", 0); err == nil {
		t.Fatal("wrong worker accepted")
	}
}

func TestClientRequestIDValidation(t *testing.T) {
	if _, err := clientRequestID("ok-id"); err != nil {
		t.Fatal(err)
	}
	if _, err := clientRequestID("bad\nid"); err == nil {
		t.Fatal("control character accepted")
	}
}
