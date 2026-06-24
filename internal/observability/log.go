// Package observability creates structured logs without request content.
package observability

import (
	"io"
	"log/slog"
)

func NewJSON(writer io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: level}))
}
