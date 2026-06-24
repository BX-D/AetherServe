// Command mock-worker starts a deterministic AetherServe mock inference worker.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aetherserve/aetherserve/internal/config"
	"github.com/aetherserve/aetherserve/internal/mockworker"
)

func main() {
	configPath := flag.String("config", "configs/mock-worker-1.yaml", "mock worker YAML configuration")
	flag.Parse()
	cfg, err := config.LoadWorker(*configPath)
	if err != nil {
		fatal(err)
	}
	worker, err := mockworker.New(cfg)
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "mock-worker:", err)
	os.Exit(1)
}
