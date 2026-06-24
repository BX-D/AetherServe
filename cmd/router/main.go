// Command router starts the AetherServe HTTP gateway and worker control plane.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aetherserve/aetherserve/internal/config"
	"github.com/aetherserve/aetherserve/internal/gateway"
)

func main() {
	configPath := flag.String("config", "configs/router.yaml", "router YAML configuration")
	flag.Parse()
	cfg, err := config.LoadRouter(*configPath)
	if err != nil {
		fatal(err)
	}
	server, err := gateway.New(cfg)
	if err != nil {
		fatal(err)
	}
	if err := server.Start(); err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout.Std())
	defer cancel()
	if err := server.Close(shutdown); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "router:", err)
	os.Exit(1)
}
