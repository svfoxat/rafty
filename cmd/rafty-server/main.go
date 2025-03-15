package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/svfoxat/rafty/internal/raftyserver"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := raftyserver.NewRafty(9000)
	server.Start(ctx)
}
