package main

import (
	"context"
	"os/signal"
	"rafty/internal/raftyserver"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := raftyserver.NewRafty()
	server.Start(ctx)
}
