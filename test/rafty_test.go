package test

import (
	"context"
	"github.com/svfoxat/rafty/internal/raftyserver"
	"log/slog"
	"testing"
	"time"
)

func TestRafty(t *testing.T) {
	ctx := context.Background()

	node1 := raftyserver.NewRafty(9000, "localhost:9000,localhost:9001,localhost:9002", "2,3", "1")
	node2 := raftyserver.NewRafty(9001, "localhost:9000,localhost:9001,localhost:9002", "1,3", "2")
	node3 := raftyserver.NewRafty(9002, "localhost:9000,localhost:9001,localhost:9002", "1,2", "3")

	go node1.Start(ctx)
	go node2.Start(ctx)
	go node3.Start(ctx)

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			slog.Info("node1", "status", node1.Status())
			slog.Info("node2", "status", node2.Status())
			slog.Info("node3", "status", node3.Status())

			numLeaders := 0
			if node1.Status().IsLeader {
				numLeaders++
			}
			if node2.Status().IsLeader {
				numLeaders++
			}
			if node3.Status().IsLeader {
				numLeaders++
			}

			slog.Info("CLUSTER", "leaders", numLeaders)
		}
	}
}
