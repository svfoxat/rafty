package raftyserver

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"rafty/internal/raft"
)

type Rafty struct {
	raft *raft.Node
}

func NewRafty() *Rafty {
	return &Rafty{}
}

func (r *Rafty) Start(ctx context.Context) {
	slog.Info("rafty starting...")

	r.raft = raft.NewNode()
	go r.raft.Start(ctx)

	err := rpc.Register(r.raft)
	if err != nil {
		return
	}

	rpc.HandleHTTP()

	l, err := net.Listen("tcp", ":9000")
	if err != nil {
		slog.Error("listen tcp", "error", err)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			slog.Error("serve http", "error", err)
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("rafty shutting down")
		return
	}
}
