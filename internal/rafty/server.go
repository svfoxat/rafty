package rafty

import (
	"context"
	"log/slog"

	"github.com/svfoxat/rafty/internal/raft"
)

type Server struct {
	ID    int32
	Peers []string
	raft  *raft.Raft
	kv    *KVStore

	logger *slog.Logger
}

type ServerConfig struct {
	ID     int32
	Peers  []string
	Logger *slog.Logger
}

func NewServer(cfg *ServerConfig) *Server {
	return &Server{
		ID:     cfg.ID,
		Peers:  cfg.Peers,
		raft:   raft.NewNode(cfg.ID, cfg.Peers),
		logger: cfg.Logger,
	}
}

func (s *Server) KV() *KVStore {
	return s.kv
}

func (s *Server) Start(ctx context.Context, port int, raftPort int) error {
	// use errorgroup
	go s.raft.Start(ctx, "0.0.0.0", raftPort)

	store, err := NewKVStore(s.raft)
	if err != nil {
		return err
	}
	s.kv = store
	go s.kv.Start()

	slog.Info("started", "port", port, "raftPort", raftPort)

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Status struct {
	ID       int32
	State    raft.NodeState
	LeaderID int32
}

func (s *Server) Status() *Status {
	return &Status{
		ID:       s.raft.ID,
		State:    s.raft.State,
		LeaderID: s.raft.LeaderID,
	}
}
