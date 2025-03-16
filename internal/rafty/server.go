package rafty

import (
	"context"
	"fmt"
	"github.com/svfoxat/rafty/internal/raft"
	"log/slog"
	"strings"
)

type Server struct {
	ID    int32
	Peers []string
	raft  *raft.Raft
	kv    *KVStore
}

type ServerConfig struct {
	ID    int32
	Peers []string
}

func NewServer(cfg *ServerConfig) *Server {
	return &Server{
		ID:    cfg.ID,
		Peers: cfg.Peers,
		raft:  raft.NewNode(cfg.ID, cfg.Peers),
	}
}

func (s *Server) KV() *KVStore {
	return s.kv
}

func (s *Server) Start(ctx context.Context, port int, raftPort int) error {
	go s.raft.Start(ctx, "0.0.0.0", raftPort)

	s.kv = NewKVStore(s.raft)
	go s.kv.Start()

	slog.Info("rafty started", "id", s.ID, "port", port, "raftPort", raftPort)

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) SendCommand(command string) (string, error) {
	if s.raft.State != raft.Leader {
		return "", fmt.Errorf("not the leader")
	}

	commandSplit := strings.Split(command, " ")
	switch commandSplit[0] {
	case "SET":
		if len(commandSplit) != 3 {
			return "", fmt.Errorf("invalid command")
		}
		err := s.raft.Submit(command)
		if err != nil {
			return "", err
		}
		return "OK", nil
	case "GET":
		if len(commandSplit) != 2 {
			return "", fmt.Errorf("invalid command")
		}
		val, ok := s.kv.Get(commandSplit[1])
		if !ok {
			return "", fmt.Errorf("key not found")
		}
		return val, nil
	default:
		return "", fmt.Errorf("invalid command")
	}
	return "", nil
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
