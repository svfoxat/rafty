package rafty

import (
	"context"
	"log/slog"
	"net"

	rafty "github.com/svfoxat/rafty/internal/api"
	"github.com/svfoxat/rafty/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	ID    int32
	Peers []string
	raft  *raft.Raft
	kv    *KVStore

	logger *slog.Logger
	grpc   *grpc.Server
}

type ServerConfig struct {
	ID          int32
	Peers       []string
	Logger      *slog.Logger
	StoragePath string
}

func NewServer(cfg *ServerConfig) *Server {
	var storage raft.Storage
	if cfg.StoragePath != "" {
		storage = raft.NewFileStorage(cfg.StoragePath)
	}

	return &Server{
		grpc:   grpc.NewServer(),
		ID:     cfg.ID,
		Peers:  cfg.Peers,
		raft:   raft.NewNode(cfg.ID, cfg.Peers, storage),
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
	go s.startRPC(ctx)

	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) startRPC(ctx context.Context) error {
	listener, err := net.Listen("tcp", ":12346")
	if err != nil {
		slog.ErrorContext(ctx, "failed to listen on :12346", "error", err)
		return err
	}

	reflection.Register(s.grpc)
	rafty.RegisterKVServiceServer(s.grpc, v1Grpc{kv: s.kv})

	go func() {
		err := s.grpc.Serve(listener)
		if err != nil {
			slog.ErrorContext(ctx, "failed to serve grpc", "error", err)
		}
	}()
	slog.InfoContext(ctx, "grpc server listening on :12346")

	<-ctx.Done()
	slog.InfoContext(ctx, "stopping rpc server")
	s.grpc.Stop()

	return ctx.Err()
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
