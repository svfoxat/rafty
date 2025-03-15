package grpc

import (
	"fmt"
	"github.com/svfoxat/rafty/internal/grpc/proto"
	"log"
	"net"

	"github.com/svfoxat/rafty/internal/raft"
	"google.golang.org/grpc"
)

// StartGRPCServer creates and starts a gRPC server on the given address and port.
func StartGRPCServer(r *raft.Raft, address string, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterRaftServiceServer(s, r)
	log.Printf("gRPC server listening on %s:%d", address, port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
