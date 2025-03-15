package raftyserver

import (
	"context"
	"fmt"
	"github.com/svfoxat/rafty/internal/raft"
	"log"
	"log/slog"
	"net"
	"net/rpc"
)

type Rafty struct {
	raft        *raft.Node
	Port        int
	Peers       string
	Connections string
	Id          string
}

func NewRafty(port int, peers string, connections string, id string) *Rafty {
	return &Rafty{Port: port, Peers: peers, Connections: connections, Id: id}
}

func (r *Rafty) Start(ctx context.Context) {
	slog.Info("rafty starting...")

	r.raft = raft.NewNode(raft.NodeArgs{
		Id:          r.Id,
		Peers:       r.Peers,
		Connections: r.Connections,
	})
	go r.raft.Start(ctx)

	rpcServer := rpc.NewServer()
	err := rpcServer.Register(r.raft)
	if err != nil {
		return
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", r.Port))
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			go func() {
				rpcServer.ServeConn(conn)
			}()
		}
	}()
	slog.Info("rafty started", "port", r.Port)

	select {}
}

type Status struct {
	IsLeader bool
	Id       string
}

func (r *Rafty) Status() Status {
	isLeader := r.raft.State == raft.NodeStateLeader
	id := r.raft.Id

	return Status{
		IsLeader: isLeader,
		Id:       fmt.Sprintf("%d", id),
	}
}
