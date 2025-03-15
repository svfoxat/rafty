package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/svfoxat/rafty/internal/grpc"
	"github.com/svfoxat/rafty/internal/raft"
)

func main() {
	// Command-line flags for node ID, port, and peers.
	id := flag.Int("id", 1, "Node ID")
	port := flag.Int("port", 12345, "Port to listen on")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses (host:port)")
	flag.Parse()

	// Parse the peer list.
	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	// Create the node with its ID and peer addresses.
	node := raft.NewNode(int32(*id), peerList)

	// Start the election timer.
	go node.ElectionTimer()

	// Start the gRPC server on the specified port.
	fmt.Printf("Starting node %d on port %d with peers %v\n", *id, *port, peerList)
	go grpc.StartGRPCServer(node, "127.0.0.1", *port)

	// Keep the node running.
	for {
		time.Sleep(1 * time.Second)
	}
}
