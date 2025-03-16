package main

import (
	"context"
	"flag"
	"github.com/svfoxat/rafty/internal/rafty"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer done()

	// Set up default values from environment variables.
	defaultID := os.Getenv("RAFTY_ID")
	if defaultID == "" {
		defaultID = "1"
	}
	defaultPort := os.Getenv("RAFTY_PORT")
	if defaultPort == "" {
		defaultPort = "12345"
	}
	defaultPeers := os.Getenv("RAFTY_PEERS")

	// Define command-line flags.
	idFlag := flag.Int("id", 0, "Node ID")
	portFlag := flag.Int("port", 0, "Port to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer addresses (host:port)")
	flag.Parse()

	// Use flag values if provided, otherwise fall back to environment variables.
	var id int
	if *idFlag != 0 {
		id = *idFlag
	} else {
		var err error
		id, err = strconv.Atoi(defaultID)
		if err != nil {
			id = 1
		}
	}

	var port int
	if *portFlag != 0 {
		port = *portFlag
	} else {
		var err error
		port, err = strconv.Atoi(defaultPort)
		if err != nil {
			port = 12345
		}
	}

	peers := *peersFlag
	if peers == "" {
		peers = defaultPeers
	}

	// Parse the peers list.
	var peerList []string
	if peers != "" {
		peerList = strings.Split(peers, ",")
	}

	// Create a new Rafty Node
	node := rafty.NewServer(&rafty.ServerConfig{
		ID:    int32(id),
		Peers: peerList,
	})

	err := node.Start(ctx, port+1, port)
	if err != nil {
		panic(err)
	}
}
