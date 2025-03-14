package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type NodeState int

const (
	NodeStateCandidate NodeState = iota
	NodeStateLeader
	NodeStateFollower
	NodeStateDead
)

type Node struct {
	mu sync.Mutex

	Id          int
	peers       map[int]net.Addr
	activePeers []int
	peerClients map[int]*rpc.Client

	state NodeState

	currentTerm int
	votedFor    int // 0 => not voted

	lastElectionReset time.Time
	lastLogIndex      int
}

func NewNode() *Node {
	return &Node{
		peers:             map[int]net.Addr{},
		peerClients:       map[int]*rpc.Client{},
		activePeers:       []int{},
		state:             NodeStateFollower,
		currentTerm:       0,
		votedFor:          0,
		lastLogIndex:      0,
		lastElectionReset: time.Now(),
	}
}

func (n *Node) Start(ctx context.Context) {
	slog.Info("raft-node start")

	peersString := os.Getenv("RAFTY_PEERS")
	peers := strings.Split(peersString, ",")
	for i, peer := range peers {
		addr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			slog.Error("raft-node failed to resolve address", "addr", peer)
			panic(err)
		}
		n.peers[i+1] = addr
	}
	slog.Info("raft-node peers", "peers", n.peers)

	connectTo := os.Getenv("RAFTY_CONNECT")
	connectTos := strings.Split(connectTo, ",")
	for _, connectTo := range connectTos {
		i, err := strconv.Atoi(connectTo)
		if err != nil {
			slog.Error("raft-node no integer value for activePeer", "peer", connectTo)
			panic(err)
		}
		n.activePeers = append(n.activePeers, i)
	}
	for _, peer := range n.activePeers {
		slog.Info("raft-node connecting to peer", "peerId", peer)
		err := n.connectToPeer(peer)
		if err != nil {
			slog.Error("raft-node failed to connect to peer", "peer", peer)
			panic(err)
		}
	}

	nodeId := os.Getenv("RAFTY_NODE_ID")
	nodeIdInt, err := strconv.Atoi(nodeId)
	if err != nil {
		slog.Error("raft-node no integer value for nodeId", "nodeId", nodeId)
	}
	n.Id = nodeIdInt

	time.Sleep(2 * time.Second)
	go n.electionTimer()

	select {
	case <-ctx.Done():
		slog.Info("raft-node stop")
	}
}

func (n *Node) electionTimer() {
	slog.Info("raft-node start electionTimer")

	random := randRange(0, 50)
	timeout := 25*time.Millisecond + time.Millisecond*time.Duration(random)

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	select {
	case <-ticker.C:
		switch n.state {
		case NodeStateFollower:
			slog.Info("raft-node just chilling")
			if time.Since(n.lastElectionReset) > timeout {
				slog.Info("raft-node going candidate")

				n.mu.Lock()
				n.state = NodeStateCandidate
				n.mu.Unlock()
			}

		case NodeStateCandidate:
			slog.Info("raft-node starting election", "node", n.Id)
			n.candidate()
		default:
			panic("raft-node state is invalid, shutting down")
		}

		n.electionTimer()
	}
}

func (n *Node) candidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, id := range n.activePeers {
		go func() {
			slog.Info("raft-node requestvote from", "peer", id)
			args := RequestVoteArgs{
				Term:        n.currentTerm,
				CandidateId: n.Id,
			}
			var reply RequestVoteReply

			err := n.callPeer(id, "Node.RequestVote", args, &reply)
			if err != nil {
				slog.Error("raft-node failed to vote", "peer", id, "err", err)
			}
			if reply.VoteGranted == true {
				slog.Info("raft-node vote granted")
			} else {
				slog.Info("raft-node vote denied")
			}
		}()
	}

	n.currentTerm++
}

func (n *Node) connectToPeer(index int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, ok := n.peerClients[index]
	if !ok {
		addr := n.peers[index]
		client, err := rpc.DialHTTP(addr.Network(), addr.String())
		if err != nil {
			return fmt.Errorf("raft-node failed to connect to peer %d", index)
		}
		n.peerClients[index] = client
	}
	return nil
}

func (n *Node) callPeer(id int, serviceMethod string, args any, reply any) error {
	n.mu.Lock()
	peer := n.peerClients[id]
	n.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

func randRange(min, max int) int {
	return rand.IntN(max-min) + min
}
