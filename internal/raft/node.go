package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/rpc"
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

	State NodeState

	currentTerm int
	votedFor    int // 0 => not voted

	lastElectionReset time.Time
	lastLogIndex      int
}

type NodeArgs struct {
	Id          string
	Peers       string
	Connections string
}

func NewNode(args NodeArgs) *Node {
	n := &Node{
		peers:             map[int]net.Addr{},
		peerClients:       map[int]*rpc.Client{},
		activePeers:       []int{},
		State:             NodeStateFollower,
		currentTerm:       0,
		votedFor:          0,
		lastLogIndex:      0,
		lastElectionReset: time.Now(),
	}
	peers := strings.Split(args.Peers, ",")
	for i, peer := range peers {
		addr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			slog.Error("raft-node failed to resolve address", "addr", peer)
			panic(err)
		}
		n.peers[i+1] = addr
	}
	slog.Info("raft-node peers", "peers", n.peers)

	connectTos := strings.Split(args.Connections, ",")
	for _, connectTo := range connectTos {
		i, err := strconv.Atoi(connectTo)
		if err != nil {
			slog.Error("raft-node no integer value for activePeer", "peer", connectTo)
			panic(err)
		}
		n.activePeers = append(n.activePeers, i)
	}

	nodeIdInt, err := strconv.Atoi(args.Id)
	if err != nil {
		slog.Error("raft-node no integer value for nodeId", "nodeId", args.Id)
	}
	n.Id = nodeIdInt

	return n
}

func (n *Node) Start(ctx context.Context) {
	slog.Info("raft-node start")

	// wait a second for other nodes to become available
	time.Sleep(time.Duration(randRange(0, 5)) * time.Second)

	for _, peer := range n.activePeers {
		slog.Info("raft-node connecting to peer", "peerId", peer)
		err := n.connectToPeer(peer)
		if err != nil {
			slog.Error("raft-node failed to connect to peer", "peer", peer, "err", err)
		}
	}

	go n.electionTimer()

	select {
	case <-ctx.Done():
		slog.Info("raft-node stop")
	}
}

func (n *Node) electionTimer() {
	slog.Info("raft-node start electionTimer")

	random := randRange(0, 50)
	timeout := 100*time.Millisecond + time.Millisecond*time.Duration(random)
	n.mu.Lock()
	termStarted := n.currentTerm
	n.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			if n.State != NodeStateCandidate && n.State != NodeStateFollower {
				n.mu.Unlock()
				return
			}

			if termStarted != n.currentTerm {
				n.mu.Unlock()
				return
			}

			if elapsed := time.Since(n.lastElectionReset); elapsed >= timeout {
				n.doElection()
				n.mu.Unlock()
				return
			}
		}
	}
	//random := randRange(0, 50)
	//timeout := 100*time.Millisecond + time.Millisecond*time.Duration(random)
	//
	//ticker := time.NewTicker(timeout)
	//defer ticker.Stop()
	//
	//select {
	//case <-ticker.C:
	//	switch n.State {
	//	case NodeStateLeader:
	//		slog.Info("I BIMS DER LEADER")
	//	case NodeStateFollower:
	//		slog.Info("raft-node just chilling")
	//		if time.Since(n.lastElectionReset) > timeout {
	//			slog.Info("raft-node going candidate")
	//
	//			n.mu.Lock()
	//			n.State = NodeStateCandidate
	//			n.mu.Unlock()
	//		}
	//
	//	case NodeStateCandidate:
	//		slog.Info("raft-node starting election", "node", n.Id)
	//		won := n.doElection()
	//		if won {
	//			slog.Info("raft-node won")
	//			n.mu.Lock()
	//			n.State = NodeStateLeader
	//			n.mu.Unlock()
	//		} else {
	//			n.doElection()
	//		}
	//	default:
	//		panic("raft-node State is invalid, shutting down")
	//	}
	//}
	//n.electionTimer()
}

func (n *Node) doElection() bool {
	slog.Info("raft-node starting election", "peers", n.activePeers)

	votesReceived := 1
	n.State = NodeStateCandidate
	n.currentTerm++
	savedCurrentTerm := n.currentTerm
	n.lastElectionReset = time.Now()
	n.votedFor = n.Id

	var wg sync.WaitGroup

	for _, id := range n.activePeers {
		go func() {
			wg.Add(1)
			defer wg.Done()

			slog.Info("raft-node requestvote from", "peer", id)
			args := RequestVoteArgs{
				Term:        n.currentTerm,
				CandidateId: n.Id,
			}
			var reply RequestVoteReply

			err := n.callPeer(id, "Node.RequestVote", args, &reply)
			if err != nil {
				slog.Error("raft-node failed to get vote", "peer", id, "err", err)
			}

			if reply.Term > savedCurrentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.Term == savedCurrentTerm {
				if reply.VoteGranted == true {
					slog.Info("raft-node vote granted")
					votesReceived++
				} else {
					slog.Info("raft-node vote denied")
				}
			}
		}()
	}
	wg.Wait()

	if votesReceived*2 >= len(n.peers)+1 {
		slog.Info("raft-node WON THE FING ELECTION")
		return true
	} else {
		slog.Info("raft-node IS NO WINNER NONO")
		return false
	}

}

func (n *Node) becomeFollower(term int) {
	slog.Info("raft-node become follower")
	n.State = NodeStateFollower
	n.currentTerm = term
	n.votedFor = 0
	n.lastElectionReset = time.Now()

	go n.electionTimer()
}

func (n *Node) connectToPeer(index int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, ok := n.peerClients[index]
	if !ok {
		addr := n.peers[index]
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		n.peerClients[index] = client
	}
	return nil
}

func (n *Node) callPeer(id int, serviceMethod string, args any, reply any) error {
	n.mu.Lock()
	peer, ok := n.peerClients[id]
	if !ok {
		err := n.connectToPeer(id)
		if err != nil {
			n.mu.Unlock()
			return err
		}
		peer = n.peerClients[id]
	}
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
