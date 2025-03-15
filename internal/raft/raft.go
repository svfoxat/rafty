package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/svfoxat/rafty/internal/grpc/proto"
	"google.golang.org/grpc"
)

// NodeState represents the role of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// Raft represents a Raft node and implements proto.RaftServiceServer.
type Raft struct {
	proto.UnimplementedRaftServiceServer

	mu          sync.Mutex
	ID          int32
	Peers       []string // List of peer addresses (e.g., "127.0.0.1:12346")
	State       NodeState
	CurrentTerm int32
	VotedFor    int32

	lastHeartbeat time.Time
}

// New creates a new Raft node with a given ID and list of peer addresses.
func NewNode(id int32, peers []string) *Raft {
	return &Raft{
		ID:            id,
		Peers:         peers,
		State:         Follower,
		CurrentTerm:   0,
		VotedFor:      -1, // -1 means no vote has been given.
		lastHeartbeat: time.Now(),
	}
}

// Start launches the gRPC server on the given address and port and starts the election timer.
// It blocks until the context is canceled or an error occurs.
func (r *Raft) Start(ctx context.Context, addr string, port int) error {
	// Create the TCP listener.
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		return fmt.Errorf("failed to listen on %s:%d: %w", addr, port, err)
	}
	slog.Info("listening", "id", r.ID, "addr", listener.Addr().String())

	// Create a new gRPC server and register the Raft service.
	server := grpc.NewServer()
	proto.RegisterRaftServiceServer(server, r)

	// Start the election timer in a separate goroutine.
	go r.ElectionTimer()

	// Channel to capture server errors.
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()

	// Wait for either context cancellation or a server error.
	select {
	case <-ctx.Done():
		slog.Info("stopping server", "id", r.ID, "addr", listener.Addr().String())
		server.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// GetState returns the current state of the Raft node.
func (r *Raft) GetState() NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.State
}

// RequestVote implements the gRPC RequestVote method.
func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.CurrentTerm {
		return &proto.RequestVoteResponse{
			Term:        r.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

	if req.Term > r.CurrentTerm {
		r.CurrentTerm = req.Term
		r.VotedFor = -1
		r.State = Follower
	}

	if r.VotedFor == -1 || r.VotedFor == req.CandidateID {
		r.VotedFor = req.CandidateID
		return &proto.RequestVoteResponse{
			Term:        r.CurrentTerm,
			VoteGranted: true,
		}, nil
	}

	return &proto.RequestVoteResponse{
		Term:        r.CurrentTerm,
		VoteGranted: false,
	}, nil
}

// AppendEntries implements the gRPC AppendEntries method.
func (r *Raft) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.lastHeartbeat = time.Now()

	if req.Term < r.CurrentTerm {
		return &proto.AppendEntriesResponse{
			Term:    r.CurrentTerm,
			Success: false,
		}, nil
	}

	if req.Term > r.CurrentTerm {
		r.CurrentTerm = req.Term
		r.VotedFor = -1
		r.State = Follower
	}

	// For this simplified example, we assume AppendEntries is always successful.
	return &proto.AppendEntriesResponse{
		Term:    r.CurrentTerm,
		Success: true,
	}, nil
}

// ElectionTimer runs the election timeout loop.
// If the node is a follower and enough time has passed since the last heartbeat,
// it becomes a candidate and starts an election.
func (r *Raft) ElectionTimer() {
	for {
		//r.mu.Lock()
		//if r.State == Dead {
		//	r.mu.Unlock()
		//	return
		//}
		//r.mu.Unlock()

		// Random timeout between 150ms and 300ms.
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		time.Sleep(timeout)

		r.mu.Lock()
		if r.State == Follower && time.Since(r.lastHeartbeat) > timeout {
			r.State = Candidate
			go r.StartElection()
		}
		r.mu.Unlock()
	}
}

// StartElection starts an election by incrementing the term, voting for itself,
// sending RequestVote RPCs to peers, and counting votes.
func (r *Raft) StartElection() {
	r.mu.Lock()
	r.CurrentTerm++
	termStarted := r.CurrentTerm
	// Vote for self.
	r.VotedFor = r.ID
	r.mu.Unlock()

	votes := 1
	totalNodes := len(r.Peers) + 1
	var wg sync.WaitGroup
	voteCh := make(chan bool, len(r.Peers))

	// Send RequestVote RPCs concurrently.
	for _, peerAddr := range r.Peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			// Create a context with timeout.
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				voteCh <- false
				return
			}
			defer conn.Close()
			client := proto.NewRaftServiceClient(conn)
			req := &proto.RequestVoteRequest{
				Term:        r.CurrentTerm,
				CandidateID: r.ID,
			}
			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				voteCh <- false
				return
			}
			voteCh <- resp.VoteGranted
		}(peerAddr)
	}

	wg.Wait()
	close(voteCh)
	for vote := range voteCh {
		if vote {
			votes++
		}
	}

	if votes >= (totalNodes/2)+1 {
		r.mu.Lock()
		r.State = Leader
		r.mu.Unlock()
		go r.SendHeartbeats()
		slog.Info("node became Leader", "node", r.ID, "term", termStarted, "votes", votes)
	} else {
		fmt.Printf("Node %d failed to become Leader in term %d; received %d votes\n", r.ID, termStarted, votes)
		r.mu.Lock()
		r.State = Follower
		r.mu.Unlock()
	}
}

// SendHeartbeats sends periodic AppendEntries RPCs (heartbeats) to all peers.
// When a follower receives these, it resets its lastHeartbeat.
func (r *Raft) SendHeartbeats() {
	for {
		r.mu.Lock()

		// Exit if not leader
		if r.State != Leader {
			r.mu.Unlock()
			return
		}

		// Capture current term and leader ID.
		currentTerm := r.CurrentTerm
		leaderID := r.ID
		r.mu.Unlock()

		// Send heartbeat to each peer concurrently.
		for _, peerAddr := range r.Peers {
			go func(addr string) {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
				if err != nil {
					// Could not connect; skip this peer.
					return
				}
				defer conn.Close()
				client := proto.NewRaftServiceClient(conn)
				req := &proto.AppendEntriesRequest{
					Term:     currentTerm,
					LeaderID: leaderID,
				}
				// We ignore the response and error here for simplicity.
				_, err = client.AppendEntries(ctx, req)
				if err != nil {
					slog.Error("failed to send heartbeat", "node", addr, "term", currentTerm, "leaderID", leaderID, "err", err)
				}
				slog.Debug("sent leader heartbeat", "node", addr, "term", currentTerm, "leaderID", leaderID)
			}(peerAddr)
		}
		// Wait before sending the next round of heartbeats.
		time.Sleep(50 * time.Millisecond)
	}
}
