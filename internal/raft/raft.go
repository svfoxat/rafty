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
	Dead
)

// Raft represents a Raft node and implements proto.RaftServiceServer.
type Raft struct {
	proto.UnimplementedRaftServiceServer
	myAddr string
	mu     sync.Mutex

	State    NodeState
	LeaderID int32
	ID       int32
	Peers    []string // List of peer addresses (e.g., "127.0.0.1:12346") including myself

	lastHeartbeat time.Time

	// persistent
	log         []*LogEntry
	CurrentTerm int32
	VotedFor    int32

	// volatile all
	commitIndex int32 // index of highest log entry known to be committed
	lastApplied int32 // index of highest log entry applied to state machine

	// volatile leader
	nextIndex  map[string]int32 // index of the next log entry to send to each follower
	matchIndex map[string]int32 // index of highest log entry of each follower known to be replicated
}

// NewNode creates a new Raft node with a given ID and list of peer addresses.
func NewNode(id int32, peers []string) *Raft {
	return &Raft{
		ID:            id,
		Peers:         peers,
		State:         Follower,
		CurrentTerm:   0,
		LeaderID:      -1, // -1 no leader has been assigned
		VotedFor:      -1, // -1 means no vote has been given.
		lastHeartbeat: time.Now(),
		nextIndex:     make(map[string]int32),
		matchIndex:    make(map[string]int32),
		commitIndex:   0,  // -1 means no log entry has been committed.
		lastApplied:   -1, // -1 means no log entry has been applied.
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

	r.myAddr = listener.Addr().String()

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

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.mu.Lock()
				if r.State == Dead {
					r.mu.Unlock()
					slog.Info("node-info [dead]", "id", r.ID, "leader", r.LeaderID, "term", r.CurrentTerm, "log", len(r.log), "commitIndex", r.commitIndex, "lastApplied", r.lastApplied)
					continue
				}
				slog.Info("node-info", "id", r.ID, "leader", r.LeaderID, "term", r.CurrentTerm, "log", len(r.log), "commitIndex", r.commitIndex, "lastApplied", r.lastApplied)
				r.mu.Unlock()
			}
		}
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

// Stop stops the Raft node.
// It sets the state to Dead, which stops the election timer.
// currently only for testing
func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.State = Dead
	//r.CurrentTerm = 0
	//r.VotedFor = -1
	//r.LeaderID = -1
}

// Restart restarts the Raft node on term 0.
// Sets the State to Follower and restarts the election timer.
// currently only for testing
func (r *Raft) Restart(term int32) {
	r.mu.Lock()
	r.State = Follower
	r.lastHeartbeat = time.Now().Add(-500 * time.Millisecond)
	r.LeaderID = -1
	if term != -1 {
		r.CurrentTerm = term
	}
	r.mu.Unlock()

	go r.ElectionTimer()
}

// ElectionTimer runs the election timeout loop.
// If the node is a follower and enough time has passed since the last heartbeat,
// it becomes a candidate and starts an election.
func (r *Raft) ElectionTimer() {
	for {
		r.mu.Lock()
		if r.State == Dead {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		// Random timeout between 150ms and 300ms.
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		time.Sleep(timeout)

		r.mu.Lock()
		if r.State == Follower && time.Since(r.lastHeartbeat) > timeout {
			r.State = Candidate
			r.LeaderID = -1
			go r.StartElection()
		}
		r.mu.Unlock()
	}
}

// StartElection starts an election by incrementing the term, voting for itself,
// sending RequestVote RPCs to peers, and counting votes.
func (r *Raft) StartElection() {
	slog.Info("node started election", "node", r.ID, "term", r.CurrentTerm)
	r.mu.Lock()
	r.CurrentTerm++
	termStarted := r.CurrentTerm

	// Vote for self.
	r.VotedFor = r.ID
	votes := 1
	r.mu.Unlock()

	totalNodes := len(r.Peers) + 1
	var wg sync.WaitGroup
	voteCh := make(chan bool, len(r.Peers))

	// Send RequestVote RPCs concurrently.
	for _, peerAddr := range r.Peers {
		// I already voted for myself
		if peerAddr == r.myAddr {
			continue
		}

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

	if votes*2 > totalNodes-1 {
		r.mu.Lock()
		r.State = Leader
		r.LeaderID = r.ID
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
	// init maps
	r.mu.Lock()
	for _, peer := range r.Peers {
		r.nextIndex[peer] = r.lastApplied + 1
		r.matchIndex[peer] = 0
	}
	r.mu.Unlock()

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
			if peerAddr == r.myAddr {
				// don't send to self
				continue
			}
			go func(addr string) {
				// TODO: LOWER TIMEOUT AGAIN
				ctx, cancel := context.WithTimeout(context.Background(), 100000*time.Millisecond)
				defer cancel()

				r.mu.Lock()

				prevLogIndex := r.nextIndex[addr] - 1
				prevLogTerm := 1

				if prevLogIndex >= 0 {
					prevLogTerm = int(r.log[prevLogIndex].Term)
				}

				var entries []*LogEntry
				if len(r.log) != 0 {
					if prevLogIndex < 0 {
						entries = r.log
						prevLogIndex = 0
					} else {
						entries = r.log[r.nextIndex[addr]:]
					}
				}

				serializedEntries := make([]*proto.LogEntry, len(entries))
				for i, entry := range entries {
					serializedEntries[i] = &proto.LogEntry{
						Command: entry.Command.(string),
						Term:    currentTerm,
						Index:   entry.Index,
					}
				}
				r.mu.Unlock()

				conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
				if err != nil {
					// Could not connect; skip this peer.
					return
				}
				defer conn.Close()

				client := proto.NewRaftServiceClient(conn)
				req := &proto.AppendEntriesRequest{
					Term:         currentTerm,
					LeaderID:     leaderID,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  int32(prevLogTerm),
					Entries:      serializedEntries,
					LeaderCommit: r.commitIndex,
				}

				// We ignore the response and error here for simplicity.
				response, err := client.AppendEntries(ctx, req)
				if err != nil {
					slog.Error("failed to send append entries", "len", len(entries), "node", addr, "term", currentTerm, "LeaderID", leaderID, "err", err)
				}

				r.mu.Lock()
				defer r.mu.Unlock()

				if response.Success {
					r.nextIndex[addr] += int32(len(entries))
					r.matchIndex[addr] = r.nextIndex[addr]

					if len(entries) != 0 {
						slog.Info(fmt.Sprintf("node %s successfully appended %d entries to term %d", addr, len(entries), currentTerm))
					}

					savedCommitIndex := r.commitIndex
					for N := r.commitIndex + 1; N < int32(len(r.log)); N++ {
						if r.log[N].Term == currentTerm {
							count := 1
							for _, peer := range r.Peers {
								if r.matchIndex[peer] >= N {
									count++
								}
							}
							if count*2 > len(r.Peers)+1 {
								r.commitIndex = N
							}
						}
					}
					if r.commitIndex != savedCommitIndex {
						slog.Info("leader committed log entry", "index", r.commitIndex, "term", currentTerm)
						// TODO: do something with the committed log entry
					}

				} else {
					r.nextIndex[addr] = prevLogIndex - 1
					slog.Info(fmt.Sprintf("id %d node %s failed to append %d entries to term %d", r.ID, addr, len(entries), currentTerm))
				}

				slog.Debug("sent append entries", "node", addr, "term", currentTerm, "LeaderID", leaderID)
			}(peerAddr)
		}
		// Wait before sending the next round of heartbeats.
		time.Sleep(50 * time.Millisecond)
	}
}
