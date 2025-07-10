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
	myAddr string
	mu     sync.Mutex

	State    NodeState
	LeaderID int32
	ID       int32
	Peers    []string // List of peer addresses (e.g., "127.0.0.1:12346") including myself

	lastHeartbeat time.Time

	commitReady chan struct{}
	commitSubs  []chan *LogEntry

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

	TestIsPartitioned bool

	// grpc client map
	connMu   sync.Mutex
	peerCons map[string]proto.RaftServiceClient

	// log batching
	proposeCh chan *LogEntry
}

// NewNode creates a new Raft node with a given ID and list of peer addresses.
func NewNode(id int32, peers []string) *Raft {
	return &Raft{
		ID:                id,
		Peers:             peers,
		State:             Follower,
		CurrentTerm:       0,
		LeaderID:          -1, // -1 no leader has been assigned
		VotedFor:          -1, // -1 means no vote has been given.
		lastHeartbeat:     time.Now(),
		nextIndex:         make(map[string]int32),
		matchIndex:        make(map[string]int32),
		commitIndex:       -1, // -1 means no log entry has been committed.
		lastApplied:       -1, // -1 means no log entry has been applied.
		TestIsPartitioned: false,
		commitReady:       make(chan struct{}, 10000), // this is a bottleneck in high concurrency
		peerCons:          make(map[string]proto.RaftServiceClient),
		proposeCh:         make(chan *LogEntry, 1000), //  for batching
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
	go r.commitReadyLoop()
	go r.logBatcher()

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
				if r.TestIsPartitioned {
					r.mu.Unlock()
					slog.Info("node-info [test_partitioned]", "id", r.ID, "leader", r.LeaderID, "term", r.CurrentTerm, "log", len(r.log), "commitIndex", r.commitIndex, "lastApplied", r.lastApplied)
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

func (r *Raft) Subscribe() chan *LogEntry {
	slog.Info("node subscribed to commits", "node", r.ID)
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan *LogEntry, 100)
	r.commitSubs = append(r.commitSubs, ch)
	return ch
}

// GetState returns the current state of the Raft node.
func (r *Raft) GetState() NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.State
}

// TestPartition simulates a network partition for this node.
// It will stop to respond to RPCs
// currently only for testing
func (r *Raft) TestPartition() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.TestIsPartitioned = true
}

// TestUnpartition restores the simulated network partition for this node.
// currently only for testing
func (r *Raft) TestUnpartition(term int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term != -1 {
		r.CurrentTerm = term
	}
	r.State = Candidate
	r.TestIsPartitioned = false
	r.VotedFor = -1
	r.lastHeartbeat = time.Now()
}

// ElectionTimer runs the election timeout loop.
// If the node is a follower and enough time has passed since the last heartbeat,
// it becomes a candidate and starts an election.
func (r *Raft) ElectionTimer() {
	slog.Info("node started election timer", "node", r.ID)
	for {
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

			if r.TestIsPartitioned {
				voteCh <- false
				return
			}

			// Create a context with timeout.
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				voteCh <- false
				return
			}
			defer conn.Close()

			lastLogTerm := int32(0)
			if r.lastApplied == -1 {
				lastLogTerm = 0
			} else {
				lastLogTerm = r.log[r.lastApplied].Term
			}

			client := proto.NewRaftServiceClient(conn)
			req := &proto.RequestVoteRequest{
				Term:         r.CurrentTerm,
				CandidateID:  r.ID,
				LastLogIndex: r.lastApplied,
				LastLogTerm:  lastLogTerm,
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

func (r *Raft) ReplicateLog() {
	r.mu.Lock()
	if r.State != Leader || r.TestIsPartitioned {
		r.mu.Unlock()
		return
	}
	currentTerm := r.CurrentTerm
	prevCommitIndex := r.commitIndex
	r.mu.Unlock()

	// Track majority responses for each index
	responses := make(map[int32]int)
	var wg sync.WaitGroup

	for _, peerAddr := range r.Peers {
		if peerAddr == r.myAddr {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			r.mu.Lock()
			prevLogIndex := r.nextIndex[addr] - 1
			var entries []*LogEntry
			if prevLogIndex < int32(len(r.log)) {
				entries = r.log[prevLogIndex+1:]
			}

			prevLogTerm := int32(-1)
			if prevLogIndex >= 0 && prevLogIndex < int32(len(r.log)) {
				prevLogTerm = r.log[prevLogIndex].Term
			}

			req := &proto.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderID:     r.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      convertLogEntries(entries),
				LeaderCommit: r.commitIndex,
			}
			r.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := r.sendAppendEntries(ctx, addr, req)
			if err != nil {
				return
			}

			r.mu.Lock()
			if resp.Success {
				r.nextIndex[addr] = prevLogIndex + int32(len(entries)) + 1
				r.matchIndex[addr] = r.nextIndex[addr] - 1

				// Track successful response for each index
				for n := prevCommitIndex + 1; n <= r.matchIndex[addr]; n++ {
					if n < int32(len(r.log)) && r.log[n].Term == currentTerm {
						responses[n]++
					}
				}
			} else {
				if r.nextIndex[addr] > 0 {
					r.nextIndex[addr]--
				}
			}
			r.mu.Unlock()
		}(peerAddr)
	}

	wg.Wait()

	// Update commit index with all responses
	r.mu.Lock()
	if r.State == Leader && r.CurrentTerm == currentTerm {
		for n := prevCommitIndex + 1; n < int32(len(r.log)); n++ {
			if r.log[n].Term == currentTerm && responses[n]+1 > len(r.Peers)/2 {
				r.commitIndex = n
				select {
				case r.commitReady <- struct{}{}:
				default:
					// Ensure commit notification
					go func() {
						r.commitReady <- struct{}{}
					}()
				}
			}
		}
	}
	r.mu.Unlock()
}

// SendHeartbeats sends periodic AppendEntries RPCs (heartbeats) to all peers.
// When a follower receives these, it resets its lastHeartbeat.
func (r *Raft) SendHeartbeats() {
	r.mu.Lock()
	for _, peer := range r.Peers {
		if peer != r.myAddr {
			r.nextIndex[peer] = int32(len(r.log))
			r.matchIndex[peer] = -1
		}
	}
	r.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.ReplicateLog()
			break
		}
	}
}

func convertLogEntries(entries []*LogEntry) []*proto.LogEntry {
	result := make([]*proto.LogEntry, len(entries))
	for i, entry := range entries {
		result[i] = &proto.LogEntry{
			Command: entry.Command,
			Term:    entry.Term,
			Index:   entry.Index,
		}
	}
	return result
}

func (r *Raft) sendAppendEntries(ctx context.Context, addr string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	r.connMu.Lock()
	client, ok := r.peerCons[addr]
	r.connMu.Unlock()

	if !ok {
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
		protoClient := proto.NewRaftServiceClient(conn)

		r.connMu.Lock()
		r.peerCons[addr] = protoClient
		r.connMu.Unlock()

		client = protoClient
	}
	return client.AppendEntries(ctx, req)
}

func (r *Raft) commitReadyLoop() {
	for {
		select {
		case <-r.commitReady:
			r.mu.Lock()
			var entries []*LogEntry
			if r.lastApplied == -1 {
				entries = r.log
				r.lastApplied = r.commitIndex
			} else if r.commitIndex > r.lastApplied {
				entries = r.log[r.lastApplied+1 : r.commitIndex+1]
				r.lastApplied = r.commitIndex
			}
			r.mu.Unlock()

			for _, entry := range entries {
				for _, ch := range r.commitSubs {
					ch <- entry
				}
			}
		}
	}
}
