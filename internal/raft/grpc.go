package raft

import (
	"context"
	"log/slog"
	"time"

	proto "github.com/svfoxat/rafty/internal/api"
)

func (r *Raft) ProposeCommand(ctx context.Context, request *proto.ProposeRequest) (*proto.AppendEntriesResponse, error) {
	slog.Info("propose command", "command", string(request.Command))
	index, err := r.Propose(request.Command)
	if err != nil {
		return nil, err
	}

	return &proto.AppendEntriesResponse{Term: r.CurrentTerm, Success: true, Index: index}, nil
}

// AppendEntries implements the gRPC AppendEntries method.
func (r *Raft) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.TestIsPartitioned {
		return nil, nil
	}

	// Update term if needed
	if req.Term > r.CurrentTerm {
		r.CurrentTerm = req.Term
		r.State = Follower
		r.VotedFor = -1
		r.persist()
	}

	// Reject if term is lower
	if req.Term < r.CurrentTerm {
		return &proto.AppendEntriesResponse{Term: r.CurrentTerm, Success: false}, nil
	}

	// Update heartbeat
	r.lastHeartbeat = time.Now()
	r.LeaderID = req.LeaderID

	// Update commit index
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, int32(len(r.log)-1))
		slog.Debug("follower commit index updated", "commitIndex", r.commitIndex)
		select {
		case r.commitReady <- struct{}{}:
		default:
			go func() {
				r.commitReady <- struct{}{}
			}()
		}
	}

	// Check log consistency
	if req.PrevLogIndex >= int32(len(r.log)) {
		slog.Info("rejecting entries: missing previous entries",
			"prevLogIndex", req.PrevLogIndex,
			"logLen", len(r.log))
		return &proto.AppendEntriesResponse{Term: r.CurrentTerm, Success: false}, nil
	}

	// Handle empty log special case
	if req.PrevLogIndex == -1 {
		if len(r.log) > 0 {
			r.log = r.log[:0]
			r.persist()
		}
	} else if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		// Term mismatch - remove conflicting entries
		slog.Info("removing conflicting entries",
			"prevLogIndex", req.PrevLogIndex,
			"prevLogTerm", req.PrevLogTerm,
			"logTerm", r.log[req.PrevLogIndex].Term)
		r.log = r.log[:req.PrevLogIndex]
		r.persist()

		return &proto.AppendEntriesResponse{Term: r.CurrentTerm, Success: false}, nil
	}

	// Append new entries
	if len(req.Entries) > 0 {
		r.log = r.log[:req.PrevLogIndex+1]
		for _, entry := range req.Entries {
			r.log = append(r.log, &LogEntry{
				Command: entry.Command,
				Term:    entry.Term,
				Index:   entry.Index,
			})
		}
		r.persist()
	}
	return &proto.AppendEntriesResponse{Term: r.CurrentTerm, Success: true}, nil
}

// RequestVote implements the gRPC RequestVote method.
func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.TestIsPartitioned {
		return nil, nil
	}

	slog.Info("received vote request",
		"from", req.CandidateID,
		"term", req.Term,
		"my_term", r.CurrentTerm,
		"my_log_len", len(r.log),
		"candidate_last_idx", req.LastLogIndex)

	// If our log is more up-to-date, reject the vote
	lastLogIndex := int32(len(r.log) - 1)
	lastLogTerm := int32(0)
	if lastLogIndex >= 0 {
		lastLogTerm = r.log[lastLogIndex].Term
	}

	if lastLogTerm > req.LastLogTerm ||
		(lastLogTerm == req.LastLogTerm && lastLogIndex > req.LastLogIndex) {
		return &proto.RequestVoteResponse{
			Term:        r.CurrentTerm,
			VoteGranted: false,
		}, nil
	}

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
		r.persist()
	}

	if r.VotedFor == -1 || r.VotedFor == req.CandidateID {
		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			r.VotedFor = req.CandidateID
			r.persist()
			r.lastHeartbeat = time.Now()
			return &proto.RequestVoteResponse{
				Term:        r.CurrentTerm,
				VoteGranted: true,
			}, nil
		}
	}

	return &proto.RequestVoteResponse{
		Term:        r.CurrentTerm,
		VoteGranted: false,
	}, nil
}
