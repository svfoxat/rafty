package raft

import (
	"context"
	"github.com/svfoxat/rafty/internal/grpc/proto"
	"log/slog"
	"time"
)

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
	}

	// Update heartbeat
	r.lastHeartbeat = time.Now()
	r.LeaderID = req.LeaderID

	// Reject if term is lower
	if req.Term < r.CurrentTerm {
		return &proto.AppendEntriesResponse{Term: r.CurrentTerm, Success: false}, nil
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
		r.log = r.log[:0]
	} else if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		// Term mismatch - remove conflicting entries
		slog.Info("removing conflicting entries",
			"prevLogIndex", req.PrevLogIndex,
			"prevLogTerm", req.PrevLogTerm,
			"logTerm", r.log[req.PrevLogIndex].Term)
		r.log = r.log[:req.PrevLogIndex]
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
	}

	// Update commit index
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, int32(len(r.log)-1))
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
	if r.lastApplied > -1 { // Only check if we have entries
		lastLogTerm := r.log[r.lastApplied].Term
		if lastLogTerm > req.LastLogTerm ||
			(lastLogTerm == req.LastLogTerm && r.lastApplied > req.LastLogIndex) {
			return &proto.RequestVoteResponse{
				Term:        r.CurrentTerm,
				VoteGranted: false,
			}, nil
		}
	}

	// Reset our term if it's unreasonably high compared to others
	if r.CurrentTerm > req.Term+100 && r.lastApplied < req.LastLogIndex {
		r.CurrentTerm = req.Term
		r.VotedFor = -1
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
	}

	if r.VotedFor == -1 || r.VotedFor == req.CandidateID {
		lastLogTerm := int32(0)
		if r.lastApplied >= 0 {
			lastLogTerm = r.log[r.lastApplied].Term
		}

		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= r.lastApplied) {
			r.VotedFor = req.CandidateID
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
