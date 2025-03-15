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

	if r.State == Dead {
		return nil, nil
	}

	isHeartbeat := false
	if len(req.Entries) == 0 {
		isHeartbeat = true
	} else {
		slog.Info("received append entries", "entries", len(req.Entries))
	}

	entries := make([]*LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = &LogEntry{
			Command: entry.Command,
			Term:    entry.Term,
			Index:   entry.Index,
		}
	}
	r.lastHeartbeat = time.Now()
	r.LeaderID = req.LeaderID

	if req.Term < r.CurrentTerm {
		return &proto.AppendEntriesResponse{
			Term:    r.CurrentTerm,
			Success: false,
		}, nil
	}

	if req.Term > r.CurrentTerm {
		slog.Warn("higher term in request than currently")
		r.CurrentTerm = req.Term
		r.VotedFor = -1
		r.State = Follower
		return nil, nil
	}

	if isHeartbeat {
		return &proto.AppendEntriesResponse{
			Term:    r.CurrentTerm,
			Success: true,
		}, nil
	}

	if len(r.log) > 0 && req.PrevLogIndex > 0 {
		if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			slog.Warn("log term mismatch", "prevLogIndex.Term", r.log[req.PrevLogIndex].Term, "prevLogTerm", req.PrevLogTerm)

			return &proto.AppendEntriesResponse{
				Term:    r.CurrentTerm,
				Success: false,
			}, nil
		} else {
			// If an existing entry conflicts with a new one (same index
			// but different terms), delete the existing entry and all that
			// follow it (§5.3)
			r.log = r.log[:req.PrevLogIndex+1]
			r.log = append(r.log, entries...)
			r.lastApplied = int32(len(r.log)) - 1
		}
	} else {
		r.log = entries
		r.lastApplied = int32(len(r.log)) - 1
	}

	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, int32(len(r.log)-1))
	}

	return &proto.AppendEntriesResponse{
		Term:    r.CurrentTerm,
		Success: true,
	}, nil
}

// RequestVote implements the gRPC RequestVote method.
func (r *Raft) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State == Dead {
		return nil, nil
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
