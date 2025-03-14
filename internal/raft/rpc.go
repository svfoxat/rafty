package raft

import "log/slog"

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	slog.Info("raft-node received request-vote", "args", args)
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if (n.votedFor == 0 || n.votedFor == args.CandidateId) && args.LastLogIndex >= n.lastLogIndex {
		reply.Term = n.currentTerm
		reply.VoteGranted = true
		return nil
	}

	reply.Term = n.currentTerm
	reply.VoteGranted = false
	return nil
}
