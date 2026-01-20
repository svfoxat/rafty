package raft

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	proto "github.com/svfoxat/rafty/internal/api"
)

type LogEntry struct {
	Command []byte
	Term    int32
	Index   int32
}

func (r *Raft) GetLogEntries() []*LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	entries := make([]*LogEntry, len(r.log))
	copy(entries, r.log)
	return entries
}

func (r *Raft) Submit(command []byte) (int32, error) {
	r.mu.Lock()

	if r.State != Leader {
		r.mu.Unlock()
		return 0, nil
	}

	currentLen := int32(len(r.log))

	log := &LogEntry{Command: command, Term: r.CurrentTerm, Index: currentLen + 1}
	r.log = append(r.log, log)
	r.persist()
	slog.Info("leader submit", "term", r.CurrentTerm, "node", r.ID, "index", len(r.log))

	r.mu.Unlock()
	return currentLen + 1, nil
}

func (r *Raft) Propose(command []byte) (int32, error) {
	r.mu.Lock()

	slog.Info("propose leadercheck", "command", string(command))
	if r.State == Leader {
		slog.Info("Iam se leader")
		log := &LogEntry{Command: command, Term: -1, Index: -1}
		r.proposeCh <- log
		idx := int32(len(r.log)) + 1
		r.mu.Unlock()
		return idx, nil
	}

	// propose the log append to the leader
	leaderId := r.LeaderID
	r.mu.Unlock()

	if leaderId == -1 {
		return 0, fmt.Errorf("no leader elected")
	}

	idx, _, err := r.forwardToLeader(leaderId, command)
	if err != nil {
		return 0, err
	}
	return idx, nil
}

func (r *Raft) forwardToLeader(leaderId int32, command []byte) (int32, bool, error) {
	leaderHost := fmt.Sprintf("rafty-%d.rafty:12345", leaderId)

	slog.Info("forwarding to leader", "leader", leaderHost, "me", r.ID)
	slog.Info("map", "peers", r.peerCons)

	leaderClient, ok := r.peerCons[leaderHost]
	if !ok {
		return 0, false, fmt.Errorf("no connection to leader %d", leaderId)
	}

	res, err := leaderClient.ProposeCommand(context.Background(), &proto.ProposeRequest{Command: command})
	if err != nil {
		return 0, false, err
	}
	return res.Index, res.Success, nil
}

func (r *Raft) logBatcher() {
	for {
		batchSize := 0
		timer := time.NewTimer(5 * time.Millisecond)
	loop:
		for {
			select {
			case entry := <-r.proposeCh:
				r.mu.Lock()
				r.log = append(r.log, &LogEntry{Command: entry.Command, Term: r.CurrentTerm, Index: int32(len(r.log)) + 1})
				r.persist()
				batchSize++
				r.mu.Unlock()
			case <-timer.C:
				break loop
			}
		}
		timer.Stop()

		if batchSize > 0 {
			// Trigger log replication to followers immediately
			slog.Info("batched replication", "size", batchSize)
			go r.ReplicateLog()
		}
	}
}
