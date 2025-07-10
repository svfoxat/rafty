package raft

import (
	"log/slog"
	"time"
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
	defer r.mu.Unlock()

	if r.State != Leader {
		return 0, nil
	}

	currentLen := int32(len(r.log))

	log := &LogEntry{Command: command, Term: r.CurrentTerm, Index: currentLen + 1}
	r.log = append(r.log, log)
	slog.Info("leader submit", "term", r.CurrentTerm, "node", r.ID, "index", len(r.log))

	return currentLen + 1, nil
}

func (r *Raft) Propose(command []byte) (int32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != Leader {
		return -1, nil
	}

	log := &LogEntry{Command: command, Term: -1, Index: -1}
	slog.Info("leader propose")

	r.proposeCh <- log
	return int32(len(r.log)) + 1, nil
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
