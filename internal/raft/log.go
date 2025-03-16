package raft

import (
	"log/slog"
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

	r.log = append(r.log, &LogEntry{Command: command, Term: r.CurrentTerm, Index: currentLen + 1})

	slog.Info("leader submit", "term", r.CurrentTerm, "node", r.ID, "index", len(r.log))
	return currentLen + 1, nil
}
