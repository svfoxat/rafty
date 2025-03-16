package raft

import "log/slog"

type LogEntry struct {
	Command any
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

func (r *Raft) Submit(command any) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != Leader {
		return nil
	}

	r.log = append(r.log, &LogEntry{Command: command, Term: r.CurrentTerm, Index: r.commitIndex + 1})

	slog.Info("log entry added", "term", r.CurrentTerm, "node", r.ID)
	return nil
}
