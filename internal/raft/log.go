package raft

import "log/slog"

type LogEntry struct {
	Command any
	Term    int32
}

func (r *Raft) Submit(command any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != Leader {
		return false
	}

	r.log = append(r.log, LogEntry{Command: command, Term: r.CurrentTerm})
	slog.Info("log entry added", "term", r.CurrentTerm, "node", r.ID)
	return true
}
