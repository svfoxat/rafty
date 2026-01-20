package raft

import (
	"encoding/json"
	"os"
	"sync"
)

type Storage interface {
	Save(term int32, votedFor int32, log []*LogEntry) error
	Load() (int32, int32, []*LogEntry, error)
}

type FileStorage struct {
	mu       sync.Mutex
	filePath string
}

type PersistentState struct {
	CurrentTerm int32       `json:"current_term"`
	VotedFor    int32       `json:"voted_for"`
	Log         []*LogEntry `json:"log"`
}

func NewFileStorage(path string) *FileStorage {
	return &FileStorage{filePath: path}
}

func (f *FileStorage) Save(term int32, votedFor int32, log []*LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         log,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Write to a temporary file first to ensure atomicity
	tmpFile := f.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}

	// Sync to disk
	file, err := os.Open(tmpFile)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	file.Close()

	// Rename to final location
	return os.Rename(tmpFile, f.filePath)
}

func (f *FileStorage) Load() (int32, int32, []*LogEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, err := os.Stat(f.filePath); os.IsNotExist(err) {
		return 0, -1, []*LogEntry{}, nil
	}

	data, err := os.ReadFile(f.filePath)
	if err != nil {
		return 0, -1, nil, err
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, -1, nil, err
	}

	return state.CurrentTerm, state.VotedFor, state.Log, nil
}
