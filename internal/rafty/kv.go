package rafty

import (
	"github.com/svfoxat/rafty/internal/raft"
	"log/slog"
	"strings"
)

type KVStore struct {
	raft       *raft.Raft
	commitChan chan *raft.LogEntry
	//commitSubs []chan *raft.LogEntry

	// preliminary in-memory store
	store map[string]string
}

func NewKVStore(r *raft.Raft) *KVStore {
	return &KVStore{
		raft:       r,
		commitChan: make(chan *raft.LogEntry),
		store:      make(map[string]string),
	}
}

func (k *KVStore) Start() {
	slog.Info("starting kv store")
	go k.processCommits()
}

func (k *KVStore) processCommits() {
	commitChan := k.raft.Subscribe()

	for entry := range commitChan {
		cmdSplit := strings.Split(entry.Command.(string), " ")
		switch cmdSplit[0] {
		case "SET":
			slog.Info("SET", "key", cmdSplit[1], "value", cmdSplit[2], "node", k.raft.ID)
			k.store[cmdSplit[1]] = cmdSplit[2]
		}
	}
}

func (k *KVStore) Get(key string) (string, bool) {
	value, ok := k.store[key]
	if !ok {
		return "", false
	}

	return value, true
}

func (k *KVStore) Set(key, value string) {
	k.raft.Submit("SET " + key + " " + value)
}
