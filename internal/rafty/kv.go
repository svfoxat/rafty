package rafty

import (
	"fmt"
	"github.com/svfoxat/rafty/internal/raft"
	"log/slog"
	"strings"
	"time"
)

type KVStore struct {
	raft *raft.Raft

	// preliminary in-memory store
	store map[string]*StoreEntry

	entryCommited chan int32
}

type StoreEntry struct {
	Value string
}

func NewKVStore(r *raft.Raft) *KVStore {
	return &KVStore{
		raft:          r,
		entryCommited: make(chan int32, 1000),
		store:         make(map[string]*StoreEntry),
	}
}

func (k *KVStore) Start() {
	slog.Info("starting kv store")
	go k.processCommits()
	//go k.TtlChecker()
}

func (k *KVStore) processCommits() {
	commitChan := k.raft.Subscribe()

	for entry := range commitChan {
		cmd := string(entry.Command)
		cmdSplit := strings.Split(cmd, " ")

		switch cmdSplit[0] {
		case "SET":
			//slog.Info("SET", "key", cmdSplit[1], "value", cmdSplit[2], "node", k.raft.ID)
			k.store[cmdSplit[1]] = &StoreEntry{cmdSplit[2]}
			k.entryCommited <- entry.Index
		case "DEL":
			//slog.Info("DEL", "key", cmdSplit[1], "node", k.raft.ID)
			delete(k.store, cmdSplit[1])
			k.entryCommited <- entry.Index
		}
	}
}

func (k *KVStore) Get(key string) (string, bool) {
	storeEntry, ok := k.store[key]
	if !ok {
		return "", false
	}
	return storeEntry.Value, true
}

type SetCommand struct {
	Key   string
	Value string
	TTL   int32
}

func (k *KVStore) Set(cmd SetCommand) error {
	if k.raft.State != raft.Leader {
		return fmt.Errorf("not leader")
	}

	index, err := k.raft.Submit([]byte(fmt.Sprintf("SET %s %s", cmd.Key, cmd.Value)))
	if err != nil {
		return fmt.Errorf("error submitting command: %v", err)
	}

	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timeout waiting for commit")
		case id := <-k.entryCommited:
			if id == index {
				return nil
			} else if id > index {
				// Check if our command was already committed
				if _, ok := k.store[cmd.Key]; ok {
					return nil
				}
			}
		}
	}
}

type DeleteCommand struct {
	Key string
}

func (k *KVStore) Delete(cmd DeleteCommand) error {
	if k.raft.State != raft.Leader {
		return fmt.Errorf("not leader")
	}

	index, err := k.raft.Submit([]byte(fmt.Sprintf("DEL %s", cmd.Key)))
	if err != nil {
		return fmt.Errorf("error submitting command")
	}

	timeout := 5 * time.Second
	for {
		select {
		case <-time.After(timeout):
			slog.Error("timeout waiting for entry to be committed, may be written")
			return nil
		case id := <-k.entryCommited:
			if id <= index {
				slog.Info("kv success", "key", cmd.Key)
				return nil
			}
		}
	}
}
