package rafty

import (
	"fmt"
	"github.com/svfoxat/rafty/internal/raft"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type KVStore struct {
	mu   sync.RWMutex
	raft *raft.Raft

	// preliminary in-memory store
	store map[string]*StoreEntry

	entryCommited chan int32
}

type StoreEntry struct {
	Value []byte
}

func NewKVStore(r *raft.Raft) *KVStore {
	return &KVStore{
		raft:          r,
		entryCommited: make(chan int32, 10000),
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
			k.mu.Lock()
			k.store[cmdSplit[1]] = &StoreEntry{[]byte(cmdSplit[2])}
			k.entryCommited <- entry.Index
			k.mu.Unlock()

		case "DEL":
			//slog.Info("DEL", "key", cmdSplit[1], "node", k.raft.ID)
			k.mu.Lock()
			delete(k.store, cmdSplit[1])
			k.entryCommited <- entry.Index
			k.mu.Unlock()
		}
	}
}

func (k *KVStore) Get(key string) ([]byte, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	storeEntry, ok := k.store[key]
	if !ok {
		return []byte{}, false
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
			k.mu.RLock()
			// Double check if command was actually committed
			if _, ok := k.store[cmd.Key]; ok {
				k.mu.RUnlock()
				return nil
			}
			k.mu.RUnlock()
			return fmt.Errorf("timeout waiting for commit")

		case id := <-k.entryCommited:
			if id >= index {
				// Verify the key exists
				k.mu.RLock()
				_, ok := k.store[cmd.Key]
				k.mu.RUnlock()
				if ok {
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
