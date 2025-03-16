package rafty

import (
	"fmt"
	"github.com/svfoxat/rafty/internal/raft"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KVStore struct {
	mu   sync.RWMutex
	raft *raft.Raft

	// preliminary in-memory store
	// TODO: maybe a sync.Map would be better suited
	store map[string]*StoreEntry

	entryCommited chan int32
}

type StoreEntry struct {
	Value      []byte
	Ttl        int32 // Time to live in seconds
	insertedAt time.Time
}

func NewKVStore(r *raft.Raft) *KVStore {
	return &KVStore{
		raft:          r,
		entryCommited: make(chan int32, 10000), // this is a bottleneck in high concurrency
		store:         make(map[string]*StoreEntry),
	}
}

func (k *KVStore) Start() {
	slog.Info("starting kv store")
	go k.processCommits()
	go k.TTLChecker()
}

// processCommits is a goroutine that listens for new commits from the raft node
// and processes them. It is responsible for applying the changes to the store
func (k *KVStore) processCommits() {
	commitChan := k.raft.Subscribe()

	for entry := range commitChan {
		cmd := string(entry.Command)
		cmdSplit := strings.Split(cmd, " ")

		switch cmdSplit[0] {
		case "SET":
			// always assume ttl is present
			ttl, err := strconv.Atoi(cmdSplit[3])
			if err != nil {
				slog.Error("error parsing ttl", "ttl", cmdSplit[3])
				continue
			}
			k.mu.Lock()
			k.store[cmdSplit[1]] = &StoreEntry{[]byte(cmdSplit[2]), int32(ttl), time.Now()}
			k.entryCommited <- entry.Index
			k.mu.Unlock()

		case "DEL":
			k.mu.Lock()
			delete(k.store, cmdSplit[1])
			k.entryCommited <- entry.Index
			k.mu.Unlock()
		}
	}
}

func (k *KVStore) TTLChecker() {
	ticker := time.NewTicker(200 * time.Millisecond)
	for {
		if k.raft.State != raft.Leader {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if <-ticker.C; true {
			k.mu.RLock()
			start := time.Now()
			deleted := 0
			var wg sync.WaitGroup
			for key, entry := range k.store {
				if entry.Ttl > 0 && time.Since(entry.insertedAt) > time.Duration(entry.Ttl)*time.Second {
					go func() {
						wg.Add(1)
						defer wg.Done()
						_, err := k.raft.Submit([]byte(fmt.Sprintf("DEL %s", key)))
						if err != nil {
							slog.Error("error submitting ttl delete", "key", key, "err", err)
							return
						}
						deleted++
					}()
				}
			}
			k.mu.RUnlock()
			wg.Wait()
			slog.Info("ttl check", "deleted", deleted, "lock_duration", time.Since(start))
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

	index, err := k.raft.Submit([]byte(fmt.Sprintf("SET %s %s %d", cmd.Key, cmd.Value, cmd.TTL)))
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
