package rafty

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/svfoxat/rafty/internal/raft"
)

type KVStore struct {
	mu   sync.RWMutex
	raft *raft.Raft

	db *pebble.DB

	entryCommited chan int32
}

type StoreEntry struct {
	Value      []byte
	Ttl        int32 // Time to live in seconds
	insertedAt time.Time
}

func NewKVStore(r *raft.Raft) (*KVStore, error) {
	db, err := pebble.Open("kvstore", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		return nil, err
	}

	return &KVStore{
		raft:          r,
		entryCommited: make(chan int32, 10000), // this is a bottleneck in high concurrency
		db:            db,
	}, nil
}

func (k *KVStore) Start() {
	slog.Info("starting kv store")
	go k.processCommits()
	// go k.TTLChecker()
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

			entry := StoreEntry{
				Value:      []byte(cmdSplit[2]),
				Ttl:        int32(ttl),
				insertedAt: time.Now(),
			}

			err = k.db.Set([]byte(cmdSplit[1]), []byte(entry.Value), pebble.Sync)
			if err != nil {
				slog.Error("error setting key", "key", cmdSplit[1], "err", err)
				continue
			}

		case "DEL":
			err := k.db.Delete([]byte(cmdSplit[1]), pebble.Sync)
			if err != nil {
				slog.Error("error deleting key", "key", cmdSplit[1], "err", err)
				continue
			}
		}
	}
}

func (k *KVStore) Get(key string) ([]byte, bool) {
	val, closer, err := k.db.Get([]byte(key))
	if err != nil {
		return []byte{}, false
	}
	defer closer.Close()
	return val, true
}

type SetCommand struct {
	Key   string
	Value string
	TTL   int32
}

type SetCommandResponse struct {
	Index    int32
	Duration time.Duration
}

func (k *KVStore) Set(cmd SetCommand) (*SetCommandResponse, error) {
	if k.raft.State != raft.Leader {
		return nil, fmt.Errorf("not leader")
	}

	start := time.Now()
	index, err := k.raft.Propose([]byte(fmt.Sprintf("SET %s %s %d", cmd.Key, cmd.Value, cmd.TTL)))
	if err != nil {
		return nil, fmt.Errorf("error submitting command: %v", err)
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Double check if command was actually committed
			_, closer, err := k.db.Get([]byte(cmd.Key))
			if err == nil {
				closer.Close()
				return &SetCommandResponse{Index: index, Duration: time.Since(start)}, nil
			}
			closer.Close()
			return nil, fmt.Errorf("timeout waiting for entry to be committed, may be written")

		case id := <-k.entryCommited:
			if id >= index {
				_, closer, err := k.db.Get([]byte(cmd.Key))
				closer.Close()
				if err == nil {
					return &SetCommandResponse{Index: index, Duration: time.Since(start)}, nil
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
