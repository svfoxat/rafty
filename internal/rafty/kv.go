package rafty

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
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
	db, err := pebble.Open("data/kvstore", &pebble.Options{})
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
		slog.Info("processing commit", "index", entry.Index, "command", cmd)
		cmdSplit := strings.Split(cmd, " ")

		switch cmdSplit[0] {
		case "SET":
			// always assume ttl is present
			if len(cmdSplit) < 4 {
				slog.Error("invalid SET command format", "cmd", cmd)
				continue
			}
			ttl, err := strconv.Atoi(cmdSplit[3])
			if err != nil {
				slog.Error("error parsing ttl", "ttl", cmdSplit[3])
				continue
			}

			entryData := StoreEntry{
				Value:      []byte(cmdSplit[2]),
				Ttl:        int32(ttl),
				insertedAt: time.Now(),
			}

			err = k.db.Set([]byte(cmdSplit[1]), entryData.Value, pebble.Sync)
			if err != nil {
				slog.Error("error setting key", "key", cmdSplit[1], "err", err)
				continue
			}

		case "DEL":
			if len(cmdSplit) < 2 {
				slog.Error("invalid DEL command format", "cmd", cmd)
				continue
			}
			err := k.db.Delete([]byte(cmdSplit[1]), pebble.Sync)
			if err != nil {
				slog.Error("error deleting key", "key", cmdSplit[1], "err", err)
				continue
			}
		}

		select {
		case k.entryCommited <- entry.Index:
			slog.Info("signaled commit", "index", entry.Index)
		default:
			slog.Warn("commit signal channel full", "index", entry.Index)
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
	start := time.Now()
	index, err := k.raft.Propose([]byte(fmt.Sprintf("SET %s %s %d", cmd.Key, cmd.Value, cmd.TTL)))
	if err != nil {
		return nil, fmt.Errorf("error submitting command: %v", err)
	}

	slog.Info("waiting for commit", "index", index, "key", cmd.Key)

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			slog.Warn("timeout waiting for commit, double checking DB", "index", index, "key", cmd.Key)
			// Double check if command was actually committed
			val, closer, err := k.db.Get([]byte(cmd.Key))
			if err == nil {
				closer.Close()
				slog.Info("found value in DB after timeout", "key", cmd.Key, "value", string(val))
				return &SetCommandResponse{Index: index, Duration: time.Since(start)}, nil
			}
			return nil, fmt.Errorf("timeout waiting for entry to be committed, may be written")

		case id := <-k.entryCommited:
			slog.Debug("received commit signal", "received_id", id, "waiting_for", index)
			if id >= index {
				_, closer, err := k.db.Get([]byte(cmd.Key))
				if err == nil {
					closer.Close()
					slog.Info("successfully committed and verified", "index", index, "key", cmd.Key, "duration", time.Since(start))
					return &SetCommandResponse{Index: index, Duration: time.Since(start)}, nil
				}
				slog.Warn("commit signaled but key not found in DB", "index", index, "key", cmd.Key)
			}
		}
	}
}

type DeleteCommand struct {
	Key string
}

func (k *KVStore) Delete(cmd DeleteCommand) error {
	index, err := k.raft.Propose([]byte(fmt.Sprintf("DEL %s", cmd.Key)))
	if err != nil {
		return fmt.Errorf("error submitting command: %v", err)
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timeout waiting for entry to be committed")
		case id := <-k.entryCommited:
			if id >= index {
				slog.Info("kv success", "key", cmd.Key)
				return nil
			}
		}
	}
}
