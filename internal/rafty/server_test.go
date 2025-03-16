package rafty_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/svfoxat/rafty/internal/raft"
	"github.com/svfoxat/rafty/internal/rafty"
	"strconv"
	"testing"
	"time"
)

func TestStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := CreateCluster(ctx, t, 3)
	leader := CheckHealthyCluster(ctx, t, servers)
	t.Log(leader)
}

// TestKeyValue tests the key-value store functionality.
// It creates a cluster of 3 nodes, sets 100 key-value pairs
// and checks if the data is replicated to all the followers.
func TestKeyValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 100

	servers := CreateCluster(ctx, t, 3)
	leader := CheckHealthyCluster(ctx, t, servers)

	for i := 0; i < count; i++ {
		servers[leader].KV().Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < count; i++ {
		response, ok := servers[leader].KV().Get(fmt.Sprintf("key%d", i))
		if ok != true {
			t.Fatal("Error getting key", i)
		}
		assert.Equal(t, fmt.Sprintf("value%d", i), response)
	}

	// check the kvs of all the servers
	for i, srv := range servers {
		if i == leader {
			continue
		}
		for j := 0; j < count; j++ {
			response, ok := srv.KV().Get(fmt.Sprintf("key%d", j))
			if ok != true {
				t.Fatal("Error getting key", j)
			}
			assert.Equal(t, fmt.Sprintf("value%d", j), response)
		}
	}
}

func CreateCluster(ctx context.Context, t *testing.T, nodeCount int) []*rafty.Server {
	peers := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		peers[i] = "127.0.0.1:" + strconv.Itoa(12345+i)
	}

	servers := make([]*rafty.Server, nodeCount)
	for i := 0; i < nodeCount; i++ {
		cfg := &rafty.ServerConfig{
			ID:    int32(i),
			Peers: peers,
		}
		srv := rafty.NewServer(cfg)
		go srv.Start(ctx, 13345+i, 12345+i)
		servers[i] = srv
	}
	return servers
}

func CheckHealthyCluster(ctx context.Context, t *testing.T, servers []*rafty.Server) int {
	timeout := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-timeout.C:
			t.Fatal("Timeout waiting for leader")
		case <-ticker.C:
			for i, srv := range servers {
				if srv.Status().State == raft.Leader {
					t.Logf("Server %d is the leader", srv.Status().ID)
					return i
				}
			}
		}
	}
}
