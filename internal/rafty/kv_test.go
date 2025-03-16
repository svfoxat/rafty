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

func TestKeyValueMultiKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 200

	servers := CreateCluster(ctx, t, 3)
	leader := CheckHealthyCluster(ctx, t, servers)

	for i := 0; i < count; i++ {
		err := servers[leader].KV().Set(rafty.SetCommand{
			Key:   fmt.Sprintf("key%d", i),
			Value: fmt.Sprintf("value%d", i),
			TTL:   0,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < count; i++ {
		response, ok := servers[leader].KV().Get(fmt.Sprintf("key%d", i))
		if ok != true {
			t.Fatal("Error getting key", i)
		}
		assert.Equal(t, fmt.Sprintf("value%d", i), response)
	}
}

func TestKeyValueOneKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := CreateCluster(ctx, t, 3)
	leader := CheckHealthyCluster(ctx, t, servers)

	err := servers[leader].KV().Set(rafty.SetCommand{
		Key:   "key0",
		Value: "value0",
		TTL:   0,
	})

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1000 * time.Millisecond)

	for i := 0; i < 1; i++ {
		response, ok := servers[leader].KV().Get(fmt.Sprintf("key%d", i))
		if ok != true {
			t.Fatal("Error getting key", i)
		}
		assert.Equal(t, fmt.Sprintf("value%d", i), response)
	}
}

func TestKeyValueDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := CreateCluster(ctx, t, 3)
	leader := CheckHealthyCluster(ctx, t, servers)

	err := servers[leader].KV().Set(rafty.SetCommand{
		Key:   "key0",
		Value: "value0",
		TTL:   0,
	})

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1000 * time.Millisecond)

	response, ok := servers[leader].KV().Get("key0")
	if ok != true {
		t.Fatal("Error getting key0")
	}
	assert.Equal(t, "value0", response)

	err = servers[leader].KV().Delete(rafty.DeleteCommand{
		Key: "key0",
	})
	if err != nil {
		t.Fatal(err)
	}

	// get again, should fail
	_, ok = servers[leader].KV().Get("key0")
	if ok != false {
		t.Fatal("Should not be able to get key0")
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
