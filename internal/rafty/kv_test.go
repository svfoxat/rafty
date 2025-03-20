package rafty_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/svfoxat/rafty/internal/raft"
	"github.com/svfoxat/rafty/internal/rafty"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := CreateCluster(ctx, 3)
	leader := CheckHealthyCluster(ctx, servers)
	t.Logf("%d is the leader", leader)
}

func TestKeyValueMultiKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 1000

	servers := CreateCluster(ctx, 3)
	leader := CheckHealthyCluster(ctx, servers)
	t.Logf("%d is the leader", leader)

	for i := 0; i < count; i++ {
		go func() {
			err := servers[leader].KV().Set(rafty.SetCommand{
				Key:   fmt.Sprintf("key%d", i),
				Value: fmt.Sprintf("value%d", i),
				TTL:   0,
			})
			if err != nil {
				t.Fatal(err)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	for i := 0; i < count; i++ {
		response, ok := servers[leader].KV().Get(fmt.Sprintf("key%d", i))
		if ok != true {
			t.Fatal("Error getting key", i)
		}
		assert.Equal(t, fmt.Sprintf("value%d", i), string(response))
	}
}

func TestKeyValueMultiKeysTtl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 100

	servers := CreateCluster(ctx, 3)
	leader := CheckHealthyCluster(ctx, servers)
	t.Logf("%d is the leader", leader)

	for i := 0; i < count; i++ {
		_, err := servers[leader].KV().Set(rafty.SetCommand{
			Key:   fmt.Sprintf("key%d", i),
			Value: fmt.Sprintf("value%d", i),
			TTL:   int32(rand.Intn(10-5) + 5),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(11 * time.Second)

	for i := 0; i < count; i++ {
		_, ok := servers[leader].KV().Get(fmt.Sprintf("key%d", i))
		if ok == true {
			t.Fatal("key should be expired", i)
		}
	}
}

func TestKeyValueMultiKeysConcurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 1000      // More entries to better test concurrency
	concurrency := 100 // Fewer goroutines for more stability
	entriesPerGoroutine := count / concurrency

	servers := CreateCluster(ctx, 3)
	leader := CheckHealthyCluster(ctx, servers)
	t.Logf("%d is the leader", leader)

	// Channel to collect errors from goroutines
	errCh := make(chan error, concurrency)

	var wg sync.WaitGroup
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				key := offset*entriesPerGoroutine + i
				response, err := servers[leader].KV().Set(rafty.SetCommand{
					Key:   fmt.Sprintf("key%d", key),
					Value: fmt.Sprintf("value%d", key),
					TTL:   0,
				})
				if err != nil {
					errCh <- fmt.Errorf("offset %d, key %d: %w", offset, key, err)
					return
				}
				t.Logf("set success with idx=%d in %dms", response.Index, response.Duration.Milliseconds())
			}
		}(c)
	}

	// Wait for all writes and check for errors
	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		t.Fatal(err)
	}

	// Give cluster time to replicate
	time.Sleep(100 * time.Millisecond)

	// Verify all entries
	verifyWg := sync.WaitGroup{}
	verifyErrors := make(chan error, count)

	for i := 0; i < count; i++ {
		verifyWg.Add(1)
		go func(key int) {
			defer verifyWg.Done()
			response, ok := servers[leader].KV().Get(fmt.Sprintf("key%d", key))
			if !ok {
				verifyErrors <- fmt.Errorf("key%d not found", key)
				return
			}
			expected := fmt.Sprintf("value%d", key)
			if string(response) != expected {
				verifyErrors <- fmt.Errorf("key%d: expected %s, got %s", key, expected, string(response))
			}
		}(i)
	}

	go func() {
		verifyWg.Wait()
		close(verifyErrors)
	}()

	for err := range verifyErrors {
		t.Error(err)
	}
}

func TestKeyValueOneKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := CreateCluster(ctx, 3)
	leader := CheckHealthyCluster(ctx, servers)
	t.Logf("%d is the leader", leader)

	response, err := servers[leader].KV().Set(rafty.SetCommand{
		Key:   "key0",
		Value: "value0",
		TTL:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("set success with idx=%d in %dms", response.Index, response.Duration.Milliseconds())

	for i := 0; i < 1; i++ {
		response, ok := servers[leader].KV().Get(fmt.Sprintf("key%d", i))
		if ok != true {
			t.Fatal("Error getting key", i)
		}
		assert.Equal(t, fmt.Sprintf("value%d", i), string(response))
	}
}

func TestKeyValueDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := CreateCluster(ctx, 3)
	leader := CheckHealthyCluster(ctx, servers)
	t.Logf("%d is the leader", leader)

	_, err := servers[leader].KV().Set(rafty.SetCommand{
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
	assert.Equal(t, "value0", string(response))

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

func BenchmarkKVStore_Set(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := CreateCluster(ctx, 3)
	leader := CheckHealthyCluster(ctx, servers)
	b.Logf("%d is the leader", leader)
	b.ResetTimer()

	for b.Loop() {
		response, err := servers[leader].KV().Set(rafty.SetCommand{
			Key:   "key0",
			Value: "value0",
			TTL:   0,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Logf("set success with idx=%d in %d", response.Index, response.Duration.Microseconds())
	}
}

func CreateCluster(ctx context.Context, nodeCount int) []*rafty.Server {
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

func CheckHealthyCluster(ctx context.Context, servers []*rafty.Server) int32 {
	timeout := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-timeout.C:
			panic("Timeout waiting for leader")
		case <-ticker.C:
			for _, srv := range servers {
				if srv.Status().State == raft.Leader {
					return srv.Status().ID
				}
			}
		}
	}
}
