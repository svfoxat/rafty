package raft_test

import (
	"context"
	"github.com/svfoxat/rafty/internal/raft"
	"testing"
	"time"
)

func getLeaderID(nodes ...*raft.Raft) int32 {
	for _, node := range nodes {
		if node.GetState() == raft.Leader {
			return node.ID
		}
	}
	return -1
}

func TestLeaderElection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Define addresses for three nodes.
	node1Addr := "127.0.0.1:12345"
	node2Addr := "127.0.0.1:12346"
	node3Addr := "127.0.0.1:12347"

	// Create three Raft nodes with unique IDs and their peer addresses.
	node1 := raft.NewNode(1, []string{node2Addr, node3Addr})
	node2 := raft.NewNode(2, []string{node1Addr, node3Addr})
	node3 := raft.NewNode(3, []string{node1Addr, node2Addr})

	go func() {
		err := node1.Start(ctx, "127.0.0.1", 12345)
		if err != nil {
			t.Errorf("failed to start node1: %v", err)
			return
		}
	}()

	go func() {
		err := node2.Start(ctx, "127.0.0.1", 12346)
		if err != nil {
			t.Errorf("failed to start node3: %v", err)
			return
		}
	}()

	go func() {
		err := node3.Start(ctx, "127.0.0.1", 12347)
		if err != nil {
			t.Errorf("failed to start node3: %v", err)
			return
		}
	}()

	// Poll the nodes for a leader for up to 10 seconds.
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	start := time.Now()
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout: no leader elected after 10 seconds")
		case <-ticker.C:
			leaderCount := 0
			if node1.GetState() == raft.Leader {
				leaderCount++
			}
			if node2.GetState() == raft.Leader {
				leaderCount++
			}
			if node3.GetState() == raft.Leader {
				leaderCount++
			}
			if leaderCount > 1 {
				t.Fatalf("Error: more than one leader elected; found %d leaders", leaderCount)
			}
			if leaderCount == 1 {
				leaderID := getLeaderID(node1, node2, node3)
				t.Logf("Leader elected after %dms: Node %d", time.Since(start).Milliseconds(), leaderID)
				return
			}
		}
	}
}
