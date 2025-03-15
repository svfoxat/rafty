package raft_test //nolint:typecheck

import (
	"context"
	"fmt"
	"github.com/svfoxat/rafty/internal/raft"
	"testing"
	"time"
)

func TestLeaderElection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	nodes := StartCluster(ctx, t, 3, 0)
	leader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("leader is %+v after %dms", leader, time.Since(start).Milliseconds())
}

func TestAllDyingAndRestarting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	nodes := StartCluster(ctx, t, 3, 100)
	leader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("leader is %+v after %dms", leader, time.Since(start).Milliseconds())

	// Kill all the nodes
	t.Log("stop all")
	for _, node := range nodes {
		node.Stop()
	}

	time.Sleep(2000 * time.Millisecond)

	// Restart all the nodes
	t.Log("restart all")
	for _, node := range nodes {
		node.Restart()
	}

	// Wait for a new leader to be elected
	start = time.Now()
	newLeader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("leader is %+v after %dms", newLeader, time.Since(start).Milliseconds())
}

func TestDyingLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	nodes := StartCluster(ctx, t, 3, 200)
	leader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("leader is %+v after %dms", leader, time.Since(start).Milliseconds())

	// Kill the leader
	start = time.Now()
	nodes[leader].Stop()

	// Wait for a new leader to be elected
	newLeader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("new leader is %+v after %dms", newLeader, time.Since(start).Milliseconds())
	if newLeader == leader {
		t.Fatal("new leader is the same as the old leader")
	}
}

func TestDyingLeaderWithRejoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	nodes := StartCluster(ctx, t, 3, 300)
	leader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("leader is %+v after %dms", leader, time.Since(start).Milliseconds())

	// Kill the leader
	start = time.Now()
	t.Log("stopping leader")
	nodes[leader].Stop()

	// Wait for a new leader to be elected
	newLeader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("new leader is %+v after %dms", newLeader, time.Since(start).Milliseconds())
	if newLeader == leader {
		t.Fatal("new leader is the same as the old leader")
	}

	time.Sleep(2000 * time.Millisecond)

	// Restart the old leader
	t.Log("restarting old leader")
	nodes[leader].Restart()
	time.Sleep(time.Millisecond * 100)

	if nodes[leader].CurrentTerm != nodes[newLeader].CurrentTerm {
		t.Fatal("old leader did not rejoin the cluster")
	}
}

func StartCluster(ctx context.Context, t *testing.T, count int, offset int) []*raft.Raft {
	var nodes []*raft.Raft
	var peers []string

	for i := 0; i < count; i++ {
		peers = append(peers, fmt.Sprintf("127.0.0.1:%d", 12345+offset+i))
	}
	for i := 0; i < count; i++ {
		nodes = append(nodes, raft.NewNode(int32(i), peers))
	}

	// start the nodes
	for idx, node := range nodes {
		go func() {
			node.Start(ctx, "127.0.0.1", 12345+offset+idx)
		}()
	}
	return nodes
}

// CheckLeader Poll the nodes for a leader for up to 10 seconds.
func CheckLeader(ctx context.Context, nodes []*raft.Raft) (int, error) {
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-timeout:
			return -1, fmt.Errorf("timeout: no leader elected after 10 seconds")
		case <-ticker.C:
			leaderCount := 0
			var leaders []int

			for _, node := range nodes {
				if node.GetState() == raft.Leader {
					leaderCount++
					leaders = append(leaders, int(node.ID))
				}
			}
			if leaderCount > 1 {
				return -1, fmt.Errorf("more than one leader elected; found %d leaders", leaderCount)
			}
			if leaderCount == 1 {
				return leaders[0], nil
			}
		}
	}
}
