package rafttest

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/svfoxat/rafty/internal/raft"
	"sync/atomic"
	"testing"
	"time"
)

// Add at the top of raft_test.go
var nextPortOffset uint32 = 0

// Add this function to get a unique port range
func getUniquePortOffset() int {
	offset := atomic.AddUint32(&nextPortOffset, 1000)
	return int(offset)
}

const (
	testTimeout     = 10 * time.Second
	ReplicationWait = 2 * time.Second
	PollInterval    = 100 * time.Millisecond
	basePort        = 12345
)

type TestCluster struct {
	Nodes     []*raft.Raft
	ctx       context.Context
	cancel    context.CancelFunc
	t         *testing.T
	portStart int
}

func SetupCluster(t *testing.T, nodeCount int) *TestCluster {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	portOffset := getUniquePortOffset()
	nodes := StartCluster(ctx, t, nodeCount, portOffset)

	return &TestCluster{
		Nodes:     nodes,
		ctx:       ctx,
		cancel:    cancel,
		t:         t,
		portStart: basePort + portOffset,
	}
}

func (tc *TestCluster) Cleanup() {
	tc.cancel()
}

func (tc *TestCluster) SubmitCommands(leaderID int, prefix string, count int) {
	leader := tc.Nodes[leaderID]
	for i := 0; i < count; i++ {
		command := fmt.Sprintf("%s_%d", prefix, i)
		_, err := leader.Submit(command) // Removed ctx parameter
		require.NoError(tc.t, err, "failed to submit command %s", command)
	}
	tc.t.Logf("Submitted %d commands with prefix '%s' to leader %d", count, prefix, leaderID)
}

func (tc *TestCluster) WaitForLeader() int {
	tc.t.Log("Waiting for leader election...")
	leader, err := CheckLeader(tc.ctx, tc.Nodes)
	require.NoError(tc.t, err, "failed to elect leader")
	tc.t.Logf("Leader elected: node %d", leader)
	return leader
}

func (tc *TestCluster) WaitForReplication(leader int) {
	tc.t.Logf("Waiting for replication from leader (node %d)...", leader)
	require.Eventually(tc.t, func() bool {
		return CheckReplication(tc.t, tc.Nodes, leader) == nil
	}, ReplicationWait, PollInterval, "replication failed")
	tc.t.Log("Replication completed successfully")
}

func CheckReplication(t *testing.T, nodes []*raft.Raft, leaderId int) error {
	leaderEntries := nodes[leaderId].GetLogEntries()
	t.Logf("Checking replication: leader has %d entries in term %d", len(leaderEntries), nodes[leaderId].CurrentTerm)

	for i, node := range nodes {
		if node.GetState() == raft.Leader || node.TestIsPartitioned {
			continue
		}

		entries := node.GetLogEntries()
		if node.CurrentTerm != nodes[leaderId].CurrentTerm {
			t.Logf("Checking replication: term mismatch")
		}

		if len(entries) != len(leaderEntries) {
			t.Logf("Node %d: mismatched entry count (got %d, want %d)",
				i, len(entries), len(leaderEntries))
			return fmt.Errorf("node %d has %d entries, leader has %d",
				i, len(entries), len(leaderEntries))
		}
	}
	return nil
}

func StartCluster(ctx context.Context, t *testing.T, count int, offset int) []*raft.Raft {
	var nodes []*raft.Raft
	var peers []string

	// Generate peer addresses
	for i := 0; i < count; i++ {
		peers = append(peers, fmt.Sprintf("127.0.0.1:%d", basePort+offset+i))
	}

	// Create nodes
	for i := 0; i < count; i++ {
		nodes = append(nodes, raft.NewNode(int32(i), peers))
	}

	// Start nodes
	for i, node := range nodes {
		port := basePort + offset + i
		go func(n *raft.Raft, p int) {
			n.Start(ctx, "127.0.0.1", p)
		}(node, port)
	}

	// Allow cluster to initialize
	time.Sleep(PollInterval)
	return nodes
}

func CheckLeader(ctx context.Context, nodes []*raft.Raft) (int, error) {
	timeout := time.After(ReplicationWait)
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-timeout:
			return -1, fmt.Errorf("timeout: no leader elected")
		case <-ticker.C:
			leaderCount := 0
			var leaders []int

			for _, node := range nodes {
				if node.GetState() == raft.Leader && !node.TestIsPartitioned {
					leaderCount++
					leaders = append(leaders, int(node.ID))
				}
			}

			if leaderCount > 1 {
				return -1, fmt.Errorf("multiple leaders elected: %v", leaders)
			}
			if leaderCount == 1 {
				return leaders[0], nil
			}
		}
	}
}
