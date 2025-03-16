package rafttest_test

import (
	"github.com/stretchr/testify/require"
	"github.com/svfoxat/rafty/test/rafttest"
	"testing"
	"time"
)

func TestSubmit(t *testing.T) {
	t.Log("Starting single command submission test")
	tc := rafttest.SetupCluster(t, 3)
	defer func() {
		tc.Cleanup()
		time.Sleep(rafttest.PollInterval) // Give goroutines time to clean up
	}()

	start := time.Now()
	leader := tc.WaitForLeader()
	t.Logf("[%.3fs] Leader elected: node %d", time.Since(start).Seconds(), leader)

	tc.SubmitCommands(leader, "cmd", 1)
	t.Log("Command submitted to leader")

	tc.WaitForReplication(leader)
	t.Logf("[%.3fs] Command successfully replicated", time.Since(start).Seconds())

	// Final replication check
	err := rafttest.CheckReplication(t, tc.Nodes, leader)
	require.NoError(t, err, "final replication check failed")
}

func TestSubmitMulti(t *testing.T) {
	t.Log("Starting multiple commands submission test")
	tc := rafttest.SetupCluster(t, 3)
	defer tc.Cleanup()

	start := time.Now()
	leader := tc.WaitForLeader()
	t.Logf("[%.3fs] Leader elected: node %d", time.Since(start).Seconds(), leader)

	// Just two batches of 25 commands each
	t.Log("Submitting first batch of commands")
	tc.SubmitCommands(leader, "batch1", 1000)
	tc.WaitForReplication(leader)
	t.Logf("[%.3fs] First batch replicated", time.Since(start).Seconds())

	time.Sleep(rafttest.ReplicationWait)

	t.Log("Submitting second batch of commands")
	tc.SubmitCommands(leader, "batch2", 256)
	tc.WaitForReplication(leader)
	t.Logf("[%.3fs] Second batch replicated", time.Since(start).Seconds())

	// Final replication check
	err := rafttest.CheckReplication(t, tc.Nodes, leader)
	require.NoError(t, err, "final replication check failed")
}

func TestSubmitMultiWithDead(t *testing.T) {
	t.Log("Starting leader failure and recovery test")
	tc := rafttest.SetupCluster(t, 3)
	defer tc.Cleanup()

	start := time.Now()
	leader := tc.WaitForLeader()
	t.Logf("[%.3fs] Initial leader elected: node %d", time.Since(start).Seconds(), leader)

	t.Log("Submitting initial commands")
	tc.SubmitCommands(leader, "initial", 25)
	tc.WaitForReplication(leader)
	t.Logf("[%.3fs] Initial commands replicated", time.Since(start).Seconds())

	t.Logf("Partitioning leader (node %d)", leader)
	tc.Nodes[leader].TestPartition()

	newLeader := tc.WaitForLeader()
	t.Logf("[%.3fs] New leader elected: node %d", time.Since(start).Seconds(), newLeader)
	require.NotEqual(t, leader, newLeader, "new leader same as old leader")

	t.Log("Submitting commands to new leader")
	tc.SubmitCommands(newLeader, "after-partition", 25)
	tc.WaitForReplication(newLeader)
	t.Logf("[%.3fs] Post-partition commands replicated", time.Since(start).Seconds())

	t.Logf("Rejoining old leader (node %d)", leader)
	tc.Nodes[leader].TestUnpartition(0)
	time.Sleep(rafttest.PollInterval) // Brief pause between chunks

	t.Log("Submitting final commands")
	tc.SubmitCommands(newLeader, "after-rejoin", 25)
	tc.WaitForReplication(newLeader)
	t.Logf("[%.3fs] Final commands replicated", time.Since(start).Seconds())
}

func TestRunawayNode(t *testing.T) {
	t.Skip()

	tc := rafttest.SetupCluster(t, 3)
	defer tc.Cleanup()

	// Initial leader commands
	leader := tc.WaitForLeader()
	tc.SubmitCommands(leader, "initial", 25)
	tc.WaitForReplication(leader)

	// Partition leader
	t.Logf("partition leader %d", leader)
	tc.Nodes[leader].TestPartition()

	// New leader commands
	newLeader := tc.WaitForLeader()
	require.NotEqual(t, leader, newLeader, "new leader same as old leader")
	tc.SubmitCommands(newLeader, "after-partition", 25)
	tc.WaitForReplication(newLeader)

	// Rejoin old leader with high term
	t.Logf("rejoin node %d", leader)
	tc.Nodes[leader].TestUnpartition(500)
	time.Sleep(rafttest.PollInterval) // Brief pause between chunks

	// Verify cluster still works
	finalLeader := tc.WaitForLeader()
	tc.SubmitCommands(finalLeader, "final", 25)
	tc.WaitForReplication(finalLeader)
}

func TestLeaderElection(t *testing.T) {
	tc := rafttest.SetupCluster(t, 3)
	defer tc.Cleanup()

	start := time.Now()
	leader := tc.WaitForLeader()
	t.Logf("leader is %d after %dms", leader, time.Since(start).Milliseconds())
}

func TestDyingLeader(t *testing.T) {
	tc := rafttest.SetupCluster(t, 3)
	defer tc.Cleanup()

	// Initial leader election
	start := time.Now()
	leader := tc.WaitForLeader()
	t.Logf("leader is %d after %dms", leader, time.Since(start).Milliseconds())

	// Kill leader
	start = time.Now()
	tc.Nodes[leader].TestPartition()

	// Verify new leader election
	newLeader := tc.WaitForLeader()
	t.Logf("new leader is %d after %dms", newLeader, time.Since(start).Milliseconds())
	require.NotEqual(t, leader, newLeader, "new leader is same as old leader")
}

func TestDyingLeaderWithRejoin(t *testing.T) {
	tc := rafttest.SetupCluster(t, 3)
	defer tc.Cleanup()

	// Initial leader election
	start := time.Now()
	leader := tc.WaitForLeader()
	t.Logf("leader is %d after %dms", leader, time.Since(start).Milliseconds())

	// Kill leader
	start = time.Now()
	t.Log("stopping leader")
	tc.Nodes[leader].TestPartition()

	// Wait for new leader
	newLeader := tc.WaitForLeader()
	t.Logf("new leader is %d after %dms", newLeader, time.Since(start).Milliseconds())
	require.NotEqual(t, leader, newLeader, "new leader is same as old leader")

	time.Sleep(2 * rafttest.ReplicationWait)

	// Rejoin old leader
	t.Log("restarting old leader")
	tc.Nodes[leader].TestUnpartition(0)
	time.Sleep(rafttest.PollInterval)

	// Verify term synchronization
	require.Eventually(t, func() bool {
		return tc.Nodes[leader].CurrentTerm == tc.Nodes[newLeader].CurrentTerm
	}, rafttest.ReplicationWait, rafttest.PollInterval, "old leader failed to rejoin cluster")
}

func TestLeaderFailureRecovery(t *testing.T) {
	tc := rafttest.SetupCluster(t, 3)
	defer tc.Cleanup()

	// Initial leader election and commands
	leader := tc.WaitForLeader()
	tc.SubmitCommands(leader, "before-failure", 10)
	tc.WaitForReplication(leader)

	// Kill leader and verify new leader
	tc.Nodes[leader].TestPartition()
	newLeader := tc.WaitForLeader()
	require.NotEqual(t, leader, newLeader, "new leader same as old leader")

	// Submit commands to new leader
	tc.SubmitCommands(newLeader, "during-failure", 10)
	tc.WaitForReplication(newLeader)

	// Rejoin old leader
	tc.Nodes[leader].TestUnpartition(0)
	time.Sleep(rafttest.PollInterval)

	// Verify log consistency
	require.Eventually(t, func() bool {
		return rafttest.CheckReplication(t, tc.Nodes, newLeader) == nil
	}, rafttest.ReplicationWait, rafttest.PollInterval, "logs not consistent after leader rejoin")
}
