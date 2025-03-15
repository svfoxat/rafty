package raft_test

import (
	"context"
	"fmt"
	"github.com/svfoxat/rafty/internal/raft"
	"testing"
	"time"
)

func TestSubmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := StartCluster(ctx, t, 3, 1000)
	leader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("leader is %+v", leader)
	t.Logf("submitting command to leader %+v", leader)
	nodes[leader].Submit("hi")

	// Wait for the command to be replicated
	time.Sleep(1000 * time.Millisecond)
	if err := CheckReplication(t, nodes, leader); err != nil {
		t.Fatal(err)
	}

}

func TestSubmitMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := StartCluster(ctx, t, 3, 1100)
	leader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("leader is %+v", leader)
	t.Logf("submitting commands to leader %+v", leader)

	for i := 0; i < 100; i++ {
		nodes[leader].Submit(fmt.Sprintf("hi %d", i))
	}

	// Wait for the command to be replicated
	time.Sleep(1000 * time.Millisecond)
	if err := CheckReplication(t, nodes, leader); err != nil {
		t.Fatal(err)
	}

	t.Logf("submitting commands to leader %+v", leader)

	for i := 0; i < 100; i++ {
		nodes[leader].Submit(fmt.Sprintf("hi %d", i))
	}
	// Wait for the command to be replicated
	time.Sleep(1000 * time.Millisecond)
	if err := CheckReplication(t, nodes, leader); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitMultiWithDead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := StartCluster(ctx, t, 3, 1200)
	leader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("leader is %+v", leader)
	t.Logf("submitting commands to leader %+v", leader)

	for i := 0; i < 25; i++ {
		nodes[leader].Submit(fmt.Sprintf("hi %d", i))
	}
	// wait for the commands to be replicated
	time.Sleep(1000 * time.Millisecond)
	if err := CheckReplication(t, nodes, leader); err != nil {
		t.Fatal(err)
	}

	// kill the leader
	t.Logf("stopping leader %+v", leader)
	nodes[leader].Stop()

	newLeader, err := CheckLeader(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("newLeader is %+v", newLeader)

	// submit more commands
	for i := 0; i < 25; i++ {
		nodes[newLeader].Submit(fmt.Sprintf("ho %d", i))
	}

	time.Sleep(5000 * time.Millisecond)
	if err := CheckReplication(t, nodes, newLeader); err != nil {
		t.Fatal(err)
	}

	// restart the old leader
	t.Logf("restarting leader %+v", leader)
	nodes[leader].Restart(0)
	time.Sleep(100 * time.Millisecond)

	// submit more commands
	for i := 0; i < 25; i++ {
		nodes[newLeader].Submit(fmt.Sprintf("he %d", i))
	}

	// Wait for the command to be replicated
	time.Sleep(1000 * time.Millisecond)
	if err := CheckReplication(t, nodes, newLeader); err != nil {
		t.Fatal(err)
	}
}

func CheckReplication(t *testing.T, nodes []*raft.Raft, leaderId int) error {
	t.Logf("checking replication for leader %+v", leaderId)
	for _, node := range nodes {
		if node.GetState() == raft.Leader || node.GetState() == raft.Dead {
			continue
		}
		entries := node.GetLogEntries()
		leaderEntries := nodes[leaderId].GetLogEntries()

		if len(entries) != len(leaderEntries) {
			return fmt.Errorf("expected %d entries, got %d", len(leaderEntries), len(entries))
		}

		for i, entry := range leaderEntries {
			if entries[i].Command != entry.Command {
				return fmt.Errorf("expected %+v, got %+v", entry.Command, entries[i].Command)
			}
		}
	}

	return nil
}
