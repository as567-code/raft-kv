package raft

import (
	"encoding/json"
	"testing"
	"time"

	pb "github.com/as567-code/raft-kv/proto/raftpb"
)

func TestReplication_BasicLogReplication(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Propose an entry
	cmd := map[string]string{"type": "put", "key": "foo", "value": "bar"}
	data, _ := json.Marshal(cmd)
	if err := leader.Propose(data); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Wait for replication
	time.Sleep(200 * time.Millisecond)

	// All nodes should have the entry committed
	for _, n := range nodes {
		if n.CommitIndex() < 2 { // index 1 is noop, index 2 is our entry
			t.Errorf("node %s commitIndex=%d, expected >= 2", n.ID(), n.CommitIndex())
		}
	}
}

func TestReplication_MultipleEntries(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Propose multiple entries
	for i := 0; i < 10; i++ {
		cmd := map[string]string{"type": "put", "key": "k", "value": "v"}
		data, _ := json.Marshal(cmd)
		if err := leader.Propose(data); err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	for _, n := range nodes {
		// noop + 10 entries = commit index should be at least 11
		if n.CommitIndex() < 11 {
			t.Errorf("node %s commitIndex=%d, expected >= 11", n.ID(), n.CommitIndex())
		}
	}
}

func TestReplication_ProposeOnFollowerFails(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)

	var follower *Node
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			follower = n
			break
		}
	}

	cmd := map[string]string{"type": "put", "key": "foo", "value": "bar"}
	data, _ := json.Marshal(cmd)
	err := follower.Propose(data)
	if err == nil {
		t.Fatal("propose on follower should fail")
	}
}

func TestReplication_ConsistencyAfterPartitionHeal(t *testing.T) {
	nodes, mt := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 5*time.Second)
	leaderID := leader.ID()

	// Fully isolate one follower from all other nodes
	var partitionedNode *Node
	for _, n := range nodes {
		if n.ID() != leaderID {
			partitionedNode = n
			break
		}
	}
	for _, n := range nodes {
		if n.ID() != partitionedNode.ID() {
			mt.partition(partitionedNode.ID(), n.ID())
			mt.partition(n.ID(), partitionedNode.ID())
		}
	}

	// Write some entries (should still succeed with 2-node majority)
	for i := 0; i < 5; i++ {
		cmd := map[string]string{"type": "put", "key": "k", "value": "v"}
		data, _ := json.Marshal(cmd)
		leader.Propose(data)
	}

	time.Sleep(200 * time.Millisecond)

	// Partitioned node should be behind
	if partitionedNode.CommitIndex() >= leader.CommitIndex() {
		t.Error("partitioned node should be behind")
	}

	// Heal partition — restore all connections
	for _, n := range nodes {
		if n.ID() != partitionedNode.ID() {
			mt.heal(partitionedNode.ID(), n.ID())
			mt.heal(n.ID(), partitionedNode.ID())
		}
	}

	// Wait for catch-up replication
	time.Sleep(1 * time.Second)

	// Now all nodes should be caught up
	if partitionedNode.CommitIndex() < leader.CommitIndex() {
		t.Errorf("after heal, partitioned node commitIndex=%d, leader=%d",
			partitionedNode.CommitIndex(), leader.CommitIndex())
	}
}

func TestAppendEntries_RejectStaleTerm(t *testing.T) {
	nodes, _ := createCluster(t)
	nodes[0].currentTerm = 5

	resp := nodes[0].HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:     3,
		LeaderId: "node2",
	})

	if resp.Success {
		t.Error("should reject AppendEntries with stale term")
	}
	if resp.Term != 5 {
		t.Errorf("response term should be 5, got %d", resp.Term)
	}
}

func TestAppendEntries_ResetsElectionTimer(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	waitForLeader(t, nodes, 3*time.Second)

	// Find a follower
	var follower *Node
	for _, n := range nodes {
		if n.State() == Follower {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	// Sending valid AppendEntries should reset the election timer
	// (follower should stay a follower)
	for i := 0; i < 5; i++ {
		follower.HandleAppendEntries(&pb.AppendEntriesRequest{
			Term:     follower.Term(),
			LeaderId: "test-leader",
		})
		time.Sleep(30 * time.Millisecond)
	}

	if follower.State() != Follower {
		t.Errorf("node should remain follower, got %s", follower.State())
	}
}

func TestAppendEntries_LogConsistencyCheck(t *testing.T) {
	nodes, _ := createCluster(t)
	node := nodes[0]
	node.currentTerm = 2

	// Add some entries to node's log
	node.log = append(node.log,
		&pb.LogEntry{Term: 1, Index: 1},
		&pb.LogEntry{Term: 1, Index: 2},
		&pb.LogEntry{Term: 2, Index: 3},
	)

	// AppendEntries with matching prevLogIndex/prevLogTerm should succeed
	resp := node.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader",
		PrevLogIndex: 3,
		PrevLogTerm:  2,
		Entries:      []*pb.LogEntry{{Term: 2, Index: 4, Data: []byte("test")}},
		LeaderCommit: 4,
	})

	if !resp.Success {
		t.Error("should succeed with matching prev entry")
	}

	// AppendEntries with non-matching prevLogTerm should fail
	resp = node.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader",
		PrevLogIndex: 2,
		PrevLogTerm:  99, // wrong term
	})

	if resp.Success {
		t.Error("should fail with non-matching prevLogTerm")
	}
}
