package raft

import (
	"sync"
	"testing"
	"time"

	pb "github.com/as567-code/raft-kv/proto/raftpb"
	"go.uber.org/zap"
)

// mockTransport is a test transport that connects nodes in-memory.
type mockTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
	// partitioned tracks which nodes can't reach each other
	partitioned map[string]map[string]bool
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		nodes:       make(map[string]*Node),
		partitioned: make(map[string]map[string]bool),
	}
}

func (t *mockTransport) addNode(id string, n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = n
}

func (t *mockTransport) partition(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.partitioned[from] == nil {
		t.partitioned[from] = make(map[string]bool)
	}
	t.partitioned[from][to] = true
}

func (t *mockTransport) heal(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.partitioned[from] != nil {
		delete(t.partitioned[from], to)
	}
}

func (t *mockTransport) isPartitioned(from, to string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.partitioned[from] != nil && t.partitioned[from][to] {
		return true
	}
	return false
}

func (t *mockTransport) getNode(id string) *Node {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nodes[id]
}

// nodeTransport wraps mockTransport for a specific node
type nodeTransport struct {
	nodeID string
	mock   *mockTransport
}

func (nt *nodeTransport) SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if nt.mock.isPartitioned(nt.nodeID, peerID) {
		return nil, errPartitioned
	}
	node := nt.mock.getNode(peerID)
	if node == nil {
		return nil, errNodeNotFound
	}
	return node.HandleRequestVote(req), nil
}

func (nt *nodeTransport) SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if nt.mock.isPartitioned(nt.nodeID, peerID) {
		return nil, errPartitioned
	}
	node := nt.mock.getNode(peerID)
	if node == nil {
		return nil, errNodeNotFound
	}
	return node.HandleAppendEntries(req), nil
}

func (nt *nodeTransport) SendInstallSnapshot(peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if nt.mock.isPartitioned(nt.nodeID, peerID) {
		return nil, errPartitioned
	}
	node := nt.mock.getNode(peerID)
	if node == nil {
		return nil, errNodeNotFound
	}
	return node.HandleInstallSnapshot(req), nil
}

var (
	errPartitioned  = &partitionError{}
	errNodeNotFound = &nodeNotFoundError{}
)

type partitionError struct{}

func (e *partitionError) Error() string { return "network partitioned" }

type nodeNotFoundError struct{}

func (e *nodeNotFoundError) Error() string { return "node not found" }

// mockFSM is a test FSM
type mockFSM struct {
	mu       sync.Mutex
	applied  [][]byte
	snapshot []byte
}

func (f *mockFSM) Apply(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, data)
	return nil
}

func (f *mockFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot, nil
}

func (f *mockFSM) Restore(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshot = data
	return nil
}

func newTestLogger() *zap.SugaredLogger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	l, _ := cfg.Build()
	return l.Sugar()
}

// createCluster creates a 3-node test cluster with in-memory transport.
func createCluster(t *testing.T) ([]*Node, *mockTransport) {
	t.Helper()

	mt := newMockTransport()
	ids := []string{"node1", "node2", "node3"}
	var nodes []*Node

	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		fsm := &mockFSM{}
		nt := &nodeTransport{nodeID: id, mock: mt}

		node := NewNode(NodeConfig{
			ID:                id,
			Peers:             peers,
			Transport:         nt,
			FSM:               fsm,
			ElectionMinMs:     50,
			ElectionMaxMs:     100,
			HeartbeatInterval: 20 * time.Millisecond,
			SnapshotThreshold: 100,
			Logger:            newTestLogger(),
		})

		mt.addNode(id, node)
		nodes = append(nodes, node)
	}

	return nodes, mt
}

func startCluster(t *testing.T, nodes []*Node) {
	t.Helper()
	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatalf("failed to start node %s: %v", n.id, err)
		}
	}
}

func stopCluster(nodes []*Node) {
	for _, n := range nodes {
		n.Stop()
	}
}

func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for leader election")
			return nil
		default:
			for _, n := range nodes {
				if n.State() == Leader {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestElection_SingleLeaderElected(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Verify exactly one leader
	leaderCount := 0
	for _, n := range nodes {
		if n.State() == Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}

func TestElection_AllNodesAgreeOnLeader(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Wait for heartbeats to propagate leader info
	time.Sleep(200 * time.Millisecond)

	for _, n := range nodes {
		if n.LeaderID() != leader.ID() {
			t.Errorf("node %s thinks leader is %s, but actual leader is %s",
				n.ID(), n.LeaderID(), leader.ID())
		}
	}
}

func TestElection_AllNodesHaveSameTerm(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	waitForLeader(t, nodes, 3*time.Second)
	time.Sleep(200 * time.Millisecond)

	terms := make(map[uint64]int)
	for _, n := range nodes {
		terms[n.Term()]++
	}
	if len(terms) != 1 {
		t.Errorf("nodes have different terms: %v", terms)
	}
}

func TestElection_ReElectionOnLeaderFailure(t *testing.T) {
	nodes, mt := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)
	oldLeaderID := leader.ID()
	oldTerm := leader.Term()

	// Partition the leader from everyone
	for _, n := range nodes {
		if n.ID() != oldLeaderID {
			mt.partition(oldLeaderID, n.ID())
			mt.partition(n.ID(), oldLeaderID)
		}
	}

	// Wait for re-election among remaining nodes
	time.Sleep(500 * time.Millisecond)

	newLeaderCount := 0
	var newLeader *Node
	for _, n := range nodes {
		if n.ID() != oldLeaderID && n.State() == Leader {
			newLeaderCount++
			newLeader = n
		}
	}

	if newLeaderCount != 1 {
		t.Fatalf("expected 1 new leader among non-partitioned nodes, got %d", newLeaderCount)
	}
	if newLeader.Term() <= oldTerm {
		t.Errorf("new leader term %d should be > old term %d", newLeader.Term(), oldTerm)
	}
}

func TestElection_SplitVoteResolution(t *testing.T) {
	// Run multiple times to test probabilistic split vote resolution
	for i := 0; i < 5; i++ {
		nodes, _ := createCluster(t)
		startCluster(t, nodes)

		leader := waitForLeader(t, nodes, 5*time.Second)
		if leader == nil {
			t.Fatal("no leader elected after potential split vote")
		}

		stopCluster(nodes)
	}
}

func TestElection_TermIncrementsOnElection(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	leader := waitForLeader(t, nodes, 3*time.Second)

	if leader.Term() < 1 {
		t.Errorf("leader term should be >= 1, got %d", leader.Term())
	}
}

func TestRequestVote_RejectStaleTerm(t *testing.T) {
	nodes, _ := createCluster(t)
	startCluster(t, nodes)
	defer stopCluster(nodes)

	waitForLeader(t, nodes, 3*time.Second)

	// Send a RequestVote with a stale term
	resp := nodes[0].HandleRequestVote(&pb.RequestVoteRequest{
		Term:         0,
		CandidateId:  "stale-node",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Error("should not grant vote for stale term")
	}
}

func TestRequestVote_GrantForHigherTerm(t *testing.T) {
	nodes, _ := createCluster(t)
	// Don't start — manually test the handler
	nodes[0].currentTerm = 1

	resp := nodes[0].HandleRequestVote(&pb.RequestVoteRequest{
		Term:         5,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Error("should grant vote for higher term with up-to-date log")
	}
	if nodes[0].Term() != 5 {
		t.Errorf("node should update to term 5, got %d", nodes[0].Term())
	}
}
