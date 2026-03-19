package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	pb "github.com/as567-code/raft-kv/proto/raftpb"
	"go.uber.org/zap"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Transport abstracts the network layer for Raft RPCs.
type Transport interface {
	SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	SendInstallSnapshot(peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}

// FSM is the finite state machine that applies committed entries.
type FSM interface {
	Apply(data []byte) error
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// ApplyResult carries the result of applying a committed log entry back to the client.
type ApplyResult struct {
	Index uint64
	Err   error
}

// Node is a Raft consensus node.
type Node struct {
	mu sync.RWMutex

	// Persistent state
	id          string
	currentTerm uint64
	votedFor    string
	log         []*pb.LogEntry // 1-indexed logically; log[0] is a sentinel

	// Volatile state
	state       NodeState
	commitIndex uint64
	lastApplied uint64

	// Leader-only volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Cluster
	peers     []string
	transport Transport
	fsm       FSM
	wal       *WAL

	// Timing
	electionMinMs     int
	electionMaxMs     int
	heartbeatInterval time.Duration
	electionTimer     *time.Timer
	heartbeatTimer    *time.Timer

	// Snapshot
	snapshotThreshold uint64
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64

	// Channels
	applyCh       chan ApplyResult
	stopCh        chan struct{}
	applyNotifyCh chan struct{} // signals new commits to apply

	// Pending client proposals awaiting commit
	pendingMu      sync.Mutex
	pendingPropose map[uint64]chan error // index -> result channel

	// Leader tracking
	leaderID string

	logger *zap.SugaredLogger
}

type NodeConfig struct {
	ID                string
	Peers             []string
	Transport         Transport
	FSM               FSM
	WAL               *WAL
	ElectionMinMs     int
	ElectionMaxMs     int
	HeartbeatInterval time.Duration
	SnapshotThreshold uint64
	Logger            *zap.SugaredLogger
}

func NewNode(cfg NodeConfig) *Node {
	if cfg.ElectionMinMs == 0 {
		cfg.ElectionMinMs = 500
	}
	if cfg.ElectionMaxMs == 0 {
		cfg.ElectionMaxMs = 1000
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 1000
	}
	if cfg.Logger == nil {
		l, _ := zap.NewProduction()
		cfg.Logger = l.Sugar()
	}

	n := &Node{
		id:                cfg.ID,
		currentTerm:       0,
		votedFor:          "",
		log:               []*pb.LogEntry{{Term: 0, Index: 0}}, // sentinel
		state:             Follower,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		peers:             cfg.Peers,
		transport:         cfg.Transport,
		fsm:               cfg.FSM,
		wal:               cfg.WAL,
		electionMinMs:     cfg.ElectionMinMs,
		electionMaxMs:     cfg.ElectionMaxMs,
		heartbeatInterval: cfg.HeartbeatInterval,
		snapshotThreshold: cfg.SnapshotThreshold,
		applyCh:           make(chan ApplyResult, 256),
		stopCh:            make(chan struct{}),
		applyNotifyCh:     make(chan struct{}, 1),
		pendingPropose:    make(map[uint64]chan error),
		logger:            cfg.Logger,
	}

	return n
}

// Start begins the Raft node's operation.
func (n *Node) Start() error {
	// Restore persistent state from WAL if available
	if n.wal != nil {
		if err := n.restoreFromWAL(); err != nil {
			return fmt.Errorf("failed to restore from WAL: %w", err)
		}
	}

	n.resetElectionTimer()
	go n.applyLoop()
	return nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() {
	close(n.stopCh)
	n.mu.Lock()
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.mu.Unlock()
}

// Propose submits a new entry to the Raft log. Returns only after the entry is committed.
func (n *Node) Propose(data []byte) error {
	n.mu.Lock()
	if n.state != Leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		return fmt.Errorf("not leader; leader is %s", leaderID)
	}

	entry := &pb.LogEntry{
		Term:  n.currentTerm,
		Index: n.lastLogIndex() + 1,
		Data:  data,
		Type:  pb.EntryType_ENTRY_NORMAL,
	}
	n.log = append(n.log, entry)

	if n.wal != nil {
		if err := n.wal.Append(entry); err != nil {
			n.mu.Unlock()
			return fmt.Errorf("WAL append failed: %w", err)
		}
	}

	resultCh := make(chan error, 1)
	n.pendingMu.Lock()
	n.pendingPropose[entry.Index] = resultCh
	n.pendingMu.Unlock()

	// For single-node cluster, commit immediately
	if len(n.peers) == 0 {
		n.commitIndex = entry.Index
		n.notifyApply()
		n.mu.Unlock()
	} else {
		n.mu.Unlock()
		// Trigger immediate replication
		n.sendHeartbeats()
	}

	return <-resultCh
}

// State returns the current node state.
func (n *Node) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// LeaderID returns the current known leader ID.
func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

// ID returns this node's ID.
func (n *Node) ID() string {
	return n.id
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// CommitIndex returns the current commit index.
func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

// IsLeader returns whether this node is the leader.
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == Leader
}

// --- Internal helpers ---

func (n *Node) lastLogIndex() uint64 {
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	return n.log[len(n.log)-1].Term
}

func (n *Node) logEntryAt(index uint64) *pb.LogEntry {
	// Account for snapshot compaction offset
	offset := index - n.lastSnapshotIndex
	if int(offset) < 0 || int(offset) >= len(n.log) {
		return nil
	}
	return n.log[offset]
}

func (n *Node) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.persistState()
	n.resetElectionTimer()
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
		n.heartbeatTimer = nil
	}
	n.logger.Infow("became follower", "term", term, "node", n.id)
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.persistState()
	n.logger.Infow("became candidate", "term", n.currentTerm, "node", n.id)
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}

	// Initialize leader state
	lastIdx := n.lastLogIndex()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}

	// Append a no-op entry to commit entries from previous terms
	noop := &pb.LogEntry{
		Term:  n.currentTerm,
		Index: lastIdx + 1,
		Type:  pb.EntryType_ENTRY_NOOP,
	}
	n.log = append(n.log, noop)
	if n.wal != nil {
		n.wal.Append(noop)
	}

	n.logger.Infow("became leader", "term", n.currentTerm, "node", n.id)

	// Stop election timer, start heartbeats
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	go n.heartbeatLoop()
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := time.Duration(n.electionMinMs+rand.Intn(n.electionMaxMs-n.electionMinMs)) * time.Millisecond
	n.electionTimer = time.AfterFunc(timeout, func() {
		n.startElection()
	})
}

func (n *Node) persistState() {
	if n.wal != nil {
		n.wal.SaveState(n.currentTerm, n.votedFor)
	}
}

func (n *Node) notifyApply() {
	select {
	case n.applyNotifyCh <- struct{}{}:
	default:
	}
}

// applyLoop continuously applies committed entries to the FSM.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyNotifyCh:
			n.applyCommitted()
		}
	}
}

type applyEntry struct {
	index     uint64
	term      uint64
	entryType pb.EntryType
	data      []byte
}

func (n *Node) applyCommitted() {
	n.mu.Lock()
	commitIndex := n.commitIndex
	lastApplied := n.lastApplied
	var toApply []applyEntry

	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry := n.logEntryAt(i)
		if entry != nil {
			toApply = append(toApply, applyEntry{
				index:     entry.Index,
				term:      entry.Term,
				entryType: entry.Type,
				data:      entry.Data,
			})
		}
	}
	n.mu.Unlock()

	for _, entry := range toApply {
		var err error
		if entry.entryType == pb.EntryType_ENTRY_NORMAL && len(entry.data) > 0 {
			err = n.fsm.Apply(entry.data)
		}

		n.mu.Lock()
		n.lastApplied = entry.index
		n.mu.Unlock()

		// Notify pending proposals
		n.pendingMu.Lock()
		if ch, ok := n.pendingPropose[entry.index]; ok {
			ch <- err
			delete(n.pendingPropose, entry.index)
		}
		n.pendingMu.Unlock()

		// Send to applyCh for external observers
		select {
		case n.applyCh <- ApplyResult{Index: entry.index, Err: err}:
		default:
		}
	}

	// Check if we should snapshot
	n.mu.RLock()
	shouldSnapshot := n.lastApplied-n.lastSnapshotIndex >= n.snapshotThreshold
	n.mu.RUnlock()

	if shouldSnapshot {
		go n.takeSnapshot()
	}
}

func (n *Node) takeSnapshot() {
	n.mu.RLock()
	if n.fsm == nil {
		n.mu.RUnlock()
		return
	}
	lastApplied := n.lastApplied
	entry := n.logEntryAt(lastApplied)
	if entry == nil {
		n.mu.RUnlock()
		return
	}
	term := entry.Term
	n.mu.RUnlock()

	data, err := n.fsm.Snapshot()
	if err != nil {
		n.logger.Errorw("snapshot failed", "error", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Compact log: remove entries up to lastApplied
	offset := int(lastApplied - n.lastSnapshotIndex)
	if offset > 0 && offset < len(n.log) {
		newLog := make([]*pb.LogEntry, len(n.log)-offset)
		copy(newLog, n.log[offset:])
		n.log = newLog
	}

	n.lastSnapshotIndex = lastApplied
	n.lastSnapshotTerm = term

	if n.wal != nil {
		n.wal.SaveSnapshot(lastApplied, term, data)
	}

	n.logger.Infow("snapshot taken", "index", lastApplied, "term", term)
}

func (n *Node) restoreFromWAL() error {
	state, err := n.wal.LoadState()
	if err != nil {
		return err
	}
	if state != nil {
		n.currentTerm = state.Term
		n.votedFor = state.VotedFor
	}

	snapIndex, snapTerm, snapData, err := n.wal.LoadSnapshot()
	if err != nil {
		return err
	}
	if snapData != nil {
		n.lastSnapshotIndex = snapIndex
		n.lastSnapshotTerm = snapTerm
		n.log = []*pb.LogEntry{{Term: snapTerm, Index: snapIndex}}
		if n.fsm != nil {
			if err := n.fsm.Restore(snapData); err != nil {
				return fmt.Errorf("FSM restore failed: %w", err)
			}
		}
		n.lastApplied = snapIndex
		n.commitIndex = snapIndex
	}

	entries, err := n.wal.LoadEntries(n.lastSnapshotIndex)
	if err != nil {
		return err
	}
	n.log = append(n.log, entries...)

	return nil
}
