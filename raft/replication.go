package raft

import (
	"sort"
	"time"

	"github.com/as567-code/raft-kv/metrics"
	pb "github.com/as567-code/raft-kv/proto/raftpb"
)

// heartbeatLoop sends periodic heartbeats while this node is leader.
func (n *Node) heartbeatLoop() {
	// Send first heartbeat immediately to establish authority
	n.sendHeartbeats()

	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.RLock()
			isLeader := n.state == Leader
			n.mu.RUnlock()
			if !isLeader {
				return
			}
			n.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends AppendEntries RPCs to all peers.
func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	term := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.RUnlock()

	for _, peer := range n.peers {
		go n.replicateTo(peer, term, commitIndex)
	}
}

// replicateTo sends an AppendEntries RPC to a specific peer.
func (n *Node) replicateTo(peerID string, term, leaderCommit uint64) {
	n.mu.RLock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.RUnlock()
		return
	}

	nextIdx := n.nextIndex[peerID]

	// If the peer is too far behind and we have a snapshot, send that instead
	if nextIdx <= n.lastSnapshotIndex && n.lastSnapshotIndex > 0 {
		n.mu.RUnlock()
		n.sendSnapshot(peerID, term)
		return
	}

	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevEntry := n.logEntryAt(prevLogIndex); prevEntry != nil {
		prevLogTerm = prevEntry.Term
	}

	// Collect entries to send
	var entries []*pb.LogEntry
	offset := int(nextIdx - n.lastSnapshotIndex)
	if offset > 0 && offset < len(n.log) {
		for i := offset; i < len(n.log); i++ {
			entries = append(entries, n.log[i])
		}
	}

	n.mu.RUnlock()

	resp, err := n.transport.SendAppendEntries(peerID, &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		n.logger.Debugw("AppendEntries RPC failed", "peer", peerID, "error", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex for the follower
		if len(entries) > 0 {
			newMatchIndex := entries[len(entries)-1].Index
			if newMatchIndex > n.matchIndex[peerID] {
				n.matchIndex[peerID] = newMatchIndex
				n.nextIndex[peerID] = newMatchIndex + 1
			}
		}
		n.advanceCommitIndex()
	} else {
		// Decrement nextIndex and retry. Use the conflict optimization.
		if resp.ConflictTerm > 0 {
			// Search our log for conflictTerm
			found := false
			for i := len(n.log) - 1; i >= 1; i-- {
				if n.log[i].Term == resp.ConflictTerm {
					n.nextIndex[peerID] = n.log[i].Index + 1
					found = true
					break
				}
			}
			if !found {
				n.nextIndex[peerID] = resp.ConflictIndex
			}
		} else if resp.ConflictIndex > 0 {
			n.nextIndex[peerID] = resp.ConflictIndex
		} else {
			if n.nextIndex[peerID] > 1 {
				n.nextIndex[peerID]--
			}
		}
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// Reply false if term < currentTerm (§5.1)
	if req.Term < n.currentTerm {
		return resp
	}

	// If RPC request contains term >= currentTerm, recognize leader
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	} else if n.state == Candidate {
		// Same term but we're a candidate — step down
		n.becomeFollower(req.Term)
	}

	n.leaderID = req.LeaderId
	n.resetElectionTimer()
	resp.Term = n.currentTerm

	// Check if log contains an entry at prevLogIndex with prevLogTerm (§5.3)
	if req.PrevLogIndex > 0 {
		prevEntry := n.logEntryAt(req.PrevLogIndex)
		if prevEntry == nil {
			// We don't have this entry at all
			resp.ConflictIndex = n.lastLogIndex() + 1
			resp.ConflictTerm = 0
			return resp
		}
		if prevEntry.Term != req.PrevLogTerm {
			// Conflict: find first index of the conflicting term
			resp.ConflictTerm = prevEntry.Term
			conflictIdx := req.PrevLogIndex
			for conflictIdx > n.lastSnapshotIndex+1 {
				e := n.logEntryAt(conflictIdx - 1)
				if e == nil || e.Term != resp.ConflictTerm {
					break
				}
				conflictIdx--
			}
			resp.ConflictIndex = conflictIdx
			return resp
		}
	}

	// Append new entries (§5.3)
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		existing := n.logEntryAt(idx)
		if existing != nil && existing.Term != entry.Term {
			// Conflict: delete this and all following entries
			offset := int(idx - n.lastSnapshotIndex)
			if offset > 0 && offset < len(n.log) {
				n.log = n.log[:offset]
			}
			existing = nil
		}
		if existing == nil {
			n.log = append(n.log, entry)
			if n.wal != nil {
				n.wal.Append(entry)
			}
		}
	}

	// Update commit index (§5.3)
	if req.LeaderCommit > n.commitIndex {
		lastNewEntry := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewEntry {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewEntry
		}
		n.notifyApply()
	}

	resp.Success = true
	return resp
}

// advanceCommitIndex checks if we can advance the commit index based on matchIndex.
// Called while holding n.mu.
func (n *Node) advanceCommitIndex() {
	// Gather matchIndex values + our own log length
	matches := make([]uint64, 0, len(n.peers)+1)
	matches = append(matches, n.lastLogIndex()) // leader
	for _, peer := range n.peers {
		matches = append(matches, n.matchIndex[peer])
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	// The median is the highest index replicated on a majority
	majority := (len(n.peers)+1)/2 + 1
	if majority > len(matches) {
		return
	}
	newCommitIndex := matches[majority-1]

	// Only commit entries from the current term (§5.4.2)
	if newCommitIndex > n.commitIndex {
		entry := n.logEntryAt(newCommitIndex)
		if entry != nil && entry.Term == n.currentTerm {
			n.commitIndex = newCommitIndex
			metrics.CommitIndex.Set(float64(newCommitIndex))
			// Update replication lag per follower
			for _, peer := range n.peers {
				lag := int64(n.lastLogIndex()) - int64(n.matchIndex[peer])
				metrics.LogReplicationLag.WithLabelValues(peer).Set(float64(lag))
			}
			n.notifyApply()
		}
	}
}

// sendSnapshot sends an InstallSnapshot RPC to a peer that's too far behind.
func (n *Node) sendSnapshot(peerID string, term uint64) {
	n.mu.RLock()
	snapIndex := n.lastSnapshotIndex
	snapTerm := n.lastSnapshotTerm
	n.mu.RUnlock()

	// Load snapshot data from WAL
	if n.wal == nil {
		return
	}
	_, _, data, err := n.wal.LoadSnapshot()
	if err != nil || data == nil {
		return
	}

	resp, err := n.transport.SendInstallSnapshot(peerID, &pb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          n.id,
		LastIncludedIndex: snapIndex,
		LastIncludedTerm:  snapTerm,
		Data:              data,
	})
	if err != nil {
		n.logger.Debugw("InstallSnapshot RPC failed", "peer", peerID, "error", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	n.nextIndex[peerID] = snapIndex + 1
	n.matchIndex[peerID] = snapIndex
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.InstallSnapshotResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	n.leaderID = req.LeaderId
	n.resetElectionTimer()

	// If we already have a snapshot past this point, ignore
	if req.LastIncludedIndex <= n.lastSnapshotIndex {
		return resp
	}

	// Apply snapshot
	if n.fsm != nil {
		if err := n.fsm.Restore(req.Data); err != nil {
			n.logger.Errorw("failed to restore snapshot", "error", err)
			return resp
		}
	}

	// Reset log to just the snapshot sentinel
	n.log = []*pb.LogEntry{{Term: req.LastIncludedTerm, Index: req.LastIncludedIndex}}
	n.lastSnapshotIndex = req.LastIncludedIndex
	n.lastSnapshotTerm = req.LastIncludedTerm
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex

	if n.wal != nil {
		n.wal.SaveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data)
	}

	resp.Term = n.currentTerm
	return resp
}
