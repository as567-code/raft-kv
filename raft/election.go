package raft

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/as567-code/raft-kv/metrics"
	pb "github.com/as567-code/raft-kv/proto/raftpb"
)

// startElection initiates a leader election.
func (n *Node) startElection() {
	n.mu.Lock()
	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	n.becomeCandidate()
	term := n.currentTerm
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	n.mu.Unlock()

	metrics.ElectionsTotal.Inc()
	electionStart := time.Now()
	n.logger.Infow("starting election", "term", term, "node", n.id)

	// Vote for self
	var votesGranted atomic.Int32
	votesGranted.Store(1) // self-vote

	majority := (len(n.peers)+1)/2 + 1

	// Single-node cluster: win immediately
	if len(n.peers) == 0 {
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == term {
			n.becomeLeader()
		}
		n.mu.Unlock()
		return
	}

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			resp, err := n.transport.SendRequestVote(peerID, &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				n.logger.Debugw("RequestVote RPC failed", "peer", peerID, "error", err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Stale response — we've moved on
			if n.currentTerm != term || n.state != Candidate {
				return
			}

			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				return
			}

			if resp.VoteGranted {
				votes := int(votesGranted.Add(1))
				if votes >= majority && n.state == Candidate && n.currentTerm == term {
					metrics.ElectionDuration.Observe(time.Since(electionStart).Seconds())
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Reply false if term < currentTerm (§5.1)
	if req.Term < n.currentTerm {
		return resp
	}

	// If RPC request contains term > currentTerm, convert to follower (§5.1)
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	resp.Term = n.currentTerm

	// Grant vote if we haven't voted yet (or already voted for this candidate)
	// AND candidate's log is at least as up-to-date as ours (§5.4.1)
	canVote := n.votedFor == "" || n.votedFor == req.CandidateId
	logOk := n.isLogUpToDate(req.LastLogTerm, req.LastLogIndex)

	if canVote && logOk {
		n.votedFor = req.CandidateId
		n.persistState()
		resp.VoteGranted = true
		n.resetElectionTimer()
		n.logger.Infow("granted vote", "to", req.CandidateId, "term", n.currentTerm)
	}

	return resp
}

// isLogUpToDate checks if the candidate's log is at least as up-to-date as ours.
// Raft determines which of two logs is more up-to-date by comparing the
// index and term of the last entries in the logs.
func (n *Node) isLogUpToDate(candidateLastTerm, candidateLastIndex uint64) bool {
	myLastTerm := n.lastLogTerm()
	myLastIndex := n.lastLogIndex()

	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= myLastIndex
}
