package transport

import (
	pb "github.com/as567-code/raft-kv/proto/raftpb"
)

// RaftHandler handles incoming Raft RPCs and delegates to the raft.Node.
type RaftHandler interface {
	HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse
	HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse
	HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse
}
