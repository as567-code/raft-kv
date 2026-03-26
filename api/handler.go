package api

import (
	"context"
	"strings"
	"time"

	"github.com/as567-code/raft-kv/metrics"
	kvpb "github.com/as567-code/raft-kv/proto/kvpb"
	"github.com/as567-code/raft-kv/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Put handles a client Put request.
func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	start := time.Now()
	defer func() {
		metrics.KVOperationDuration.WithLabelValues("put").Observe(time.Since(start).Seconds())
	}()

	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	// Check if client requests eventual consistency or if we're in eventual mode
	useEventual := req.Consistency == kvpb.ConsistencyMode_CONSISTENCY_EVENTUAL
	if s.dualStore != nil && !useEventual {
		mode := s.dualStore.EffectiveMode()
		if mode == store.Eventual {
			useEventual = true
		}
	}

	if useEventual && s.dualStore != nil {
		// Write to CRDT register (no Raft required)
		s.dualStore.PutEventual(req.Key, req.Value)
		metrics.KVOperationsTotal.WithLabelValues("put", "ok").Inc()
		return &kvpb.PutResponse{Success: true}, nil
	}

	// Strong mode: write through Raft
	cmd := store.Command{
		Type:  store.CmdPut,
		Key:   req.Key,
		Value: req.Value,
	}
	data, err := store.EncodeCommand(cmd)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to encode command")
	}

	err = s.node.Propose(data)
	if err != nil {
		if strings.Contains(err.Error(), "not leader") {
			// In adaptive mode, try CRDT fallback
			if s.dualStore != nil && s.dualStore.Mode() == store.Adaptive {
				s.dualStore.SetQuorumAvailable(false)
				s.dualStore.PutEventual(req.Key, req.Value)
				return &kvpb.PutResponse{Success: true}, nil
			}
			// Forward to leader
			if lc := s.leaderClient(); lc != nil {
				return lc.Put(ctx, req)
			}
			return &kvpb.PutResponse{
				Success:    false,
				LeaderHint: s.leaderHint(),
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "propose failed: %v", err)
	}

	metrics.KVOperationsTotal.WithLabelValues("put", "ok").Inc()
	return &kvpb.PutResponse{Success: true}, nil
}

// Get handles a client Get request with three read modes.
func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	start := time.Now()
	defer func() {
		metrics.KVOperationDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
		metrics.KVOperationsTotal.WithLabelValues("get", "ok").Inc()
	}()

	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	switch req.ReadMode {
	case kvpb.ReadMode_READ_LEADER:
		// Linearizable read: must be leader
		if !s.node.IsLeader() {
			if lc := s.leaderClient(); lc != nil {
				return lc.Get(ctx, req)
			}
			return &kvpb.GetResponse{
				Found:      false,
				LeaderHint: s.leaderHint(),
			}, nil
		}
		val, found := s.readFromStore(req.Key)
		return &kvpb.GetResponse{Value: val, Found: found}, nil

	case kvpb.ReadMode_READ_LEASE_BASED:
		// Bounded staleness: read from local store with lease validation
		// In a full implementation, we'd check lease freshness here.
		// For now, read from local store which is bounded by replication lag.
		val, found := s.readFromStore(req.Key)
		return &kvpb.GetResponse{Value: val, Found: found}, nil

	case kvpb.ReadMode_READ_FOLLOWER:
		// Eventual consistency: read from any node's local store
		val, found := s.readFromStore(req.Key)
		return &kvpb.GetResponse{Value: val, Found: found}, nil

	default:
		// Default to leader read
		if !s.node.IsLeader() {
			if lc := s.leaderClient(); lc != nil {
				return lc.Get(ctx, req)
			}
			return &kvpb.GetResponse{
				Found:      false,
				LeaderHint: s.leaderHint(),
			}, nil
		}
		val, found := s.readFromStore(req.Key)
		return &kvpb.GetResponse{Value: val, Found: found}, nil
	}
}

// readFromStore reads from the dual store if available, otherwise from FSM.
func (s *Server) readFromStore(key string) (string, bool) {
	if s.dualStore != nil {
		return s.dualStore.Get(key)
	}
	return s.fsm.Store().Get(key)
}

// Delete handles a client Delete request.
func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	// Check eventual mode
	if s.dualStore != nil && s.dualStore.EffectiveMode() == store.Eventual {
		s.dualStore.DeleteEventual(req.Key)
		return &kvpb.DeleteResponse{Success: true}, nil
	}

	cmd := store.Command{
		Type: store.CmdDelete,
		Key:  req.Key,
	}
	data, err := store.EncodeCommand(cmd)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to encode command")
	}

	err = s.node.Propose(data)
	if err != nil {
		if strings.Contains(err.Error(), "not leader") {
			if s.dualStore != nil && s.dualStore.Mode() == store.Adaptive {
				s.dualStore.SetQuorumAvailable(false)
				s.dualStore.DeleteEventual(req.Key)
				return &kvpb.DeleteResponse{Success: true}, nil
			}
			// Forward to leader
			if lc := s.leaderClient(); lc != nil {
				return lc.Delete(ctx, req)
			}
			return &kvpb.DeleteResponse{
				Success:    false,
				LeaderHint: s.leaderHint(),
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "propose failed: %v", err)
	}

	return &kvpb.DeleteResponse{Success: true}, nil
}

// Scan handles a client Scan request.
func (s *Server) Scan(ctx context.Context, req *kvpb.ScanRequest) (*kvpb.ScanResponse, error) {
	pairs := s.fsm.Store().Scan(req.Prefix)
	return &kvpb.ScanResponse{Pairs: pairs}, nil
}
