package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/as567-code/raft-kv/proto/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	rpcTimeout = 500 * time.Millisecond
)

// GRPCTransport implements both the Raft Transport interface (client side)
// and the gRPC RaftService server (server side).
type GRPCTransport struct {
	pb.UnimplementedRaftServiceServer

	handler RaftHandler
	server  *grpc.Server

	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
	peers map[string]string // peerID -> address

	logger *zap.SugaredLogger
}

func NewGRPCTransport(peers map[string]string, logger *zap.SugaredLogger) *GRPCTransport {
	if logger == nil {
		l, _ := zap.NewProduction()
		logger = l.Sugar()
	}
	return &GRPCTransport{
		conns:  make(map[string]*grpc.ClientConn),
		peers:  peers,
		logger: logger,
	}
}

// SetHandler sets the Raft handler that processes incoming RPCs.
func (t *GRPCTransport) SetHandler(h RaftHandler) {
	t.handler = h
}

// Start begins listening for incoming gRPC connections.
func (t *GRPCTransport) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	t.server = grpc.NewServer()
	pb.RegisterRaftServiceServer(t.server, t)

	go func() {
		if err := t.server.Serve(lis); err != nil {
			t.logger.Errorw("gRPC server error", "error", err)
		}
	}()

	t.logger.Infow("transport started", "addr", addr)
	return nil
}

// Stop gracefully stops the gRPC server and closes all client connections.
func (t *GRPCTransport) Stop() {
	if t.server != nil {
		t.server.GracefulStop()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, conn := range t.conns {
		conn.Close()
		delete(t.conns, id)
	}
}

// getConn returns a cached or newly created connection to a peer.
func (t *GRPCTransport) getConn(peerID string) (*grpc.ClientConn, error) {
	t.mu.RLock()
	conn, ok := t.conns[peerID]
	t.mu.RUnlock()
	if ok {
		return conn, nil
	}

	addr, ok := t.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", peerID)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := t.conns[peerID]; ok {
		return conn, nil
	}

	conn, err := grpc.DialContext(context.Background(), addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s at %s: %w", peerID, addr, err)
	}

	t.conns[peerID] = conn
	return conn, nil
}

// --- Client-side Transport interface implementation ---

func (t *GRPCTransport) SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	conn, err := t.getConn(peerID)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	return client.RequestVote(ctx, req)
}

func (t *GRPCTransport) SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	conn, err := t.getConn(peerID)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	return client.AppendEntries(ctx, req)
}

func (t *GRPCTransport) SendInstallSnapshot(peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	conn, err := t.getConn(peerID)
	if err != nil {
		return nil, err
	}

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.InstallSnapshot(ctx, req)
}

// --- Server-side gRPC handlers ---

func (t *GRPCTransport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if t.handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}
	return t.handler.HandleRequestVote(req), nil
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if t.handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}
	return t.handler.HandleAppendEntries(req), nil
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if t.handler == nil {
		return nil, fmt.Errorf("no handler registered")
	}
	return t.handler.HandleInstallSnapshot(req), nil
}
