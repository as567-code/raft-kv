package api

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	kvpb "github.com/as567-code/raft-kv/proto/kvpb"
	"github.com/as567-code/raft-kv/raft"
	"github.com/as567-code/raft-kv/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server is the client-facing gRPC API server.
type Server struct {
	kvpb.UnimplementedKVServiceServer

	node      *raft.Node
	fsm       *store.FSM
	dualStore *store.DualStore // optional, for CRDT + adaptive consistency
	server    *grpc.Server
	logger    *zap.SugaredLogger

	// peerClientAddrs maps peer node IDs to their client-facing addresses
	peerClientAddrs map[string]string
	selfClientAddr  string

	// connection pool for forwarding to leader
	fwdMu    sync.RWMutex
	fwdConns map[string]*grpc.ClientConn
}

type ServerConfig struct {
	Node            *raft.Node
	FSM             *store.FSM
	DualStore       *store.DualStore
	Logger          *zap.SugaredLogger
	PeerClientAddrs map[string]string
	SelfClientAddr  string
}

func NewServer(cfg ServerConfig) *Server {
	if cfg.Logger == nil {
		l, _ := zap.NewProduction()
		cfg.Logger = l.Sugar()
	}
	return &Server{
		node:            cfg.Node,
		fsm:             cfg.FSM,
		dualStore:       cfg.DualStore,
		logger:          cfg.Logger,
		peerClientAddrs: cfg.PeerClientAddrs,
		selfClientAddr:  cfg.SelfClientAddr,
		fwdConns:        make(map[string]*grpc.ClientConn),
	}
}

// Start begins listening for client gRPC requests.
func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.server = grpc.NewServer()
	kvpb.RegisterKVServiceServer(s.server, s)

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.Errorw("client gRPC server error", "error", err)
		}
	}()

	s.logger.Infow("client API started", "addr", addr)
	return nil
}

// Stop gracefully stops the client API server.
func (s *Server) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
	s.fwdMu.Lock()
	for _, conn := range s.fwdConns {
		conn.Close()
	}
	s.fwdMu.Unlock()
}

// leaderClient returns a KVServiceClient connected to the current leader.
// Returns nil if this node is the leader or if no leader is known.
func (s *Server) leaderClient() kvpb.KVServiceClient {
	leaderAddr := s.leaderHint()
	if leaderAddr == "" || leaderAddr == s.selfClientAddr {
		return nil
	}

	s.fwdMu.RLock()
	conn, ok := s.fwdConns[leaderAddr]
	s.fwdMu.RUnlock()
	if ok {
		return kvpb.NewKVServiceClient(conn)
	}

	s.fwdMu.Lock()
	defer s.fwdMu.Unlock()
	if conn, ok := s.fwdConns[leaderAddr]; ok {
		return kvpb.NewKVServiceClient(conn)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		s.logger.Debugw("failed to connect to leader", "addr", leaderAddr, "error", err)
		return nil
	}
	s.fwdConns[leaderAddr] = conn
	return kvpb.NewKVServiceClient(conn)
}

func (s *Server) leaderHint() string {
	leaderID := s.node.LeaderID()
	if leaderID == "" {
		return ""
	}
	if leaderID == s.node.ID() {
		return s.selfClientAddr
	}
	if addr, ok := s.peerClientAddrs[leaderID]; ok {
		return addr
	}
	return ""
}
