package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/as567-code/raft-kv/api"
	"github.com/as567-code/raft-kv/config"
	"github.com/as567-code/raft-kv/raft"
	"github.com/as567-code/raft-kv/store"
	"github.com/as567-code/raft-kv/transport"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	cfg := config.LoadFromEnv()

	zapCfg := zap.NewProductionConfig()
	zapCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	zapLogger, err := zapCfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	logger := zapLogger.Sugar()
	defer zapLogger.Sync()

	logger.Infow("starting raft-kv node",
		"node_id", cfg.NodeID,
		"listen_addr", cfg.ListenAddr,
		"client_addr", cfg.ClientAddr,
		"peers", cfg.Peers,
		"data_dir", cfg.DataDir,
	)

	// Initialize WAL
	walDir := fmt.Sprintf("%s/%s", cfg.DataDir, cfg.NodeID)
	wal, err := raft.NewWAL(walDir)
	if err != nil {
		logger.Fatalw("failed to create WAL", "error", err)
	}
	defer wal.Close()

	// Initialize KV store and FSM
	kvStore := store.NewKVStore()
	fsm := store.NewFSM(kvStore)

	// Build peer map for transport
	peerMap := make(map[string]string)
	peerClientMap := make(map[string]string)
	var peerIDs []string
	// Extract client port from our own ClientAddr (e.g., ":50061" -> "50061")
	clientPort := cfg.ClientAddr
	if idx := strings.LastIndex(clientPort, ":"); idx >= 0 {
		clientPort = clientPort[idx+1:]
	}
	for _, p := range cfg.Peers {
		peerMap[p.ID] = p.Addr
		peerIDs = append(peerIDs, p.ID)
		// Derive client address: replace raft port with client port
		host := p.Addr
		if idx := strings.LastIndex(host, ":"); idx >= 0 {
			host = host[:idx]
		}
		peerClientMap[p.ID] = host + ":" + clientPort
	}

	// Initialize transport
	grpcTransport := transport.NewGRPCTransport(peerMap, logger)

	// Initialize Raft node
	node := raft.NewNode(raft.NodeConfig{
		ID:                cfg.NodeID,
		Peers:             peerIDs,
		Transport:         grpcTransport,
		FSM:               fsm,
		WAL:               wal,
		ElectionMinMs:     cfg.ElectionMinMs,
		ElectionMaxMs:     cfg.ElectionMaxMs,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SnapshotThreshold: cfg.SnapshotThreshold,
		Logger:            logger,
	})

	// Wire transport handler
	grpcTransport.SetHandler(node)

	// Start transport (listen for Raft RPCs)
	if err := grpcTransport.Start(cfg.ListenAddr); err != nil {
		logger.Fatalw("failed to start transport", "error", err)
	}
	defer grpcTransport.Stop()

	// Start Raft node
	if err := node.Start(); err != nil {
		logger.Fatalw("failed to start raft node", "error", err)
	}
	defer node.Stop()

	// Initialize dual store (CRDT + adaptive consistency)
	dualStore := store.NewDualStore(cfg.NodeID, fsm)

	// Start client API
	apiServer := api.NewServer(api.ServerConfig{
		Node:            node,
		FSM:             fsm,
		DualStore:       dualStore,
		Logger:          logger,
		PeerClientAddrs: peerClientMap,
		SelfClientAddr:  cfg.ClientAddr,
	})
	if err := apiServer.Start(cfg.ClientAddr); err != nil {
		logger.Fatalw("failed to start client API", "error", err)
	}
	defer apiServer.Stop()

	// Start Prometheus metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ok","node_id":"%s","state":"%s","leader":"%s"}`,
				cfg.NodeID, node.State(), node.LeaderID())
		})
		logger.Infow("metrics server started", "addr", cfg.MetricsAddr)
		if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
			logger.Errorw("metrics server error", "error", err)
		}
	}()

	logger.Infow("raft-kv node started successfully",
		"node_id", cfg.NodeID,
		"raft_addr", cfg.ListenAddr,
		"client_addr", cfg.ClientAddr,
		"metrics_addr", cfg.MetricsAddr,
	)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Infow("received signal, shutting down", "signal", sig)
}
