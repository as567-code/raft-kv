package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	NodeID            string
	ListenAddr        string // gRPC listen address for raft inter-node
	ClientAddr        string // gRPC listen address for client API
	Peers             []Peer // Other nodes in the cluster
	DataDir           string // Directory for WAL and snapshots
	ElectionMinMs     int
	ElectionMaxMs     int
	HeartbeatInterval time.Duration
	SnapshotThreshold uint64 // Number of log entries before snapshotting
	MetricsAddr       string // Prometheus metrics address
}

type Peer struct {
	ID   string
	Addr string
}

func DefaultConfig() *Config {
	return &Config{
		NodeID:            "node1",
		ListenAddr:        ":50051",
		ClientAddr:        ":50061",
		DataDir:           "/tmp/raft-kv",
		ElectionMinMs:     150,
		ElectionMaxMs:     300,
		HeartbeatInterval: 50 * time.Millisecond,
		SnapshotThreshold: 1000,
		MetricsAddr:       ":9090",
	}
}

// LoadFromEnv populates config from environment variables.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if v := os.Getenv("NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("CLIENT_ADDR"); v != "" {
		cfg.ClientAddr = v
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("METRICS_ADDR"); v != "" {
		cfg.MetricsAddr = v
	}
	if v := os.Getenv("ELECTION_MIN_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ElectionMinMs = n
		}
	}
	if v := os.Getenv("ELECTION_MAX_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ElectionMaxMs = n
		}
	}
	if v := os.Getenv("HEARTBEAT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HeartbeatInterval = time.Duration(n) * time.Millisecond
		}
	}
	if v := os.Getenv("SNAPSHOT_THRESHOLD"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			cfg.SnapshotThreshold = n
		}
	}

	// PEERS format: "node2=host2:port2,node3=host3:port3"
	if v := os.Getenv("PEERS"); v != "" {
		cfg.Peers = parsePeers(v)
	}

	return cfg
}

func parsePeers(s string) []Peer {
	var peers []Peer
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "warning: invalid peer format %q (expected id=addr)\n", entry)
			continue
		}
		peers = append(peers, Peer{
			ID:   strings.TrimSpace(parts[0]),
			Addr: strings.TrimSpace(parts[1]),
		})
	}
	return peers
}
