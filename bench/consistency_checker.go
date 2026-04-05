package bench

import (
	"context"
	"fmt"
	"time"

	kvpb "github.com/as567-code/raft-kv/proto/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConsistencyResult holds the output of a consistency check.
type ConsistencyResult struct {
	PartitionType      string  `json:"partition_type"`
	TotalWrites        int     `json:"total_writes"`
	WritesSucceeded    int     `json:"writes_succeeded"`
	PostHealConsistent int     `json:"post_heal_consistent_keys"`
	ConsistencyPercent float64 `json:"consistency_percentage"`
}

// CheckConsistency reads all keys from all nodes and compares values.
func CheckConsistency(addrs []string, keys []string) (*ConsistencyResult, error) {
	if len(addrs) == 0 || len(keys) == 0 {
		return nil, fmt.Errorf("need at least one address and one key")
	}

	// Read all keys from all nodes
	nodeValues := make(map[string]map[string]string) // addr -> key -> value
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
		}
		client := kvpb.NewKVServiceClient(conn)

		nodeValues[addr] = make(map[string]string)
		for _, key := range keys {
			resp, err := client.Get(context.Background(), &kvpb.GetRequest{
				Key:      key,
				ReadMode: kvpb.ReadMode_READ_FOLLOWER, // read local state
			})
			if err == nil && resp.Found {
				nodeValues[addr][key] = resp.Value
			}
		}
		conn.Close()
	}

	// Compare values across nodes
	consistent := 0
	for _, key := range keys {
		values := make(map[string]bool)
		allFound := true
		for _, addr := range addrs {
			v, ok := nodeValues[addr][key]
			if !ok {
				allFound = false
				break
			}
			values[v] = true
		}
		if allFound && len(values) == 1 {
			consistent++
		}
	}

	pct := 0.0
	if len(keys) > 0 {
		pct = float64(consistent) / float64(len(keys)) * 100.0
	}

	return &ConsistencyResult{
		TotalWrites:        len(keys),
		PostHealConsistent: consistent,
		ConsistencyPercent: pct,
	}, nil
}
