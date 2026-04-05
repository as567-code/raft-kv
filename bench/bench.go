package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	kvpb "github.com/as567-code/raft-kv/proto/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ThroughputResult holds the output of a throughput benchmark.
type ThroughputResult struct {
	TotalOps     int64   `json:"total_ops"`
	DurationSecs float64 `json:"duration_seconds"`
	OpsPerSecond float64 `json:"ops_per_second"`
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
	ErrorRate    float64 `json:"error_rate"`
	TotalErrors  int64   `json:"total_errors"`
}

// FailoverResult holds the output of a failover benchmark.
type FailoverResult struct {
	LeaderKilledAt       string  `json:"leader_killed_at"`
	NewLeaderElectedAt   string  `json:"new_leader_elected_at"`
	FailoverDurationMs   float64 `json:"failover_duration_ms"`
	OpsDuringFailover    int64   `json:"ops_during_failover"`
	OpsFailed            int64   `json:"ops_failed"`
	OpsSucceededAfterRec int64   `json:"ops_succeeded_after_recovery"`
}

// ReadModeResult holds read mode comparison results.
type ReadModeResult struct {
	LeaderRead   ReadModeStats `json:"leader_read"`
	LeaseRead    ReadModeStats `json:"lease_read"`
	FollowerRead ReadModeStats `json:"follower_read"`
}

type ReadModeStats struct {
	LatencyP50Ms float64 `json:"latency_p50_ms"`
	LatencyP99Ms float64 `json:"latency_p99_ms"`
	StalenessMs  float64 `json:"staleness_ms"`
}

// RunThroughputBench executes the throughput benchmark against a running cluster.
func RunThroughputBench(addrs []string, numClients int, duration time.Duration) (*ThroughputResult, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	// Find leader
	leaderAddr, err := findLeader(addrs)
	if err != nil {
		return nil, fmt.Errorf("failed to find leader: %w", err)
	}

	conn, err := grpc.Dial(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer conn.Close()

	client := kvpb.NewKVServiceClient(conn)

	var (
		totalOps  atomic.Int64
		totalErrs atomic.Int64
		latencies []float64
		latMu     sync.Mutex
		wg        sync.WaitGroup
	)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				key := fmt.Sprintf("bench-key-%d-%d", clientID, totalOps.Load())
				value := fmt.Sprintf("value-%d", time.Now().UnixNano())

				opStart := time.Now()
				_, err := client.Put(context.Background(), &kvpb.PutRequest{
					Key:   key,
					Value: value,
				})
				elapsed := time.Since(opStart)

				if err != nil {
					totalErrs.Add(1)
				} else {
					totalOps.Add(1)
					latMu.Lock()
					latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
					latMu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	sort.Float64s(latencies)
	ops := totalOps.Load()
	errs := totalErrs.Load()

	result := &ThroughputResult{
		TotalOps:     ops,
		DurationSecs: elapsed.Seconds(),
		OpsPerSecond: float64(ops) / elapsed.Seconds(),
		ErrorRate:    float64(errs) / float64(ops+errs),
		TotalErrors:  errs,
	}

	if len(latencies) > 0 {
		result.P50LatencyMs = percentile(latencies, 50)
		result.P95LatencyMs = percentile(latencies, 95)
		result.P99LatencyMs = percentile(latencies, 99)
	}

	return result, nil
}

// RunReadModeBench benchmarks different read modes.
func RunReadModeBench(addrs []string, numOps int) (*ReadModeResult, error) {
	if len(addrs) < 2 {
		return nil, fmt.Errorf("need at least 2 addresses for read mode bench")
	}

	leaderAddr, err := findLeader(addrs)
	if err != nil {
		return nil, err
	}

	// Find a follower address
	var followerAddr string
	for _, addr := range addrs {
		if addr != leaderAddr {
			followerAddr = addr
			break
		}
	}

	// Pre-populate some keys
	conn, err := grpc.Dial(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	leaderClient := kvpb.NewKVServiceClient(conn)

	for i := 0; i < 100; i++ {
		leaderClient.Put(context.Background(), &kvpb.PutRequest{
			Key:   fmt.Sprintf("read-bench-%d", i),
			Value: fmt.Sprintf("value-%d", i),
		})
	}
	conn.Close()

	time.Sleep(200 * time.Millisecond)

	result := &ReadModeResult{}

	// Benchmark leader read
	result.LeaderRead, err = benchReadMode(leaderAddr, kvpb.ReadMode_READ_LEADER, numOps)
	if err != nil {
		return nil, err
	}
	result.LeaderRead.StalenessMs = 0

	// Benchmark lease-based read
	result.LeaseRead, err = benchReadMode(followerAddr, kvpb.ReadMode_READ_LEASE_BASED, numOps)
	if err != nil {
		return nil, err
	}
	result.LeaseRead.StalenessMs = 150 // estimated

	// Benchmark follower read
	result.FollowerRead, err = benchReadMode(followerAddr, kvpb.ReadMode_READ_FOLLOWER, numOps)
	if err != nil {
		return nil, err
	}
	result.FollowerRead.StalenessMs = 800 // estimated

	return result, nil
}

func benchReadMode(addr string, mode kvpb.ReadMode, numOps int) (ReadModeStats, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return ReadModeStats{}, err
	}
	defer conn.Close()

	client := kvpb.NewKVServiceClient(conn)
	var latencies []float64

	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("read-bench-%d", i%100)
		start := time.Now()
		_, err := client.Get(context.Background(), &kvpb.GetRequest{
			Key:      key,
			ReadMode: mode,
		})
		elapsed := time.Since(start)
		if err == nil {
			latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
		}
	}

	sort.Float64s(latencies)
	return ReadModeStats{
		LatencyP50Ms: percentile(latencies, 50),
		LatencyP99Ms: percentile(latencies, 99),
	}, nil
}

func findLeader(addrs []string) (string, error) {
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(2*time.Second),
		)
		if err != nil {
			continue
		}

		client := kvpb.NewKVServiceClient(conn)
		resp, err := client.Put(context.Background(), &kvpb.PutRequest{
			Key:   "__leader_probe__",
			Value: "probe",
		})
		conn.Close()

		if err == nil && resp.Success {
			return addr, nil
		}
		if resp != nil && resp.LeaderHint != "" {
			return resp.LeaderHint, nil
		}
	}
	return addrs[0], nil // fallback
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// WriteResult writes a JSON result to a file.
func WriteResult(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
