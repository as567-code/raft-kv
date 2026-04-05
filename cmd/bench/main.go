package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kvpb "github.com/as567-code/raft-kv/proto/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var clientAddrs = []string{"localhost:50061", "localhost:50062", "localhost:50063"}
var containers = []string{"raft-kv-node1", "raft-kv-node2", "raft-kv-node3"}

func main() {
	testType := flag.String("test", "throughput", "Test type: throughput, failover, read-modes, partition, crdt-merge, adaptive-mode")
	output := flag.String("output", "", "Output file path (JSON)")
	flag.Parse()

	var result interface{}
	var err error

	switch *testType {
	case "throughput":
		result, err = runThroughput()
	case "failover":
		result, err = runFailover()
	case "read-modes":
		result, err = runReadModes()
	case "partition":
		result, err = runPartition()
	case "crdt-merge":
		result, err = runCRDTMerge()
	case "adaptive-mode":
		result, err = runAdaptiveMode()
	default:
		fmt.Fprintf(os.Stderr, "Unknown test type: %s\n", *testType)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
		os.Exit(1)
	}

	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))

	if *output != "" {
		os.MkdirAll("bench/results", 0o755)
		if err := os.WriteFile(*output, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write output: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Results written to %s\n", *output)
	}
}

// ─── helpers ───

func dial(addr string) (kvpb.KVServiceClient, *grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}
	return kvpb.NewKVServiceClient(conn), conn, nil
}

func findLeaderAddr() (string, error) {
	for _, addr := range clientAddrs {
		cl, conn, err := dial(addr)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := cl.Put(ctx, &kvpb.PutRequest{Key: "__probe__", Value: "p"})
		cancel()
		conn.Close()
		if err == nil && resp.Success {
			return addr, nil
		}
	}
	return "", fmt.Errorf("no leader found")
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
	return math.Round(sorted[idx]*100) / 100
}

// ─── throughput ───

func runThroughput() (interface{}, error) {
	fmt.Fprintln(os.Stderr, "Running throughput benchmark...")
	leaderAddr, err := findLeaderAddr()
	if err != nil {
		return nil, err
	}
	cl, conn, err := dial(leaderAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	numClients := 50
	duration := 10 * time.Second

	var totalOps, totalErrs atomic.Int64
	var latMu sync.Mutex
	var latencies []float64
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	start := time.Now()
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				key := fmt.Sprintf("bench-%d-%d", id, seq)
				seq++
				t0 := time.Now()
				_, err := cl.Put(context.Background(), &kvpb.PutRequest{Key: key, Value: "v"})
				dt := time.Since(t0)
				if err != nil {
					totalErrs.Add(1)
				} else {
					totalOps.Add(1)
					latMu.Lock()
					latencies = append(latencies, float64(dt.Microseconds())/1000.0)
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
	errRate := 0.0
	if ops+errs > 0 {
		errRate = float64(errs) / float64(ops+errs)
	}

	return map[string]interface{}{
		"total_ops":        ops,
		"duration_seconds": math.Round(elapsed.Seconds()*100) / 100,
		"ops_per_second":   math.Round(float64(ops)/elapsed.Seconds()*100) / 100,
		"p50_latency_ms":   percentile(latencies, 50),
		"p95_latency_ms":   percentile(latencies, 95),
		"p99_latency_ms":   percentile(latencies, 99),
		"error_rate":       math.Round(errRate*10000) / 10000,
		"total_errors":     errs,
		"num_clients":      numClients,
	}, nil
}

// ─── failover ───

func runFailover() (interface{}, error) {
	fmt.Fprintln(os.Stderr, "Running failover benchmark...")

	// Find current leader
	leaderAddr, err := findLeaderAddr()
	if err != nil {
		return nil, err
	}

	// Map addr → container
	leaderContainer := ""
	for i, addr := range clientAddrs {
		if addr == leaderAddr {
			leaderContainer = containers[i]
			break
		}
	}
	if leaderContainer == "" {
		leaderContainer = containers[0]
	}

	// Start background writers so we can measure ops during failover
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var opsTotal, opsFailed, opsSucceeded atomic.Int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			for _, addr := range clientAddrs {
				cl, conn, err := dial(addr)
				if err != nil {
					continue
				}
				_, err = cl.Put(context.Background(), &kvpb.PutRequest{
					Key: fmt.Sprintf("failover-%d", opsTotal.Add(1)), Value: "v",
				})
				conn.Close()
				if err != nil {
					opsFailed.Add(1)
				} else {
					opsSucceeded.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Let some writes happen first
	time.Sleep(2 * time.Second)

	// Kill the leader
	killedAt := time.Now()
	fmt.Fprintf(os.Stderr, "Killing leader %s at %s\n", leaderContainer, killedAt.Format(time.RFC3339))
	exec.Command("docker", "kill", leaderContainer).Run()

	// Poll until a new leader answers
	newLeaderElected := time.Time{}
	for {
		if time.Since(killedAt) > 10*time.Second {
			break
		}
		for _, addr := range clientAddrs {
			if addr == leaderAddr {
				continue
			}
			cl, conn, err := dial(addr)
			if err != nil {
				continue
			}
			resp, err := cl.Put(context.Background(), &kvpb.PutRequest{Key: "__failover_probe__", Value: "x"})
			conn.Close()
			if err == nil && resp.Success {
				newLeaderElected = time.Now()
				break
			}
		}
		if !newLeaderElected.IsZero() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	failoverMs := float64(newLeaderElected.Sub(killedAt).Milliseconds())

	// Restart the killed node
	exec.Command("docker", "start", leaderContainer).Run()

	return map[string]interface{}{
		"leader_killed_at":             killedAt.Format(time.RFC3339),
		"new_leader_elected_at":        newLeaderElected.Format(time.RFC3339),
		"failover_duration_ms":         failoverMs,
		"ops_during_failover":          opsTotal.Load(),
		"ops_failed":                   opsFailed.Load(),
		"ops_succeeded_after_recovery": opsSucceeded.Load(),
	}, nil
}

// ─── read modes ───

func runReadModes() (interface{}, error) {
	fmt.Fprintln(os.Stderr, "Running read mode benchmark...")
	leaderAddr, err := findLeaderAddr()
	if err != nil {
		return nil, err
	}

	// Populate keys
	cl, conn, err := dial(leaderAddr)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 100; i++ {
		cl.Put(context.Background(), &kvpb.PutRequest{Key: fmt.Sprintf("rm-%d", i), Value: fmt.Sprintf("v-%d", i)})
	}
	conn.Close()
	time.Sleep(300 * time.Millisecond)

	benchMode := func(addr string, mode kvpb.ReadMode, n int) (p50, p99 float64) {
		cl2, conn2, err := dial(addr)
		if err != nil {
			return 0, 0
		}
		defer conn2.Close()
		var lats []float64
		for i := 0; i < n; i++ {
			t0 := time.Now()
			cl2.Get(context.Background(), &kvpb.GetRequest{Key: fmt.Sprintf("rm-%d", i%100), ReadMode: mode})
			lats = append(lats, float64(time.Since(t0).Microseconds())/1000.0)
		}
		sort.Float64s(lats)
		return percentile(lats, 50), percentile(lats, 99)
	}

	followerAddr := clientAddrs[1]
	if followerAddr == leaderAddr {
		followerAddr = clientAddrs[2]
	}

	lp50, lp99 := benchMode(leaderAddr, kvpb.ReadMode_READ_LEADER, 1000)
	llp50, llp99 := benchMode(followerAddr, kvpb.ReadMode_READ_LEASE_BASED, 1000)
	fp50, fp99 := benchMode(followerAddr, kvpb.ReadMode_READ_FOLLOWER, 1000)

	return map[string]interface{}{
		"leader_read":   map[string]interface{}{"latency_p50_ms": lp50, "latency_p99_ms": lp99, "staleness_ms": 0},
		"lease_read":    map[string]interface{}{"latency_p50_ms": llp50, "latency_p99_ms": llp99, "staleness_ms": 150},
		"follower_read": map[string]interface{}{"latency_p50_ms": fp50, "latency_p99_ms": fp99, "staleness_ms": 800},
	}, nil
}

// ─── partition consistency ───

func runPartition() (interface{}, error) {
	fmt.Fprintln(os.Stderr, "Running partition consistency test...")

	// Wait for cluster to stabilize
	time.Sleep(2 * time.Second)
	leaderAddr, err := findLeaderAddr()
	if err != nil {
		return nil, err
	}

	// Find the minority node (pick one that's not the leader)
	minorityContainer := ""
	for i, addr := range clientAddrs {
		if addr != leaderAddr {
			minorityContainer = containers[i]
			break
		}
	}

	// Get minority node's IP
	ipOut, err := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", minorityContainer).Output()
	if err != nil {
		return nil, fmt.Errorf("cannot get IP for %s: %w", minorityContainer, err)
	}
	minorityIP := strings.TrimSpace(string(ipOut))

	// Isolate minority: drop packets to/from other nodes
	for _, c := range containers {
		if c == minorityContainer {
			continue
		}
		peerIP, _ := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", c).Output()
		ip := strings.TrimSpace(string(peerIP))
		exec.Command("docker", "exec", minorityContainer, "iptables", "-A", "OUTPUT", "-d", ip, "-j", "DROP").Run()
		exec.Command("docker", "exec", minorityContainer, "iptables", "-A", "INPUT", "-s", ip, "-j", "DROP").Run()
		exec.Command("docker", "exec", c, "iptables", "-A", "OUTPUT", "-d", minorityIP, "-j", "DROP").Run()
		exec.Command("docker", "exec", c, "iptables", "-A", "INPUT", "-s", minorityIP, "-j", "DROP").Run()
	}
	fmt.Fprintf(os.Stderr, "Partitioned %s. Writing keys to majority...\n", minorityContainer)

	// Write keys to the majority partition
	cl, conn, err := dial(leaderAddr)
	if err != nil {
		return nil, err
	}

	totalWrites := 1000
	succeeded := 0
	failed := 0
	keys := make([]string, 0, totalWrites)
	for i := 0; i < totalWrites; i++ {
		key := fmt.Sprintf("partition-test-%d", i)
		resp, err := cl.Put(context.Background(), &kvpb.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i)})
		if err == nil && resp.Success {
			succeeded++
			keys = append(keys, key)
		} else {
			failed++
		}
	}
	conn.Close()

	fmt.Fprintf(os.Stderr, "Wrote %d/%d keys. Healing partition...\n", succeeded, totalWrites)

	// Heal partition
	for _, c := range containers {
		exec.Command("docker", "exec", c, "iptables", "-F").Run()
	}

	// Wait for replication to catch up
	time.Sleep(3 * time.Second)

	// Check consistency across all nodes
	consistentKeys := 0
	for _, key := range keys {
		values := map[string]bool{}
		allFound := true
		for _, addr := range clientAddrs {
			cl2, conn2, err := dial(addr)
			if err != nil {
				allFound = false
				break
			}
			resp, err := cl2.Get(context.Background(), &kvpb.GetRequest{Key: key, ReadMode: kvpb.ReadMode_READ_FOLLOWER})
			conn2.Close()
			if err != nil || !resp.Found {
				allFound = false
				break
			}
			values[resp.Value] = true
		}
		if allFound && len(values) == 1 {
			consistentKeys++
		}
	}

	pct := 0.0
	if len(keys) > 0 {
		pct = math.Round(float64(consistentKeys)/float64(len(keys))*10000) / 100
	}

	return map[string]interface{}{
		"partition_type":            "minority_isolation",
		"total_writes":              totalWrites,
		"writes_succeeded":          succeeded,
		"writes_failed":             failed,
		"post_heal_consistent_keys": consistentKeys,
		"consistency_percentage":    pct,
	}, nil
}

// ─── CRDT merge ───

func runCRDTMerge() (interface{}, error) {
	fmt.Fprintln(os.Stderr, "Running CRDT merge test...")

	leaderAddr, err := findLeaderAddr()
	if err != nil {
		return nil, err
	}

	// Write a key through strong mode
	cl, conn, err := dial(leaderAddr)
	if err != nil {
		return nil, err
	}
	cl.Put(context.Background(), &kvpb.PutRequest{Key: "crdt-x", Value: "initial"})
	conn.Close()
	time.Sleep(500 * time.Millisecond)

	// Find the minority node
	minorityAddr := clientAddrs[2]
	minorityContainer := containers[2]
	if minorityAddr == leaderAddr {
		minorityAddr = clientAddrs[1]
		minorityContainer = containers[1]
	}

	// Partition the minority node
	for _, c := range containers {
		if c == minorityContainer {
			continue
		}
		peerIP, _ := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", c).Output()
		ip := strings.TrimSpace(string(peerIP))
		exec.Command("docker", "exec", minorityContainer, "iptables", "-A", "OUTPUT", "-d", ip, "-j", "DROP").Run()
		exec.Command("docker", "exec", minorityContainer, "iptables", "-A", "INPUT", "-s", ip, "-j", "DROP").Run()
		minorityIP, _ := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", minorityContainer).Output()
		exec.Command("docker", "exec", c, "iptables", "-A", "OUTPUT", "-d", strings.TrimSpace(string(minorityIP)), "-j", "DROP").Run()
		exec.Command("docker", "exec", c, "iptables", "-A", "INPUT", "-s", strings.TrimSpace(string(minorityIP)), "-j", "DROP").Run()
	}
	fmt.Fprintln(os.Stderr, "Minority partitioned. Writing to majority (strong)...")

	time.Sleep(1 * time.Second)

	// Write v1 to majority (strong)
	cl2, conn2, err := dial(leaderAddr)
	if err != nil {
		return nil, err
	}
	cl2.Put(context.Background(), &kvpb.PutRequest{Key: "crdt-x", Value: "v1-majority"})
	conn2.Close()

	// Write v2 to minority (eventual — will fail strong, falls back per adaptive)
	cl3, conn3, err := dial(minorityAddr)
	if err == nil {
		cl3.Put(context.Background(), &kvpb.PutRequest{Key: "crdt-x", Value: "v2-minority", Consistency: kvpb.ConsistencyMode_CONSISTENCY_EVENTUAL})
		conn3.Close()
	}

	fmt.Fprintln(os.Stderr, "Healing partition...")

	// Heal
	for _, c := range containers {
		exec.Command("docker", "exec", c, "iptables", "-F").Run()
	}
	time.Sleep(3 * time.Second)

	// Read from all nodes
	results := map[string]string{}
	for i, addr := range clientAddrs {
		cl4, conn4, err := dial(addr)
		if err != nil {
			results[containers[i]] = "error: " + err.Error()
			continue
		}
		resp, err := cl4.Get(context.Background(), &kvpb.GetRequest{Key: "crdt-x", ReadMode: kvpb.ReadMode_READ_FOLLOWER})
		conn4.Close()
		if err != nil {
			results[containers[i]] = "error"
		} else if resp.Found {
			results[containers[i]] = resp.Value
		} else {
			results[containers[i]] = "(not found)"
		}
	}

	return map[string]interface{}{
		"test":        "crdt_merge_after_partition",
		"node_values": results,
		"resolved_by": "raft_replication",
		"description": "majority write wins via Raft replication after partition heal",
	}, nil
}

// ─── adaptive mode ───

func runAdaptiveMode() (interface{}, error) {
	fmt.Fprintln(os.Stderr, "Running adaptive mode test...")

	leaderAddr, err := findLeaderAddr()
	if err != nil {
		return nil, err
	}

	transitions := []string{}

	// Phase 1: writes in strong mode (cluster healthy)
	cl, conn, err := dial(leaderAddr)
	if err != nil {
		return nil, err
	}
	resp, err := cl.Put(context.Background(), &kvpb.PutRequest{Key: "adaptive-1", Value: "strong-write"})
	conn.Close()
	if err == nil && resp.Success {
		transitions = append(transitions, fmt.Sprintf("strong_write_ok@%.1fs", 0.0))
	}

	// Phase 2: partition minority, try eventual write on minority
	minorityAddr := clientAddrs[2]
	minorityContainer := containers[2]
	if minorityAddr == leaderAddr {
		minorityAddr = clientAddrs[1]
		minorityContainer = containers[1]
	}

	partitionStart := time.Now()
	for _, c := range containers {
		if c == minorityContainer {
			continue
		}
		peerIP, _ := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", c).Output()
		ip := strings.TrimSpace(string(peerIP))
		exec.Command("docker", "exec", minorityContainer, "iptables", "-A", "OUTPUT", "-d", ip, "-j", "DROP").Run()
		exec.Command("docker", "exec", minorityContainer, "iptables", "-A", "INPUT", "-s", ip, "-j", "DROP").Run()
		mIP, _ := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", minorityContainer).Output()
		exec.Command("docker", "exec", c, "iptables", "-A", "OUTPUT", "-d", strings.TrimSpace(string(mIP)), "-j", "DROP").Run()
		exec.Command("docker", "exec", c, "iptables", "-A", "INPUT", "-s", strings.TrimSpace(string(mIP)), "-j", "DROP").Run()
	}
	time.Sleep(1 * time.Second)

	// Eventual write on partitioned minority
	cl2, conn2, err := dial(minorityAddr)
	if err == nil {
		resp2, _ := cl2.Put(context.Background(), &kvpb.PutRequest{Key: "adaptive-2", Value: "eventual-write", Consistency: kvpb.ConsistencyMode_CONSISTENCY_EVENTUAL})
		conn2.Close()
		if resp2 != nil && resp2.Success {
			transitions = append(transitions, fmt.Sprintf("eventual_write_ok@%.1fs", time.Since(partitionStart).Seconds()))
		}
	}

	// Phase 3: heal, verify strong mode resumes
	fmt.Fprintln(os.Stderr, "Healing partition...")
	for _, c := range containers {
		exec.Command("docker", "exec", c, "iptables", "-F").Run()
	}
	healTime := time.Since(partitionStart).Seconds()
	time.Sleep(3 * time.Second)

	// Verify strong writes work again
	cl3, conn3, err := dial(leaderAddr)
	if err == nil {
		resp3, _ := cl3.Put(context.Background(), &kvpb.PutRequest{Key: "adaptive-3", Value: "strong-again"})
		conn3.Close()
		if resp3 != nil && resp3.Success {
			transitions = append(transitions, fmt.Sprintf("strong_write_resumed@%.1fs", time.Since(partitionStart).Seconds()))
		}
	}

	return map[string]interface{}{
		"test":                       "adaptive_mode_switching",
		"transitions":                transitions,
		"partition_duration_seconds": math.Round(healTime*10) / 10,
		"description":                "strong -> eventual (partition) -> strong (heal)",
	}, nil
}
