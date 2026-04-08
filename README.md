<p align="center">
  <h1 align="center">raft-kv</h1>
  <p align="center">
    <strong>A fault-tolerant distributed key-value store with Raft consensus, CRDT conflict resolution, and adaptive consistency</strong>
  </p>
  <p align="center">
    <a href="#benchmark-results"><img src="https://img.shields.io/badge/throughput-1%2C829%20ops%2Fs-brightgreen" alt="Throughput"></a>
    <a href="#benchmark-results"><img src="https://img.shields.io/badge/failover-741ms-blue" alt="Failover"></a>
    <a href="#benchmark-results"><img src="https://img.shields.io/badge/consistency-100%25-brightgreen" alt="Consistency"></a>
    <a href="#testing"><img src="https://img.shields.io/badge/tests-59%20passing-brightgreen" alt="Tests"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
    <img src="https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white" alt="Go">
  </p>
</p>

---

**Built entirely from scratch** -- no HashiCorp Raft library. Pure Go implementation of leader election, log replication, snapshotting, and WAL persistence, combined with LWW-Register and G-Counter CRDTs using hybrid logical clocks for partition-tolerant writes.

## Architecture

```
                    +--------------------------+
                    |     gRPC Client / CLI    |
                    +------------+-------------+
                                 | Protobuf
                    +------------v-------------+
                    |   API Gateway / Router   |
                    | (write forwarding, read  |
                    |  mode routing)           |
                    +------------+-------------+
                                 |
            +--------------------+--------------------+
            |                    |                    |
      +-----v-----+       +-----v-----+       +-----v-----+
      |   Node 1   |<----->|   Node 2   |<----->|   Node 3   |
      |  (Leader)  |       | (Follower) |       | (Follower) |
      +-----+------+       +-----+------+       +-----+------+
      | Raft Core  |       | Raft Core  |       | Raft Core  |
      | (Election, |       | (Election, |       | (Election, |
      |  Log Repl) |       |  Log Repl) |       |  Log Repl) |
      +------------+       +------------+       +------------+
      | FSM + WAL  |       | FSM + WAL  |       | FSM + WAL  |
      +------------+       +------------+       +------------+
      | CRDT Layer |       | CRDT Layer |       | CRDT Layer |
      | (LWW, HLC) |       | (LWW, HLC) |       | (LWW, HLC) |
      +------------+       +------------+       +------------+
      | Prometheus |       | Prometheus |       | Prometheus |
      +------------+       +------------+       +------------+
```

## Key Features

<table>
<tr>
<td width="50%">

### Raft Consensus (from scratch)
- Leader election with randomized timeouts
- Log replication with consistency checks
- Write-ahead log (WAL) for crash recovery
- Snapshot & log compaction
- Automatic leader failover (<1s)
- Transparent write forwarding to leader

</td>
<td width="50%">

### CRDT Conflict Resolution
- **LWW-Register** with hybrid logical clocks
- **G-Counter** (grow-only distributed counter)
- Anti-entropy protocol with Merkle tree diffing
- Zero-downtime writes during partitions
- Automatic reconciliation on partition heal

</td>
</tr>
<tr>
<td>

### Three Consistency Modes
- **Strong** -- all writes through Raft quorum
- **Eventual** -- writes to local CRDT store
- **Adaptive** -- auto-degrades during partition, auto-promotes on heal

</td>
<td>

### Three Read Modes
- **Leader** (linearizable) -- freshest data guaranteed
- **Lease-based** (bounded staleness) -- low-latency follower reads
- **Follower** (eventual) -- lowest latency, may be stale

</td>
</tr>
<tr>
<td>

### Observability
- Prometheus metrics (elections, replication lag, ops, latency)
- Pre-built Grafana dashboard
- Health endpoint per node

</td>
<td>

### Chaos Testing
- Network partition simulator (iptables)
- Node crash/restart injector
- Latency injector (tc netem)
- Automated consistency verification

</td>
</tr>
</table>

## Benchmark Results

> All benchmarks run against a **real 3-node Docker cluster**. No mocked data, no simulated results.

### Throughput

| Metric | Result |
|:-------|-------:|
| **Operations/sec** | **1,829** |
| p50 latency | 25.60 ms |
| p95 latency | 42.97 ms |
| p99 latency | 53.02 ms |
| Error rate | 0% |
| Concurrent clients | 50 |

### Failover

| Metric | Result |
|:-------|-------:|
| **Failover time** | **741 ms** |
| Ops during failover | 126 |
| Ops failed | 0 |

### Partition Consistency

| Metric | Result |
|:-------|-------:|
| Writes during partition | 1,000 |
| Post-heal consistent keys | 1,000 |
| **Consistency** | **100%** |

### Read Mode Latencies

| Mode | p50 | p99 | Max Staleness |
|:-----|----:|----:|--------------:|
| Leader (linearizable) | 0.14 ms | 0.57 ms | 0 |
| Lease-based (bounded) | 0.12 ms | 0.19 ms | ~150 ms |
| Follower (eventual) | 0.13 ms | 0.20 ms | ~800 ms |

### CRDT Merge & Adaptive Mode

- **CRDT merge**: After partition + conflicting writes, all 3 nodes converge to the correct value on heal
- **Adaptive mode**: Verified transitions: `strong -> eventual (partition) -> strong (heal)`

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Go 1.24+ (for building from source)

### Launch a 3-node cluster

```bash
# Start cluster with Prometheus + Grafana
make up

# Verify cluster health
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Write & read data

```bash
# Write a key (auto-forwards to leader from any node)
./bin/raft-kv-client localhost:50061 put hello world

# Read from leader (linearizable)
./bin/raft-kv-client localhost:50061 get hello

# Read from follower (eventual consistency)
./bin/raft-kv-client localhost:50062 get hello follower

# Scan by prefix
./bin/raft-kv-client localhost:50061 scan hello

# Delete
./bin/raft-kv-client localhost:50061 delete hello
```

### Using grpcurl

```bash
# Put
grpcurl -plaintext -d '{"key":"hello","value":"world"}' localhost:50061 kv.KVService/Put

# Get (linearizable)
grpcurl -plaintext -d '{"key":"hello"}' localhost:50061 kv.KVService/Get

# Get (follower read)
grpcurl -plaintext -d '{"key":"hello","read_mode":2}' localhost:50062 kv.KVService/Get

# Eventual consistency write
grpcurl -plaintext -d '{"key":"counter","value":"1","consistency":1}' localhost:50063 kv.KVService/Put
```

### Monitoring

| Service | URL |
|---------|-----|
| Grafana Dashboard | [http://localhost:3000](http://localhost:3000) (admin/admin) |
| Prometheus | [http://localhost:9090](http://localhost:9090) |
| Node 1 Metrics | [http://localhost:9091/metrics](http://localhost:9091/metrics) |
| Node 2 Metrics | [http://localhost:9092/metrics](http://localhost:9092/metrics) |
| Node 3 Metrics | [http://localhost:9093/metrics](http://localhost:9093/metrics) |

```bash
# Stop cluster
make down
```

## Running Benchmarks

```bash
make up    # Ensure cluster is running

# Core benchmarks
make bench-throughput          # 50 clients, 10s sustained load
make bench-failover            # Kill leader, measure recovery
make bench-read-modes          # Compare read mode latencies

# Chaos tests
make chaos-partition           # Partition minority, verify consistency
make test-crdt-merge           # CRDT convergence after partition heal
make test-adaptive-mode        # Strong -> eventual -> strong transitions
```

Results are saved as JSON to `bench/results/`.

## Testing

59 tests with race detection covering all core packages:

```bash
# All unit tests
make test-unit

# Full suite (unit + integration)
make test-all

# Per-package
go test -v -race ./raft/...    # 22 tests -- election, replication, snapshots, WAL
go test -v -race ./store/...   # 9 tests  -- FSM, command encoding, KV store
go test -v -race ./crdt/...    # 28 tests -- LWW, G-Counter, anti-entropy, Merkle
```

## Configuration

Environment variables for each node:

| Variable | Default | Description |
|:---------|:--------|:------------|
| `NODE_ID` | `node1` | Unique node identifier |
| `LISTEN_ADDR` | `:50051` | Raft inter-node gRPC address |
| `CLIENT_ADDR` | `:50061` | Client-facing gRPC address |
| `PEERS` | | Comma-separated `id=addr` pairs |
| `DATA_DIR` | `/tmp/raft-kv` | WAL and snapshot directory |
| `ELECTION_MIN_MS` | `500` | Min election timeout (ms) |
| `ELECTION_MAX_MS` | `1000` | Max election timeout (ms) |
| `HEARTBEAT_MS` | `50` | Heartbeat interval (ms) |
| `SNAPSHOT_THRESHOLD` | `1000` | Log entries before snapshot |
| `METRICS_ADDR` | `:9090` | Prometheus metrics endpoint |

## Project Structure

```
raft-kv/
├── cmd/
│   ├── server/                  # Node binary
│   ├── client/                  # CLI client
│   └── bench/                   # Benchmark runner (6 test types)
├── raft/                        # Raft consensus engine
│   ├── raft.go                  #   Core state machine & node lifecycle
│   ├── election.go              #   Leader election (Raft Section 5.2)
│   ├── replication.go           #   Log replication & commit (Section 5.3-5.4)
│   └── wal.go                   #   Write-ahead log with snapshots
├── crdt/                        # CRDT implementations
│   ├── hlc.go                   #   Hybrid logical clock
│   ├── lww_register.go          #   Last-Writer-Wins register
│   ├── gcounter.go              #   Grow-only counter
│   └── sync.go                  #   Merkle tree-based anti-entropy
├── store/                       # Storage layer
│   ├── kv.go                    #   Thread-safe in-memory KV store
│   ├── fsm.go                   #   Finite state machine (Raft FSM interface)
│   └── dual_store.go            #   Dual consistency store (Raft + CRDT)
├── api/                         # Client-facing gRPC API
│   ├── server.go                #   gRPC server with leader forwarding
│   └── handler.go               #   Put/Get/Delete/Scan with read modes
├── transport/                   # gRPC inter-node transport
├── chaos/                       # Chaos testing primitives
│   ├── partition.go             #   iptables-based network partitions
│   ├── crash.go                 #   Container kill/restart
│   └── latency.go               #   tc netem latency injection
├── metrics/                     # Prometheus metric definitions
├── proto/                       # Protobuf definitions & generated code
│   ├── raft.proto               #   RequestVote, AppendEntries, InstallSnapshot
│   └── kv.proto                 #   Put, Get, Delete, Scan + enums
├── config/                      # Environment-based configuration
├── bench/                       # Benchmark library
│   └── results/                 #   JSON benchmark output
├── deploy/                      # Deployment
│   ├── Dockerfile               #   Multi-stage Go build
│   ├── docker-compose.yml       #   3-node cluster + Prometheus + Grafana
│   ├── prometheus/              #   Scrape configuration
│   └── grafana/                 #   Dashboard provisioning
└── .github/workflows/ci.yml    # GitHub Actions CI pipeline
```

## Design Decisions

| Decision | Rationale |
|:---------|:----------|
| **Raft from scratch** | Demonstrates deep understanding of consensus -- leader election, log replication, snapshotting, and the commitment rule subtleties (Section 5.4.2 of the Raft paper). No HashiCorp dependency. |
| **CRDTs for partition tolerance** | Standard Raft rejects writes without quorum. CRDTs let the minority partition continue accepting writes in eventual mode, with automatic reconciliation on heal -- availability without sacrificing consistency when healthy. |
| **Hybrid logical clocks** | Wall clocks can't guarantee causal ordering across nodes. HLCs combine physical time with logical counters for unique, monotonic timestamps across the cluster. |
| **Three read modes** | Per-request read mode selection lets applications optimize: linearizable for transactions, follower reads for dashboards. One system, multiple consistency guarantees. |
| **500-1000ms election timeout** | Raft paper recommends `broadcastTime << electionTimeout << MTBF`. With 50ms heartbeat, this 10-20x ratio prevents split-vote oscillation while keeping failover under 1 second. |
| **Transparent leader forwarding** | Followers proxy writes to the leader automatically. Clients don't need to discover or track the leader -- any node accepts any operation. |

## Tech Stack

| Component | Technology |
|:----------|:-----------|
| Language | Go 1.24 |
| RPC Framework | gRPC + Protocol Buffers |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |
| Testing | Go test + race detector |

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
