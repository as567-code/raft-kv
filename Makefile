.PHONY: build proto docker-build up down logs test-unit test-integration test-all \
       bench-throughput bench-failover bench-read-modes \
       chaos-partition chaos-crash chaos-latency \
       test-crdt-merge test-adaptive-mode validate-all clean

BINARY_SERVER=raft-kv-server
BINARY_CLIENT=raft-kv-client

# Build
build:
	go build -o bin/$(BINARY_SERVER) ./cmd/server
	go build -o bin/$(BINARY_CLIENT) ./cmd/client

proto:
	protoc --go_out=proto/raftpb --go_opt=paths=source_relative \
		--go-grpc_out=proto/raftpb --go-grpc_opt=paths=source_relative \
		-I proto proto/raft.proto
	protoc --go_out=proto/kvpb --go_opt=paths=source_relative \
		--go-grpc_out=proto/kvpb --go-grpc_opt=paths=source_relative \
		-I proto proto/kv.proto

docker-build:
	docker build -t raft-kv -f deploy/Dockerfile .

# Run
up:
	cd deploy && docker-compose up -d --build

down:
	cd deploy && docker-compose down -v

logs:
	cd deploy && docker-compose logs -f

# Test
test-unit:
	go test -v -race -count=1 ./raft/... ./store/... ./crdt/...

test-integration:
	cd deploy && docker-compose up -d --build
	sleep 5
	go test -v -tags=integration -count=1 ./bench/...
	cd deploy && docker-compose down -v

test-all: test-unit test-integration

# Benchmarks (produce JSON in bench/results/)
bench-throughput:
	go run ./cmd/bench -test=throughput -output=bench/results/throughput.json

bench-failover:
	go run ./cmd/bench -test=failover -output=bench/results/failover.json

bench-read-modes:
	go run ./cmd/bench -test=read-modes -output=bench/results/read_modes.json

# Chaos testing
chaos-partition:
	go run ./cmd/bench -test=partition -output=bench/results/partition_consistency.json

chaos-crash:
	go run ./cmd/bench -test=failover -output=bench/results/failover.json

chaos-latency:
	go run ./cmd/bench -test=throughput -output=bench/results/throughput.json

# Stage 3 tests
test-crdt-merge:
	go run ./cmd/bench -test=crdt-merge -output=bench/results/crdt_merge.json

test-adaptive-mode:
	go run ./cmd/bench -test=adaptive-mode -output=bench/results/adaptive_mode.json

# Full validation
validate-all: test-all bench-throughput bench-failover chaos-partition
	@echo "=== Full validation complete ==="
	@echo "Results in bench/results/"
	@ls -la bench/results/

clean:
	rm -rf bin/
	rm -rf /tmp/raft-kv*
	cd deploy && docker-compose down -v 2>/dev/null || true
