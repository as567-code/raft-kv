package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ElectionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "raft_elections_total",
		Help: "Total number of leader elections initiated",
	})

	ElectionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "raft_election_duration_seconds",
		Help:    "Time taken for leader election",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})

	LogReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_log_replication_lag",
		Help: "Replication lag per follower (entries behind leader)",
	}, []string{"follower"})

	CommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_commit_index",
		Help: "Current commit index",
	})

	KVOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_operations_total",
		Help: "Total KV operations",
	}, []string{"op_type", "status"})

	KVOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_operation_duration_seconds",
		Help:    "Duration of KV operations",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15),
	}, []string{"op_type"})

	KVThroughput = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kv_throughput_ops_per_second",
		Help: "Current throughput in operations per second",
	})
)
