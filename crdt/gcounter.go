package crdt

import (
	"encoding/json"
	"sync"
)

// GCounter implements a Grow-only Counter CRDT.
// Each node maintains its own counter; the total is the sum of all counters.
// Merge takes the element-wise max.
type GCounter struct {
	mu       sync.RWMutex
	counters map[string]uint64 // nodeID -> count
	nodeID   string
}

// GCounterState is the serializable state.
type GCounterState struct {
	Counters map[string]uint64 `json:"counters"`
}

func NewGCounter(nodeID string) *GCounter {
	return &GCounter{
		counters: make(map[string]uint64),
		nodeID:   nodeID,
	}
}

// Increment adds 1 to this node's counter.
func (g *GCounter) Increment() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.counters[g.nodeID]++
	return g.counters[g.nodeID]
}

// IncrementBy adds n to this node's counter.
func (g *GCounter) IncrementBy(n uint64) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.counters[g.nodeID] += n
	return g.counters[g.nodeID]
}

// Value returns the total count across all nodes.
func (g *GCounter) Value() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var total uint64
	for _, v := range g.counters {
		total += v
	}
	return total
}

// LocalValue returns this node's counter value.
func (g *GCounter) LocalValue() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.counters[g.nodeID]
}

// Merge merges a remote counter state using element-wise max.
func (g *GCounter) Merge(remote *GCounterState) {
	if remote == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	for nodeID, count := range remote.Counters {
		if count > g.counters[nodeID] {
			g.counters[nodeID] = count
		}
	}
}

// State returns the serializable state.
func (g *GCounter) State() *GCounterState {
	g.mu.RLock()
	defer g.mu.RUnlock()

	counters := make(map[string]uint64, len(g.counters))
	for k, v := range g.counters {
		counters[k] = v
	}
	return &GCounterState{Counters: counters}
}

// MarshalState serializes the counter state.
func (g *GCounter) MarshalState() ([]byte, error) {
	return json.Marshal(g.State())
}

// UnmarshalAndMerge deserializes and merges remote state.
func (g *GCounter) UnmarshalAndMerge(data []byte) error {
	var state GCounterState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	g.Merge(&state)
	return nil
}
