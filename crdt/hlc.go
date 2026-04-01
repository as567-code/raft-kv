package crdt

import (
	"sync"
	"time"
)

// HLC implements a Hybrid Logical Clock for CRDT timestamp ordering.
// Combines wall-clock time with a logical counter to ensure unique, monotonic timestamps.
type HLC struct {
	mu      sync.Mutex
	wallMs  int64 // wall clock in milliseconds
	logical uint32
	nodeID  string
}

// Timestamp represents a hybrid logical clock timestamp.
type Timestamp struct {
	WallMs  int64  `json:"wall_ms"`
	Logical uint32 `json:"logical"`
	NodeID  string `json:"node_id"`
}

func NewHLC(nodeID string) *HLC {
	return &HLC{
		wallMs: time.Now().UnixMilli(),
		nodeID: nodeID,
	}
}

// Now returns a new timestamp that is guaranteed to be greater than any
// previously returned timestamp.
func (h *HLC) Now() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	physMs := time.Now().UnixMilli()
	if physMs > h.wallMs {
		h.wallMs = physMs
		h.logical = 0
	} else {
		h.logical++
	}

	return Timestamp{
		WallMs:  h.wallMs,
		Logical: h.logical,
		NodeID:  h.nodeID,
	}
}

// Update merges a received timestamp with the local clock.
func (h *HLC) Update(remote Timestamp) Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	physMs := time.Now().UnixMilli()

	if physMs > h.wallMs && physMs > remote.WallMs {
		h.wallMs = physMs
		h.logical = 0
	} else if remote.WallMs > h.wallMs {
		h.wallMs = remote.WallMs
		h.logical = remote.Logical + 1
	} else if h.wallMs > remote.WallMs {
		h.logical++
	} else {
		// Equal wall times
		if remote.Logical > h.logical {
			h.logical = remote.Logical + 1
		} else {
			h.logical++
		}
	}

	return Timestamp{
		WallMs:  h.wallMs,
		Logical: h.logical,
		NodeID:  h.nodeID,
	}
}

// After reports whether ts is strictly after other.
func (ts Timestamp) After(other Timestamp) bool {
	if ts.WallMs != other.WallMs {
		return ts.WallMs > other.WallMs
	}
	if ts.Logical != other.Logical {
		return ts.Logical > other.Logical
	}
	return ts.NodeID > other.NodeID // deterministic tiebreaker
}
