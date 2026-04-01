package crdt

import (
	"encoding/json"
	"sync"
)

// LWWRegister implements a Last-Writer-Wins Register CRDT.
// Each key maps to a value with a timestamp; on merge, the highest timestamp wins.
type LWWRegister struct {
	mu      sync.RWMutex
	entries map[string]*LWWEntry
	clock   *HLC
}

// LWWEntry is a timestamped value for a key.
type LWWEntry struct {
	Value     string    `json:"value"`
	Timestamp Timestamp `json:"timestamp"`
	Deleted   bool      `json:"deleted"`
}

// LWWState is the serializable state of the entire register.
type LWWState struct {
	Entries map[string]*LWWEntry `json:"entries"`
}

func NewLWWRegister(clock *HLC) *LWWRegister {
	return &LWWRegister{
		entries: make(map[string]*LWWEntry),
		clock:   clock,
	}
}

// Set sets a key to a value with a new timestamp.
func (r *LWWRegister) Set(key, value string) Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()

	ts := r.clock.Now()
	r.entries[key] = &LWWEntry{
		Value:     value,
		Timestamp: ts,
		Deleted:   false,
	}
	return ts
}

// Get returns the value and whether the key exists and is not deleted.
func (r *LWWRegister) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.entries[key]
	if !ok || entry.Deleted {
		return "", false
	}
	return entry.Value, true
}

// Delete marks a key as deleted with a new timestamp.
func (r *LWWRegister) Delete(key string) Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()

	ts := r.clock.Now()
	r.entries[key] = &LWWEntry{
		Value:     "",
		Timestamp: ts,
		Deleted:   true,
	}
	return ts
}

// Merge merges a remote entry for a key. The entry with the higher timestamp wins.
func (r *LWWRegister) Merge(key string, remote *LWWEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	local, ok := r.entries[key]
	if !ok || remote.Timestamp.After(local.Timestamp) {
		r.entries[key] = remote
		r.clock.Update(remote.Timestamp)
	}
}

// MergeState merges an entire remote state into this register.
func (r *LWWRegister) MergeState(remote *LWWState) {
	if remote == nil {
		return
	}
	for key, entry := range remote.Entries {
		r.Merge(key, entry)
	}
}

// State returns the full serializable state of the register.
func (r *LWWRegister) State() *LWWState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries := make(map[string]*LWWEntry, len(r.entries))
	for k, v := range r.entries {
		e := *v
		entries[k] = &e
	}
	return &LWWState{Entries: entries}
}

// Keys returns all non-deleted keys.
func (r *LWWRegister) Keys() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var keys []string
	for k, v := range r.entries {
		if !v.Deleted {
			keys = append(keys, k)
		}
	}
	return keys
}

// MarshalState serializes the state to JSON.
func (r *LWWRegister) MarshalState() ([]byte, error) {
	return json.Marshal(r.State())
}

// UnmarshalAndMerge deserializes remote state and merges it.
func (r *LWWRegister) UnmarshalAndMerge(data []byte) error {
	var state LWWState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	r.MergeState(&state)
	return nil
}
