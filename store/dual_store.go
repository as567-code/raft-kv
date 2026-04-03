package store

import (
	"sync"

	"github.com/as567-code/raft-kv/crdt"
)

// ConsistencyMode determines how writes are handled.
type ConsistencyMode int

const (
	Strong   ConsistencyMode = iota // All writes go through Raft
	Eventual                        // Writes go to local CRDT store
	Adaptive                        // Auto-switches based on quorum availability
)

func (m ConsistencyMode) String() string {
	switch m {
	case Strong:
		return "strong"
	case Eventual:
		return "eventual"
	case Adaptive:
		return "adaptive"
	default:
		return "unknown"
	}
}

// DualStore wraps both the Raft-backed FSM store and the CRDT layer,
// supporting dual consistency modes.
type DualStore struct {
	mu sync.RWMutex

	// Strong consistency (Raft-backed)
	fsm *FSM

	// Eventual consistency (CRDT-backed)
	lwwRegister *crdt.LWWRegister
	gCounters   map[string]*crdt.GCounter
	antiEntropy *crdt.AntiEntropy
	clock       *crdt.HLC

	// Current mode
	mode   ConsistencyMode
	nodeID string

	// For adaptive mode: tracks whether quorum is available
	quorumAvailable bool
}

func NewDualStore(nodeID string, fsm *FSM) *DualStore {
	clock := crdt.NewHLC(nodeID)
	lww := crdt.NewLWWRegister(clock)
	return &DualStore{
		fsm:             fsm,
		lwwRegister:     lww,
		gCounters:       make(map[string]*crdt.GCounter),
		antiEntropy:     crdt.NewAntiEntropy(lww),
		clock:           clock,
		mode:            Strong,
		nodeID:          nodeID,
		quorumAvailable: true,
	}
}

// SetMode changes the consistency mode.
func (ds *DualStore) SetMode(mode ConsistencyMode) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.mode = mode
}

// Mode returns the current consistency mode.
func (ds *DualStore) Mode() ConsistencyMode {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.mode
}

// EffectiveMode returns the actual mode in use (resolves adaptive).
func (ds *DualStore) EffectiveMode() ConsistencyMode {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if ds.mode == Adaptive {
		if ds.quorumAvailable {
			return Strong
		}
		return Eventual
	}
	return ds.mode
}

// SetQuorumAvailable updates quorum status (for adaptive mode).
func (ds *DualStore) SetQuorumAvailable(available bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.quorumAvailable = available
}

// Get reads a key. In strong mode, reads from FSM. In eventual mode,
// reads from CRDT register.
func (ds *DualStore) Get(key string) (string, bool) {
	mode := ds.EffectiveMode()

	switch mode {
	case Strong:
		return ds.fsm.Store().Get(key)
	case Eventual:
		// Check CRDT first, fall back to FSM
		if val, ok := ds.lwwRegister.Get(key); ok {
			return val, true
		}
		return ds.fsm.Store().Get(key)
	default:
		return ds.fsm.Store().Get(key)
	}
}

// PutEventual writes to the CRDT register (for eventual consistency mode).
func (ds *DualStore) PutEventual(key, value string) {
	ds.lwwRegister.Set(key, value)
}

// DeleteEventual marks a key as deleted in the CRDT register.
func (ds *DualStore) DeleteEventual(key string) {
	ds.lwwRegister.Delete(key)
}

// GetCounter returns or creates a G-Counter for the given name.
func (ds *DualStore) GetCounter(name string) *crdt.GCounter {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	gc, ok := ds.gCounters[name]
	if !ok {
		gc = crdt.NewGCounter(ds.nodeID)
		ds.gCounters[name] = gc
	}
	return gc
}

// CRDTState returns the CRDT sync state for anti-entropy exchange.
func (ds *DualStore) CRDTState() *crdt.SyncState {
	return ds.antiEntropy.PrepareSync()
}

// MergeCRDTState merges remote CRDT state.
func (ds *DualStore) MergeCRDTState(remote *crdt.SyncState) int {
	return ds.antiEntropy.ApplySync(remote)
}

// ReconcileToFSM pushes CRDT values into the FSM store after partition heal.
// This is called when transitioning from eventual back to strong mode.
func (ds *DualStore) ReconcileToFSM() []Command {
	var cmds []Command
	for _, key := range ds.lwwRegister.Keys() {
		val, ok := ds.lwwRegister.Get(key)
		if ok {
			// Check if FSM already has this value
			fsmVal, fsmOk := ds.fsm.Store().Get(key)
			if !fsmOk || fsmVal != val {
				cmds = append(cmds, Command{
					Type:  CmdPut,
					Key:   key,
					Value: val,
				})
			}
		}
	}
	return cmds
}

// FSM returns the underlying FSM.
func (ds *DualStore) FSM() *FSM {
	return ds.fsm
}

// LWWRegister returns the CRDT register (for testing/inspection).
func (ds *DualStore) LWWRegister() *crdt.LWWRegister {
	return ds.lwwRegister
}
