package store

import (
	"encoding/json"
	"fmt"
)

// CommandType identifies the type of operation in a log entry.
type CommandType string

const (
	CmdPut    CommandType = "put"
	CmdDelete CommandType = "delete"
)

// Command represents a state machine command serialized in log entries.
type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value,omitempty"`
}

// FSM is a finite state machine backed by a KVStore.
// It implements the raft.FSM interface.
type FSM struct {
	store *KVStore
}

func NewFSM(store *KVStore) *FSM {
	return &FSM{store: store}
}

// Apply applies a committed log entry to the state machine.
func (f *FSM) Apply(data []byte) error {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CmdPut:
		f.store.Put(cmd.Key, cmd.Value)
	case CmdDelete:
		f.store.Delete(cmd.Key)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
	return nil
}

// Snapshot returns a serialized snapshot of the state machine.
func (f *FSM) Snapshot() ([]byte, error) {
	return f.store.Snapshot()
}

// Restore replaces the state machine from a snapshot.
func (f *FSM) Restore(data []byte) error {
	return f.store.Restore(data)
}

// Store returns the underlying KVStore for direct reads.
func (f *FSM) Store() *KVStore {
	return f.store
}

// EncodeCommand serializes a command for inclusion in a log entry.
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}
