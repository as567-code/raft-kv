package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/as567-code/raft-kv/proto/raftpb"
)

// WAL provides persistent storage for Raft state.
type WAL struct {
	mu      sync.Mutex
	dir     string
	logFile *os.File
}

type persistentState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// NewWAL creates a new WAL in the given directory.
func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	logPath := filepath.Join(dir, "raft.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL log file: %w", err)
	}

	return &WAL{dir: dir, logFile: f}, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.logFile != nil {
		return w.logFile.Close()
	}
	return nil
}

// SaveState persists currentTerm and votedFor.
func (w *WAL) SaveState(term uint64, votedFor string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	state := persistentState{Term: term, VotedFor: votedFor}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(w.dir, "state.json"), data, 0o644)
}

// LoadState loads persisted term and votedFor.
func (w *WAL) LoadState() (*persistentState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := os.ReadFile(filepath.Join(w.dir, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// Append writes a log entry to the WAL.
func (w *WAL) Append(entry *pb.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(&entry)
	if err != nil {
		return err
	}

	// Length-prefixed format: [4 bytes length][data]
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := w.logFile.Write(lenBuf); err != nil {
		return err
	}
	if _, err := w.logFile.Write(data); err != nil {
		return err
	}
	return w.logFile.Sync()
}

// LoadEntries reads all log entries from the WAL after the given snapshot index.
func (w *WAL) LoadEntries(afterIndex uint64) ([]*pb.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	logPath := filepath.Join(w.dir, "raft.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var entries []*pb.LogEntry
	offset := 0
	for offset+4 <= len(data) {
		length := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if offset+int(length) > len(data) {
			break // truncated entry
		}

		entry := new(pb.LogEntry)
		if err := json.Unmarshal(data[offset:offset+int(length)], entry); err != nil {
			break // corrupted entry
		}
		offset += int(length)

		if entry.Index > afterIndex {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// SaveSnapshot persists a snapshot.
func (w *WAL) SaveSnapshot(index, term uint64, snapData []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	meta := struct {
		Index uint64 `json:"index"`
		Term  uint64 `json:"term"`
	}{Index: index, Term: term}

	metaData, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(w.dir, "snapshot.meta"), metaData, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(w.dir, "snapshot.data"), snapData, 0o644); err != nil {
		return err
	}

	// Truncate the log file since entries before snapshot are no longer needed
	w.logFile.Close()
	logPath := filepath.Join(w.dir, "raft.log")
	w.logFile, err = os.OpenFile(logPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0o644)
	return err
}

// LoadSnapshot loads a previously saved snapshot.
func (w *WAL) LoadSnapshot() (index, term uint64, data []byte, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	metaData, err := os.ReadFile(filepath.Join(w.dir, "snapshot.meta"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, nil
		}
		return 0, 0, nil, err
	}

	var meta struct {
		Index uint64 `json:"index"`
		Term  uint64 `json:"term"`
	}
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return 0, 0, nil, err
	}

	data, err = os.ReadFile(filepath.Join(w.dir, "snapshot.data"))
	if err != nil {
		return 0, 0, nil, err
	}

	return meta.Index, meta.Term, data, nil
}
