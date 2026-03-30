package raft

import (
	"os"
	"path/filepath"
	"testing"

	pb "github.com/as567-code/raft-kv/proto/raftpb"
)

func TestWAL_PersistAndLoadState(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	if err := wal.SaveState(5, "node2"); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	state, err := wal.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	if state.Term != 5 || state.VotedFor != "node2" {
		t.Errorf("expected term=5, votedFor=node2, got term=%d, votedFor=%s",
			state.Term, state.VotedFor)
	}
}

func TestWAL_AppendAndLoadEntries(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Data: []byte("entry1")},
		{Term: 1, Index: 2, Data: []byte("entry2")},
		{Term: 2, Index: 3, Data: []byte("entry3")},
	}

	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	loaded, err := wal.LoadEntries(0)
	if err != nil {
		t.Fatalf("LoadEntries failed: %v", err)
	}

	if len(loaded) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(loaded))
	}

	for i, e := range loaded {
		if e.Index != entries[i].Index || e.Term != entries[i].Term {
			t.Errorf("entry %d mismatch: got index=%d term=%d, want index=%d term=%d",
				i, e.Index, e.Term, entries[i].Index, entries[i].Term)
		}
	}
}

func TestWAL_LoadEntriesAfterIndex(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	for i := uint64(1); i <= 5; i++ {
		wal.Append(&pb.LogEntry{Term: 1, Index: i, Data: []byte("data")})
	}

	// Load entries after index 3
	loaded, err := wal.LoadEntries(3)
	if err != nil {
		t.Fatalf("LoadEntries failed: %v", err)
	}

	if len(loaded) != 2 {
		t.Fatalf("expected 2 entries after index 3, got %d", len(loaded))
	}
	if loaded[0].Index != 4 || loaded[1].Index != 5 {
		t.Errorf("wrong entries: %v", loaded)
	}
}

func TestWAL_SaveAndLoadSnapshot(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	snapData := []byte(`{"key1":"val1","key2":"val2"}`)
	if err := wal.SaveSnapshot(10, 3, snapData); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	index, term, data, err := wal.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if index != 10 || term != 3 {
		t.Errorf("snapshot meta mismatch: index=%d term=%d", index, term)
	}
	if string(data) != string(snapData) {
		t.Errorf("snapshot data mismatch: %s", string(data))
	}
}

func TestWAL_SnapshotTruncatesLog(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append some entries
	for i := uint64(1); i <= 5; i++ {
		wal.Append(&pb.LogEntry{Term: 1, Index: i, Data: []byte("data")})
	}

	// Save snapshot — this should truncate the log
	wal.SaveSnapshot(5, 1, []byte("snapshot"))

	// Verify log file is truncated
	logPath := filepath.Join(dir, "raft.log")
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("log file should be empty after snapshot, size=%d", info.Size())
	}
}

func TestWAL_LoadStateNoFile(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	state, err := wal.LoadState()
	if err != nil {
		t.Fatalf("LoadState should not error for missing file: %v", err)
	}
	if state != nil {
		t.Error("state should be nil for fresh WAL")
	}
}

func TestWAL_LoadSnapshotNoFile(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	index, term, data, err := wal.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot should not error for missing file: %v", err)
	}
	if index != 0 || term != 0 || data != nil {
		t.Error("should return zeros for missing snapshot")
	}
}
