package store

import (
	"encoding/json"
	"testing"
)

func TestFSM_ApplyPut(t *testing.T) {
	kvStore := NewKVStore()
	fsm := NewFSM(kvStore)

	cmd := Command{Type: CmdPut, Key: "hello", Value: "world"}
	data, _ := json.Marshal(cmd)

	if err := fsm.Apply(data); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	val, ok := kvStore.Get("hello")
	if !ok || val != "world" {
		t.Errorf("expected hello=world, got %s (found=%v)", val, ok)
	}
}

func TestFSM_ApplyDelete(t *testing.T) {
	kvStore := NewKVStore()
	fsm := NewFSM(kvStore)

	// Put then Delete
	putCmd := Command{Type: CmdPut, Key: "hello", Value: "world"}
	data, _ := json.Marshal(putCmd)
	fsm.Apply(data)

	delCmd := Command{Type: CmdDelete, Key: "hello"}
	data, _ = json.Marshal(delCmd)
	if err := fsm.Apply(data); err != nil {
		t.Fatalf("Apply delete failed: %v", err)
	}

	_, ok := kvStore.Get("hello")
	if ok {
		t.Error("key should be deleted")
	}
}

func TestFSM_ApplyInvalidCommand(t *testing.T) {
	kvStore := NewKVStore()
	fsm := NewFSM(kvStore)

	err := fsm.Apply([]byte("not json"))
	if err == nil {
		t.Error("should fail on invalid JSON")
	}
}

func TestFSM_ApplyUnknownCommandType(t *testing.T) {
	kvStore := NewKVStore()
	fsm := NewFSM(kvStore)

	cmd := Command{Type: "unknown", Key: "k", Value: "v"}
	data, _ := json.Marshal(cmd)

	err := fsm.Apply(data)
	if err == nil {
		t.Error("should fail on unknown command type")
	}
}

func TestFSM_SnapshotAndRestore(t *testing.T) {
	kvStore := NewKVStore()
	fsm := NewFSM(kvStore)

	// Apply some commands
	cmds := []Command{
		{Type: CmdPut, Key: "k1", Value: "v1"},
		{Type: CmdPut, Key: "k2", Value: "v2"},
		{Type: CmdPut, Key: "k3", Value: "v3"},
	}
	for _, cmd := range cmds {
		data, _ := json.Marshal(cmd)
		fsm.Apply(data)
	}

	// Take snapshot
	snapData, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Create a new FSM and restore
	kvStore2 := NewKVStore()
	fsm2 := NewFSM(kvStore2)

	if err := fsm2.Restore(snapData); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify all keys present
	for _, cmd := range cmds {
		val, ok := kvStore2.Get(cmd.Key)
		if !ok || val != cmd.Value {
			t.Errorf("after restore: expected %s=%s, got %s (found=%v)",
				cmd.Key, cmd.Value, val, ok)
		}
	}
}

func TestKVStore_Scan(t *testing.T) {
	kvStore := NewKVStore()
	kvStore.Put("user:1", "alice")
	kvStore.Put("user:2", "bob")
	kvStore.Put("order:1", "pizza")

	results := kvStore.Scan("user:")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results["user:1"] != "alice" || results["user:2"] != "bob" {
		t.Errorf("unexpected scan results: %v", results)
	}
}

func TestKVStore_Keys(t *testing.T) {
	kvStore := NewKVStore()
	kvStore.Put("c", "3")
	kvStore.Put("a", "1")
	kvStore.Put("b", "2")

	keys := kvStore.Keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Errorf("keys should be sorted: %v", keys)
	}
}

func TestKVStore_Len(t *testing.T) {
	kvStore := NewKVStore()
	if kvStore.Len() != 0 {
		t.Errorf("empty store should have len 0")
	}
	kvStore.Put("k", "v")
	if kvStore.Len() != 1 {
		t.Errorf("store with 1 key should have len 1")
	}
}

func TestEncodeCommand(t *testing.T) {
	cmd := Command{Type: CmdPut, Key: "test", Value: "value"}
	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}

	var decoded Command
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if decoded.Type != cmd.Type || decoded.Key != cmd.Key || decoded.Value != cmd.Value {
		t.Errorf("decoded command mismatch: %+v", decoded)
	}
}
