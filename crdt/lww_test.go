package crdt

import (
	"testing"
	"time"
)

func TestLWWRegister_SetAndGet(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)

	reg.Set("key1", "value1")

	val, ok := reg.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("expected key1=value1, got %s (found=%v)", val, ok)
	}
}

func TestLWWRegister_GetMissing(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)

	_, ok := reg.Get("missing")
	if ok {
		t.Error("expected missing key to not be found")
	}
}

func TestLWWRegister_Delete(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)

	reg.Set("key1", "value1")
	reg.Delete("key1")

	_, ok := reg.Get("key1")
	if ok {
		t.Error("deleted key should not be found")
	}
}

func TestLWWRegister_OverwriteValue(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)

	reg.Set("key1", "v1")
	reg.Set("key1", "v2")

	val, ok := reg.Get("key1")
	if !ok || val != "v2" {
		t.Errorf("expected v2, got %s", val)
	}
}

func TestLWWRegister_MergeLastWriterWins(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node2")
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	// Node1 writes first
	reg1.Set("key1", "from-node1")
	time.Sleep(2 * time.Millisecond)
	// Node2 writes later (higher timestamp)
	reg2.Set("key1", "from-node2")

	// Merge node2's state into node1
	reg1.MergeState(reg2.State())

	val, ok := reg1.Get("key1")
	if !ok || val != "from-node2" {
		t.Errorf("expected from-node2 (later write), got %s", val)
	}
}

func TestLWWRegister_MergeOlderWriteIgnored(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node2")
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	// Node2 writes first
	reg2.Set("key1", "old-value")
	time.Sleep(2 * time.Millisecond)
	// Node1 writes later
	reg1.Set("key1", "new-value")

	// Merge node2's (older) state into node1
	reg1.MergeState(reg2.State())

	val, ok := reg1.Get("key1")
	if !ok || val != "new-value" {
		t.Errorf("expected new-value (newer local), got %s", val)
	}
}

func TestLWWRegister_BidirectionalMerge(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node2")
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	// Different keys on different nodes
	reg1.Set("a", "from-node1")
	reg2.Set("b", "from-node2")

	// After merge, both should have both keys
	state1 := reg1.State()
	state2 := reg2.State()

	reg1.MergeState(state2)
	reg2.MergeState(state1)

	for _, reg := range []*LWWRegister{reg1, reg2} {
		va, ok := reg.Get("a")
		if !ok || va != "from-node1" {
			t.Errorf("expected a=from-node1, got %s", va)
		}
		vb, ok := reg.Get("b")
		if !ok || vb != "from-node2" {
			t.Errorf("expected b=from-node2, got %s", vb)
		}
	}
}

func TestLWWRegister_DeleteWinsOverOlderSet(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node2")
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	reg1.Set("key1", "value1")
	time.Sleep(2 * time.Millisecond)
	reg2.Set("key1", "value2")
	time.Sleep(2 * time.Millisecond)
	reg2.Delete("key1")

	reg1.MergeState(reg2.State())

	_, ok := reg1.Get("key1")
	if ok {
		t.Error("key should be deleted after merge")
	}
}

func TestLWWRegister_SerializationRoundTrip(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)
	reg.Set("a", "1")
	reg.Set("b", "2")

	data, err := reg.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState failed: %v", err)
	}

	clock2 := NewHLC("node2")
	reg2 := NewLWWRegister(clock2)
	if err := reg2.UnmarshalAndMerge(data); err != nil {
		t.Fatalf("UnmarshalAndMerge failed: %v", err)
	}

	va, ok := reg2.Get("a")
	if !ok || va != "1" {
		t.Errorf("expected a=1 after restore, got %s", va)
	}
}

func TestLWWRegister_Keys(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)

	reg.Set("a", "1")
	reg.Set("b", "2")
	reg.Set("c", "3")
	reg.Delete("b")

	keys := reg.Keys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys (a,c), got %d: %v", len(keys), keys)
	}
}
