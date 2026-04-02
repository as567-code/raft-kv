package crdt

import (
	"testing"
	"time"
)

func TestAntiEntropy_BuildMerkleTree(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)
	ae := NewAntiEntropy(reg)

	// Empty tree
	tree := ae.BuildMerkleTree()
	if tree == nil {
		t.Fatal("tree should not be nil")
	}
	if tree.Hash == "" {
		t.Error("empty tree should still have a hash")
	}

	// Add entries and rebuild
	reg.Set("a", "1")
	reg.Set("b", "2")
	tree2 := ae.BuildMerkleTree()
	if tree2.Hash == tree.Hash {
		t.Error("tree hash should change after adding entries")
	}
}

func TestAntiEntropy_RootHashDeterministic(t *testing.T) {
	clock := NewHLC("node1")
	reg := NewLWWRegister(clock)
	ae := NewAntiEntropy(reg)

	reg.Set("x", "1")
	reg.Set("y", "2")

	hash1 := ae.RootHash()
	hash2 := ae.RootHash()

	if hash1 != hash2 {
		t.Error("root hash should be deterministic")
	}
}

func TestAntiEntropy_IdenticalStatesMatchHash(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node1") // same node ID for identical timestamps
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	ts := clock1.Now()
	// Set identical entries with same timestamps
	reg1.Merge("a", &LWWEntry{Value: "1", Timestamp: ts})
	reg2.Merge("a", &LWWEntry{Value: "1", Timestamp: ts})

	ae1 := NewAntiEntropy(reg1)
	ae2 := NewAntiEntropy(reg2)

	if ae1.RootHash() != ae2.RootHash() {
		t.Error("identical states should have matching root hashes")
	}
}

func TestAntiEntropy_DifferentStatesDetected(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node2")
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	reg1.Set("a", "1")
	reg2.Set("a", "2") // different value

	ae1 := NewAntiEntropy(reg1)
	ae2 := NewAntiEntropy(reg2)

	if ae1.RootHash() == ae2.RootHash() {
		t.Error("different states should have different root hashes")
	}
}

func TestAntiEntropy_SyncReconcilesDifferences(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node2")
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	ae1 := NewAntiEntropy(reg1)
	ae2 := NewAntiEntropy(reg2)

	// Node1 has key "a", node2 has key "b"
	reg1.Set("a", "from-node1")
	reg2.Set("b", "from-node2")

	// Exchange sync states
	sync1 := ae1.PrepareSync()
	sync2 := ae2.PrepareSync()

	merged1 := ae1.ApplySync(sync2)
	merged2 := ae2.ApplySync(sync1)

	if merged1 == 0 {
		t.Error("node1 should have merged entries from node2")
	}
	if merged2 == 0 {
		t.Error("node2 should have merged entries from node1")
	}

	// Both should now have both keys
	v, ok := reg1.Get("b")
	if !ok || v != "from-node2" {
		t.Errorf("node1 should have b=from-node2, got %s (found=%v)", v, ok)
	}
	v, ok = reg2.Get("a")
	if !ok || v != "from-node1" {
		t.Errorf("node2 should have a=from-node1, got %s (found=%v)", v, ok)
	}
}

func TestAntiEntropy_SyncNoOpWhenIdentical(t *testing.T) {
	clock := NewHLC("node1")
	reg1 := NewLWWRegister(clock)
	reg2 := NewLWWRegister(clock)

	ts := clock.Now()
	reg1.Merge("a", &LWWEntry{Value: "1", Timestamp: ts})
	reg2.Merge("a", &LWWEntry{Value: "1", Timestamp: ts})

	ae1 := NewAntiEntropy(reg1)
	ae2 := NewAntiEntropy(reg2)

	sync2 := ae2.PrepareSync()
	merged := ae1.ApplySync(sync2)

	if merged != 0 {
		t.Errorf("no merge needed for identical states, got %d merges", merged)
	}
}

func TestAntiEntropy_SyncResolvesConflictByTimestamp(t *testing.T) {
	clock1 := NewHLC("node1")
	clock2 := NewHLC("node2")
	reg1 := NewLWWRegister(clock1)
	reg2 := NewLWWRegister(clock2)

	ae1 := NewAntiEntropy(reg1)
	ae2 := NewAntiEntropy(reg2)

	// Both write same key at different times
	reg1.Set("conflict", "old-value")
	time.Sleep(2 * time.Millisecond)
	reg2.Set("conflict", "new-value") // later write

	// Sync: node1 receives node2's state
	sync2 := ae2.PrepareSync()
	ae1.ApplySync(sync2)

	// Node1 should have the newer value
	v, _ := reg1.Get("conflict")
	if v != "new-value" {
		t.Errorf("expected new-value (later timestamp), got %s", v)
	}

	// Sync other direction
	sync1 := ae1.PrepareSync()
	ae2.ApplySync(sync1)

	v, _ = reg2.Get("conflict")
	if v != "new-value" {
		t.Errorf("expected new-value on node2 as well, got %s", v)
	}
}

func TestDiffKeys_EmptyTrees(t *testing.T) {
	diff := DiffKeys(nil, nil)
	if len(diff) != 0 {
		t.Errorf("expected no diffs for two nil trees, got %v", diff)
	}
}

func TestDiffKeys_OneNilTree(t *testing.T) {
	node := &MerkleNode{Hash: "abc", Key: "key1"}
	diff := DiffKeys(node, nil)
	if len(diff) != 1 || diff[0] != "key1" {
		t.Errorf("expected [key1], got %v", diff)
	}
}
