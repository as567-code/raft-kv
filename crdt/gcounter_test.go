package crdt

import (
	"testing"
)

func TestGCounter_Increment(t *testing.T) {
	gc := NewGCounter("node1")
	gc.Increment()
	gc.Increment()
	gc.Increment()

	if gc.Value() != 3 {
		t.Errorf("expected 3, got %d", gc.Value())
	}
}

func TestGCounter_IncrementBy(t *testing.T) {
	gc := NewGCounter("node1")
	gc.IncrementBy(10)
	gc.IncrementBy(5)

	if gc.Value() != 15 {
		t.Errorf("expected 15, got %d", gc.Value())
	}
}

func TestGCounter_LocalValue(t *testing.T) {
	gc := NewGCounter("node1")
	gc.Increment()
	gc.Increment()

	if gc.LocalValue() != 2 {
		t.Errorf("expected local value 2, got %d", gc.LocalValue())
	}
}

func TestGCounter_MergeElementWiseMax(t *testing.T) {
	gc1 := NewGCounter("node1")
	gc2 := NewGCounter("node2")

	gc1.IncrementBy(10) // node1: 10
	gc2.IncrementBy(5)  // node2: 5

	// Merge gc2 into gc1
	gc1.Merge(gc2.State())

	// gc1 should now have node1=10, node2=5, total=15
	if gc1.Value() != 15 {
		t.Errorf("expected 15, got %d", gc1.Value())
	}
}

func TestGCounter_MergeIsIdempotent(t *testing.T) {
	gc1 := NewGCounter("node1")
	gc2 := NewGCounter("node2")

	gc1.IncrementBy(10)
	gc2.IncrementBy(5)

	state := gc2.State()
	gc1.Merge(state)
	gc1.Merge(state) // merge again — should be idempotent

	if gc1.Value() != 15 {
		t.Errorf("expected 15 (idempotent merge), got %d", gc1.Value())
	}
}

func TestGCounter_MergeIsCommutative(t *testing.T) {
	gc1 := NewGCounter("node1")
	gc2 := NewGCounter("node2")
	gc3 := NewGCounter("node3")

	gc1.IncrementBy(10)
	gc2.IncrementBy(20)
	gc3.IncrementBy(30)

	// Merge in different orders — result should be the same
	gcA := NewGCounter("nodeA")
	gcA.Merge(gc1.State())
	gcA.Merge(gc2.State())
	gcA.Merge(gc3.State())

	gcB := NewGCounter("nodeB")
	gcB.Merge(gc3.State())
	gcB.Merge(gc1.State())
	gcB.Merge(gc2.State())

	if gcA.Value() != gcB.Value() {
		t.Errorf("merge should be commutative: A=%d B=%d", gcA.Value(), gcB.Value())
	}
	if gcA.Value() != 60 {
		t.Errorf("expected 60, got %d", gcA.Value())
	}
}

func TestGCounter_MergeThreeNodes(t *testing.T) {
	gc1 := NewGCounter("node1")
	gc2 := NewGCounter("node2")
	gc3 := NewGCounter("node3")

	// Each node increments independently
	gc1.IncrementBy(5)
	gc2.IncrementBy(3)
	gc3.IncrementBy(7)

	// All exchange state with each other
	s1, s2, s3 := gc1.State(), gc2.State(), gc3.State()

	gc1.Merge(s2)
	gc1.Merge(s3)
	gc2.Merge(s1)
	gc2.Merge(s3)
	gc3.Merge(s1)
	gc3.Merge(s2)

	// All should converge to 15
	for i, gc := range []*GCounter{gc1, gc2, gc3} {
		if gc.Value() != 15 {
			t.Errorf("node %d: expected 15, got %d", i+1, gc.Value())
		}
	}
}

func TestGCounter_MergeMaxNotSum(t *testing.T) {
	gc1 := NewGCounter("node1")
	gc2 := NewGCounter("node1") // same node ID!

	gc1.IncrementBy(10)
	gc2.IncrementBy(5) // node1=5 (lower)

	gc1.Merge(gc2.State())

	// Should take max(10, 5) = 10, not sum
	if gc1.Value() != 10 {
		t.Errorf("merge should take max, expected 10, got %d", gc1.Value())
	}
}

func TestGCounter_SerializationRoundTrip(t *testing.T) {
	gc := NewGCounter("node1")
	gc.IncrementBy(42)

	data, err := gc.MarshalState()
	if err != nil {
		t.Fatalf("MarshalState failed: %v", err)
	}

	gc2 := NewGCounter("node2")
	if err := gc2.UnmarshalAndMerge(data); err != nil {
		t.Fatalf("UnmarshalAndMerge failed: %v", err)
	}

	if gc2.Value() != 42 {
		t.Errorf("expected 42 after restore, got %d", gc2.Value())
	}
}
