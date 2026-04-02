package crdt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
)

// MerkleNode represents a node in a Merkle tree for anti-entropy.
type MerkleNode struct {
	Hash     string        `json:"hash"`
	Key      string        `json:"key,omitempty"` // leaf only
	Children []*MerkleNode `json:"children,omitempty"`
}

// AntiEntropy provides background synchronization between nodes using
// Merkle tree hashes to efficiently detect and reconcile differences.
type AntiEntropy struct {
	mu       sync.RWMutex
	register *LWWRegister
}

func NewAntiEntropy(register *LWWRegister) *AntiEntropy {
	return &AntiEntropy{register: register}
}

// BuildMerkleTree builds a Merkle tree from the current register state.
func (ae *AntiEntropy) BuildMerkleTree() *MerkleNode {
	state := ae.register.State()

	keys := make([]string, 0, len(state.Entries))
	for k := range state.Entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) == 0 {
		return &MerkleNode{Hash: hashBytes(nil)}
	}

	// Build leaf nodes
	leaves := make([]*MerkleNode, len(keys))
	for i, key := range keys {
		entry := state.Entries[key]
		data, _ := json.Marshal(entry)
		leaves[i] = &MerkleNode{
			Hash: hashBytes(data),
			Key:  key,
		}
	}

	// Build tree bottom-up
	return buildTreeLevel(leaves)
}

// RootHash returns just the root hash for quick comparison.
func (ae *AntiEntropy) RootHash() string {
	tree := ae.BuildMerkleTree()
	if tree == nil {
		return ""
	}
	return tree.Hash
}

// DiffKeys compares two Merkle trees and returns keys that differ.
func DiffKeys(local, remote *MerkleNode) []string {
	if local == nil && remote == nil {
		return nil
	}
	if local == nil || remote == nil {
		return collectKeys(local)
	}
	if local.Hash == remote.Hash {
		return nil
	}

	// If this is a leaf node
	if local.Key != "" || remote.Key != "" {
		var keys []string
		if local.Key != "" {
			keys = append(keys, local.Key)
		}
		if remote.Key != "" && remote.Key != local.Key {
			keys = append(keys, remote.Key)
		}
		return keys
	}

	// Recursively diff children
	var diffed []string
	maxChildren := len(local.Children)
	if len(remote.Children) > maxChildren {
		maxChildren = len(remote.Children)
	}

	for i := 0; i < maxChildren; i++ {
		var lChild, rChild *MerkleNode
		if i < len(local.Children) {
			lChild = local.Children[i]
		}
		if i < len(remote.Children) {
			rChild = remote.Children[i]
		}
		diffed = append(diffed, DiffKeys(lChild, rChild)...)
	}
	return diffed
}

// SyncState represents the state to exchange during anti-entropy.
type SyncState struct {
	RootHash string               `json:"root_hash"`
	Entries  map[string]*LWWEntry `json:"entries,omitempty"`
}

// PrepareSync creates a sync state for exchange with a peer.
func (ae *AntiEntropy) PrepareSync() *SyncState {
	state := ae.register.State()
	return &SyncState{
		RootHash: ae.RootHash(),
		Entries:  state.Entries,
	}
}

// ApplySync receives remote sync state and merges differing entries.
func (ae *AntiEntropy) ApplySync(remote *SyncState) int {
	if remote == nil {
		return 0
	}

	// If hashes match, nothing to do
	if remote.RootHash == ae.RootHash() {
		return 0
	}

	// Merge all remote entries (the LWWRegister handles conflict resolution)
	merged := 0
	for key, entry := range remote.Entries {
		localVal, exists := ae.register.Get(key)
		if !exists || localVal != entry.Value {
			ae.register.Merge(key, entry)
			merged++
		}
	}
	return merged
}

func buildTreeLevel(nodes []*MerkleNode) *MerkleNode {
	if len(nodes) == 1 {
		return nodes[0]
	}

	var parents []*MerkleNode
	for i := 0; i < len(nodes); i += 2 {
		parent := &MerkleNode{}
		if i+1 < len(nodes) {
			parent.Children = []*MerkleNode{nodes[i], nodes[i+1]}
			parent.Hash = hashBytes([]byte(nodes[i].Hash + nodes[i+1].Hash))
		} else {
			parent.Children = []*MerkleNode{nodes[i]}
			parent.Hash = nodes[i].Hash
		}
		parents = append(parents, parent)
	}

	return buildTreeLevel(parents)
}

func collectKeys(node *MerkleNode) []string {
	if node == nil {
		return nil
	}
	if node.Key != "" {
		return []string{node.Key}
	}
	var keys []string
	for _, child := range node.Children {
		keys = append(keys, collectKeys(child)...)
	}
	return keys
}

func hashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
