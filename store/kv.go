package store

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

// KVStore is a thread-safe in-memory key-value store.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *KVStore) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Scan returns all key-value pairs with keys starting with the given prefix.
func (s *KVStore) Scan(prefix string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range s.data {
		if strings.HasPrefix(k, prefix) {
			result[k] = v
		}
	}
	return result
}

// Keys returns all keys sorted.
func (s *KVStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Snapshot serializes the entire store to JSON bytes.
func (s *KVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

// Restore replaces the store contents from a JSON snapshot.
func (s *KVStore) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newData := make(map[string]string)
	if err := json.Unmarshal(data, &newData); err != nil {
		return err
	}
	s.data = newData
	return nil
}

// Len returns the number of keys in the store.
func (s *KVStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
