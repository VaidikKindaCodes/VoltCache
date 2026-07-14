package storage

import (
	"sync"
	"time"

	"github.com/VaidikKindaCodes/VoltCache/internals/domain"
)

type inMemoryStore struct {
	data map[string]domain.Entry
	mu   sync.Mutex
}

// NewInMemoryStore creates a new in-memory store.
func NewInMemoryStore() domain.Store {
	return &inMemoryStore{
		data: make(map[string]domain.Entry),
	}
}

func (s *inMemoryStore) Set(key, value string, px time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var expirationPtr *time.Time
	if px > 0 {
		expiration := time.Now().Add(px)
		expirationPtr = &expiration
	}
	s.data[key] = domain.Entry{Value: value, Expiration: expirationPtr}
}

func (s *inMemoryStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists {
		return "", false
	}

	if entry.Expiration != nil && time.Now().After(*entry.Expiration) {
		delete(s.data, key)
		return "", false
	}

	return entry.Value, true
}

func (s *inMemoryStore) Entries() map[string]domain.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	copy := make(map[string]domain.Entry, len(s.data))
	for k, v := range s.data {
		copy[k] = v
	}

	return copy
}
