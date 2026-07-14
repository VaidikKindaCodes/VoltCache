package domain

import "time"

type Entry struct {
	Value      string
	Expiration *time.Time
}

// Store defines the interface for the key-value store.
type Store interface {
	Set(key, value string, expiration time.Duration)
	Get(key string) (string, bool)
	Entries() map[string]Entry
}
