package storage

import (
	"time"
)

// Entry represents a key-value entry in the storage
type Entry struct {
	Value     []byte
	Version   uint64
	ExpiryMs  int64 // -1 means no expiry
	SizeBytes uint32
}

// IsExpired checks if the entry has expired
func (e *Entry) IsExpired() bool {
	if e.ExpiryMs < 0 {
		return false
	}
	return time.Now().UnixMilli() > e.ExpiryMs
}

// TTL returns the time to live in milliseconds
func (e *Entry) TTL() int64 {
	if e.ExpiryMs < 0 {
		return -1
	}
	ttl := e.ExpiryMs - time.Now().UnixMilli()
	if ttl < 0 {
		return -2 // expired
	}
	return ttl
}