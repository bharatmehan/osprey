package storage

import (
	"container/heap"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/bharatmehan/osprey/internal/config"
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrKeyExists       = errors.New("key already exists")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrNotInteger      = errors.New("value is not an integer")
	ErrKeyTooLarge     = errors.New("key too large")
	ErrValueTooLarge   = errors.New("value too large")
	ErrKeyInvalid      = errors.New("key contains invalid characters")
)

// validateKey checks if a key contains invalid characters (ASCII spaces or control chars)
func validateKey(key string) error {
	for i := 0; i < len(key); i++ {
		c := key[i]
		// Check for ASCII space (0x20) or control characters (0x00-0x1F, 0x7F)
		if c == 0x20 || c <= 0x1F || c == 0x7F {
			return ErrKeyInvalid
		}
	}
	return nil
}

// Store is the main in-memory key-value store
type Store struct {
	mu         sync.RWMutex
	data       map[string]*Entry
	expiryHeap *ExpiryHeap
	config     *config.Config

	// Statistics
	stats Stats
}

// Stats holds runtime statistics
type Stats struct {
	mu           sync.RWMutex
	CmdGet       uint64
	CmdSet       uint64
	CmdDel       uint64
	CmdIncr      uint64
	ExpiredTotal uint64
	EvictedTotal uint64
	StartTimeMs  int64
}

// New creates a new Store instance
func New(cfg *config.Config) *Store {
	s := &Store{
		data:       make(map[string]*Entry),
		expiryHeap: &ExpiryHeap{},
		config:     cfg,
		stats: Stats{
			StartTimeMs: time.Now().UnixMilli(),
		},
	}
	heap.Init(s.expiryHeap)
	return s
}

// Get retrieves a value by key, checking for expiry
func (s *Store) Get(key string) (*Entry, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.stats.CmdGet++

	entry, exists := s.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	if entry.IsExpired() {
		// Lazy deletion - upgrade to write lock
		s.mu.RUnlock()
		s.mu.Lock()

		// Re-check after acquiring write lock
		entry, exists = s.data[key]
		if exists && entry.IsExpired() {
			delete(s.data, key)
			s.stats.ExpiredTotal++
		}

		s.mu.Unlock()
		s.mu.RLock()
		return nil, ErrKeyNotFound
	}

	return entry, nil
}

// Set stores a key-value pair with optional expiry and conditions
func (s *Store) Set(key string, value []byte, opts SetOptions) (uint64, error) {
	if len(key) > s.config.MaxKeyBytes {
		return 0, ErrKeyTooLarge
	}
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if len(value) > s.config.MaxValueBytes {
		return 0, ErrValueTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.CmdSet++

	existing, exists := s.data[key]

	// Check NX/XX conditions
	if opts.NX && exists && !existing.IsExpired() {
		return 0, ErrKeyExists
	}
	if opts.XX && (!exists || existing.IsExpired()) {
		return 0, ErrKeyNotFound
	}

	// Check version condition
	if opts.CheckVersion && exists && !existing.IsExpired() {
		if existing.Version != opts.Version {
			return 0, ErrVersionMismatch
		}
	}

	// Calculate new version
	var newVersion uint64 = 1
	if exists && !existing.IsExpired() {
		newVersion = existing.Version + 1
	}

	// Calculate expiry
	var expiryMs int64 = -1
	if opts.ExpiryMs > 0 {
		expiryMs = time.Now().UnixMilli() + opts.ExpiryMs
	} else if opts.AbsoluteExpiryMs > 0 {
		expiryMs = opts.AbsoluteExpiryMs
	}

	entry := &Entry{
		Value:     value,
		Version:   newVersion,
		ExpiryMs:  expiryMs,
		SizeBytes: uint32(len(value)),
	}

	s.data[key] = entry

	// Add to expiry heap if needed
	if expiryMs > 0 {
		heap.Push(s.expiryHeap, &ExpiryItem{
			Key:      key,
			ExpiryMs: expiryMs,
		})
	}

	return newVersion, nil
}

// Delete removes a key from the store
func (s *Store) Delete(key string) bool {
	if err := validateKey(key); err != nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.CmdDel++

	entry, exists := s.data[key]
	if !exists || entry.IsExpired() {
		return false
	}

	delete(s.data, key)
	return true
}

// Exists checks if a key exists (not expired)
func (s *Store) Exists(key string) bool {
	if err := validateKey(key); err != nil {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	return exists && !entry.IsExpired()
}

// Expire sets a TTL on a key
func (s *Store) Expire(key string, ttlMs int64) error {
	if err := validateKey(key); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.IsExpired() {
		return ErrKeyNotFound
	}

	entry.ExpiryMs = time.Now().UnixMilli() + ttlMs

	heap.Push(s.expiryHeap, &ExpiryItem{
		Key:      key,
		ExpiryMs: entry.ExpiryMs,
	})

	return nil
}

// TTL returns the time to live for a key
func (s *Store) TTL(key string) int64 {
	if err := validateKey(key); err != nil {
		return -2 // Invalid key treated as not found
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		return -2
	}

	return entry.TTL()
}

// Incr increments a numeric value
func (s *Store) Incr(key string, delta int64) (int64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats.CmdIncr++

	entry, exists := s.data[key]

	var currentVal int64
	if !exists || entry.IsExpired() {
		currentVal = 0
	} else {
		// Try to parse as integer
		val, err := strconv.ParseInt(string(entry.Value), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		currentVal = val
	}

	newVal := currentVal + delta
	newValStr := strconv.FormatInt(newVal, 10)

	// Create new entry
	var newVersion uint64 = 1
	if exists && !entry.IsExpired() {
		newVersion = entry.Version + 1
	}

	s.data[key] = &Entry{
		Value:     []byte(newValStr),
		Version:   newVersion,
		ExpiryMs:  -1,
		SizeBytes: uint32(len(newValStr)),
	}

	return newVal, nil
}

// GetStats returns current statistics
func (s *Store) GetStats() map[string]string {
	s.mu.RLock()
	s.stats.mu.RLock()
	defer s.mu.RUnlock()
	defer s.stats.mu.RUnlock()

	uptime := time.Now().UnixMilli() - s.stats.StartTimeMs

	// Count non-expired keys
	keyCount := 0
	for _, entry := range s.data {
		if !entry.IsExpired() {
			keyCount++
		}
	}

	return map[string]string{
		"uptime_ms":     strconv.FormatInt(uptime, 10),
		"keys":          strconv.Itoa(keyCount),
		"expired_total": strconv.FormatUint(s.stats.ExpiredTotal, 10),
		"evicted_total": strconv.FormatUint(s.stats.EvictedTotal, 10),
		"cmd_get":       strconv.FormatUint(s.stats.CmdGet, 10),
		"cmd_set":       strconv.FormatUint(s.stats.CmdSet, 10),
		"cmd_del":       strconv.FormatUint(s.stats.CmdDel, 10),
		"cmd_incr":      strconv.FormatUint(s.stats.CmdIncr, 10),
	}
}

// SetOptions contains options for SET command
type SetOptions struct {
	ExpiryMs         int64
	AbsoluteExpiryMs int64
	NX               bool
	XX               bool
	CheckVersion     bool
	Version          uint64
}
