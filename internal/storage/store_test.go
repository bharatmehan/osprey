package storage

import (
	"testing"
	"time"

	"github.com/bharatmehan/osprey/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore() *Store {
	cfg := config.DefaultConfig()
	return New(cfg)
}

func TestStore_Set_Get(t *testing.T) {
	store := newTestStore()

	// Test basic set/get
	version, err := store.Set("key1", []byte("value1"), SetOptions{})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), version)

	entry, err := store.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry.Value)
	assert.Equal(t, uint64(1), entry.Version)
	assert.Equal(t, int64(-1), entry.ExpiryMs)
}

func TestStore_Set_WithExpiry(t *testing.T) {
	store := newTestStore()

	// Set with relative expiry
	_, err := store.Set("key1", []byte("value1"), SetOptions{ExpiryMs: 1000})
	require.NoError(t, err)

	entry, err := store.Get("key1")
	require.NoError(t, err)
	assert.True(t, entry.ExpiryMs > time.Now().UnixMilli())

	// Set with absolute expiry
	futureMs := time.Now().UnixMilli() + 2000
	_, err = store.Set("key2", []byte("value2"), SetOptions{AbsoluteExpiryMs: futureMs})
	require.NoError(t, err)

	entry, err = store.Get("key2")
	require.NoError(t, err)
	assert.Equal(t, futureMs, entry.ExpiryMs)
}

func TestStore_Set_NX_XX(t *testing.T) {
	store := newTestStore()

	// Test NX (only if not exists)
	_, err := store.Set("key1", []byte("value1"), SetOptions{NX: true})
	require.NoError(t, err)

	// Should fail because key exists
	_, err = store.Set("key1", []byte("value2"), SetOptions{NX: true})
	assert.Equal(t, ErrKeyExists, err)

	// Test XX (only if exists)
	_, err = store.Set("key2", []byte("value2"), SetOptions{XX: true})
	assert.Equal(t, ErrKeyNotFound, err)

	// Should succeed because key1 exists
	_, err = store.Set("key1", []byte("value3"), SetOptions{XX: true})
	require.NoError(t, err)

	entry, err := store.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value3"), entry.Value)
}

func TestStore_Set_Version(t *testing.T) {
	store := newTestStore()

	// Set initial value
	version1, err := store.Set("key1", []byte("value1"), SetOptions{})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), version1)

	// Update with correct version
	version2, err := store.Set("key1", []byte("value2"), SetOptions{
		CheckVersion: true,
		Version:      version1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), version2)

	// Should fail with wrong version
	_, err = store.Set("key1", []byte("value3"), SetOptions{
		CheckVersion: true,
		Version:      version1, // Old version
	})
	assert.Equal(t, ErrVersionMismatch, err)
}

func TestStore_Delete(t *testing.T) {
	store := newTestStore()

	// Delete non-existent key
	deleted := store.Delete("nonexistent")
	assert.False(t, deleted)

	// Set and delete key
	_, err := store.Set("key1", []byte("value1"), SetOptions{})
	require.NoError(t, err)

	deleted = store.Delete("key1")
	assert.True(t, deleted)

	// Key should not exist anymore
	_, err = store.Get("key1")
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestStore_Exists(t *testing.T) {
	store := newTestStore()

	// Non-existent key
	assert.False(t, store.Exists("key1"))

	// Set key
	_, err := store.Set("key1", []byte("value1"), SetOptions{})
	require.NoError(t, err)

	assert.True(t, store.Exists("key1"))

	// Delete key
	store.Delete("key1")
	assert.False(t, store.Exists("key1"))
}

func TestStore_Expire_TTL(t *testing.T) {
	store := newTestStore()

	// Set key
	_, err := store.Set("key1", []byte("value1"), SetOptions{})
	require.NoError(t, err)

	// Set expiry
	err = store.Expire("key1", 1000)
	require.NoError(t, err)

	// Check TTL
	ttl := store.TTL("key1")
	assert.True(t, ttl > 0 && ttl <= 1000)

	// Non-existent key
	ttl = store.TTL("nonexistent")
	assert.Equal(t, int64(-2), ttl)

	// Key with no expiry
	_, err = store.Set("key2", []byte("value2"), SetOptions{})
	require.NoError(t, err)

	ttl = store.TTL("key2")
	assert.Equal(t, int64(-1), ttl)
}

func TestStore_Incr_Decr(t *testing.T) {
	store := newTestStore()

	// Incr non-existent key (should create with 0)
	newVal, err := store.Incr("counter", 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), newVal)

	// Incr existing key
	newVal, err = store.Incr("counter", 3)
	require.NoError(t, err)
	assert.Equal(t, int64(8), newVal)

	// Decr
	newVal, err = store.Incr("counter", -2)
	require.NoError(t, err)
	assert.Equal(t, int64(6), newVal)

	// Try to incr non-numeric value
	_, err = store.Set("text", []byte("hello"), SetOptions{})
	require.NoError(t, err)

	_, err = store.Incr("text", 1)
	assert.Equal(t, ErrNotInteger, err)
}

func TestStore_Expiry_Lazy(t *testing.T) {
	store := newTestStore()

	// Set key with very short expiry
	_, err := store.Set("key1", []byte("value1"), SetOptions{ExpiryMs: 1})
	require.NoError(t, err)

	// Wait for expiry
	time.Sleep(10 * time.Millisecond)

	// Should not be found due to lazy deletion
	_, err = store.Get("key1")
	assert.Equal(t, ErrKeyNotFound, err)

	// Should not exist
	assert.False(t, store.Exists("key1"))
}

func TestStore_SizeLimits(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxKeyBytes = 10
	cfg.MaxValueBytes = 20
	store := &Store{
		data:   make(map[string]*Entry),
		config: cfg,
	}

	// Key too large
	longKey := string(make([]byte, 11))
	_, err := store.Set(longKey, []byte("value"), SetOptions{})
	assert.Equal(t, ErrKeyTooLarge, err)

	// Value too large
	longValue := make([]byte, 21)
	_, err = store.Set("key", longValue, SetOptions{})
	assert.Equal(t, ErrValueTooLarge, err)

	// Valid sizes should work
	_, err = store.Set("key", []byte("value"), SetOptions{})
	require.NoError(t, err)
}

func TestStore_Stats(t *testing.T) {
	store := newTestStore()

	// Initial stats
	stats := store.GetStats()
	assert.NotEmpty(t, stats["uptime_ms"])
	assert.Equal(t, "0", stats["keys"])

	// Add some data and operations
	store.Set("key1", []byte("value1"), SetOptions{})
	store.Set("key2", []byte("value2"), SetOptions{})
	store.Get("key1")
	store.Delete("key3") // Non-existent
	store.Incr("counter", 1)

	stats = store.GetStats()
	assert.Equal(t, "3", stats["keys"]) // key1, key2, counter
	assert.Equal(t, "1", stats["cmd_get"])
	assert.Equal(t, "2", stats["cmd_set"]) // 2 sets only
	assert.Equal(t, "1", stats["cmd_del"])
	assert.Equal(t, "1", stats["cmd_incr"])
}
