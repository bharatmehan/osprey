package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateKey(t *testing.T) {
	// Valid keys
	validKeys := []string{
		"a",
		"test_key",
		"key-with-dash",
		"key.with.dots",
		"key:with:colons",
		"CamelCaseKey",
		"key123",
		"user:1234",
		"namespace/resource",
		"αβγ", // Unicode is allowed
		"日本語", // Unicode is allowed
	}

	for _, key := range validKeys {
		t.Run("valid_"+key, func(t *testing.T) {
			err := validateKey(key)
			assert.NoError(t, err, "Key %q should be valid", key)
		})
	}

	// Invalid keys - ASCII space (0x20)
	invalidKeys := []string{
		"key with space",
		" leading_space",
		"trailing_space ",
		"multiple   spaces",
	}

	for _, key := range invalidKeys {
		t.Run("invalid_space_"+key, func(t *testing.T) {
			err := validateKey(key)
			assert.Equal(t, ErrKeyInvalid, err, "Key %q should be invalid (contains space)", key)
		})
	}

	// Invalid keys - Control characters (0x00-0x1F)
	controlChars := []string{
		"key\x00null",     // NULL (0x00)
		"key\x01soh",      // SOH (0x01)
		"key\x07bell",     // BEL (0x07)
		"key\x08bs",       // Backspace (0x08)
		"key\x09tab",      // Tab (0x09)
		"key\x0Anewline",  // LF (0x0A)
		"key\x0Dcarriage", // CR (0x0D)
		"key\x1Besc",      // ESC (0x1B)
		"key\x1Fus",       // US (0x1F)
	}

	for i, key := range controlChars {
		t.Run("invalid_control_"+string(rune(i)), func(t *testing.T) {
			err := validateKey(key)
			assert.Equal(t, ErrKeyInvalid, err, "Key %q should be invalid (contains control char)", key)
		})
	}

	// Invalid key - DEL character (0x7F)
	t.Run("invalid_del_char", func(t *testing.T) {
		key := "key\x7Fdel"
		err := validateKey(key)
		assert.Equal(t, ErrKeyInvalid, err, "Key %q should be invalid (contains DEL char)", key)
	})

	// Edge cases
	t.Run("empty_key", func(t *testing.T) {
		err := validateKey("")
		assert.NoError(t, err, "Empty key should be valid (length validation is separate)")
	})

	t.Run("max_ascii_printable", func(t *testing.T) {
		// Characters 0x21-0x7E are valid ASCII printable characters
		key := string([]byte{0x21, 0x22, 0x7E}) // !"~
		err := validateKey(key)
		assert.NoError(t, err, "Printable ASCII characters should be valid")
	})
}

func TestStore_KeyValidation(t *testing.T) {
	store := newTestStore()

	// Test SET with invalid key
	t.Run("set_invalid_key", func(t *testing.T) {
		_, err := store.Set("invalid key", []byte("value"), SetOptions{})
		assert.Equal(t, ErrKeyInvalid, err)
	})

	// Test GET with invalid key
	t.Run("get_invalid_key", func(t *testing.T) {
		_, err := store.Get("invalid\x00key")
		assert.Equal(t, ErrKeyInvalid, err)
	})

	// Test DELETE with invalid key (returns false)
	t.Run("delete_invalid_key", func(t *testing.T) {
		deleted := store.Delete("invalid\tkey")
		assert.False(t, deleted)
	})

	// Test EXISTS with invalid key (returns false)
	t.Run("exists_invalid_key", func(t *testing.T) {
		exists := store.Exists("invalid\nkey")
		assert.False(t, exists)
	})

	// Test EXPIRE with invalid key
	t.Run("expire_invalid_key", func(t *testing.T) {
		err := store.Expire("invalid key", 1000)
		assert.Equal(t, ErrKeyInvalid, err)
	})

	// Test TTL with invalid key (returns -2)
	t.Run("ttl_invalid_key", func(t *testing.T) {
		ttl := store.TTL("invalid\x1Fkey")
		assert.Equal(t, int64(-2), ttl)
	})

	// Test INCR with invalid key
	t.Run("incr_invalid_key", func(t *testing.T) {
		_, err := store.Incr("invalid key", 1)
		assert.Equal(t, ErrKeyInvalid, err)
	})

	// Test valid operations work normally
	t.Run("valid_operations", func(t *testing.T) {
		validKey := "valid-key_123"

		// SET should work
		version, err := store.Set(validKey, []byte("test"), SetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), version)

		// GET should work
		entry, err := store.Get(validKey)
		assert.NoError(t, err)
		assert.Equal(t, []byte("test"), entry.Value)

		// EXISTS should work
		exists := store.Exists(validKey)
		assert.True(t, exists)

		// TTL should work
		ttl := store.TTL(validKey)
		assert.Equal(t, int64(-1), ttl)

		// DELETE should work
		deleted := store.Delete(validKey)
		assert.True(t, deleted)
	})
}
