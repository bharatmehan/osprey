package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bharatmehan/osprey/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_WriteRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	snapPath := filepath.Join(tempDir, "test.osnap")

	// Create test entries
	entries := map[string]*Entry{
		"key1": {
			Value:     []byte("value1"),
			Version:   1,
			ExpiryMs:  -1,
			SizeBytes: 6,
		},
		"key2": {
			Value:     []byte("value2"),
			Version:   2,
			ExpiryMs:  time.Now().UnixMilli() + 60000,
			SizeBytes: 6,
		},
		"key3": {
			Value:     []byte(""),
			Version:   1,
			ExpiryMs:  -1,
			SizeBytes: 0,
		},
	}

	// Write snapshot
	writer, err := NewSnapshotWriter(snapPath)
	require.NoError(t, err)

	for key, entry := range entries {
		err := writer.WriteEntry(key, entry)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Read snapshot
	reader, err := OpenSnapshotReader(snapPath)
	require.NoError(t, err)
	defer reader.Close()

	readEntries := make(map[string]*Entry)
	for {
		key, entry, err := reader.ReadEntry()
		if err != nil {
			break
		}
		readEntries[key] = entry
	}

	// Verify entries
	assert.Equal(t, len(entries), len(readEntries))
	for key, expected := range entries {
		actual, exists := readEntries[key]
		require.True(t, exists, "Key %s not found", key)
		assert.Equal(t, expected.Value, actual.Value)
		assert.Equal(t, expected.Version, actual.Version)
		assert.Equal(t, expected.ExpiryMs, actual.ExpiryMs)
		assert.Equal(t, expected.SizeBytes, actual.SizeBytes)
	}
}

func TestSnapshot_SkipExpired(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	snapPath := filepath.Join(tempDir, "test.osnap")

	// Create test entries with one expired
	entries := map[string]*Entry{
		"key1": {
			Value:     []byte("value1"),
			Version:   1,
			ExpiryMs:  -1,
			SizeBytes: 6,
		},
		"expired": {
			Value:     []byte("expired_value"),
			Version:   1,
			ExpiryMs:  time.Now().UnixMilli() - 1000, // Expired
			SizeBytes: 13,
		},
	}

	// Write snapshot
	writer, err := NewSnapshotWriter(snapPath)
	require.NoError(t, err)

	for key, entry := range entries {
		err := writer.WriteEntry(key, entry)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Read snapshot
	reader, err := OpenSnapshotReader(snapPath)
	require.NoError(t, err)
	defer reader.Close()

	readEntries := make(map[string]*Entry)
	for {
		key, entry, err := reader.ReadEntry()
		if err != nil {
			break
		}
		readEntries[key] = entry
	}

	// Should only have non-expired entry
	assert.Equal(t, 1, len(readEntries))
	_, exists := readEntries["key1"]
	assert.True(t, exists)
	_, exists = readEntries["expired"]
	assert.False(t, exists)
}

func TestSnapshot_BinaryData(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	snapPath := filepath.Join(tempDir, "test.osnap")

	// Create entry with binary data
	binaryData := []byte{0, 1, 2, 3, 255, 254, 253}
	entry := &Entry{
		Value:     binaryData,
		Version:   1,
		ExpiryMs:  -1,
		SizeBytes: uint32(len(binaryData)),
	}

	// Write snapshot
	writer, err := NewSnapshotWriter(snapPath)
	require.NoError(t, err)

	err = writer.WriteEntry("binary_key", entry)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read snapshot
	reader, err := OpenSnapshotReader(snapPath)
	require.NoError(t, err)
	defer reader.Close()

	key, readEntry, err := reader.ReadEntry()
	require.NoError(t, err)

	assert.Equal(t, "binary_key", key)
	assert.Equal(t, binaryData, readEntry.Value)
}

func TestManifest_WriteRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create manifest
	manifest := &Manifest{
		Version:   1,
		Snap:      "snap-00000001.osnap",
		NextWAL:   "wal-00000002.oswal",
		CreatedMs: time.Now().UnixMilli(),
	}

	// Write manifest
	err = WriteManifest(tempDir, manifest)
	require.NoError(t, err)

	// Read manifest
	readManifest, err := ReadManifest(tempDir)
	require.NoError(t, err)
	require.NotNil(t, readManifest)

	assert.Equal(t, manifest.Version, readManifest.Version)
	assert.Equal(t, manifest.Snap, readManifest.Snap)
	assert.Equal(t, manifest.NextWAL, readManifest.NextWAL)
	assert.Equal(t, manifest.CreatedMs, readManifest.CreatedMs)
}

func TestManifest_NoFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Try to read non-existent manifest
	manifest, err := ReadManifest(tempDir)
	require.NoError(t, err)
	assert.Nil(t, manifest)
}

func TestSnapshotManager_Create(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tempDir

	manager, err := NewSnapshotManager(cfg)
	require.NoError(t, err)

	// Create test store
	store := New(cfg)
	store.Set("key1", []byte("value1"), SetOptions{})
	store.Set("key2", []byte("value2"), SetOptions{})

	// Create snapshot
	err = manager.CreateSnapshot(store, "wal-00000001.oswal")
	require.NoError(t, err)

	// Verify manifest was created
	manifest, err := ReadManifest(tempDir)
	require.NoError(t, err)
	require.NotNil(t, manifest)

	assert.Equal(t, 1, manifest.Version)
	assert.Equal(t, "snap-00000001.osnap", manifest.Snap)
	assert.Equal(t, "wal-00000001.oswal", manifest.NextWAL)

	// Verify snapshot file exists
	snapPath := filepath.Join(tempDir, manifest.Snap)
	_, err = os.Stat(snapPath)
	require.NoError(t, err)
}

func TestSnapshotManager_Load(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tempDir

	manager, err := NewSnapshotManager(cfg)
	require.NoError(t, err)

	// Create and populate store
	originalStore := New(cfg)
	originalStore.Set("key1", []byte("value1"), SetOptions{})
	originalStore.Set("key2", []byte("value2"), SetOptions{})

	// Create snapshot
	err = manager.CreateSnapshot(originalStore, "wal-00000001.oswal")
	require.NoError(t, err)

	// Load into new store
	newStore := New(cfg)
	nextWAL, err := manager.LoadSnapshot(newStore)
	require.NoError(t, err)

	assert.Equal(t, "wal-00000001.oswal", nextWAL)

	// Verify data was loaded
	entry1, err := newStore.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), entry1.Value)

	entry2, err := newStore.Get("key2")
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), entry2.Value)
}

func TestSnapshotManager_NeedsSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.WALMaxBytes = 1000

	manager := &SnapshotManager{
		config:         cfg,
		lastSnapshotMs: time.Now().UnixMilli(), // Set recent snapshot time
	}

	// Should need snapshot if WAL exceeds threshold
	assert.True(t, manager.NeedsSnapshot(1001, 500, 100))

	// Should need snapshot if live/dead ratio is low
	assert.True(t, manager.NeedsSnapshot(500, 100, 300))

	// Should not need snapshot if both conditions are fine
	assert.False(t, manager.NeedsSnapshot(500, 800, 100))
}
