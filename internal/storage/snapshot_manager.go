package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bharatmehan/osprey/internal/config"
)

// SnapshotManager manages snapshot creation and cleanup
type SnapshotManager struct {
	mu             sync.Mutex
	dataDir        string
	config         *config.Config
	snapIndex      int
	lastSnapshotMs int64
	snapshotting   int32
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(cfg *config.Config) (*SnapshotManager, error) {
	manager := &SnapshotManager{
		dataDir:        cfg.DataDir,
		config:         cfg,
		lastSnapshotMs: time.Now().UnixMilli(),
	}

	// Find the next snapshot index
	snapFiles, err := manager.listSnapshotFiles()
	if err != nil {
		return nil, err
	}

	if len(snapFiles) > 0 {
		lastSnap := snapFiles[len(snapFiles)-1]
		index, err := manager.extractSnapIndex(lastSnap)
		if err != nil {
			return nil, err
		}
		manager.snapIndex = index + 1
	} else {
		manager.snapIndex = 1
	}

	return manager, nil
}

// NeedsSnapshot checks if a snapshot is needed
func (sm *SnapshotManager) NeedsSnapshot(walSize int64, liveBytes int64, deadBytes int64) bool {
	// Check WAL size threshold
	if walSize > sm.config.WALMaxBytes {
		return true
	}

	// Check live/dead ratio
	if deadBytes > 0 && float64(liveBytes)/float64(deadBytes) < 0.5 {
		return true
	}

	// Check time since last snapshot (10 minutes default)
	if time.Now().UnixMilli()-sm.lastSnapshotMs > 10*60*1000 {
		return true
	}

	return false
}

// CreateSnapshot creates a new snapshot
func (sm *SnapshotManager) CreateSnapshot(store *Store, currentWAL string) error {
	if !atomic.CompareAndSwapInt32(&sm.snapshotting, 0, 1) {
		return fmt.Errorf("snapshot already in progress")
	}
	defer atomic.StoreInt32(&sm.snapshotting, 0)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	startTime := time.Now()
	log.Printf("Starting snapshot %d", sm.snapIndex)

	// Create snapshot filename
	snapFile := fmt.Sprintf("snap-%08d.osnap", sm.snapIndex)
	snapPath := filepath.Join(sm.dataDir, snapFile)

	// Create temp file first
	tempPath := snapPath + ".tmp"

	// Create snapshot writer
	writer, err := NewSnapshotWriter(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot writer: %w", err)
	}

	// Write all entries
	count := 0
	store.mu.RLock()
	for key, entry := range store.data {
		if !entry.IsExpired() {
			if err := writer.WriteEntry(key, entry); err != nil {
				store.mu.RUnlock()
				writer.Close()
				os.Remove(tempPath)
				return fmt.Errorf("failed to write entry: %w", err)
			}
			count++
		}
	}
	store.mu.RUnlock()

	// Close snapshot
	if err := writer.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close snapshot: %w", err)
	}

	// Rename to final name
	if err := os.Rename(tempPath, snapPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}

	// Write manifest
	manifest := &Manifest{
		Version:   1,
		Snap:      snapFile,
		NextWAL:   currentWAL,
		CreatedMs: time.Now().UnixMilli(),
	}

	if err := WriteManifest(sm.dataDir, manifest); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	duration := time.Since(startTime)
	log.Printf("Snapshot %d completed: %d entries in %v", sm.snapIndex, count, duration)

	sm.snapIndex++
	sm.lastSnapshotMs = time.Now().UnixMilli()

	return nil
}

// LoadSnapshot loads the latest snapshot
func (sm *SnapshotManager) LoadSnapshot(store *Store) (string, error) {
	manifest, err := ReadManifest(sm.dataDir)
	if err != nil {
		return "", err
	}

	if manifest == nil {
		// No snapshot yet
		return "", nil
	}

	snapPath := filepath.Join(sm.dataDir, manifest.Snap)
	reader, err := OpenSnapshotReader(snapPath)
	if err != nil {
		return "", fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer reader.Close()

	log.Printf("Loading snapshot %s", manifest.Snap)
	count := 0

	for {
		key, entry, err := reader.ReadEntry()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return "", fmt.Errorf("failed to read snapshot entry: %w", err)
		}

		// Skip expired entries
		if !entry.IsExpired() {
			store.data[key] = entry
			count++
		}
	}

	log.Printf("Loaded %d entries from snapshot", count)

	return manifest.NextWAL, nil
}

// CleanupOldFiles removes old snapshots and WALs
func (sm *SnapshotManager) CleanupOldFiles(keepFromWAL string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// List all snapshots except the latest
	snapFiles, err := sm.listSnapshotFiles()
	if err != nil {
		return err
	}

	// Keep only the latest snapshot
	if len(snapFiles) > 1 {
		for i := 0; i < len(snapFiles)-1; i++ {
			path := filepath.Join(sm.dataDir, snapFiles[i])
			if err := os.Remove(path); err != nil {
				log.Printf("Failed to remove old snapshot %s: %v", snapFiles[i], err)
			} else {
				log.Printf("Removed old snapshot %s", snapFiles[i])
			}
		}
	}

	return nil
}

// listSnapshotFiles lists all snapshot files in order
func (sm *SnapshotManager) listSnapshotFiles() ([]string, error) {
	files, err := os.ReadDir(sm.dataDir)
	if err != nil {
		return nil, err
	}

	var snapFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "snap-") && strings.HasSuffix(file.Name(), ".osnap") {
			snapFiles = append(snapFiles, file.Name())
		}
	}

	sort.Strings(snapFiles)
	return snapFiles, nil
}

// extractSnapIndex extracts the index from a snapshot filename
func (sm *SnapshotManager) extractSnapIndex(filename string) (int, error) {
	// Format: snap-00000001.osnap
	parts := strings.Split(filename, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid snapshot filename: %s", filename)
	}

	indexStr := strings.TrimSuffix(parts[1], ".osnap")
	return strconv.Atoi(indexStr)
}

// GetStats returns snapshot statistics
func (sm *SnapshotManager) GetStats() map[string]string {
	stats := make(map[string]string)

	snapFiles, _ := sm.listSnapshotFiles()
	stats["snapshots_total"] = strconv.Itoa(len(snapFiles))
	stats["last_snapshot_ms"] = strconv.FormatInt(sm.lastSnapshotMs, 10)

	return stats
}
