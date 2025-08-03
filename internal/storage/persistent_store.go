package storage

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alignecoderepos/osprey/internal/config"
)

// PersistentStore is a Store with WAL persistence
type PersistentStore struct {
	*Store
	walManager      *WALManager
	snapshotManager *SnapshotManager
	mu              sync.Mutex

	// Sweeper control
	sweeperStop chan struct{}
	sweeperDone chan struct{}
	sweeping    int32

	// Snapshot control
	snapshotStop   chan struct{}
	snapshotDone   chan struct{}
	snapshotPaused int32
}

// NewPersistentStore creates a new persistent store
func NewPersistentStore(cfg *config.Config) (*PersistentStore, error) {
	walManager, err := NewWALManager(cfg)
	if err != nil {
		return nil, err
	}

	snapshotManager, err := NewSnapshotManager(cfg)
	if err != nil {
		return nil, err
	}

	ps := &PersistentStore{
		Store:           New(cfg),
		walManager:      walManager,
		snapshotManager: snapshotManager,
		sweeperStop:     make(chan struct{}),
		sweeperDone:     make(chan struct{}),
		snapshotStop:    make(chan struct{}),
		snapshotDone:    make(chan struct{}),
	}

	// Load data from disk
	if err := ps.recover(); err != nil {
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	// Start background tasks
	go ps.expirySweeper()
	go ps.snapshotWorker()

	return ps, nil
}

// Set stores a key-value pair with WAL persistence
func (ps *PersistentStore) Set(key string, value []byte, opts SetOptions) (uint64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// First perform the in-memory operation to get the version
	version, err := ps.Store.Set(key, value, opts)
	if err != nil {
		return 0, err
	}

	// Get the entry to get the final state
	entry, _ := ps.Store.Get(key)

	// Write to WAL
	record := &WALRecord{
		Type:     RecordTypeSET,
		Key:      key,
		Value:    value,
		ExpiryMs: entry.ExpiryMs,
		Version:  version,
	}

	if err := ps.walManager.AppendRecord(record); err != nil {
		// Rollback the in-memory change
		ps.Store.Delete(key)
		return 0, fmt.Errorf("WAL write failed: %w", err)
	}

	return version, nil
}

// Delete removes a key with WAL persistence
func (ps *PersistentStore) Delete(key string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Get the entry before deletion for version
	entry, err := ps.Store.Get(key)
	if err != nil {
		return false
	}

	deleted := ps.Store.Delete(key)
	if !deleted {
		return false
	}

	// Write to WAL
	record := &WALRecord{
		Type:     RecordTypeDEL,
		Key:      key,
		Version:  entry.Version,
		ExpiryMs: -1,
	}

	if err := ps.walManager.AppendRecord(record); err != nil {
		// We can't rollback a delete easily, log the error
		log.Printf("WAL write failed for DELETE: %v", err)
	}

	return true
}

// Expire sets a TTL with WAL persistence
func (ps *PersistentStore) Expire(key string, ttlMs int64) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Get current entry
	entry, err := ps.Store.Get(key)
	if err != nil {
		return err
	}

	err = ps.Store.Expire(key, ttlMs)
	if err != nil {
		return err
	}

	// Write to WAL
	record := &WALRecord{
		Type:     RecordTypeEXPIRE,
		Key:      key,
		ExpiryMs: time.Now().UnixMilli() + ttlMs,
		Version:  entry.Version,
	}

	if err := ps.walManager.AppendRecord(record); err != nil {
		// Rollback by removing expiry
		entry.ExpiryMs = -1
		return fmt.Errorf("WAL write failed: %w", err)
	}

	return nil
}

// Incr increments with WAL persistence
func (ps *PersistentStore) Incr(key string, delta int64) (int64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	newVal, err := ps.Store.Incr(key, delta)
	if err != nil {
		return 0, err
	}

	// Get the updated entry
	entry, _ := ps.Store.Get(key)

	// Write to WAL as a SET operation
	record := &WALRecord{
		Type:     RecordTypeSET,
		Key:      key,
		Value:    entry.Value,
		ExpiryMs: entry.ExpiryMs,
		Version:  entry.Version,
	}

	if err := ps.walManager.AppendRecord(record); err != nil {
		// Rollback
		ps.Store.Delete(key)
		return 0, fmt.Errorf("WAL write failed: %w", err)
	}

	return newVal, nil
}

// recover loads data from snapshot and WAL files
func (ps *PersistentStore) recover() error {
	// First load from snapshot if available
	nextWAL, err := ps.snapshotManager.LoadSnapshot(ps.Store)
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	// Get WAL files to replay starting from snapshot's next WAL
	walFiles, err := ps.walManager.GetWALsForReplay(nextWAL)
	if err != nil {
		return err
	}

	log.Printf("Recovering from %d WAL files", len(walFiles))

	for _, walPath := range walFiles {
		if err := ps.replayWAL(walPath); err != nil {
			log.Printf("Error replaying WAL %s: %v", walPath, err)
			// Continue with other WALs
		}
	}

	// Rebuild expiry heap
	ps.rebuildExpiryHeap()

	return nil
}

// replayWAL replays a single WAL file
func (ps *PersistentStore) replayWAL(path string) error {
	reader, err := OpenWALReader(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	count := 0
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			// Truncate at first bad record
			log.Printf("Truncating WAL at record %d due to error: %v", count, err)
			break
		}

		// Apply the record
		switch record.Type {
		case RecordTypeSET:
			ps.applySetRecord(record)
		case RecordTypeDEL:
			ps.applyDelRecord(record)
		case RecordTypeEXPIRE:
			ps.applyExpireRecord(record)
		}

		count++
	}

	log.Printf("Replayed %d records from %s", count, path)
	return nil
}

// applySetRecord applies a SET record during recovery
func (ps *PersistentStore) applySetRecord(record *WALRecord) {
	ps.Store.data[record.Key] = &Entry{
		Value:     record.Value,
		Version:   record.Version,
		ExpiryMs:  record.ExpiryMs,
		SizeBytes: uint32(len(record.Value)),
	}
}

// applyDelRecord applies a DEL record during recovery
func (ps *PersistentStore) applyDelRecord(record *WALRecord) {
	delete(ps.Store.data, record.Key)
}

// applyExpireRecord applies an EXPIRE record during recovery
func (ps *PersistentStore) applyExpireRecord(record *WALRecord) {
	if entry, exists := ps.Store.data[record.Key]; exists {
		entry.ExpiryMs = record.ExpiryMs
	}
}

// rebuildExpiryHeap rebuilds the expiry heap after recovery
func (ps *PersistentStore) rebuildExpiryHeap() {
	ps.Store.expiryHeap = &ExpiryHeap{}
	heap.Init(ps.Store.expiryHeap)

	for key, entry := range ps.Store.data {
		if entry.ExpiryMs > 0 {
			heap.Push(ps.Store.expiryHeap, &ExpiryItem{
				Key:      key,
				ExpiryMs: entry.ExpiryMs,
			})
		}
	}
}

// Close closes the persistent store
func (ps *PersistentStore) Close() error {
	// Stop background tasks
	close(ps.sweeperStop)
	close(ps.snapshotStop)
	<-ps.sweeperDone
	<-ps.snapshotDone

	return ps.walManager.Close()
}

// GetWALStats returns WAL and snapshot statistics
func (ps *PersistentStore) GetWALStats() map[string]string {
	stats := make(map[string]string)
	stats["wal_current"] = ps.walManager.GetCurrentWALName()

	// Add snapshot stats
	snapStats := ps.snapshotManager.GetStats()
	for k, v := range snapStats {
		stats[k] = v
	}

	return stats
}

// IsSnapshotPaused returns true if snapshot is in progress
func (ps *PersistentStore) IsSnapshotPaused() bool {
	return atomic.LoadInt32(&ps.snapshotPaused) == 1
}

// expirySweeper runs the background expiry sweeper
func (ps *PersistentStore) expirySweeper() {
	defer close(ps.sweeperDone)

	ticker := time.NewTicker(ps.config.SweepInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ps.sweeperStop:
			return
		case <-ticker.C:
			ps.sweepExpired()
		}
	}
}

// sweepExpired removes expired keys
func (ps *PersistentStore) sweepExpired() {
	// Mark that we're sweeping
	if !atomic.CompareAndSwapInt32(&ps.sweeping, 0, 1) {
		return // Already sweeping
	}
	defer atomic.StoreInt32(&ps.sweeping, 0)

	ps.Store.mu.Lock()
	defer ps.Store.mu.Unlock()

	now := time.Now().UnixMilli()
	deleted := 0

	// Process up to SweepBatch items
	for i := 0; i < ps.config.SweepBatch && ps.Store.expiryHeap.Len() > 0; i++ {
		// Peek at the top item
		if ps.Store.expiryHeap.Len() == 0 {
			break
		}

		top := (*ps.Store.expiryHeap)[0]
		if top.ExpiryMs > now {
			// No more expired items
			break
		}

		// Pop the expired item
		heap.Pop(ps.Store.expiryHeap)

		// Check if the key still exists and is expired
		if entry, exists := ps.Store.data[top.Key]; exists {
			if entry.IsExpired() {
				delete(ps.Store.data, top.Key)
				ps.Store.stats.ExpiredTotal++
				deleted++

				// Log to WAL
				ps.mu.Lock()
				record := &WALRecord{
					Type:     RecordTypeDEL,
					Key:      top.Key,
					Version:  entry.Version,
					ExpiryMs: -1,
				}
				if err := ps.walManager.AppendRecord(record); err != nil {
					log.Printf("Failed to log expiry deletion: %v", err)
				}
				ps.mu.Unlock()
			} else if entry.ExpiryMs > 0 {
				// Re-add to heap with new expiry time
				heap.Push(ps.Store.expiryHeap, &ExpiryItem{
					Key:      top.Key,
					ExpiryMs: entry.ExpiryMs,
				})
			}
		}
	}

	if deleted > 0 {
		log.Printf("Expiry sweeper deleted %d keys", deleted)
	}
}

// snapshotWorker runs the background snapshot worker
func (ps *PersistentStore) snapshotWorker() {
	defer close(ps.snapshotDone)

	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ps.snapshotStop:
			return
		case <-ticker.C:
			ps.maybeSnapshot()
		}
	}
}

// maybeSnapshot checks if a snapshot is needed and creates one
func (ps *PersistentStore) maybeSnapshot() {
	if !ps.config.EnableSnapshot {
		return
	}

	// Check if we need a snapshot
	// TODO: Calculate live/dead bytes properly
	walSize := ps.walManager.currentWAL.Size()
	liveBytes := int64(len(ps.Store.data)) * 1000 // Rough estimate
	deadBytes := walSize - liveBytes

	if !ps.snapshotManager.NeedsSnapshot(walSize, liveBytes, deadBytes) {
		return
	}

	// Create snapshot
	if err := ps.createSnapshot(); err != nil {
		log.Printf("Failed to create snapshot: %v", err)
	}
}

// createSnapshot creates a new snapshot
func (ps *PersistentStore) createSnapshot() error {
	log.Println("Starting snapshot...")

	// Mark snapshot as paused
	atomic.StoreInt32(&ps.snapshotPaused, 1)
	defer atomic.StoreInt32(&ps.snapshotPaused, 0)

	// Measure pause time
	pauseStart := time.Now()

	// Get current WAL before rotating
	currentWAL := ps.walManager.GetCurrentWALName()

	// Create the snapshot
	err := ps.snapshotManager.CreateSnapshot(ps.Store, currentWAL)

	pauseDuration := time.Since(pauseStart)
	if pauseDuration.Milliseconds() > int64(ps.config.BusyWarnMs) {
		log.Printf("WARNING: Snapshot pause exceeded threshold: %v", pauseDuration)
	}

	if err != nil {
		return err
	}

	// Rotate WAL after successful snapshot
	ps.walManager.mu.Lock()
	if err := ps.walManager.rotateWAL(); err != nil {
		ps.walManager.mu.Unlock()
		return fmt.Errorf("failed to rotate WAL: %w", err)
	}
	newWAL := ps.walManager.GetCurrentWALName()
	ps.walManager.mu.Unlock()

	// Clean up old files
	if err := ps.snapshotManager.CleanupOldFiles(newWAL); err != nil {
		log.Printf("Failed to cleanup old files: %v", err)
	}

	if err := ps.walManager.DeleteOldWALs(currentWAL); err != nil {
		log.Printf("Failed to delete old WALs: %v", err)
	}

	return nil
}
