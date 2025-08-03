package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bharatmehan/osprey/internal/config"
)

// WALManager manages WAL files and rotation
type WALManager struct {
	mu         sync.Mutex
	dataDir    string
	currentWAL *WAL
	walIndex   int
	config     *config.Config
}

// NewWALManager creates a new WAL manager
func NewWALManager(cfg *config.Config) (*WALManager, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	manager := &WALManager{
		dataDir: cfg.DataDir,
		config:  cfg,
	}

	// Find the next WAL index
	walFiles, err := manager.listWALFiles()
	if err != nil {
		return nil, err
	}

	if len(walFiles) > 0 {
		// Extract index from last WAL file
		lastWAL := walFiles[len(walFiles)-1]
		index, err := manager.extractWALIndex(lastWAL)
		if err != nil {
			return nil, err
		}
		manager.walIndex = index + 1
	} else {
		manager.walIndex = 1
	}

	// Create initial WAL
	wal, err := NewWAL(cfg.DataDir, manager.walIndex, cfg.WALMaxBytes, cfg.SyncPolicy)
	if err != nil {
		return nil, err
	}
	manager.currentWAL = wal

	return manager, nil
}

// AppendRecord appends a record to the current WAL
func (m *WALManager) AppendRecord(record *WALRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we need to rotate
	if m.currentWAL.IsFull() {
		if err := m.rotateWAL(); err != nil {
			return err
		}
	}

	return m.currentWAL.Append(record)
}

// rotateWAL rotates to a new WAL file
func (m *WALManager) rotateWAL() error {
	// Close current WAL
	if err := m.currentWAL.Close(); err != nil {
		return err
	}

	// Create new WAL
	m.walIndex++
	wal, err := NewWAL(m.config.DataDir, m.walIndex, m.config.WALMaxBytes, m.config.SyncPolicy)
	if err != nil {
		return err
	}

	m.currentWAL = wal
	return nil
}

// listWALFiles lists all WAL files in order
func (m *WALManager) listWALFiles() ([]string, error) {
	files, err := os.ReadDir(m.dataDir)
	if err != nil {
		return nil, err
	}

	var walFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "wal-") && strings.HasSuffix(file.Name(), ".oswal") {
			walFiles = append(walFiles, file.Name())
		}
	}

	sort.Strings(walFiles)
	return walFiles, nil
}

// extractWALIndex extracts the index from a WAL filename
func (m *WALManager) extractWALIndex(filename string) (int, error) {
	// Format: wal-00000001.oswal
	parts := strings.Split(filename, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid WAL filename: %s", filename)
	}

	indexStr := strings.TrimSuffix(parts[1], ".oswal")
	return strconv.Atoi(indexStr)
}

// GetWALsForReplay returns all WAL files that need to be replayed
func (m *WALManager) GetWALsForReplay(startWAL string) ([]string, error) {
	walFiles, err := m.listWALFiles()
	if err != nil {
		return nil, err
	}

	if startWAL == "" {
		// Return all WAL files
		var paths []string
		for _, file := range walFiles {
			paths = append(paths, filepath.Join(m.dataDir, file))
		}
		return paths, nil
	}

	// Find starting point
	startIndex := -1
	for i, file := range walFiles {
		if file == startWAL {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return nil, fmt.Errorf("start WAL not found: %s", startWAL)
	}

	// Return WALs from starting point
	var paths []string
	for i := startIndex; i < len(walFiles); i++ {
		paths = append(paths, filepath.Join(m.dataDir, walFiles[i]))
	}

	return paths, nil
}

// DeleteOldWALs deletes WAL files older than the specified WAL
func (m *WALManager) DeleteOldWALs(keepFromWAL string) error {
	walFiles, err := m.listWALFiles()
	if err != nil {
		return err
	}

	for _, file := range walFiles {
		if file < keepFromWAL {
			path := filepath.Join(m.dataDir, file)
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close closes the WAL manager
func (m *WALManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentWAL != nil {
		return m.currentWAL.Close()
	}
	return nil
}

// GetCurrentWALName returns the name of the current WAL file
func (m *WALManager) GetCurrentWALName() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.currentWAL != nil {
		return filepath.Base(m.currentWAL.Path())
	}
	return ""
}
