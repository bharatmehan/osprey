package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	WALMagic   = 0x4F535057 // 'OSPW'
	WALVersion = 1
	
	// Record types
	RecordTypeSET    = 0
	RecordTypeDEL    = 1
	RecordTypeEXPIRE = 2
)

var (
	ErrCorruptedRecord = errors.New("corrupted WAL record")
	ErrInvalidMagic    = errors.New("invalid WAL magic")
	ErrInvalidVersion  = errors.New("invalid WAL version")
)

// WALRecord represents a single WAL record
type WALRecord struct {
	Type      uint8
	Key       string
	Value     []byte
	ExpiryMs  int64
	Version   uint64
}

// WAL represents the write-ahead log
type WAL struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	size       int64
	maxSize    int64
	
	// Sync policy
	syncPolicy string
	lastSync   time.Time
	syncBytes  int64
	
	// Buffering
	buffer     []byte
	bufferSize int64
}

// NewWAL creates a new WAL file
func NewWAL(dir string, index int, maxSize int64, syncPolicy string) (*WAL, error) {
	filename := fmt.Sprintf("wal-%08d.oswal", index)
	path := filepath.Join(dir, filename)
	
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	
	return &WAL{
		file:       file,
		path:       path,
		size:       stat.Size(),
		maxSize:    maxSize,
		syncPolicy: syncPolicy,
		lastSync:   time.Now(),
		buffer:     make([]byte, 0, 64*1024), // 64KB buffer
	}, nil
}

// Append appends a record to the WAL
func (w *WAL) Append(record *WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Serialize record
	data, err := w.serializeRecord(record)
	if err != nil {
		return err
	}
	
	// Write to file
	n, err := w.file.Write(data)
	if err != nil {
		return err
	}
	
	w.size += int64(n)
	w.syncBytes += int64(n)
	
	// Handle sync policy
	if err := w.maybeSync(); err != nil {
		return err
	}
	
	return nil
}

// serializeRecord serializes a WAL record
func (w *WAL) serializeRecord(record *WALRecord) ([]byte, error) {
	keyBytes := []byte(record.Key)
	
	// Calculate total size
	totalSize := 4 + 2 + 1 + 4 + 4 + 8 + 8 + len(keyBytes) + len(record.Value) + 4
	buf := make([]byte, totalSize)
	
	offset := 0
	
	// Magic
	binary.LittleEndian.PutUint32(buf[offset:], WALMagic)
	offset += 4
	
	// Version
	binary.LittleEndian.PutUint16(buf[offset:], WALVersion)
	offset += 2
	
	// Record type
	buf[offset] = record.Type
	offset += 1
	
	// Key length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(keyBytes)))
	offset += 4
	
	// Value length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(record.Value)))
	offset += 4
	
	// Expiry
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.ExpiryMs))
	offset += 8
	
	// Version
	binary.LittleEndian.PutUint64(buf[offset:], record.Version)
	offset += 8
	
	// Key
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)
	
	// Value
	copy(buf[offset:], record.Value)
	offset += len(record.Value)
	
	// CRC32C (Castagnoli)
	crc := crc32.Checksum(buf[6:offset], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[offset:], crc)
	
	return buf, nil
}

// maybeSync syncs the WAL based on the sync policy
func (w *WAL) maybeSync() error {
	switch w.syncPolicy {
	case "always":
		return w.file.Sync()
		
	case "batch":
		// Sync if enough time has passed or enough bytes written
		if time.Since(w.lastSync) > 100*time.Millisecond || w.syncBytes > 1024*1024 {
			err := w.file.Sync()
			w.lastSync = time.Now()
			w.syncBytes = 0
			return err
		}
		
	case "os":
		// Let OS handle it
	}
	
	return nil
}

// Size returns the current size of the WAL
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// IsFull checks if the WAL has reached its max size
func (w *WAL) IsFull() bool {
	return w.Size() >= w.maxSize
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.syncPolicy != "os" {
		w.file.Sync()
	}
	
	return w.file.Close()
}

// Path returns the WAL file path
func (w *WAL) Path() string {
	return w.path
}

// WALReader reads WAL records
type WALReader struct {
	file   *os.File
	reader *io.Reader
}

// OpenWALReader opens a WAL file for reading
func OpenWALReader(path string) (*WALReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	
	var reader io.Reader = file
	return &WALReader{
		file:   file,
		reader: &reader,
	}, nil
}

// ReadRecord reads the next record from the WAL
func (r *WALReader) ReadRecord() (*WALRecord, error) {
	reader := *r.reader
	
	// Read header
	header := make([]byte, 7) // magic(4) + version(2) + type(1)
	if _, err := io.ReadFull(reader, header); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	
	// Check magic
	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != WALMagic {
		return nil, ErrInvalidMagic
	}
	
	// Check version
	version := binary.LittleEndian.Uint16(header[4:6])
	if version != WALVersion {
		return nil, ErrInvalidVersion
	}
	
	recordType := header[6]
	
	// Read lengths
	lengths := make([]byte, 8) // key_len(4) + val_len(4)
	if _, err := io.ReadFull(reader, lengths); err != nil {
		return nil, err
	}
	
	keyLen := binary.LittleEndian.Uint32(lengths[0:4])
	valLen := binary.LittleEndian.Uint32(lengths[4:8])
	
	// Read metadata
	metadata := make([]byte, 16) // expiry(8) + version(8)
	if _, err := io.ReadFull(reader, metadata); err != nil {
		return nil, err
	}
	
	expiryMs := int64(binary.LittleEndian.Uint64(metadata[0:8]))
	recordVersion := binary.LittleEndian.Uint64(metadata[8:16])
	
	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, err
	}
	
	// Read value
	value := make([]byte, valLen)
	if valLen > 0 {
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
	}
	
	// Read CRC
	crcBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, crcBytes); err != nil {
		return nil, err
	}
	
	expectedCRC := binary.LittleEndian.Uint32(crcBytes)
	
	// Verify CRC
	dataLen := 1 + 4 + 4 + 8 + 8 + len(key) + len(value)
	data := make([]byte, dataLen)
	offset := 0
	
	data[offset] = recordType
	offset += 1
	binary.LittleEndian.PutUint32(data[offset:], keyLen)
	offset += 4
	binary.LittleEndian.PutUint32(data[offset:], valLen)
	offset += 4
	binary.LittleEndian.PutUint64(data[offset:], uint64(expiryMs))
	offset += 8
	binary.LittleEndian.PutUint64(data[offset:], recordVersion)
	offset += 8
	copy(data[offset:], key)
	offset += len(key)
	copy(data[offset:], value)
	
	actualCRC := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if actualCRC != expectedCRC {
		return nil, ErrCorruptedRecord
	}
	
	return &WALRecord{
		Type:     recordType,
		Key:      string(key),
		Value:    value,
		ExpiryMs: expiryMs,
		Version:  recordVersion,
	}, nil
}

// Close closes the WAL reader
func (r *WALReader) Close() error {
	return r.file.Close()
}