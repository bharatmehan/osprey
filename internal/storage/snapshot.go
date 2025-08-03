package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	SnapMagic   = 0x4F535053 // 'OSPS'
	SnapVersion = 1
)

// Manifest represents the manifest file
type Manifest struct {
	Version   int    `json:"version"`
	Snap      string `json:"snap"`
	NextWAL   string `json:"next_wal"`
	CreatedMs int64  `json:"created_ms"`
}

// SnapshotWriter writes snapshot files
type SnapshotWriter struct {
	file   *os.File
	writer io.Writer
	count  uint64
}

// NewSnapshotWriter creates a new snapshot writer
func NewSnapshotWriter(path string) (*SnapshotWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	sw := &SnapshotWriter{
		file:   file,
		writer: file,
	}

	// Write header
	if err := sw.writeHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return sw, nil
}

// writeHeader writes the snapshot file header
func (sw *SnapshotWriter) writeHeader() error {
	header := make([]byte, 14) // magic(4) + version(2) + count(8)

	binary.LittleEndian.PutUint32(header[0:4], SnapMagic)
	binary.LittleEndian.PutUint16(header[4:6], SnapVersion)
	// Count will be updated at the end
	binary.LittleEndian.PutUint64(header[6:14], 0)

	_, err := sw.writer.Write(header)
	return err
}

// WriteEntry writes a single entry to the snapshot
func (sw *SnapshotWriter) WriteEntry(key string, entry *Entry) error {
	keyBytes := []byte(key)

	// Skip expired entries
	if entry.IsExpired() {
		return nil
	}

	// Calculate sizes
	recordSize := 4 + 4 + 8 + 8 + len(keyBytes) + len(entry.Value) + 4
	record := make([]byte, recordSize)

	offset := 0

	// Key length
	binary.LittleEndian.PutUint32(record[offset:], uint32(len(keyBytes)))
	offset += 4

	// Value length
	binary.LittleEndian.PutUint32(record[offset:], uint32(len(entry.Value)))
	offset += 4

	// Expiry
	binary.LittleEndian.PutUint64(record[offset:], uint64(entry.ExpiryMs))
	offset += 8

	// Version
	binary.LittleEndian.PutUint64(record[offset:], entry.Version)
	offset += 8

	// Key
	copy(record[offset:], keyBytes)
	offset += len(keyBytes)

	// Value
	copy(record[offset:], entry.Value)
	offset += len(entry.Value)

	// CRC32C
	crc := crc32.Checksum(record[:offset], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(record[offset:], crc)

	// Write record
	if _, err := sw.writer.Write(record); err != nil {
		return err
	}

	sw.count++
	return nil
}

// Close finalizes and closes the snapshot
func (sw *SnapshotWriter) Close() error {
	// Update count in header
	if _, err := sw.file.Seek(6, 0); err != nil {
		return err
	}

	countBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBytes, sw.count)
	if _, err := sw.file.Write(countBytes); err != nil {
		return err
	}

	// Sync and close
	if err := sw.file.Sync(); err != nil {
		return err
	}

	return sw.file.Close()
}

// SnapshotReader reads snapshot files
type SnapshotReader struct {
	file   *os.File
	reader io.Reader
	count  uint64
	read   uint64
}

// OpenSnapshotReader opens a snapshot file for reading
func OpenSnapshotReader(path string) (*SnapshotReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	sr := &SnapshotReader{
		file:   file,
		reader: file,
	}

	// Read header
	if err := sr.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return sr, nil
}

// readHeader reads and validates the snapshot header
func (sr *SnapshotReader) readHeader() error {
	header := make([]byte, 14)
	if _, err := io.ReadFull(sr.reader, header); err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != SnapMagic {
		return fmt.Errorf("invalid snapshot magic: %x", magic)
	}

	version := binary.LittleEndian.Uint16(header[4:6])
	if version != SnapVersion {
		return fmt.Errorf("unsupported snapshot version: %d", version)
	}

	sr.count = binary.LittleEndian.Uint64(header[6:14])
	return nil
}

// ReadEntry reads the next entry from the snapshot
func (sr *SnapshotReader) ReadEntry() (string, *Entry, error) {
	if sr.read >= sr.count {
		return "", nil, io.EOF
	}

	// Read lengths
	lengths := make([]byte, 8)
	if _, err := io.ReadFull(sr.reader, lengths); err != nil {
		return "", nil, err
	}

	keyLen := binary.LittleEndian.Uint32(lengths[0:4])
	valLen := binary.LittleEndian.Uint32(lengths[4:8])

	// Read metadata
	metadata := make([]byte, 16)
	if _, err := io.ReadFull(sr.reader, metadata); err != nil {
		return "", nil, err
	}

	expiryMs := int64(binary.LittleEndian.Uint64(metadata[0:8]))
	version := binary.LittleEndian.Uint64(metadata[8:16])

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(sr.reader, key); err != nil {
		return "", nil, err
	}

	// Read value
	value := make([]byte, valLen)
	if valLen > 0 {
		if _, err := io.ReadFull(sr.reader, value); err != nil {
			return "", nil, err
		}
	}

	// Read and verify CRC
	crcBytes := make([]byte, 4)
	if _, err := io.ReadFull(sr.reader, crcBytes); err != nil {
		return "", nil, err
	}

	expectedCRC := binary.LittleEndian.Uint32(crcBytes)

	// Reconstruct data for CRC check
	dataLen := 4 + 4 + 8 + 8 + len(key) + len(value)
	data := make([]byte, dataLen)
	offset := 0

	binary.LittleEndian.PutUint32(data[offset:], keyLen)
	offset += 4
	binary.LittleEndian.PutUint32(data[offset:], valLen)
	offset += 4
	binary.LittleEndian.PutUint64(data[offset:], uint64(expiryMs))
	offset += 8
	binary.LittleEndian.PutUint64(data[offset:], version)
	offset += 8
	copy(data[offset:], key)
	offset += len(key)
	copy(data[offset:], value)

	actualCRC := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if actualCRC != expectedCRC {
		return "", nil, fmt.Errorf("CRC mismatch in snapshot record")
	}

	entry := &Entry{
		Value:     value,
		Version:   version,
		ExpiryMs:  expiryMs,
		SizeBytes: uint32(len(value)),
	}

	sr.read++
	return string(key), entry, nil
}

// Close closes the snapshot reader
func (sr *SnapshotReader) Close() error {
	return sr.file.Close()
}

// WriteManifest writes a manifest file
func WriteManifest(dataDir string, manifest *Manifest) error {
	// Write to temp file first
	tempPath := filepath.Join(dataDir, "MANIFEST.tmp")
	finalPath := filepath.Join(dataDir, "MANIFEST.json")

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	// Write temp file
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	// Sync directory
	dir, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	defer dir.Close()

	if err := dir.Sync(); err != nil {
		return err
	}

	// Atomic rename
	return os.Rename(tempPath, finalPath)
}

// ReadManifest reads the manifest file
func ReadManifest(dataDir string) (*Manifest, error) {
	path := filepath.Join(dataDir, "MANIFEST.json")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No manifest yet
		}
		return nil, err
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}
