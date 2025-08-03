package storage

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAL_WriteRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create WAL
	wal, err := NewWAL(tempDir, 1, 1024*1024, "os")
	require.NoError(t, err)

	// Write records
	records := []*WALRecord{
		{
			Type:     RecordTypeSET,
			Key:      "key1",
			Value:    []byte("value1"),
			ExpiryMs: -1,
			Version:  1,
		},
		{
			Type:     RecordTypeSET,
			Key:      "key2",
			Value:    []byte("value2"),
			ExpiryMs: 1234567890,
			Version:  1,
		},
		{
			Type:     RecordTypeDEL,
			Key:      "key1",
			Version:  2,
			ExpiryMs: -1,
		},
		{
			Type:     RecordTypeEXPIRE,
			Key:      "key2",
			ExpiryMs: 9876543210,
			Version:  2,
		},
	}

	for _, record := range records {
		err := wal.Append(record)
		require.NoError(t, err)
	}

	// Close WAL
	err = wal.Close()
	require.NoError(t, err)

	// Read back records
	reader, err := OpenWALReader(wal.Path())
	require.NoError(t, err)
	defer reader.Close()

	var readRecords []*WALRecord
	for {
		record, err := reader.ReadRecord()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		readRecords = append(readRecords, record)
	}

	// Verify records
	require.Equal(t, len(records), len(readRecords))
	for i, expected := range records {
		actual := readRecords[i]
		assert.Equal(t, expected.Type, actual.Type)
		assert.Equal(t, expected.Key, actual.Key)
		assert.Equal(t, expected.Value, actual.Value)
		assert.Equal(t, expected.ExpiryMs, actual.ExpiryMs)
		assert.Equal(t, expected.Version, actual.Version)
	}
}

func TestWAL_EmptyValues(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	wal, err := NewWAL(tempDir, 1, 1024*1024, "os")
	require.NoError(t, err)

	// Write record with empty value
	record := &WALRecord{
		Type:     RecordTypeSET,
		Key:      "empty",
		Value:    []byte{},
		ExpiryMs: -1,
		Version:  1,
	}

	err = wal.Append(record)
	require.NoError(t, err)
	wal.Close()

	// Read back
	reader, err := OpenWALReader(wal.Path())
	require.NoError(t, err)
	defer reader.Close()

	readRecord, err := reader.ReadRecord()
	require.NoError(t, err)

	assert.Equal(t, record.Type, readRecord.Type)
	assert.Equal(t, record.Key, readRecord.Key)
	assert.Equal(t, []byte{}, readRecord.Value)
	assert.Equal(t, record.ExpiryMs, readRecord.ExpiryMs)
	assert.Equal(t, record.Version, readRecord.Version)
}

func TestWAL_BinaryValues(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	wal, err := NewWAL(tempDir, 1, 1024*1024, "os")
	require.NoError(t, err)

	// Write record with binary value
	binaryValue := []byte{0, 1, 2, 3, 255, 254, 253}
	record := &WALRecord{
		Type:     RecordTypeSET,
		Key:      "binary",
		Value:    binaryValue,
		ExpiryMs: -1,
		Version:  1,
	}

	err = wal.Append(record)
	require.NoError(t, err)
	wal.Close()

	// Read back
	reader, err := OpenWALReader(wal.Path())
	require.NoError(t, err)
	defer reader.Close()

	readRecord, err := reader.ReadRecord()
	require.NoError(t, err)

	assert.Equal(t, binaryValue, readRecord.Value)
}

func TestWAL_InvalidMagic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create file with invalid magic
	path := filepath.Join(tempDir, "invalid.oswal")
	file, err := os.Create(path)
	require.NoError(t, err)

	// Write invalid magic
	_, err = file.Write([]byte{0x12, 0x34, 0x56, 0x78})
	require.NoError(t, err)
	file.Close()

	// Try to read
	reader, err := OpenWALReader(path)
	require.NoError(t, err)
	defer reader.Close()

	_, err = reader.ReadRecord()
	assert.Equal(t, ErrInvalidMagic, err)
}

func TestWAL_CorruptedRecord(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	wal, err := NewWAL(tempDir, 1, 1024*1024, "os")
	require.NoError(t, err)

	// Write valid record
	record := &WALRecord{
		Type:     RecordTypeSET,
		Key:      "key1",
		Value:    []byte("value1"),
		ExpiryMs: -1,
		Version:  1,
	}

	err = wal.Append(record)
	require.NoError(t, err)
	wal.Close()

	// Corrupt the file by truncating it
	file, err := os.OpenFile(wal.Path(), os.O_WRONLY, 0)
	require.NoError(t, err)

	stat, err := file.Stat()
	require.NoError(t, err)

	// Truncate to remove the CRC
	err = file.Truncate(stat.Size() - 4)
	require.NoError(t, err)
	file.Close()

	// Try to read
	reader, err := OpenWALReader(wal.Path())
	require.NoError(t, err)
	defer reader.Close()

	_, err = reader.ReadRecord()
	assert.Error(t, err) // Should fail due to incomplete record
}

func TestWAL_Size(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	wal, err := NewWAL(tempDir, 1, 1024*1024, "os")
	require.NoError(t, err)

	// Initial size should be 0
	assert.Equal(t, int64(0), wal.Size())

	// Write a record
	record := &WALRecord{
		Type:     RecordTypeSET,
		Key:      "key1",
		Value:    []byte("value1"),
		ExpiryMs: -1,
		Version:  1,
	}

	err = wal.Append(record)
	require.NoError(t, err)

	// Size should be greater than 0
	assert.True(t, wal.Size() > 0)

	wal.Close()
}

func TestWAL_IsFull(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "osprey-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create WAL with very small max size
	wal, err := NewWAL(tempDir, 1, 100, "os")
	require.NoError(t, err)

	// Should not be full initially
	assert.False(t, wal.IsFull())

	// Write enough data to fill it
	for i := 0; i < 10; i++ {
		record := &WALRecord{
			Type:     RecordTypeSET,
			Key:      "key" + string(rune(i)),
			Value:    []byte("value" + string(rune(i))),
			ExpiryMs: -1,
			Version:  uint64(i + 1),
		}
		wal.Append(record)
	}

	// Should be full now
	assert.True(t, wal.IsFull())

	wal.Close()
}
