package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"streaming-engine/pkg/types"
)

const (
	// Segment constants
	DefaultSegmentSize  = 1024 * 1024 * 1024 // 1GB default segment size
	IndexEntrySize      = 12                 // offset(8) + position(4)
	IndexIntervalBytes  = 4096               // Index entry every 4KB
	LogEntryHeaderSize  = 24                 // Fixed header size for log entries
	MaxIndexFileSize    = 1024 * 1024 * 10   // 10MB max index file
	SegmentFileSuffix   = ".log"
	IndexFileSuffix     = ".index"
	TimestampFileSuffix = ".timeindex"
)

// LogEntry represents a single entry in the log segment
type LogEntry struct {
	Offset    int64             `json:"offset"`
	Timestamp time.Time         `json:"timestamp"`
	KeySize   int32             `json:"key_size"`
	ValueSize int32             `json:"value_size"`
	Key       []byte            `json:"key"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers"`
	CRC32     uint32            `json:"crc32"`
}

// IndexEntry represents an entry in the index file
type IndexEntry struct {
	Offset   int64 `json:"offset"`
	Position int32 `json:"position"`
}

// TimestampIndexEntry represents an entry in the timestamp index
type TimestampIndexEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Offset    int64     `json:"offset"`
}

// Segment represents a log segment with data, index, and timestamp index files
type Segment struct {
	baseOffset int64
	dir        string
	mu         sync.RWMutex

	// File handles
	logFile       *os.File
	indexFile     *os.File
	timeIndexFile *os.File

	// Memory mapped regions
	logMmap       []byte
	indexMmap     []byte
	timeIndexMmap []byte

	// Segment state
	size          int64
	nextOffset    int64
	lastTimestamp time.Time
	closed        bool

	// Index state
	indexEntries      []IndexEntry
	timeIndexEntries  []TimestampIndexEntry
	lastIndexPosition int32

	// Configuration
	maxSize       int64
	indexInterval int32
	compression   types.CompressionType
}

// NewSegment creates a new log segment
func NewSegment(dir string, baseOffset int64, maxSize int64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	s := &Segment{
		baseOffset:    baseOffset,
		dir:           dir,
		maxSize:       maxSize,
		indexInterval: IndexIntervalBytes,
		nextOffset:    baseOffset,
		compression:   types.CompressionNone,
	}

	if err := s.openFiles(); err != nil {
		return nil, fmt.Errorf("failed to open files: %w", err)
	}

	if err := s.loadIndex(); err != nil {
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return s, nil
}

// OpenSegment opens an existing log segment
func OpenSegment(dir string, baseOffset int64) (*Segment, error) {
	s := &Segment{
		baseOffset:    baseOffset,
		dir:           dir,
		maxSize:       DefaultSegmentSize,
		indexInterval: IndexIntervalBytes,
		compression:   types.CompressionNone,
	}

	if err := s.openFiles(); err != nil {
		return nil, fmt.Errorf("failed to open files: %w", err)
	}

	if err := s.loadIndex(); err != nil {
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	if err := s.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover segment: %w", err)
	}

	return s, nil
}

// Append appends a batch of messages to the segment
func (s *Segment) Append(messages []*types.Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, fmt.Errorf("segment is closed")
	}

	// Check if segment has enough space
	estimatedSize := s.estimateBatchSize(messages)
	if s.size+estimatedSize > s.maxSize {
		return 0, fmt.Errorf("segment full")
	}

	startOffset := s.nextOffset
	position := s.size

	for _, msg := range messages {
		entry := &LogEntry{
			Offset:    s.nextOffset,
			Timestamp: msg.Timestamp,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   msg.Headers,
			KeySize:   int32(len(msg.Key)),
			ValueSize: int32(len(msg.Value)),
		}

		// Calculate CRC32
		entry.CRC32 = s.calculateCRC32(entry)

		// Serialize and write entry
		data, err := s.serializeEntry(entry)
		if err != nil {
			return 0, fmt.Errorf("failed to serialize entry: %w", err)
		}

		if _, err := s.logFile.Write(data); err != nil {
			return 0, fmt.Errorf("failed to write entry: %w", err)
		}

		// Update index if needed
		if s.size-s.lastIndexPosition >= s.indexInterval {
			if err := s.addIndexEntry(s.nextOffset, int32(position)); err != nil {
				return 0, fmt.Errorf("failed to add index entry: %w", err)
			}
		}

		// Update segment state
		s.size += int64(len(data))
		s.nextOffset++
		s.lastTimestamp = entry.Timestamp
		position += int64(len(data))
	}

	// Sync to disk
	if err := s.logFile.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync log file: %w", err)
	}

	return startOffset, nil
}

// Read reads messages from the segment starting at the given offset
func (s *Segment) Read(startOffset, maxOffset int64, maxBytes int32) ([]*types.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("segment is closed")
	}

	if startOffset < s.baseOffset || startOffset >= s.nextOffset {
		return nil, fmt.Errorf("offset out of range: %d", startOffset)
	}

	// Find starting position using index
	position, err := s.findPosition(startOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to find position: %w", err)
	}

	var messages []*types.Message
	var totalBytes int32

	// Read entries from the position
	reader := &SegmentReader{
		segment:  s,
		position: position,
	}

	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read entry: %w", err)
		}

		if entry.Offset < startOffset {
			continue
		}

		if entry.Offset >= maxOffset {
			break
		}

		// Check size limit
		entrySize := int32(LogEntryHeaderSize + entry.KeySize + entry.ValueSize)
		if totalBytes+entrySize > maxBytes && len(messages) > 0 {
			break
		}

		msg := &types.Message{
			Key:       entry.Key,
			Value:     entry.Value,
			Headers:   entry.Headers,
			Timestamp: entry.Timestamp,
			Offset:    entry.Offset,
		}

		messages = append(messages, msg)
		totalBytes += entrySize
	}

	return messages, nil
}

// FindOffsetByTimestamp finds the offset closest to the given timestamp
func (s *Segment) FindOffsetByTimestamp(timestamp time.Time) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.timeIndexEntries) == 0 {
		return s.baseOffset, nil
	}

	// Binary search in time index
	idx := sort.Search(len(s.timeIndexEntries), func(i int) bool {
		return s.timeIndexEntries[i].Timestamp.After(timestamp)
	})

	if idx == 0 {
		return s.baseOffset, nil
	}

	if idx == len(s.timeIndexEntries) {
		return s.timeIndexEntries[len(s.timeIndexEntries)-1].Offset, nil
	}

	return s.timeIndexEntries[idx-1].Offset, nil
}

// Size returns the current size of the segment
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// BaseOffset returns the base offset of the segment
func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

// NextOffset returns the next offset to be written
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

// IsFull checks if the segment is full
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size >= s.maxSize
}

// Close closes the segment and releases resources
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	var errs []error

	// Sync all files
	if s.logFile != nil {
		if err := s.logFile.Sync(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.indexFile != nil {
		if err := s.indexFile.Sync(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.timeIndexFile != nil {
		if err := s.timeIndexFile.Sync(); err != nil {
			errs = append(errs, err)
		}
	}

	// Unmap memory-mapped regions
	if s.logMmap != nil {
		if err := syscall.Munmap(s.logMmap); err != nil {
			errs = append(errs, err)
		}
	}

	if s.indexMmap != nil {
		if err := syscall.Munmap(s.indexMmap); err != nil {
			errs = append(errs, err)
		}
	}

	if s.timeIndexMmap != nil {
		if err := syscall.Munmap(s.timeIndexMmap); err != nil {
			errs = append(errs, err)
		}
	}

	// Close files
	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.indexFile != nil {
		if err := s.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.timeIndexFile != nil {
		if err := s.timeIndexFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	s.closed = true

	if len(errs) > 0 {
		return fmt.Errorf("errors closing segment: %v", errs)
	}

	return nil
}

// Private methods

func (s *Segment) openFiles() error {
	logPath := s.logFilePath()
	indexPath := s.indexFilePath()
	timeIndexPath := s.timeIndexFilePath()

	// Open log file
	var err error
	s.logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// Open index file
	s.indexFile, err = os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open index file: %w", err)
	}

	// Open time index file
	s.timeIndexFile, err = os.OpenFile(timeIndexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open time index file: %w", err)
	}

	// Get current file sizes
	if stat, err := s.logFile.Stat(); err == nil {
		s.size = stat.Size()
	}

	return nil
}

func (s *Segment) loadIndex() error {
	// Load index entries
	indexData, err := io.ReadAll(s.indexFile)
	if err != nil {
		return fmt.Errorf("failed to read index file: %w", err)
	}

	for i := 0; i < len(indexData); i += IndexEntrySize {
		if i+IndexEntrySize > len(indexData) {
			break
		}

		offset := binary.BigEndian.Uint64(indexData[i : i+8])
		position := binary.BigEndian.Uint32(indexData[i+8 : i+12])

		s.indexEntries = append(s.indexEntries, IndexEntry{
			Offset:   int64(offset),
			Position: int32(position),
		})
	}

	// Load time index entries
	timeIndexData, err := io.ReadAll(s.timeIndexFile)
	if err != nil {
		return fmt.Errorf("failed to read time index file: %w", err)
	}

	for i := 0; i < len(timeIndexData); i += 16 { // 8 bytes timestamp + 8 bytes offset
		if i+16 > len(timeIndexData) {
			break
		}

		timestamp := int64(binary.BigEndian.Uint64(timeIndexData[i : i+8]))
		offset := binary.BigEndian.Uint64(timeIndexData[i+8 : i+16])

		s.timeIndexEntries = append(s.timeIndexEntries, TimestampIndexEntry{
			Timestamp: time.Unix(0, timestamp),
			Offset:    int64(offset),
		})
	}

	return nil
}

func (s *Segment) recover() error {
	// Scan log file to rebuild state
	reader := &SegmentReader{
		segment:  s,
		position: 0,
	}

	var lastOffset int64 = s.baseOffset - 1
	var lastTimestamp time.Time

	for {
		entry, err := reader.ReadEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read entry during recovery: %w", err)
		}

		lastOffset = entry.Offset
		lastTimestamp = entry.Timestamp
	}

	s.nextOffset = lastOffset + 1
	s.lastTimestamp = lastTimestamp

	return nil
}

func (s *Segment) findPosition(offset int64) (int64, error) {
	if len(s.indexEntries) == 0 {
		return 0, nil
	}

	// Binary search in index
	idx := sort.Search(len(s.indexEntries), func(i int) bool {
		return s.indexEntries[i].Offset > offset
	})

	if idx == 0 {
		return 0, nil
	}

	return int64(s.indexEntries[idx-1].Position), nil
}

func (s *Segment) addIndexEntry(offset int64, position int32) error {
	// Write to index file
	entry := make([]byte, IndexEntrySize)
	binary.BigEndian.PutUint64(entry[0:8], uint64(offset))
	binary.BigEndian.PutUint32(entry[8:12], uint32(position))

	if _, err := s.indexFile.Write(entry); err != nil {
		return err
	}

	// Add to in-memory index
	s.indexEntries = append(s.indexEntries, IndexEntry{
		Offset:   offset,
		Position: position,
	})

	s.lastIndexPosition = position

	// Add time index entry
	timeEntry := make([]byte, 16)
	binary.BigEndian.PutUint64(timeEntry[0:8], uint64(s.lastTimestamp.UnixNano()))
	binary.BigEndian.PutUint64(timeEntry[8:16], uint64(offset))

	if _, err := s.timeIndexFile.Write(timeEntry); err != nil {
		return err
	}

	s.timeIndexEntries = append(s.timeIndexEntries, TimestampIndexEntry{
		Timestamp: s.lastTimestamp,
		Offset:    offset,
	})

	return nil
}

func (s *Segment) serializeEntry(entry *LogEntry) ([]byte, error) {
	headerSize := LogEntryHeaderSize
	headersSize := s.calculateHeadersSize(entry.Headers)
	totalSize := headerSize + int(entry.KeySize) + int(entry.ValueSize) + headersSize

	buf := make([]byte, totalSize)

	// Write header
	binary.BigEndian.PutUint64(buf[0:8], uint64(entry.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(entry.Timestamp.UnixNano()))
	binary.BigEndian.PutUint32(buf[16:20], uint32(entry.KeySize))
	binary.BigEndian.PutUint32(buf[20:24], uint32(entry.ValueSize))

	offset := headerSize

	// Write key
	copy(buf[offset:offset+int(entry.KeySize)], entry.Key)
	offset += int(entry.KeySize)

	// Write value
	copy(buf[offset:offset+int(entry.ValueSize)], entry.Value)
	offset += int(entry.ValueSize)

	// Write headers
	headersData := s.serializeHeaders(entry.Headers)
	copy(buf[offset:], headersData)

	return buf, nil
}

func (s *Segment) calculateCRC32(entry *LogEntry) uint32 {
	hash := crc32.NewIEEE()
	hash.Write(entry.Key)
	hash.Write(entry.Value)

	for k, v := range entry.Headers {
		hash.Write([]byte(k))
		hash.Write([]byte(v))
	}

	return hash.Sum32()
}

func (s *Segment) calculateHeadersSize(headers map[string]string) int {
	size := 4 // headers count
	for k, v := range headers {
		size += 4 + len(k) + 4 + len(v) // key length + key + value length + value
	}
	return size
}

func (s *Segment) serializeHeaders(headers map[string]string) []byte {
	if len(headers) == 0 {
		return []byte{0, 0, 0, 0} // count = 0
	}

	size := s.calculateHeadersSize(headers)
	buf := make([]byte, size)

	binary.BigEndian.PutUint32(buf[0:4], uint32(len(headers)))
	offset := 4

	for k, v := range headers {
		// Write key
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(k)))
		offset += 4
		copy(buf[offset:offset+len(k)], []byte(k))
		offset += len(k)

		// Write value
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(v)))
		offset += 4
		copy(buf[offset:offset+len(v)], []byte(v))
		offset += len(v)
	}

	return buf
}

func (s *Segment) estimateBatchSize(messages []*types.Message) int64 {
	var size int64
	for _, msg := range messages {
		size += int64(LogEntryHeaderSize + len(msg.Key) + len(msg.Value))
		for k, v := range msg.Headers {
			size += int64(8 + len(k) + len(v)) // overhead + key + value
		}
	}
	return size
}

func (s *Segment) logFilePath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, SegmentFileSuffix))
}

func (s *Segment) indexFilePath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, IndexFileSuffix))
}

func (s *Segment) timeIndexFilePath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, TimestampFileSuffix))
}

// SegmentReader provides sequential reading of log entries
type SegmentReader struct {
	segment  *Segment
	position int64
	buf      *bufio.Reader
}

// ReadEntry reads the next log entry
func (r *SegmentReader) ReadEntry() (*LogEntry, error) {
	if r.buf == nil {
		if _, err := r.segment.logFile.Seek(r.position, 0); err != nil {
			return nil, err
		}
		r.buf = bufio.NewReader(r.segment.logFile)
	}

	// Read header
	header := make([]byte, LogEntryHeaderSize)
	if _, err := io.ReadFull(r.buf, header); err != nil {
		return nil, err
	}

	entry := &LogEntry{
		Offset:    int64(binary.BigEndian.Uint64(header[0:8])),
		Timestamp: time.Unix(0, int64(binary.BigEndian.Uint64(header[8:16]))),
		KeySize:   int32(binary.BigEndian.Uint32(header[16:20])),
		ValueSize: int32(binary.BigEndian.Uint32(header[20:24])),
	}

	// Read key
	if entry.KeySize > 0 {
		entry.Key = make([]byte, entry.KeySize)
		if _, err := io.ReadFull(r.buf, entry.Key); err != nil {
			return nil, err
		}
	}

	// Read value
	if entry.ValueSize > 0 {
		entry.Value = make([]byte, entry.ValueSize)
		if _, err := io.ReadFull(r.buf, entry.Value); err != nil {
			return nil, err
		}
	}

	// Read headers
	var err error
	entry.Headers, err = r.readHeaders()
	if err != nil {
		return nil, err
	}

	r.position += int64(LogEntryHeaderSize + entry.KeySize + entry.ValueSize)

	return entry, nil
}

func (r *SegmentReader) readHeaders() (map[string]string, error) {
	countBytes := make([]byte, 4)
	if _, err := io.ReadFull(r.buf, countBytes); err != nil {
		return nil, err
	}

	count := binary.BigEndian.Uint32(countBytes)
	headers := make(map[string]string, count)

	for i := uint32(0); i < count; i++ {
		// Read key
		keyLenBytes := make([]byte, 4)
		if _, err := io.ReadFull(r.buf, keyLenBytes); err != nil {
			return nil, err
		}

		keyLen := binary.BigEndian.Uint32(keyLenBytes)
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r.buf, keyBytes); err != nil {
			return nil, err
		}

		// Read value
		valueLenBytes := make([]byte, 4)
		if _, err := io.ReadFull(r.buf, valueLenBytes); err != nil {
			return nil, err
		}

		valueLen := binary.BigEndian.Uint32(valueLenBytes)
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(r.buf, valueBytes); err != nil {
			return nil, err
		}

		headers[string(keyBytes)] = string(valueBytes)
	}

	return headers, nil
}
