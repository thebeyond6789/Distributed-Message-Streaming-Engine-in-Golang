package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"streaming-engine/pkg/types"
)

// LogEngine represents the main log storage engine
type LogEngine struct {
	dir               string
	segments          map[int32]*PartitionLog // partition -> log
	mu                sync.RWMutex
	config            *LogConfig
	compactionManager *CompactionManager
	snapshotManager   *SnapshotManager
	closed            bool
}

// PartitionLog represents the log for a single partition
type PartitionLog struct {
	partition     int32
	dir           string
	segments      []*Segment
	activeSegment *Segment
	mu            sync.RWMutex
	config        *LogConfig
	baseOffset    int64
	nextOffset    int64
	size          int64
}

// LogConfig represents configuration for the log engine
type LogConfig struct {
	Dir                string                `json:"dir"`
	SegmentSize        int64                 `json:"segment_size"`
	RetentionTime      time.Duration         `json:"retention_time"`
	RetentionSize      int64                 `json:"retention_size"`
	CleanupInterval    time.Duration         `json:"cleanup_interval"`
	CompactionEnabled  bool                  `json:"compaction_enabled"`
	CompactionInterval time.Duration         `json:"compaction_interval"`
	CompressionType    types.CompressionType `json:"compression_type"`
	FlushInterval      time.Duration         `json:"flush_interval"`
	MaxMessageSize     int32                 `json:"max_message_size"`
	IndexIntervalBytes int32                 `json:"index_interval_bytes"`
	FileDeleteDelayMs  int64                 `json:"file_delete_delay_ms"`
	MaxIndexSize       int64                 `json:"max_index_size"`
}

// AppendResult represents the result of an append operation
type AppendResult struct {
	BaseOffset     int64           `json:"base_offset"`
	LastOffset     int64           `json:"last_offset"`
	LogAppendTime  time.Time       `json:"log_append_time"`
	LogStartOffset int64           `json:"log_start_offset"`
	ErrorCode      types.ErrorCode `json:"error_code"`
}

// ReadResult represents the result of a read operation
type ReadResult struct {
	Messages         []*types.Message `json:"messages"`
	NextOffset       int64            `json:"next_offset"`
	HighWatermark    int64            `json:"high_watermark"`
	LastStableOffset int64            `json:"last_stable_offset"`
	LogStartOffset   int64            `json:"log_start_offset"`
	ErrorCode        types.ErrorCode  `json:"error_code"`
}

// DefaultLogConfig returns default log configuration
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		Dir:                "./data",
		SegmentSize:        DefaultSegmentSize,
		RetentionTime:      24 * 7 * time.Hour,      // 7 days
		RetentionSize:      1024 * 1024 * 1024 * 10, // 10GB
		CleanupInterval:    5 * time.Minute,
		CompactionEnabled:  true,
		CompactionInterval: 30 * time.Minute,
		CompressionType:    types.CompressionNone,
		FlushInterval:      time.Second,
		MaxMessageSize:     1024 * 1024, // 1MB
		IndexIntervalBytes: IndexIntervalBytes,
		FileDeleteDelayMs:  60000, // 1 minute
		MaxIndexSize:       MaxIndexFileSize,
	}
}

// NewLogEngine creates a new log engine
func NewLogEngine(config *LogConfig) (*LogEngine, error) {
	if config == nil {
		config = DefaultLogConfig()
	}

	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	engine := &LogEngine{
		dir:      config.Dir,
		segments: make(map[int32]*PartitionLog),
		config:   config,
	}

	// Initialize compaction manager
	engine.compactionManager = NewCompactionManager(config)

	// Initialize snapshot manager
	engine.snapshotManager = NewSnapshotManager(config.Dir)

	// Load existing partitions
	if err := engine.loadPartitions(); err != nil {
		return nil, fmt.Errorf("failed to load partitions: %w", err)
	}

	// Start background tasks
	engine.startBackgroundTasks()

	return engine, nil
}

// Append appends messages to the specified partition
func (e *LogEngine) Append(partition int32, messages []*types.Message) (*AppendResult, error) {
	e.mu.RLock()
	log, exists := e.segments[partition]
	e.mu.RUnlock()

	if !exists {
		// Create new partition log
		var err error
		log, err = e.createPartitionLog(partition)
		if err != nil {
			return nil, fmt.Errorf("failed to create partition log: %w", err)
		}
	}

	return log.Append(messages)
}

// Read reads messages from the specified partition
func (e *LogEngine) Read(partition int32, offset int64, maxBytes int) (*ReadResult, error) {
	e.mu.RLock()
	log, exists := e.segments[partition]
	e.mu.RUnlock()

	if !exists {
		return &ReadResult{
			Messages:  []*types.Message{},
			ErrorCode: types.ErrUnknownTopicOrPartition,
		}, nil
	}

	return log.Read(offset, maxBytes)
}

// FindOffsetByTimestamp finds the offset closest to the given timestamp
func (e *LogEngine) FindOffsetByTimestamp(partition int32, timestamp time.Time) (int64, error) {
	e.mu.RLock()
	log, exists := e.segments[partition]
	e.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("partition %d not found", partition)
	}

	return log.FindOffsetByTimestamp(timestamp)
}

// GetHighWatermark returns the high watermark for the partition
func (e *LogEngine) GetHighWatermark(partition int32) (int64, error) {
	e.mu.RLock()
	log, exists := e.segments[partition]
	e.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("partition %d not found", partition)
	}

	return log.GetHighWatermark(), nil
}

// GetLogStartOffset returns the log start offset for the partition
func (e *LogEngine) GetLogStartOffset(partition int32) (int64, error) {
	e.mu.RLock()
	log, exists := e.segments[partition]
	e.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("partition %d not found", partition)
	}

	return log.GetLogStartOffset(), nil
}

// CreateSnapshot creates a snapshot of the current state
func (e *LogEngine) CreateSnapshot() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.snapshotManager.CreateSnapshot(e.segments)
}

// RestoreFromSnapshot restores state from a snapshot
func (e *LogEngine) RestoreFromSnapshot(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	segments, err := e.snapshotManager.RestoreSnapshot(path)
	if err != nil {
		return err
	}

	e.segments = segments
	return nil
}

// Truncate truncates the log at the given offset
func (e *LogEngine) Truncate(partition int32, offset int64) error {
	e.mu.RLock()
	log, exists := e.segments[partition]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("partition %d not found", partition)
	}

	return log.Truncate(offset)
}

// Close closes the log engine
func (e *LogEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	var errs []error

	// Close all partition logs
	for _, log := range e.segments {
		if err := log.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Stop background tasks
	e.stopBackgroundTasks()

	e.closed = true

	if len(errs) > 0 {
		return fmt.Errorf("errors closing log engine: %v", errs)
	}

	return nil
}

// GetPartitions returns all partition numbers
func (e *LogEngine) GetPartitions() []int32 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	partitions := make([]int32, 0, len(e.segments))
	for partition := range e.segments {
		partitions = append(partitions, partition)
	}

	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})

	return partitions
}

// Private methods

func (e *LogEngine) loadPartitions() error {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return err
	}

	partitionDirs := make(map[int32]string)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Parse partition number from directory name
		if strings.HasPrefix(entry.Name(), "partition-") {
			partitionStr := strings.TrimPrefix(entry.Name(), "partition-")
			partition, err := strconv.ParseInt(partitionStr, 10, 32)
			if err != nil {
				continue
			}

			partitionDirs[int32(partition)] = filepath.Join(e.dir, entry.Name())
		}
	}

	// Load each partition
	for partition, dir := range partitionDirs {
		log, err := e.loadPartitionLog(partition, dir)
		if err != nil {
			return fmt.Errorf("failed to load partition %d: %w", partition, err)
		}

		e.segments[partition] = log
	}

	return nil
}

func (e *LogEngine) createPartitionLog(partition int32) (*PartitionLog, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if log, exists := e.segments[partition]; exists {
		return log, nil
	}

	dir := filepath.Join(e.config.Dir, fmt.Sprintf("partition-%d", partition))
	log, err := NewPartitionLog(partition, dir, e.config)
	if err != nil {
		return nil, err
	}

	e.segments[partition] = log
	return log, nil
}

func (e *LogEngine) loadPartitionLog(partition int32, dir string) (*PartitionLog, error) {
	return LoadPartitionLog(partition, dir, e.config)
}

func (e *LogEngine) startBackgroundTasks() {
	// Start cleanup task
	go e.cleanupTask()

	// Start compaction task if enabled
	if e.config.CompactionEnabled {
		go e.compactionTask()
	}
}

func (e *LogEngine) stopBackgroundTasks() {
	// Background tasks will stop when the engine is closed
	// In a production system, you'd want proper signaling here
}

func (e *LogEngine) cleanupTask() {
	ticker := time.NewTicker(e.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		if e.closed {
			break
		}

		e.performCleanup()
	}
}

func (e *LogEngine) compactionTask() {
	ticker := time.NewTicker(e.config.CompactionInterval)
	defer ticker.Stop()

	for range ticker.C {
		if e.closed {
			break
		}

		e.performCompaction()
	}
}

func (e *LogEngine) performCleanup() {
	e.mu.RLock()
	partitions := make([]*PartitionLog, 0, len(e.segments))
	for _, log := range e.segments {
		partitions = append(partitions, log)
	}
	e.mu.RUnlock()

	for _, log := range partitions {
		log.Cleanup()
	}
}

func (e *LogEngine) performCompaction() {
	e.mu.RLock()
	partitions := make([]*PartitionLog, 0, len(e.segments))
	for _, log := range e.segments {
		partitions = append(partitions, log)
	}
	e.mu.RUnlock()

	for _, log := range partitions {
		if err := e.compactionManager.CompactPartition(log); err != nil {
			// Log error but continue
			fmt.Printf("Compaction failed for partition %d: %v\n", log.partition, err)
		}
	}
}

// PartitionLog implementation

// NewPartitionLog creates a new partition log
func NewPartitionLog(partition int32, dir string, config *LogConfig) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	log := &PartitionLog{
		partition:  partition,
		dir:        dir,
		config:     config,
		segments:   []*Segment{},
		baseOffset: 0,
		nextOffset: 0,
	}

	// Create initial segment
	segment, err := NewSegment(dir, 0, config.SegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial segment: %w", err)
	}

	log.segments = append(log.segments, segment)
	log.activeSegment = segment

	return log, nil
}

// LoadPartitionLog loads an existing partition log
func LoadPartitionLog(partition int32, dir string, config *LogConfig) (*PartitionLog, error) {
	log := &PartitionLog{
		partition: partition,
		dir:       dir,
		config:    config,
		segments:  []*Segment{},
	}

	// Load existing segments
	if err := log.loadSegments(); err != nil {
		return nil, fmt.Errorf("failed to load segments: %w", err)
	}

	// Set active segment
	if len(log.segments) > 0 {
		log.activeSegment = log.segments[len(log.segments)-1]
		log.nextOffset = log.activeSegment.NextOffset()
		log.baseOffset = log.segments[0].BaseOffset()
	} else {
		// Create initial segment if none exist
		segment, err := NewSegment(dir, 0, config.SegmentSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create initial segment: %w", err)
		}

		log.segments = append(log.segments, segment)
		log.activeSegment = segment
		log.baseOffset = 0
		log.nextOffset = 0
	}

	// Calculate total size
	for _, segment := range log.segments {
		log.size += segment.Size()
	}

	return log, nil
}

// Append appends messages to the partition log
func (pl *PartitionLog) Append(messages []*types.Message) (*AppendResult, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// Set offsets for messages
	for i, msg := range messages {
		msg.Offset = pl.nextOffset + int64(i)
		msg.Partition = pl.partition
		if msg.Timestamp.IsZero() {
			msg.Timestamp = time.Now()
		}
	}

	// Check if we need to roll to a new segment
	if pl.activeSegment.IsFull() {
		if err := pl.rollSegment(); err != nil {
			return nil, fmt.Errorf("failed to roll segment: %w", err)
		}
	}

	// Append to active segment
	baseOffset, err := pl.activeSegment.Append(messages)
	if err != nil {
		return nil, fmt.Errorf("failed to append to segment: %w", err)
	}

	// Update state
	pl.nextOffset += int64(len(messages))

	return &AppendResult{
		BaseOffset:     baseOffset,
		LastOffset:     pl.nextOffset - 1,
		LogAppendTime:  time.Now(),
		LogStartOffset: pl.baseOffset,
		ErrorCode:      types.ErrNoError,
	}, nil
}

// Read reads messages from the partition log
func (pl *PartitionLog) Read(offset int64, maxBytes int) (*ReadResult, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	if offset < pl.baseOffset {
		return &ReadResult{
			ErrorCode: types.ErrOffsetOutOfRange,
		}, nil
	}

	if offset >= pl.nextOffset {
		return &ReadResult{
			Messages:         []*types.Message{},
			NextOffset:       pl.nextOffset,
			HighWatermark:    pl.nextOffset,
			LastStableOffset: pl.nextOffset,
			LogStartOffset:   pl.baseOffset,
			ErrorCode:        types.ErrNoError,
		}, nil
	}

	// Find the segment containing the offset
	segment := pl.findSegment(offset)
	if segment == nil {
		return &ReadResult{
			ErrorCode: types.ErrOffsetOutOfRange,
		}, nil
	}

	// Read from segment
	messages, err := segment.Read(offset, pl.nextOffset, int32(maxBytes))
	if err != nil {
		return &ReadResult{
			ErrorCode: types.ErrUnknown,
		}, nil
	}

	nextOffset := offset
	if len(messages) > 0 {
		nextOffset = messages[len(messages)-1].Offset + 1
	}

	return &ReadResult{
		Messages:         messages,
		NextOffset:       nextOffset,
		HighWatermark:    pl.nextOffset,
		LastStableOffset: pl.nextOffset,
		LogStartOffset:   pl.baseOffset,
		ErrorCode:        types.ErrNoError,
	}, nil
}

// FindOffsetByTimestamp finds the offset closest to the given timestamp
func (pl *PartitionLog) FindOffsetByTimestamp(timestamp time.Time) (int64, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	// Search through segments
	for _, segment := range pl.segments {
		offset, err := segment.FindOffsetByTimestamp(timestamp)
		if err != nil {
			continue
		}

		if offset >= segment.BaseOffset() && offset < segment.NextOffset() {
			return offset, nil
		}
	}

	return pl.baseOffset, nil
}

// GetHighWatermark returns the high watermark
func (pl *PartitionLog) GetHighWatermark() int64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.nextOffset
}

// GetLogStartOffset returns the log start offset
func (pl *PartitionLog) GetLogStartOffset() int64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.baseOffset
}

// Truncate truncates the log at the given offset
func (pl *PartitionLog) Truncate(offset int64) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// Find segments to remove
	var removeCount int
	for i, segment := range pl.segments {
		if segment.BaseOffset() >= offset {
			removeCount = len(pl.segments) - i
			break
		}
	}

	// Close and remove segments
	for i := len(pl.segments) - removeCount; i < len(pl.segments); i++ {
		pl.segments[i].Close()
	}

	pl.segments = pl.segments[:len(pl.segments)-removeCount]

	// Update active segment
	if len(pl.segments) > 0 {
		pl.activeSegment = pl.segments[len(pl.segments)-1]
		pl.nextOffset = offset
	} else {
		// Create new segment
		segment, err := NewSegment(pl.dir, offset, pl.config.SegmentSize)
		if err != nil {
			return err
		}

		pl.segments = append(pl.segments, segment)
		pl.activeSegment = segment
		pl.nextOffset = offset
	}

	return nil
}

// Close closes the partition log
func (pl *PartitionLog) Close() error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	var errs []error
	for _, segment := range pl.segments {
		if err := segment.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing partition log: %v", errs)
	}

	return nil
}

// Cleanup removes old segments based on retention policy
func (pl *PartitionLog) Cleanup() error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-pl.config.RetentionTime)

	var removeCount int
	var totalSize int64

	// Calculate current total size
	for _, segment := range pl.segments {
		totalSize += segment.Size()
	}

	// Remove segments based on time retention
	for i, segment := range pl.segments {
		if segment.lastTimestamp.Before(cutoff) && i < len(pl.segments)-1 {
			removeCount++
		} else {
			break
		}
	}

	// Remove segments based on size retention
	if totalSize > pl.config.RetentionSize {
		for i := removeCount; i < len(pl.segments)-1; i++ {
			totalSize -= pl.segments[i].Size()
			removeCount++
			if totalSize <= pl.config.RetentionSize {
				break
			}
		}
	}

	// Remove segments
	if removeCount > 0 {
		for i := 0; i < removeCount; i++ {
			pl.segments[i].Close()
		}

		pl.segments = pl.segments[removeCount:]
		if len(pl.segments) > 0 {
			pl.baseOffset = pl.segments[0].BaseOffset()
		}
	}

	return nil
}

func (pl *PartitionLog) loadSegments() error {
	entries, err := os.ReadDir(pl.dir)
	if err != nil {
		return err
	}

	var segmentFiles []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), SegmentFileSuffix) {
			segmentFiles = append(segmentFiles, entry.Name())
		}
	}

	sort.Strings(segmentFiles)

	for _, filename := range segmentFiles {
		baseOffsetStr := strings.TrimSuffix(filename, SegmentFileSuffix)
		baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
		if err != nil {
			continue
		}

		segment, err := OpenSegment(pl.dir, baseOffset)
		if err != nil {
			return fmt.Errorf("failed to open segment %d: %w", baseOffset, err)
		}

		pl.segments = append(pl.segments, segment)
	}

	return nil
}

func (pl *PartitionLog) rollSegment() error {
	// Create new segment
	baseOffset := pl.nextOffset
	segment, err := NewSegment(pl.dir, baseOffset, pl.config.SegmentSize)
	if err != nil {
		return err
	}

	pl.segments = append(pl.segments, segment)
	pl.activeSegment = segment

	return nil
}

func (pl *PartitionLog) findSegment(offset int64) *Segment {
	// Binary search for the segment
	idx := sort.Search(len(pl.segments), func(i int) bool {
		return pl.segments[i].BaseOffset() > offset
	})

	if idx == 0 {
		return nil
	}

	return pl.segments[idx-1]
}
