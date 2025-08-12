package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"streaming-engine/pkg/types"
)

// CompactionManager handles log compaction operations
type CompactionManager struct {
	config *LogConfig
}

// CompactionJob represents a compaction job
type CompactionJob struct {
	PartitionLog *PartitionLog
	Segments     []*Segment
	TargetSize   int64
	StartTime    time.Time
}

// CompactionResult represents the result of a compaction operation
type CompactionResult struct {
	Success           bool
	CompactedSegments int
	SpaceSaved        int64
	Duration          time.Duration
	Error             error
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(config *LogConfig) *CompactionManager {
	return &CompactionManager{
		config: config,
	}
}

// CompactPartition compacts segments in a partition log
func (cm *CompactionManager) CompactPartition(partitionLog *PartitionLog) error {
	partitionLog.mu.Lock()
	defer partitionLog.mu.Unlock()

	// Find segments eligible for compaction
	eligibleSegments := cm.findEligibleSegments(partitionLog)
	if len(eligibleSegments) < 2 {
		return nil // Need at least 2 segments to compact
	}

	job := &CompactionJob{
		PartitionLog: partitionLog,
		Segments:     eligibleSegments,
		TargetSize:   cm.config.SegmentSize,
		StartTime:    time.Now(),
	}

	result := cm.executeCompactionJob(job)
	if result.Error != nil {
		return fmt.Errorf("compaction failed: %w", result.Error)
	}

	return nil
}

// executeCompactionJob executes a compaction job
func (cm *CompactionManager) executeCompactionJob(job *CompactionJob) *CompactionResult {
	result := &CompactionResult{
		StartTime: job.StartTime,
	}

	// Create temporary compacted segment
	tempDir := filepath.Join(job.PartitionLog.dir, "compaction-temp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		result.Error = fmt.Errorf("failed to create temp directory: %w", err)
		return result
	}
	defer os.RemoveAll(tempDir)

	// Calculate compacted segment base offset
	baseOffset := job.Segments[0].BaseOffset()

	// Create new compacted segment
	compactedSegment, err := NewSegment(tempDir, baseOffset, job.TargetSize)
	if err != nil {
		result.Error = fmt.Errorf("failed to create compacted segment: %w", err)
		return result
	}
	defer compactedSegment.Close()

	// Compact segments using key-based deduplication
	if err := cm.compactSegments(job.Segments, compactedSegment); err != nil {
		result.Error = fmt.Errorf("failed to compact segments: %w", err)
		return result
	}

	// Replace original segments with compacted segment
	if err := cm.replaceSegments(job, compactedSegment, tempDir); err != nil {
		result.Error = fmt.Errorf("failed to replace segments: %w", err)
		return result
	}

	// Calculate results
	result.Success = true
	result.CompactedSegments = len(job.Segments)
	result.Duration = time.Since(job.StartTime)

	// Calculate space saved
	var originalSize int64
	for _, segment := range job.Segments {
		originalSize += segment.Size()
	}
	result.SpaceSaved = originalSize - compactedSegment.Size()

	return result
}

// compactSegments compacts multiple segments into one, removing duplicates
func (cm *CompactionManager) compactSegments(sourceSegments []*Segment, targetSegment *Segment) error {
	// Use a map to track the latest value for each key
	latestMessages := make(map[string]*types.Message)
	var messageOrder []string // To maintain order

	// Read all messages from source segments
	for _, segment := range sourceSegments {
		reader := &SegmentReader{
			segment:  segment,
			position: 0,
		}

		for {
			entry, err := reader.ReadEntry()
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to read entry: %w", err)
			}

			keyStr := string(entry.Key)

			// Check if this is a tombstone (null value indicates deletion)
			if entry.Value == nil {
				// Remove from map if it exists
				delete(latestMessages, keyStr)
				// Remove from order slice
				for i, k := range messageOrder {
					if k == keyStr {
						messageOrder = append(messageOrder[:i], messageOrder[i+1:]...)
						break
					}
				}
				continue
			}

			// If key doesn't exist in map, add to order
			if _, exists := latestMessages[keyStr]; !exists {
				messageOrder = append(messageOrder, keyStr)
			}

			// Update latest message for this key
			latestMessages[keyStr] = &types.Message{
				Key:       entry.Key,
				Value:     entry.Value,
				Headers:   entry.Headers,
				Timestamp: entry.Timestamp,
				Offset:    entry.Offset,
			}
		}
	}

	// Write compacted messages to target segment
	batch := make([]*types.Message, 0, 1000) // Process in batches

	for _, keyStr := range messageOrder {
		msg, exists := latestMessages[keyStr]
		if !exists {
			continue // Key was deleted
		}

		batch = append(batch, msg)

		// Flush batch when it gets large enough
		if len(batch) >= 1000 {
			if _, err := targetSegment.Append(batch); err != nil {
				return fmt.Errorf("failed to append batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	// Flush remaining messages
	if len(batch) > 0 {
		if _, err := targetSegment.Append(batch); err != nil {
			return fmt.Errorf("failed to append final batch: %w", err)
		}
	}

	return nil
}

// replaceSegments replaces original segments with the compacted segment
func (cm *CompactionManager) replaceSegments(job *CompactionJob, compactedSegment *Segment, tempDir string) error {
	// Close compacted segment to flush data
	if err := compactedSegment.Close(); err != nil {
		return fmt.Errorf("failed to close compacted segment: %w", err)
	}

	// Move compacted files to partition directory
	baseOffset := job.Segments[0].BaseOffset()

	// Source paths in temp directory
	tempLogPath := compactedSegment.logFilePath()
	tempIndexPath := compactedSegment.indexFilePath()
	tempTimeIndexPath := compactedSegment.timeIndexFilePath()

	// Target paths in partition directory
	targetLogPath := filepath.Join(job.PartitionLog.dir, fmt.Sprintf("%020d%s", baseOffset, SegmentFileSuffix))
	targetIndexPath := filepath.Join(job.PartitionLog.dir, fmt.Sprintf("%020d%s", baseOffset, IndexFileSuffix))
	targetTimeIndexPath := filepath.Join(job.PartitionLog.dir, fmt.Sprintf("%020d%s", baseOffset, TimestampFileSuffix))

	// Close original segments
	for _, segment := range job.Segments {
		if err := segment.Close(); err != nil {
			return fmt.Errorf("failed to close original segment: %w", err)
		}
	}

	// Remove original segment files
	for _, segment := range job.Segments {
		os.Remove(segment.logFilePath())
		os.Remove(segment.indexFilePath())
		os.Remove(segment.timeIndexFilePath())
	}

	// Move compacted files
	if err := os.Rename(tempLogPath, targetLogPath); err != nil {
		return fmt.Errorf("failed to move log file: %w", err)
	}

	if err := os.Rename(tempIndexPath, targetIndexPath); err != nil {
		return fmt.Errorf("failed to move index file: %w", err)
	}

	if err := os.Rename(tempTimeIndexPath, targetTimeIndexPath); err != nil {
		return fmt.Errorf("failed to move time index file: %w", err)
	}

	// Open new segment
	newSegment, err := OpenSegment(job.PartitionLog.dir, baseOffset)
	if err != nil {
		return fmt.Errorf("failed to open new segment: %w", err)
	}

	// Update partition log segments
	// Find position of first segment to replace
	var startIdx int
	for i, segment := range job.PartitionLog.segments {
		if segment.BaseOffset() == job.Segments[0].BaseOffset() {
			startIdx = i
			break
		}
	}

	// Replace segments
	endIdx := startIdx + len(job.Segments)
	newSegments := make([]*Segment, 0, len(job.PartitionLog.segments)-len(job.Segments)+1)

	// Add segments before compacted range
	newSegments = append(newSegments, job.PartitionLog.segments[:startIdx]...)

	// Add compacted segment
	newSegments = append(newSegments, newSegment)

	// Add segments after compacted range
	newSegments = append(newSegments, job.PartitionLog.segments[endIdx:]...)

	job.PartitionLog.segments = newSegments

	// Update active segment if necessary
	if len(job.PartitionLog.segments) > 0 {
		job.PartitionLog.activeSegment = job.PartitionLog.segments[len(job.PartitionLog.segments)-1]
	}

	return nil
}

// findEligibleSegments finds segments eligible for compaction
func (cm *CompactionManager) findEligibleSegments(partitionLog *PartitionLog) []*Segment {
	if len(partitionLog.segments) < 2 {
		return nil
	}

	// Don't compact the active segment
	eligibleSegments := partitionLog.segments[:len(partitionLog.segments)-1]

	// Sort by base offset
	sort.Slice(eligibleSegments, func(i, j int) bool {
		return eligibleSegments[i].BaseOffset() < eligibleSegments[j].BaseOffset()
	})

	// Find consecutive segments that can be compacted together
	var candidates []*Segment
	var totalSize int64

	for _, segment := range eligibleSegments {
		// Check if adding this segment would exceed target size
		if totalSize+segment.Size() > cm.config.SegmentSize && len(candidates) > 0 {
			break
		}

		candidates = append(candidates, segment)
		totalSize += segment.Size()

		// If we have enough segments, we can proceed
		if len(candidates) >= 2 {
			break
		}
	}

	// Only return candidates if we have at least 2 segments
	if len(candidates) >= 2 {
		return candidates
	}

	return nil
}

// GetCompactionStats returns compaction statistics
func (cm *CompactionManager) GetCompactionStats(partitionLog *PartitionLog) map[string]interface{} {
	partitionLog.mu.RLock()
	defer partitionLog.mu.RUnlock()

	stats := make(map[string]interface{})

	var totalSize int64
	var compactableSize int64
	eligibleSegments := cm.findEligibleSegments(partitionLog)

	for _, segment := range partitionLog.segments {
		totalSize += segment.Size()
	}

	for _, segment := range eligibleSegments {
		compactableSize += segment.Size()
	}

	stats["total_segments"] = len(partitionLog.segments)
	stats["eligible_segments"] = len(eligibleSegments)
	stats["total_size"] = totalSize
	stats["compactable_size"] = compactableSize
	stats["compaction_ratio"] = float64(compactableSize) / float64(totalSize)

	return stats
}

// ForceCompaction forces compaction of all eligible segments
func (cm *CompactionManager) ForceCompaction(partitionLog *PartitionLog) error {
	partitionLog.mu.Lock()
	defer partitionLog.mu.Unlock()

	// Get all segments except the active one
	if len(partitionLog.segments) < 2 {
		return nil
	}

	segments := partitionLog.segments[:len(partitionLog.segments)-1]

	job := &CompactionJob{
		PartitionLog: partitionLog,
		Segments:     segments,
		TargetSize:   cm.config.SegmentSize,
		StartTime:    time.Now(),
	}

	result := cm.executeCompactionJob(job)
	if result.Error != nil {
		return fmt.Errorf("forced compaction failed: %w", result.Error)
	}

	return nil
}
