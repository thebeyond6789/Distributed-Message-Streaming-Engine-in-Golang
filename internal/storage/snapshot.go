package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SnapshotManager handles snapshot creation and restoration
type SnapshotManager struct {
	baseDir string
}

// SnapshotMetadata represents metadata for a snapshot
type SnapshotMetadata struct {
	SnapshotID string              `json:"snapshot_id"`
	Timestamp  time.Time           `json:"timestamp"`
	Partitions []PartitionSnapshot `json:"partitions"`
	Version    string              `json:"version"`
	Checksum   string              `json:"checksum"`
	TotalSize  int64               `json:"total_size"`
}

// PartitionSnapshot represents snapshot info for a partition
type PartitionSnapshot struct {
	Partition      int32    `json:"partition"`
	BaseOffset     int64    `json:"base_offset"`
	NextOffset     int64    `json:"next_offset"`
	SegmentFiles   []string `json:"segment_files"`
	IndexFiles     []string `json:"index_files"`
	TimeIndexFiles []string `json:"time_index_files"`
	Size           int64    `json:"size"`
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(baseDir string) *SnapshotManager {
	return &SnapshotManager{
		baseDir: baseDir,
	}
}

// CreateSnapshot creates a snapshot of the current state
func (sm *SnapshotManager) CreateSnapshot(partitionLogs map[int32]*PartitionLog) error {
	snapshotID := fmt.Sprintf("snapshot-%d", time.Now().Unix())
	snapshotDir := filepath.Join(sm.baseDir, "snapshots", snapshotID)

	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	metadata := &SnapshotMetadata{
		SnapshotID: snapshotID,
		Timestamp:  time.Now(),
		Version:    "1.0",
		Partitions: make([]PartitionSnapshot, 0, len(partitionLogs)),
	}

	var totalSize int64

	// Create snapshots for each partition
	for partition, log := range partitionLogs {
		partitionSnapshot, size, err := sm.createPartitionSnapshot(log, snapshotDir)
		if err != nil {
			return fmt.Errorf("failed to create partition snapshot %d: %w", partition, err)
		}

		metadata.Partitions = append(metadata.Partitions, *partitionSnapshot)
		totalSize += size
	}

	metadata.TotalSize = totalSize

	// Calculate checksum
	checksum, err := sm.calculateSnapshotChecksum(metadata)
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}
	metadata.Checksum = checksum

	// Write metadata
	metadataPath := filepath.Join(snapshotDir, "metadata.json")
	if err := sm.writeMetadata(metadata, metadataPath); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// RestoreSnapshot restores state from a snapshot
func (sm *SnapshotManager) RestoreSnapshot(snapshotPath string) (map[int32]*PartitionLog, error) {
	// Read metadata
	metadataPath := filepath.Join(snapshotPath, "metadata.json")
	metadata, err := sm.readMetadata(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	// Verify checksum
	actualChecksum, err := sm.calculateSnapshotChecksum(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}

	if actualChecksum != metadata.Checksum {
		return nil, fmt.Errorf("snapshot checksum mismatch: expected %s, got %s",
			metadata.Checksum, actualChecksum)
	}

	partitionLogs := make(map[int32]*PartitionLog)

	// Restore each partition
	for _, partitionSnapshot := range metadata.Partitions {
		log, err := sm.restorePartitionSnapshot(&partitionSnapshot, snapshotPath)
		if err != nil {
			return nil, fmt.Errorf("failed to restore partition %d: %w",
				partitionSnapshot.Partition, err)
		}

		partitionLogs[partitionSnapshot.Partition] = log
	}

	return partitionLogs, nil
}

// ListSnapshots returns a list of available snapshots
func (sm *SnapshotManager) ListSnapshots() ([]*SnapshotMetadata, error) {
	snapshotsDir := filepath.Join(sm.baseDir, "snapshots")

	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*SnapshotMetadata{}, nil
		}
		return nil, err
	}

	var snapshots []*SnapshotMetadata

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		metadataPath := filepath.Join(snapshotsDir, entry.Name(), "metadata.json")
		metadata, err := sm.readMetadata(metadataPath)
		if err != nil {
			continue // Skip invalid snapshots
		}

		snapshots = append(snapshots, metadata)
	}

	return snapshots, nil
}

// DeleteSnapshot deletes a snapshot
func (sm *SnapshotManager) DeleteSnapshot(snapshotID string) error {
	snapshotDir := filepath.Join(sm.baseDir, "snapshots", snapshotID)
	return os.RemoveAll(snapshotDir)
}

// GetSnapshotInfo returns information about a specific snapshot
func (sm *SnapshotManager) GetSnapshotInfo(snapshotID string) (*SnapshotMetadata, error) {
	metadataPath := filepath.Join(sm.baseDir, "snapshots", snapshotID, "metadata.json")
	return sm.readMetadata(metadataPath)
}

// Private methods

func (sm *SnapshotManager) createPartitionSnapshot(log *PartitionLog, snapshotDir string) (*PartitionSnapshot, int64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()

	partitionDir := filepath.Join(snapshotDir, fmt.Sprintf("partition-%d", log.partition))
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return nil, 0, err
	}

	snapshot := &PartitionSnapshot{
		Partition:      log.partition,
		BaseOffset:     log.baseOffset,
		NextOffset:     log.nextOffset,
		SegmentFiles:   make([]string, 0, len(log.segments)),
		IndexFiles:     make([]string, 0, len(log.segments)),
		TimeIndexFiles: make([]string, 0, len(log.segments)),
	}

	var totalSize int64

	// Copy segment files
	for _, segment := range log.segments {
		// Copy log file
		logFileName := fmt.Sprintf("%020d%s", segment.BaseOffset(), SegmentFileSuffix)
		logSrcPath := segment.logFilePath()
		logDstPath := filepath.Join(partitionDir, logFileName)

		size, err := sm.copyFile(logSrcPath, logDstPath)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to copy log file: %w", err)
		}
		totalSize += size
		snapshot.SegmentFiles = append(snapshot.SegmentFiles, logFileName)

		// Copy index file
		indexFileName := fmt.Sprintf("%020d%s", segment.BaseOffset(), IndexFileSuffix)
		indexSrcPath := segment.indexFilePath()
		indexDstPath := filepath.Join(partitionDir, indexFileName)

		size, err = sm.copyFile(indexSrcPath, indexDstPath)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to copy index file: %w", err)
		}
		totalSize += size
		snapshot.IndexFiles = append(snapshot.IndexFiles, indexFileName)

		// Copy time index file
		timeIndexFileName := fmt.Sprintf("%020d%s", segment.BaseOffset(), TimestampFileSuffix)
		timeIndexSrcPath := segment.timeIndexFilePath()
		timeIndexDstPath := filepath.Join(partitionDir, timeIndexFileName)

		size, err = sm.copyFile(timeIndexSrcPath, timeIndexDstPath)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to copy time index file: %w", err)
		}
		totalSize += size
		snapshot.TimeIndexFiles = append(snapshot.TimeIndexFiles, timeIndexFileName)
	}

	snapshot.Size = totalSize

	return snapshot, totalSize, nil
}

func (sm *SnapshotManager) restorePartitionSnapshot(snapshot *PartitionSnapshot, snapshotDir string) (*PartitionLog, error) {
	// Create partition directory in the main data directory
	partitionDir := filepath.Join(sm.baseDir, fmt.Sprintf("partition-%d", snapshot.Partition))
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return nil, err
	}

	snapshotPartitionDir := filepath.Join(snapshotDir, fmt.Sprintf("partition-%d", snapshot.Partition))

	// Copy all files back
	allFiles := make([]string, 0, len(snapshot.SegmentFiles)+len(snapshot.IndexFiles)+len(snapshot.TimeIndexFiles))
	allFiles = append(allFiles, snapshot.SegmentFiles...)
	allFiles = append(allFiles, snapshot.IndexFiles...)
	allFiles = append(allFiles, snapshot.TimeIndexFiles...)

	for _, fileName := range allFiles {
		srcPath := filepath.Join(snapshotPartitionDir, fileName)
		dstPath := filepath.Join(partitionDir, fileName)

		if _, err := sm.copyFile(srcPath, dstPath); err != nil {
			return nil, fmt.Errorf("failed to restore file %s: %w", fileName, err)
		}
	}

	// Load the partition log
	config := DefaultLogConfig()
	config.Dir = sm.baseDir

	return LoadPartitionLog(snapshot.Partition, partitionDir, config)
}

func (sm *SnapshotManager) copyFile(src, dst string) (int64, error) {
	srcFile, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	return dstFile.ReadFrom(srcFile)
}

func (sm *SnapshotManager) writeMetadata(metadata *SnapshotMetadata, path string) error {
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func (sm *SnapshotManager) readMetadata(path string) (*SnapshotMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var metadata SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (sm *SnapshotManager) calculateSnapshotChecksum(metadata *SnapshotMetadata) (string, error) {
	// Simple checksum based on metadata content
	// In production, you'd want to use file checksums
	data, err := json.Marshal(metadata.Partitions)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", len(data)), nil
}

// CleanupOldSnapshots removes old snapshots based on retention policy
func (sm *SnapshotManager) CleanupOldSnapshots(maxAge time.Duration, maxCount int) error {
	snapshots, err := sm.ListSnapshots()
	if err != nil {
		return err
	}

	now := time.Now()
	cutoff := now.Add(-maxAge)

	// Remove snapshots older than maxAge
	for _, snapshot := range snapshots {
		if snapshot.Timestamp.Before(cutoff) {
			if err := sm.DeleteSnapshot(snapshot.SnapshotID); err != nil {
				return fmt.Errorf("failed to delete old snapshot %s: %w",
					snapshot.SnapshotID, err)
			}
		}
	}

	// Remove excess snapshots if we have more than maxCount
	if len(snapshots) > maxCount {
		// Sort by timestamp (newest first)
		for i := 0; i < len(snapshots)-1; i++ {
			for j := i + 1; j < len(snapshots); j++ {
				if snapshots[i].Timestamp.Before(snapshots[j].Timestamp) {
					snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
				}
			}
		}

		// Delete excess snapshots
		for i := maxCount; i < len(snapshots); i++ {
			if err := sm.DeleteSnapshot(snapshots[i].SnapshotID); err != nil {
				return fmt.Errorf("failed to delete excess snapshot %s: %w",
					snapshots[i].SnapshotID, err)
			}
		}
	}

	return nil
}

// GetSnapshotStats returns statistics about snapshots
func (sm *SnapshotManager) GetSnapshotStats() (map[string]interface{}, error) {
	snapshots, err := sm.ListSnapshots()
	if err != nil {
		return nil, err
	}

	stats := make(map[string]interface{})

	var totalSize int64
	var oldestTime, newestTime time.Time

	for i, snapshot := range snapshots {
		totalSize += snapshot.TotalSize

		if i == 0 {
			oldestTime = snapshot.Timestamp
			newestTime = snapshot.Timestamp
		} else {
			if snapshot.Timestamp.Before(oldestTime) {
				oldestTime = snapshot.Timestamp
			}
			if snapshot.Timestamp.After(newestTime) {
				newestTime = snapshot.Timestamp
			}
		}
	}

	stats["total_snapshots"] = len(snapshots)
	stats["total_size"] = totalSize
	if len(snapshots) > 0 {
		stats["oldest_snapshot"] = oldestTime
		stats["newest_snapshot"] = newestTime
		stats["average_size"] = totalSize / int64(len(snapshots))
	}

	return stats, nil
}
