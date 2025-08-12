package replication

import (
	"time"

	"streaming-engine/pkg/types"
)

// PartitionAddedEvent represents a partition being added for replication
type PartitionAddedEvent struct {
	Topic     string         `json:"topic"`
	Partition int32          `json:"partition"`
	Leader    types.NodeID   `json:"leader"`
	Replicas  []types.NodeID `json:"replicas"`
	Timestamp time.Time      `json:"timestamp"`
}

func (e *PartitionAddedEvent) Type() string {
	return "partition_added"
}

func (e *PartitionAddedEvent) Data() interface{} {
	return e
}

// PartitionRemovedEvent represents a partition being removed from replication
type PartitionRemovedEvent struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *PartitionRemovedEvent) Type() string {
	return "partition_removed"
}

func (e *PartitionRemovedEvent) Data() interface{} {
	return e
}

// HighWatermarkUpdatedEvent represents a high watermark update
type HighWatermarkUpdatedEvent struct {
	Topic         string    `json:"topic"`
	Partition     int32     `json:"partition"`
	HighWatermark int64     `json:"high_watermark"`
	Timestamp     time.Time `json:"timestamp"`
}

func (e *HighWatermarkUpdatedEvent) Type() string {
	return "high_watermark_updated"
}

func (e *HighWatermarkUpdatedEvent) Data() interface{} {
	return e
}

// ISRChangedEvent represents a change in the in-sync replica set
type ISRChangedEvent struct {
	Topic     string         `json:"topic"`
	Partition int32          `json:"partition"`
	NewISR    []types.NodeID `json:"new_isr"`
	Timestamp time.Time      `json:"timestamp"`
}

func (e *ISRChangedEvent) Type() string {
	return "isr_changed"
}

func (e *ISRChangedEvent) Data() interface{} {
	return e
}

// ReplicationFailedEvent represents a replication failure
type ReplicationFailedEvent struct {
	Topic     string       `json:"topic"`
	Partition int32        `json:"partition"`
	Replica   types.NodeID `json:"replica"`
	Error     string       `json:"error"`
	Timestamp time.Time    `json:"timestamp"`
}

func (e *ReplicationFailedEvent) Type() string {
	return "replication_failed"
}

func (e *ReplicationFailedEvent) Data() interface{} {
	return e
}

// ConflictDetectedEvent represents a detected conflict
type ConflictDetectedEvent struct {
	Topic        string       `json:"topic"`
	Partition    int32        `json:"partition"`
	Offset       int64        `json:"offset"`
	ConflictType ConflictType `json:"conflict_type"`
	Timestamp    time.Time    `json:"timestamp"`
}

func (e *ConflictDetectedEvent) Type() string {
	return "conflict_detected"
}

func (e *ConflictDetectedEvent) Data() interface{} {
	return e
}

// ConflictResolvedEvent represents a resolved conflict
type ConflictResolvedEvent struct {
	Topic      string                     `json:"topic"`
	Partition  int32                      `json:"partition"`
	Offset     int64                      `json:"offset"`
	Strategy   ConflictResolutionStrategy `json:"strategy"`
	Resolution *Resolution                `json:"resolution"`
	Timestamp  time.Time                  `json:"timestamp"`
}

func (e *ConflictResolvedEvent) Type() string {
	return "conflict_resolved"
}

func (e *ConflictResolvedEvent) Data() interface{} {
	return e
}

// ReplicaLagEvent represents a replica falling behind
type ReplicaLagEvent struct {
	Topic       string        `json:"topic"`
	Partition   int32         `json:"partition"`
	Replica     types.NodeID  `json:"replica"`
	LagMessages int64         `json:"lag_messages"`
	LagTime     time.Duration `json:"lag_time"`
	Timestamp   time.Time     `json:"timestamp"`
}

func (e *ReplicaLagEvent) Type() string {
	return "replica_lag"
}

func (e *ReplicaLagEvent) Data() interface{} {
	return e
}

// ReplicaCaughtUpEvent represents a replica catching up
type ReplicaCaughtUpEvent struct {
	Topic     string       `json:"topic"`
	Partition int32        `json:"partition"`
	Replica   types.NodeID `json:"replica"`
	Timestamp time.Time    `json:"timestamp"`
}

func (e *ReplicaCaughtUpEvent) Type() string {
	return "replica_caught_up"
}

func (e *ReplicaCaughtUpEvent) Data() interface{} {
	return e
}

// LeaderChangedEvent represents a leader change
type LeaderChangedEvent struct {
	Topic     string       `json:"topic"`
	Partition int32        `json:"partition"`
	OldLeader types.NodeID `json:"old_leader"`
	NewLeader types.NodeID `json:"new_leader"`
	Timestamp time.Time    `json:"timestamp"`
}

func (e *LeaderChangedEvent) Type() string {
	return "leader_changed"
}

func (e *LeaderChangedEvent) Data() interface{} {
	return e
}

// ReplicationStartedEvent represents replication starting for a partition
type ReplicationStartedEvent struct {
	Topic     string         `json:"topic"`
	Partition int32          `json:"partition"`
	Leader    types.NodeID   `json:"leader"`
	Replicas  []types.NodeID `json:"replicas"`
	Timestamp time.Time      `json:"timestamp"`
}

func (e *ReplicationStartedEvent) Type() string {
	return "replication_started"
}

func (e *ReplicationStartedEvent) Data() interface{} {
	return e
}

// ReplicationStoppedEvent represents replication stopping for a partition
type ReplicationStoppedEvent struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *ReplicationStoppedEvent) Type() string {
	return "replication_stopped"
}

func (e *ReplicationStoppedEvent) Data() interface{} {
	return e
}

// CrossDatacenterReplicationEvent represents cross-datacenter replication activity
type CrossDatacenterReplicationEvent struct {
	SourceDatacenter string    `json:"source_datacenter"`
	TargetDatacenter string    `json:"target_datacenter"`
	Topic            string    `json:"topic"`
	Partition        int32     `json:"partition"`
	MessagesCount    int64     `json:"messages_count"`
	BytesTransferred int64     `json:"bytes_transferred"`
	Timestamp        time.Time `json:"timestamp"`
}

func (e *CrossDatacenterReplicationEvent) Type() string {
	return "cross_datacenter_replication"
}

func (e *CrossDatacenterReplicationEvent) Data() interface{} {
	return e
}
