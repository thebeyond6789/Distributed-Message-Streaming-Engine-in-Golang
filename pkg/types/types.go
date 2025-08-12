package types

import (
	"time"
)

// Message represents a single message in the streaming engine
type Message struct {
	Key       []byte            `json:"key"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
	Offset    int64             `json:"offset"`
	Partition int32             `json:"partition"`
	Topic     string            `json:"topic"`
	CRC32     uint32            `json:"crc32"`
}

// MessageBatch represents a batch of messages for efficient processing
type MessageBatch struct {
	Messages  []*Message `json:"messages"`
	BatchID   string     `json:"batch_id"`
	Timestamp time.Time  `json:"timestamp"`
	Count     int32      `json:"count"`
}

// TopicConfig represents topic configuration
type TopicConfig struct {
	Name              string        `json:"name"`
	Partitions        int32         `json:"partitions"`
	ReplicationFactor int32         `json:"replication_factor"`
	RetentionTime     time.Duration `json:"retention_time"`
	RetentionSize     int64         `json:"retention_size"`
	CompactionPolicy  string        `json:"compaction_policy"`
	CompressionType   string        `json:"compression_type"`
	MaxMessageSize    int32         `json:"max_message_size"`
}

// PartitionInfo represents partition metadata
type PartitionInfo struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Leader    NodeID   `json:"leader"`
	Replicas  []NodeID `json:"replicas"`
	ISR       []NodeID `json:"isr"` // In-Sync Replicas
	Offset    int64    `json:"offset"`
	Size      int64    `json:"size"`
}

// NodeID represents a unique node identifier
type NodeID string

// BrokerInfo represents broker metadata
type BrokerInfo struct {
	NodeID    NodeID    `json:"node_id"`
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	Rack      string    `json:"rack"`
	Timestamp time.Time `json:"timestamp"`
}

// ConsumerGroupInfo represents consumer group metadata
type ConsumerGroupInfo struct {
	GroupID     string                           `json:"group_id"`
	Members     []ConsumerMemberInfo             `json:"members"`
	Coordinator NodeID                           `json:"coordinator"`
	State       ConsumerGroupState               `json:"state"`
	Offsets     map[string]map[int32]int64       `json:"offsets"`    // topic -> partition -> offset
	Assignment  map[string][]PartitionAssignment `json:"assignment"` // member -> partitions
}

// ConsumerMemberInfo represents individual consumer information
type ConsumerMemberInfo struct {
	MemberID       string            `json:"member_id"`
	ClientID       string            `json:"client_id"`
	Host           string            `json:"host"`
	SessionTimeout time.Duration     `json:"session_timeout"`
	Metadata       map[string]string `json:"metadata"`
	LastHeartbeat  time.Time         `json:"last_heartbeat"`
}

// PartitionAssignment represents partition assignment to a consumer
type PartitionAssignment struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// ConsumerGroupState represents the state of a consumer group
type ConsumerGroupState string

const (
	ConsumerGroupStateEmpty       ConsumerGroupState = "Empty"
	ConsumerGroupStateStable      ConsumerGroupState = "Stable"
	ConsumerGroupStateRebalancing ConsumerGroupState = "Rebalancing"
	ConsumerGroupStateDead        ConsumerGroupState = "Dead"
)

// OffsetCommitRequest represents a request to commit consumer offsets
type OffsetCommitRequest struct {
	GroupID   string                          `json:"group_id"`
	MemberID  string                          `json:"member_id"`
	Offsets   map[string]map[int32]OffsetData `json:"offsets"` // topic -> partition -> offset data
	Timestamp time.Time                       `json:"timestamp"`
}

// OffsetData represents offset information with metadata
type OffsetData struct {
	Offset   int64  `json:"offset"`
	Metadata string `json:"metadata"`
}

// ProducerConfig represents producer configuration
type ProducerConfig struct {
	ClientID         string        `json:"client_id"`
	Acks             int32         `json:"acks"` // 0, 1, or -1 (all)
	Retries          int32         `json:"retries"`
	BatchSize        int32         `json:"batch_size"`
	LingerMs         int32         `json:"linger_ms"`
	CompressionType  string        `json:"compression_type"`
	MaxRequestSize   int32         `json:"max_request_size"`
	RequestTimeout   time.Duration `json:"request_timeout"`
	DeliveryTimeout  time.Duration `json:"delivery_timeout"`
	EnableIdempotent bool          `json:"enable_idempotent"`
}

// ConsumerConfig represents consumer configuration
type ConsumerConfig struct {
	GroupID                string        `json:"group_id"`
	ClientID               string        `json:"client_id"`
	AutoOffsetReset        string        `json:"auto_offset_reset"` // earliest, latest, none
	EnableAutoCommit       bool          `json:"enable_auto_commit"`
	AutoCommitInterval     time.Duration `json:"auto_commit_interval"`
	SessionTimeout         time.Duration `json:"session_timeout"`
	HeartbeatInterval      time.Duration `json:"heartbeat_interval"`
	MaxPollRecords         int32         `json:"max_poll_records"`
	FetchMinBytes          int32         `json:"fetch_min_bytes"`
	FetchMaxWait           time.Duration `json:"fetch_max_wait"`
	MaxPartitionFetchBytes int32         `json:"max_partition_fetch_bytes"`
}

// ErrorCode represents standard error codes
type ErrorCode int32

const (
	ErrNoError                      ErrorCode = 0
	ErrUnknown                      ErrorCode = -1
	ErrOffsetOutOfRange             ErrorCode = 1
	ErrInvalidMessage               ErrorCode = 2
	ErrUnknownTopicOrPartition      ErrorCode = 3
	ErrInvalidMessageSize           ErrorCode = 4
	ErrLeaderNotAvailable           ErrorCode = 5
	ErrNotLeaderForPartition        ErrorCode = 6
	ErrRequestTimedOut              ErrorCode = 7
	ErrGroupCoordinatorNotAvailable ErrorCode = 15
	ErrNotCoordinatorForGroup       ErrorCode = 16
	ErrTopicAlreadyExists           ErrorCode = 36
	ErrInvalidPartitions            ErrorCode = 37
	ErrInvalidReplicationFactor     ErrorCode = 38
)

// Error represents a structured error response
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details string    `json:"details"`
}

func (e Error) Error() string {
	return e.Message
}

// MetricType represents different types of metrics
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// Metric represents a single metric data point
type Metric struct {
	Name      string            `json:"name"`
	Type      MetricType        `json:"type"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
	Help      string            `json:"help"`
}

// ClusterInfo represents overall cluster information
type ClusterInfo struct {
	ClusterID string                 `json:"cluster_id"`
	Brokers   map[NodeID]BrokerInfo  `json:"brokers"`
	Topics    map[string]TopicConfig `json:"topics"`
	Leader    NodeID                 `json:"leader"`
	Version   string                 `json:"version"`
	Timestamp time.Time              `json:"timestamp"`
}

// StreamWindow represents a time window for stream processing
type StreamWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Type  string    `json:"type"` // tumbling, sliding, session
}

// StreamEvent represents an event in stream processing
type StreamEvent struct {
	Key       string                 `json:"key"`
	Value     interface{}            `json:"value"`
	Timestamp time.Time              `json:"timestamp"`
	Window    *StreamWindow          `json:"window"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// CompressionType represents supported compression algorithms
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
	CompressionLZ4    CompressionType = "lz4"
	CompressionZstd   CompressionType = "zstd"
)

// ConsistencyLevel represents replication consistency requirements
type ConsistencyLevel string

const (
	ConsistencyLevelOne      ConsistencyLevel = "one"
	ConsistencyLevelAll      ConsistencyLevel = "all"
	ConsistencyLevelQuorum   ConsistencyLevel = "quorum"
	ConsistencyLevelEventual ConsistencyLevel = "eventual"
)
