package streaming

import (
	"time"
)

// WindowType represents different types of windows
type WindowType string

const (
	WindowTypeTumbling WindowType = "tumbling"
	WindowTypeSliding  WindowType = "sliding"
	WindowTypeSession  WindowType = "session"
	WindowTypeGlobal   WindowType = "global"
)

// Window represents a time window for stream processing
type Window struct {
	Type       WindowType    `json:"type"`
	Start      time.Time     `json:"start"`
	End        time.Time     `json:"end"`
	Duration   time.Duration `json:"duration"`
	SessionGap time.Duration `json:"session_gap,omitempty"`
	SlideSize  time.Duration `json:"slide_size,omitempty"`
}

// StreamEvent represents an event in the stream
type StreamEvent struct {
	Key       string                 `json:"key"`
	Value     interface{}            `json:"value"`
	Timestamp time.Time              `json:"timestamp"`
	Window    *Window                `json:"window,omitempty"`
	Metadata  map[string]interface{} `json:"metadata"`
	Headers   map[string]string      `json:"headers"`
}

// StreamRecord represents a record in the stream processing pipeline
type StreamRecord struct {
	Topic     string                 `json:"topic"`
	Partition int32                  `json:"partition"`
	Offset    int64                  `json:"offset"`
	Key       string                 `json:"key"`
	Value     interface{}            `json:"value"`
	Timestamp time.Time              `json:"timestamp"`
	Headers   map[string]string      `json:"headers"`
	Context   map[string]interface{} `json:"context"`
}

// AggregateFunction represents different aggregation functions
type AggregateFunction string

const (
	AggregateFunctionSum    AggregateFunction = "sum"
	AggregateFunctionCount  AggregateFunction = "count"
	AggregateFunctionAvg    AggregateFunction = "avg"
	AggregateFunctionMin    AggregateFunction = "min"
	AggregateFunctionMax    AggregateFunction = "max"
	AggregateFunctionFirst  AggregateFunction = "first"
	AggregateFunctionLast   AggregateFunction = "last"
	AggregateFunctionCustom AggregateFunction = "custom"
)

// AggregateResult represents the result of an aggregation
type AggregateResult struct {
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	Count     int64       `json:"count"`
	Window    *Window     `json:"window"`
	Timestamp time.Time   `json:"timestamp"`
}

// JoinType represents different types of joins
type JoinType string

const (
	JoinTypeInner JoinType = "inner"
	JoinTypeLeft  JoinType = "left"
	JoinTypeRight JoinType = "right"
	JoinTypeFull  JoinType = "full"
)

// JoinCondition represents a join condition
type JoinCondition struct {
	LeftKey    string        `json:"left_key"`
	RightKey   string        `json:"right_key"`
	TimeWindow time.Duration `json:"time_window"`
}

// JoinResult represents the result of a join operation
type JoinResult struct {
	LeftRecord  *StreamRecord `json:"left_record"`
	RightRecord *StreamRecord `json:"right_record"`
	JoinKey     string        `json:"join_key"`
	Timestamp   time.Time     `json:"timestamp"`
}

// FilterPredicate represents a filter predicate function
type FilterPredicate func(*StreamRecord) bool

// MapFunction represents a map transformation function
type MapFunction func(*StreamRecord) (*StreamRecord, error)

// ReduceFunction represents a reduce aggregation function
type ReduceFunction func(interface{}, interface{}) interface{}

// ProcessorFunction represents a generic processor function
type ProcessorFunction func(*StreamRecord) ([]*StreamRecord, error)

// StateStore represents a key-value state store for stateful processing
type StateStore interface {
	Get(key string) (interface{}, error)
	Put(key string, value interface{}) error
	Delete(key string) error
	Range(startKey, endKey string) (map[string]interface{}, error)
	Clear() error
	Size() int64
	Keys() []string
	Close() error
}

// StreamProcessor represents a stream processor
type StreamProcessor interface {
	Process(record *StreamRecord) ([]*StreamRecord, error)
	GetState() StateStore
	Close() error
}

// StreamTopology represents the processing topology
type StreamTopology struct {
	Name        string                    `json:"name"`
	Sources     map[string]*SourceNode    `json:"sources"`
	Processors  map[string]*ProcessorNode `json:"processors"`
	Sinks       map[string]*SinkNode      `json:"sinks"`
	Connections []Connection              `json:"connections"`
}

// SourceNode represents a source in the topology
type SourceNode struct {
	Name       string   `json:"name"`
	Topics     []string `json:"topics"`
	Partitions []int32  `json:"partitions,omitempty"`
}

// ProcessorNode represents a processor in the topology
type ProcessorNode struct {
	Name      string                 `json:"name"`
	Type      string                 `json:"type"`
	Config    map[string]interface{} `json:"config"`
	Processor StreamProcessor        `json:"-"`
}

// SinkNode represents a sink in the topology
type SinkNode struct {
	Name   string                 `json:"name"`
	Topic  string                 `json:"topic"`
	Config map[string]interface{} `json:"config"`
}

// Connection represents a connection between nodes
type Connection struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// StreamConfig represents stream processing configuration
type StreamConfig struct {
	ApplicationID          string        `json:"application_id"`
	BootstrapServers       []string      `json:"bootstrap_servers"`
	DefaultKeySerializer   string        `json:"default_key_serializer"`
	DefaultValueSerializer string        `json:"default_value_serializer"`
	CommitInterval         time.Duration `json:"commit_interval"`
	PollTimeout            time.Duration `json:"poll_timeout"`
	ProcessingGuarantee    string        `json:"processing_guarantee"`
	NumStreamThreads       int           `json:"num_stream_threads"`
	StateDir               string        `json:"state_dir"`
	CacheMaxBytesBuffering int64         `json:"cache_max_bytes_buffering"`
	MetricsReporting       bool          `json:"metrics_reporting"`
}

// DefaultStreamConfig returns a default stream configuration
func DefaultStreamConfig() *StreamConfig {
	return &StreamConfig{
		CommitInterval:         1 * time.Second,
		PollTimeout:            100 * time.Millisecond,
		ProcessingGuarantee:    "at_least_once",
		NumStreamThreads:       1,
		StateDir:               "/tmp/streaming-state",
		CacheMaxBytesBuffering: 10 * 1024 * 1024, // 10MB
		MetricsReporting:       true,
	}
}

// StreamMetrics represents stream processing metrics
type StreamMetrics struct {
	RecordsProcessed  int64         `json:"records_processed"`
	RecordsPerSecond  float64       `json:"records_per_second"`
	ProcessingLatency time.Duration `json:"processing_latency"`
	ErrorRate         float64       `json:"error_rate"`
	WindowsCreated    int64         `json:"windows_created"`
	WindowsExpired    int64         `json:"windows_expired"`
	StateStoreSize    int64         `json:"state_store_size"`
	ThreadUtilization float64       `json:"thread_utilization"`
}

// ErrorType represents different types of stream processing errors
type ErrorType string

const (
	ErrorTypeDeserialization ErrorType = "deserialization"
	ErrorTypeProcessing      ErrorType = "processing"
	ErrorTypeSerialization   ErrorType = "serialization"
	ErrorTypeCommit          ErrorType = "commit"
	ErrorTypeStateStore      ErrorType = "state_store"
	ErrorTypeWindow          ErrorType = "window"
	ErrorTypeJoin            ErrorType = "join"
)

// StreamError represents a stream processing error
type StreamError struct {
	Type      ErrorType     `json:"type"`
	Message   string        `json:"message"`
	Record    *StreamRecord `json:"record,omitempty"`
	Timestamp time.Time     `json:"timestamp"`
	Retryable bool          `json:"retryable"`
}

func (e *StreamError) Error() string {
	return e.Message
}

// RetryPolicy represents retry configuration
type RetryPolicy struct {
	MaxRetries    int           `json:"max_retries"`
	RetryDelay    time.Duration `json:"retry_delay"`
	BackoffFactor float64       `json:"backoff_factor"`
	MaxDelay      time.Duration `json:"max_delay"`
}

// DefaultRetryPolicy returns a default retry policy
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxRetries:    3,
		RetryDelay:    100 * time.Millisecond,
		BackoffFactor: 2.0,
		MaxDelay:      5 * time.Second,
	}
}

// SerializationFormat represents different serialization formats
type SerializationFormat string

const (
	SerializationFormatJSON     SerializationFormat = "json"
	SerializationFormatAvro     SerializationFormat = "avro"
	SerializationFormatProtobuf SerializationFormat = "protobuf"
	SerializationFormatString   SerializationFormat = "string"
	SerializationFormatBytes    SerializationFormat = "bytes"
)

// Serializer represents a serializer interface
type Serializer interface {
	Serialize(topic string, data interface{}) ([]byte, error)
}

// Deserializer represents a deserializer interface
type Deserializer interface {
	Deserialize(topic string, data []byte) (interface{}, error)
}

// Checkpoint represents a processing checkpoint
type Checkpoint struct {
	ApplicationID string            `json:"application_id"`
	Partitions    map[string]int64  `json:"partitions"` // topic-partition -> offset
	Timestamp     time.Time         `json:"timestamp"`
	StateSnapshot map[string][]byte `json:"state_snapshot"`
}

// WindowStore represents a windowed state store
type WindowStore interface {
	Put(key string, value interface{}, window *Window) error
	Get(key string, window *Window) (interface{}, error)
	GetRange(key string, from, to time.Time) (map[*Window]interface{}, error)
	DeleteExpired(retentionTime time.Duration) error
	Size() int64
	Windows() []*Window
	Close() error
}

// SessionStore represents a session-based state store
type SessionStore interface {
	Put(key string, value interface{}, sessionStart, sessionEnd time.Time) error
	Get(key string, sessionStart, sessionEnd time.Time) (interface{}, error)
	GetSessions(key string) (map[string]interface{}, error)
	MergeSessions(key string, sessionGap time.Duration) error
	DeleteExpired(retentionTime time.Duration) error
	Close() error
}
