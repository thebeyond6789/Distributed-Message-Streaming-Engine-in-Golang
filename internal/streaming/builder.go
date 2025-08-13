package streaming

import (
	"fmt"
	"time"

	"streaming-engine/internal/storage"
)

// StreamBuilder provides a fluent API for building stream processing topologies
type StreamBuilder struct {
	topology    *StreamTopology
	nodeCounter int
}

// NewStreamBuilder creates a new stream builder
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		topology: &StreamTopology{
			Sources:     make(map[string]*SourceNode),
			Processors:  make(map[string]*ProcessorNode),
			Sinks:       make(map[string]*SinkNode),
			Connections: []Connection{},
		},
	}
}

// Stream represents a stream in the topology
type Stream struct {
	builder  *StreamBuilder
	nodeName string
}

// Source creates a source stream from topics
func (sb *StreamBuilder) Source(topics ...string) *Stream {
	nodeName := sb.generateNodeName("source")

	sourceNode := &SourceNode{
		Name:   nodeName,
		Topics: topics,
	}

	sb.topology.Sources[nodeName] = sourceNode

	return &Stream{
		builder:  sb,
		nodeName: nodeName,
	}
}

// SourceWithPartitions creates a source stream from specific topic partitions
func (sb *StreamBuilder) SourceWithPartitions(topic string, partitions []int32) *Stream {
	nodeName := sb.generateNodeName("source")

	sourceNode := &SourceNode{
		Name:       nodeName,
		Topics:     []string{topic},
		Partitions: partitions,
	}

	sb.topology.Sources[nodeName] = sourceNode

	return &Stream{
		builder:  sb,
		nodeName: nodeName,
	}
}

// Map applies a transformation to each record
func (s *Stream) Map(mapFunc MapFunction) *Stream {
	nodeName := s.builder.generateNodeName("map")

	processorNode := &ProcessorNode{
		Name: nodeName,
		Type: string(ProcessorTypeMap),
		Config: map[string]interface{}{
			"map_function": mapFunc,
		},
	}

	s.builder.topology.Processors[nodeName] = processorNode
	s.builder.addConnection(s.nodeName, nodeName)

	return &Stream{
		builder:  s.builder,
		nodeName: nodeName,
	}
}

// Filter filters records based on a predicate
func (s *Stream) Filter(predicate FilterPredicate) *Stream {
	nodeName := s.builder.generateNodeName("filter")

	processorNode := &ProcessorNode{
		Name: nodeName,
		Type: string(ProcessorTypeFilter),
		Config: map[string]interface{}{
			"predicate": predicate,
		},
	}

	s.builder.topology.Processors[nodeName] = processorNode
	s.builder.addConnection(s.nodeName, nodeName)

	return &Stream{
		builder:  s.builder,
		nodeName: nodeName,
	}
}

// FlatMap applies a flat map transformation
func (s *Stream) FlatMap(flatMapFunc func(*StreamRecord) ([]*StreamRecord, error)) *Stream {
	nodeName := s.builder.generateNodeName("flatmap")

	processorNode := &ProcessorNode{
		Name: nodeName,
		Type: string(ProcessorTypeFlatMap),
		Config: map[string]interface{}{
			"flatmap_function": flatMapFunc,
		},
	}

	s.builder.topology.Processors[nodeName] = processorNode
	s.builder.addConnection(s.nodeName, nodeName)

	return &Stream{
		builder:  s.builder,
		nodeName: nodeName,
	}
}

// WindowedStream represents a windowed stream
type WindowedStream struct {
	stream     *Stream
	windowType WindowType
	windowSize time.Duration
	slideSize  time.Duration
	sessionGap time.Duration
}

// Window creates a windowed stream with tumbling windows
func (s *Stream) Window(size time.Duration) *WindowedStream {
	return &WindowedStream{
		stream:     s,
		windowType: WindowTypeTumbling,
		windowSize: size,
	}
}

// SlidingWindow creates a windowed stream with sliding windows
func (s *Stream) SlidingWindow(size, slide time.Duration) *WindowedStream {
	return &WindowedStream{
		stream:     s,
		windowType: WindowTypeSliding,
		windowSize: size,
		slideSize:  slide,
	}
}

// SessionWindow creates a windowed stream with session windows
func (s *Stream) SessionWindow(sessionGap time.Duration) *WindowedStream {
	return &WindowedStream{
		stream:     s,
		windowType: WindowTypeSession,
		sessionGap: sessionGap,
	}
}

// GlobalWindow creates a global window (no windowing)
func (s *Stream) GlobalWindow() *WindowedStream {
	return &WindowedStream{
		stream:     s,
		windowType: WindowTypeGlobal,
	}
}

// Aggregate performs windowed aggregation
func (ws *WindowedStream) Aggregate(aggregateFunc AggregateFunction, reduceFunc ReduceFunction) *Stream {
	nodeName := ws.stream.builder.generateNodeName("aggregate")

	processorNode := &ProcessorNode{
		Name: nodeName,
		Type: string(ProcessorTypeAggregate),
		Config: map[string]interface{}{
			"window_type":    ws.windowType,
			"window_size":    ws.windowSize,
			"slide_size":     ws.slideSize,
			"session_gap":    ws.sessionGap,
			"aggregate_func": aggregateFunc,
			"reduce_func":    reduceFunc,
		},
	}

	ws.stream.builder.topology.Processors[nodeName] = processorNode
	ws.stream.builder.addConnection(ws.stream.nodeName, nodeName)

	return &Stream{
		builder:  ws.stream.builder,
		nodeName: nodeName,
	}
}

// Count counts records in windows
func (ws *WindowedStream) Count() *Stream {
	return ws.Aggregate(AggregateFunctionCount, func(a, b interface{}) interface{} {
		aVal, aOk := a.(int64)
		bVal, bOk := b.(int64)
		if !aOk {
			aVal = 0
		}
		if !bOk {
			bVal = 1
		}
		return aVal + bVal
	})
}

// Sum sums numeric values in windows
func (ws *WindowedStream) Sum() *Stream {
	return ws.Aggregate(AggregateFunctionSum, func(a, b interface{}) interface{} {
		// Simplified - assumes numeric values
		switch aVal := a.(type) {
		case int64:
			if bVal, ok := b.(int64); ok {
				return aVal + bVal
			}
		case float64:
			if bVal, ok := b.(float64); ok {
				return aVal + bVal
			}
		}
		return b
	})
}

// Reduce performs windowed reduction with a custom reduce function
func (ws *WindowedStream) Reduce(reduceFunc ReduceFunction) *Stream {
	return ws.Aggregate(AggregateFunctionCustom, reduceFunc)
}

// JoinedStream represents a joined stream
type JoinedStream struct {
	leftStream  *Stream
	rightStream *Stream
	joinType    JoinType
	condition   *JoinCondition
}

// Join joins this stream with another stream
func (s *Stream) Join(other *Stream, joinType JoinType, condition *JoinCondition) *JoinedStream {
	return &JoinedStream{
		leftStream:  s,
		rightStream: other,
		joinType:    joinType,
		condition:   condition,
	}
}

// InnerJoin performs an inner join
func (s *Stream) InnerJoin(other *Stream, leftKey, rightKey string, timeWindow time.Duration) *JoinedStream {
	condition := &JoinCondition{
		LeftKey:    leftKey,
		RightKey:   rightKey,
		TimeWindow: timeWindow,
	}

	return s.Join(other, JoinTypeInner, condition)
}

// LeftJoin performs a left join
func (s *Stream) LeftJoin(other *Stream, leftKey, rightKey string, timeWindow time.Duration) *JoinedStream {
	condition := &JoinCondition{
		LeftKey:    leftKey,
		RightKey:   rightKey,
		TimeWindow: timeWindow,
	}

	return s.Join(other, JoinTypeLeft, condition)
}

// Select completes the join and returns a new stream
func (js *JoinedStream) Select(selectFunc func(*JoinResult) *StreamRecord) *Stream {
	nodeName := js.leftStream.builder.generateNodeName("join")

	processorNode := &ProcessorNode{
		Name: nodeName,
		Type: string(ProcessorTypeJoin),
		Config: map[string]interface{}{
			"join_type":      js.joinType,
			"join_condition": js.condition,
			"select_func":    selectFunc,
		},
	}

	js.leftStream.builder.topology.Processors[nodeName] = processorNode
	js.leftStream.builder.addConnection(js.leftStream.nodeName, nodeName)
	js.leftStream.builder.addConnection(js.rightStream.nodeName, nodeName)

	return &Stream{
		builder:  js.leftStream.builder,
		nodeName: nodeName,
	}
}

// Branch splits a stream into multiple streams based on predicates
func (s *Stream) Branch(predicates ...FilterPredicate) []*Stream {
	var branches []*Stream

	for i, predicate := range predicates {
		branchName := s.builder.generateNodeName(fmt.Sprintf("branch-%d", i))

		processorNode := &ProcessorNode{
			Name: branchName,
			Type: string(ProcessorTypeFilter),
			Config: map[string]interface{}{
				"predicate": predicate,
			},
		}

		s.builder.topology.Processors[branchName] = processorNode
		s.builder.addConnection(s.nodeName, branchName)

		branches = append(branches, &Stream{
			builder:  s.builder,
			nodeName: branchName,
		})
	}

	return branches
}

// Merge merges multiple streams into one
func (sb *StreamBuilder) Merge(streams ...*Stream) *Stream {
	if len(streams) == 0 {
		return nil
	}

	if len(streams) == 1 {
		return streams[0]
	}

	// Create a merge processor (simplified - just forwards all records)
	mergeNodeName := sb.generateNodeName("merge")

	processorNode := &ProcessorNode{
		Name: mergeNodeName,
		Type: string(ProcessorTypeCustom),
		Config: map[string]interface{}{
			"merge": true,
		},
	}

	sb.topology.Processors[mergeNodeName] = processorNode

	// Connect all input streams to the merge node
	for _, stream := range streams {
		sb.addConnection(stream.nodeName, mergeNodeName)
	}

	return &Stream{
		builder:  sb,
		nodeName: mergeNodeName,
	}
}

// To sends the stream to a topic (creates a sink)
func (s *Stream) To(topic string) {
	sinkName := s.builder.generateNodeName("sink")

	sinkNode := &SinkNode{
		Name:   sinkName,
		Topic:  topic,
		Config: make(map[string]interface{}),
	}

	s.builder.topology.Sinks[sinkName] = sinkNode
	s.builder.addConnection(s.nodeName, sinkName)
}

// ToWithConfig sends the stream to a topic with configuration
func (s *Stream) ToWithConfig(topic string, config map[string]interface{}) {
	sinkName := s.builder.generateNodeName("sink")

	sinkNode := &SinkNode{
		Name:   sinkName,
		Topic:  topic,
		Config: config,
	}

	s.builder.topology.Sinks[sinkName] = sinkNode
	s.builder.addConnection(s.nodeName, sinkName)
}

// Peek allows inspection of records without modifying the stream
func (s *Stream) Peek(peekFunc func(*StreamRecord)) *Stream {
	return s.Map(func(record *StreamRecord) (*StreamRecord, error) {
		peekFunc(record)
		return record, nil
	})
}

// Build builds and returns the topology
func (sb *StreamBuilder) Build() *StreamTopology {
	sb.topology.Name = fmt.Sprintf("topology-%d", time.Now().Unix())
	return sb.topology
}

// BuildWithName builds and returns the topology with a specific name
func (sb *StreamBuilder) BuildWithName(name string) *StreamTopology {
	sb.topology.Name = name
	return sb.topology
}

// Helper methods

func (sb *StreamBuilder) generateNodeName(prefix string) string {
	sb.nodeCounter++
	return fmt.Sprintf("%s-%d", prefix, sb.nodeCounter)
}

func (sb *StreamBuilder) addConnection(from, to string) {
	connection := Connection{
		From: from,
		To:   to,
	}
	sb.topology.Connections = append(sb.topology.Connections, connection)
}

// StreamApplication represents a complete streaming application
type StreamApplication struct {
	name     string
	config   *StreamConfig
	engine   *StreamingEngine
	topology *StreamTopology
	storage  *storage.LogEngine
}

// NewStreamApplication creates a new stream application
func NewStreamApplication(name string, config *StreamConfig, storage *storage.LogEngine) *StreamApplication {
	if config == nil {
		config = DefaultStreamConfig()
	}
	config.ApplicationID = name

	return &StreamApplication{
		name:    name,
		config:  config,
		storage: storage,
		engine:  NewStreamingEngine(config, storage),
	}
}

// SetTopology sets the application topology
func (sa *StreamApplication) SetTopology(topology *StreamTopology) error {
	sa.topology = topology
	return sa.engine.SetTopology(topology)
}

// AddStateStore adds a state store to the application
func (sa *StreamApplication) AddStateStore(name string, store StateStore) {
	sa.engine.AddStateStore(name, store)
}

// AddWindowStore adds a window store to the application
func (sa *StreamApplication) AddWindowStore(name string, store WindowStore) {
	sa.engine.AddWindowStore(name, store)
}

// AddSessionStore adds a session store to the application
func (sa *StreamApplication) AddSessionStore(name string, store SessionStore) {
	sa.engine.AddSessionStore(name, store)
}

// SetErrorHandler sets the error handler
func (sa *StreamApplication) SetErrorHandler(handler func(*StreamError)) {
	sa.engine.SetErrorHandler(handler)
}

// Start starts the application
func (sa *StreamApplication) Start() error {
	return sa.engine.Start()
}

// Stop stops the application
func (sa *StreamApplication) Stop() error {
	return sa.engine.Stop()
}

// GetMetrics returns application metrics
func (sa *StreamApplication) GetMetrics() *StreamMetrics {
	return sa.engine.GetMetrics()
}

// IsRunning returns whether the application is running
func (sa *StreamApplication) IsRunning() bool {
	return sa.engine.IsRunning()
}

// Example usage functions

// ExampleWordCount demonstrates a word count stream processing application
func ExampleWordCount() *StreamTopology {
	builder := NewStreamBuilder()

	// Source stream from "input" topic
	stream := builder.Source("input")

	// Flat map to split lines into words
	words := stream.FlatMap(func(record *StreamRecord) ([]*StreamRecord, error) {
		line := string(record.Value.([]byte))
		words := splitWords(line)

		var results []*StreamRecord
		for _, word := range words {
			wordRecord := &StreamRecord{
				Key:       word,
				Value:     []byte(word),
				Timestamp: record.Timestamp,
				Headers:   record.Headers,
			}
			results = append(results, wordRecord)
		}

		return results, nil
	})

	// Count words in tumbling 5-minute windows
	counts := words.Window(5 * time.Minute).Count()

	// Send results to "word-counts" topic
	counts.To("word-counts")

	return builder.Build()
}

// ExampleJoinStreams demonstrates stream joining
func ExampleJoinStreams() *StreamTopology {
	builder := NewStreamBuilder()

	// User stream
	users := builder.Source("users")

	// Purchase stream
	purchases := builder.Source("purchases")

	// Join purchases with user data
	enriched := purchases.InnerJoin(users, "user_id", "id", 1*time.Hour)

	// Select joined data
	result := enriched.Select(func(joinResult *JoinResult) *StreamRecord {
		// Combine purchase and user data
		return &StreamRecord{
			Key:       joinResult.JoinKey,
			Value:     fmt.Sprintf("Purchase by user: %v", joinResult.LeftRecord.Value),
			Timestamp: time.Now(),
		}
	})

	// Send to enriched topic
	result.To("enriched-purchases")

	return builder.Build()
}

// Helper function for word splitting
func splitWords(line string) []string {
	// Simplified word splitting
	words := make([]string, 0)
	current := ""

	for _, char := range line {
		if char == ' ' || char == '\t' || char == '\n' {
			if current != "" {
				words = append(words, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}

	if current != "" {
		words = append(words, current)
	}

	return words
}

