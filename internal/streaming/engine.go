package streaming

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"streaming-engine/internal/storage"
	"streaming-engine/pkg/types"
)

// StreamingEngine represents the main streaming processing engine
type StreamingEngine struct {
	config        *StreamConfig
	topology      *StreamTopology
	windowManager *WindowManager
	stateStores   map[string]StateStore
	windowStores  map[string]WindowStore
	sessionStores map[string]SessionStore
	processors    map[string]StreamProcessor
	threads       []*StreamThread
	storage       *storage.LogEngine

	// Control
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running int32

	// Metrics
	metrics *StreamMetrics

	// Error handling
	errorHandler func(*StreamError)
	retryPolicy  *RetryPolicy
}

// NewStreamingEngine creates a new streaming engine
func NewStreamingEngine(config *StreamConfig, storage *storage.LogEngine) *StreamingEngine {
	if config == nil {
		config = DefaultStreamConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	engine := &StreamingEngine{
		config:        config,
		storage:       storage,
		windowManager: NewWindowManager(DefaultWindowConfig()),
		stateStores:   make(map[string]StateStore),
		windowStores:  make(map[string]WindowStore),
		sessionStores: make(map[string]SessionStore),
		processors:    make(map[string]StreamProcessor),
		ctx:           ctx,
		cancel:        cancel,
		metrics:       &StreamMetrics{},
		retryPolicy:   DefaultRetryPolicy(),
	}

	return engine
}

// SetTopology sets the processing topology
func (se *StreamingEngine) SetTopology(topology *StreamTopology) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	if atomic.LoadInt32(&se.running) == 1 {
		return fmt.Errorf("cannot set topology while engine is running")
	}

	se.topology = topology
	return nil
}

// AddStateStore adds a state store
func (se *StreamingEngine) AddStateStore(name string, store StateStore) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.stateStores[name] = store
}

// AddWindowStore adds a window store
func (se *StreamingEngine) AddWindowStore(name string, store WindowStore) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.windowStores[name] = store
}

// AddSessionStore adds a session store
func (se *StreamingEngine) AddSessionStore(name string, store SessionStore) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.sessionStores[name] = store
}

// AddProcessor adds a processor
func (se *StreamingEngine) AddProcessor(name string, processor StreamProcessor) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.processors[name] = processor
}

// SetErrorHandler sets the error handler
func (se *StreamingEngine) SetErrorHandler(handler func(*StreamError)) {
	se.errorHandler = handler
}

// SetRetryPolicy sets the retry policy
func (se *StreamingEngine) SetRetryPolicy(policy *RetryPolicy) {
	se.retryPolicy = policy
}

// Start starts the streaming engine
func (se *StreamingEngine) Start() error {
	if !atomic.CompareAndSwapInt32(&se.running, 0, 1) {
		return fmt.Errorf("engine is already running")
	}

	se.mu.Lock()
	defer se.mu.Unlock()

	if se.topology == nil {
		atomic.StoreInt32(&se.running, 0)
		return fmt.Errorf("no topology set")
	}

	// Validate topology
	if err := se.validateTopology(); err != nil {
		atomic.StoreInt32(&se.running, 0)
		return fmt.Errorf("invalid topology: %w", err)
	}

	// Create stream threads
	for i := 0; i < se.config.NumStreamThreads; i++ {
		thread := NewStreamThread(
			fmt.Sprintf("stream-thread-%d", i),
			se.config,
			se.topology,
			se.processors,
			se.stateStores,
			se.windowStores,
			se.sessionStores,
			se.windowManager,
			se.storage,
		)

		thread.SetErrorHandler(se.handleError)
		se.threads = append(se.threads, thread)
	}

	// Start all threads
	for _, thread := range se.threads {
		se.wg.Add(1)
		go func(t *StreamThread) {
			defer se.wg.Done()
			if err := t.Run(se.ctx); err != nil {
				se.handleError(&StreamError{
					Type:      ErrorTypeProcessing,
					Message:   fmt.Sprintf("thread error: %v", err),
					Timestamp: time.Now(),
					Retryable: false,
				})
			}
		}(thread)
	}

	// Start metrics collection
	se.wg.Add(1)
	go se.metricsCollector()

	return nil
}

// Stop stops the streaming engine
func (se *StreamingEngine) Stop() error {
	if !atomic.CompareAndSwapInt32(&se.running, 1, 0) {
		return fmt.Errorf("engine is not running")
	}

	se.cancel()
	se.wg.Wait()

	// Close all resources
	se.mu.Lock()
	defer se.mu.Unlock()

	// Close threads
	for _, thread := range se.threads {
		thread.Close()
	}
	se.threads = nil

	// Close state stores
	for _, store := range se.stateStores {
		store.Close()
	}

	// Close window stores
	for _, store := range se.windowStores {
		store.Close()
	}

	// Close session stores
	for _, store := range se.sessionStores {
		store.Close()
	}

	// Close processors
	for _, processor := range se.processors {
		processor.Close()
	}

	return nil
}

// GetMetrics returns current metrics
func (se *StreamingEngine) GetMetrics() *StreamMetrics {
	se.mu.RLock()
	defer se.mu.RUnlock()

	// Aggregate metrics from all threads
	metrics := &StreamMetrics{}

	for _, thread := range se.threads {
		threadMetrics := thread.GetMetrics()
		metrics.RecordsProcessed += threadMetrics.RecordsProcessed
		metrics.ErrorRate += threadMetrics.ErrorRate
		metrics.WindowsCreated += threadMetrics.WindowsCreated
		metrics.WindowsExpired += threadMetrics.WindowsExpired

		if threadMetrics.ProcessingLatency > metrics.ProcessingLatency {
			metrics.ProcessingLatency = threadMetrics.ProcessingLatency
		}
	}

	if len(se.threads) > 0 {
		metrics.ErrorRate /= float64(len(se.threads))
		metrics.RecordsPerSecond = float64(metrics.RecordsProcessed) / time.Since(time.Now()).Seconds()
	}

	// Add state store sizes
	for _, store := range se.stateStores {
		metrics.StateStoreSize += store.Size()
	}

	return metrics
}

// IsRunning returns whether the engine is running
func (se *StreamingEngine) IsRunning() bool {
	return atomic.LoadInt32(&se.running) == 1
}

func (se *StreamingEngine) validateTopology() error {
	if se.topology == nil {
		return fmt.Errorf("topology is nil")
	}

	if len(se.topology.Sources) == 0 {
		return fmt.Errorf("topology has no sources")
	}

	// Validate connections
	nodeNames := make(map[string]bool)

	// Add all node names
	for name := range se.topology.Sources {
		nodeNames[name] = true
	}
	for name := range se.topology.Processors {
		nodeNames[name] = true
	}
	for name := range se.topology.Sinks {
		nodeNames[name] = true
	}

	// Validate connections reference valid nodes
	for _, conn := range se.topology.Connections {
		if !nodeNames[conn.From] {
			return fmt.Errorf("connection references invalid source node: %s", conn.From)
		}
		if !nodeNames[conn.To] {
			return fmt.Errorf("connection references invalid target node: %s", conn.To)
		}
	}

	return nil
}

func (se *StreamingEngine) handleError(err *StreamError) {
	if se.errorHandler != nil {
		se.errorHandler(err)
	}

	// Default error handling - log and potentially retry
	fmt.Printf("Stream processing error: %v\n", err)
}

func (se *StreamingEngine) metricsCollector() {
	defer se.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-se.ctx.Done():
			return
		case <-ticker.C:
			se.collectMetrics()
		}
	}
}

func (se *StreamingEngine) collectMetrics() {
	// Update metrics from threads and stores
	metrics := se.GetMetrics()

	se.mu.Lock()
	se.metrics = metrics
	se.mu.Unlock()

	// Trigger cleanup of expired windows
	watermark := time.Now().Add(-se.windowManager.config.AllowedLateness)
	expiredWindows := se.windowManager.GetExpiredWindows(watermark)

	if len(expiredWindows) > 0 {
		atomic.AddInt64(&se.metrics.WindowsExpired, int64(len(expiredWindows)))
	}
}

// StreamThread represents a single stream processing thread
type StreamThread struct {
	name          string
	config        *StreamConfig
	topology      *StreamTopology
	processors    map[string]StreamProcessor
	stateStores   map[string]StateStore
	windowStores  map[string]WindowStore
	sessionStores map[string]SessionStore
	windowManager *WindowManager
	storage       *storage.LogEngine

	// Processing state
	consumers map[string]*StreamConsumer
	producers map[string]*StreamProducer

	// Control
	running bool
	mu      sync.RWMutex

	// Metrics
	metrics *StreamMetrics

	// Error handling
	errorHandler func(*StreamError)
}

// NewStreamThread creates a new stream thread
func NewStreamThread(name string, config *StreamConfig, topology *StreamTopology,
	processors map[string]StreamProcessor, stateStores map[string]StateStore,
	windowStores map[string]WindowStore, sessionStores map[string]SessionStore,
	windowManager *WindowManager, storage *storage.LogEngine) *StreamThread {

	return &StreamThread{
		name:          name,
		config:        config,
		topology:      topology,
		processors:    processors,
		stateStores:   stateStores,
		windowStores:  windowStores,
		sessionStores: sessionStores,
		windowManager: windowManager,
		storage:       storage,
		consumers:     make(map[string]*StreamConsumer),
		producers:     make(map[string]*StreamProducer),
		metrics:       &StreamMetrics{},
	}
}

// SetErrorHandler sets the error handler
func (st *StreamThread) SetErrorHandler(handler func(*StreamError)) {
	st.errorHandler = handler
}

// Run runs the stream thread
func (st *StreamThread) Run(ctx context.Context) error {
	st.mu.Lock()
	st.running = true
	st.mu.Unlock()

	defer func() {
		st.mu.Lock()
		st.running = false
		st.mu.Unlock()
	}()

	// Create consumers for sources
	for _, source := range st.topology.Sources {
		consumer, err := NewStreamConsumer(source.Topics, st.config, st.storage)
		if err != nil {
			return fmt.Errorf("failed to create consumer: %w", err)
		}
		st.consumers[source.Name] = consumer
	}

	// Create producers for sinks
	for _, sink := range st.topology.Sinks {
		producer, err := NewStreamProducer(sink.Topic, st.config, st.storage)
		if err != nil {
			return fmt.Errorf("failed to create producer: %w", err)
		}
		st.producers[sink.Name] = producer
	}

	// Main processing loop
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := st.processOnce(); err != nil {
				if st.errorHandler != nil {
					st.errorHandler(&StreamError{
						Type:      ErrorTypeProcessing,
						Message:   err.Error(),
						Timestamp: time.Now(),
						Retryable: true,
					})
				}
			}
		}
	}
}

// GetMetrics returns thread metrics
func (st *StreamThread) GetMetrics() *StreamMetrics {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.metrics
}

// Close closes the stream thread
func (st *StreamThread) Close() error {
	// Close consumers
	for _, consumer := range st.consumers {
		consumer.Close()
	}

	// Close producers
	for _, producer := range st.producers {
		producer.Close()
	}

	return nil
}

func (st *StreamThread) processOnce() error {
	// Poll records from all consumers
	var allRecords []*StreamRecord

	for sourceName, consumer := range st.consumers {
		records, err := consumer.Poll(st.config.PollTimeout)
		if err != nil {
			return fmt.Errorf("failed to poll from %s: %w", sourceName, err)
		}
		allRecords = append(allRecords, records...)
	}

	if len(allRecords) == 0 {
		return nil
	}

	// Process records through topology
	for _, record := range allRecords {
		if err := st.processRecord(record); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
	}

	// Commit offsets
	for _, consumer := range st.consumers {
		if err := consumer.Commit(); err != nil {
			return fmt.Errorf("failed to commit offsets: %w", err)
		}
	}

	return nil
}

func (st *StreamThread) processRecord(record *StreamRecord) error {
	start := time.Now()
	defer func() {
		processingTime := time.Since(start)
		atomic.AddInt64(&st.metrics.RecordsProcessed, 1)
		if processingTime > st.metrics.ProcessingLatency {
			st.metrics.ProcessingLatency = processingTime
		}
	}()

	// Find source node for this record
	var sourceNode *SourceNode
	for _, source := range st.topology.Sources {
		for _, topic := range source.Topics {
			if topic == record.Topic {
				sourceNode = source
				break
			}
		}
		if sourceNode != nil {
			break
		}
	}

	if sourceNode == nil {
		return fmt.Errorf("no source node found for topic: %s", record.Topic)
	}

	// Process through topology starting from source
	return st.processNode(sourceNode.Name, record)
}

func (st *StreamThread) processNode(nodeName string, record *StreamRecord) error {
	// Check if this is a processor node
	if processorNode, exists := st.topology.Processors[nodeName]; exists {
		processor, exists := st.processors[processorNode.Name]
		if !exists {
			return fmt.Errorf("processor not found: %s", processorNode.Name)
		}

		// Process record
		results, err := processor.Process(record)
		if err != nil {
			return fmt.Errorf("processor %s failed: %w", processorNode.Name, err)
		}

		// Send results to downstream nodes
		for _, result := range results {
			if err := st.sendToDownstream(nodeName, result); err != nil {
				return err
			}
		}

		return nil
	}

	// Check if this is a sink node
	if sinkNode, exists := st.topology.Sinks[nodeName]; exists {
		producer, exists := st.producers[sinkNode.Name]
		if !exists {
			return fmt.Errorf("producer not found: %s", sinkNode.Name)
		}

		return producer.Send(record)
	}

	// If it's a source node, send to downstream
	return st.sendToDownstream(nodeName, record)
}

func (st *StreamThread) sendToDownstream(fromNode string, record *StreamRecord) error {
	// Find all connections from this node
	for _, conn := range st.topology.Connections {
		if conn.From == fromNode {
			if err := st.processNode(conn.To, record); err != nil {
				return err
			}
		}
	}

	return nil
}

// StreamConsumer represents a stream consumer
type StreamConsumer struct {
	topics  []string
	config  *StreamConfig
	storage *storage.LogEngine
	offsets map[string]int64 // topic-partition -> offset
}

// NewStreamConsumer creates a new stream consumer
func NewStreamConsumer(topics []string, config *StreamConfig, storage *storage.LogEngine) (*StreamConsumer, error) {
	return &StreamConsumer{
		topics:  topics,
		config:  config,
		storage: storage,
		offsets: make(map[string]int64),
	}, nil
}

// Poll polls for new records
func (sc *StreamConsumer) Poll(timeout time.Duration) ([]*StreamRecord, error) {
	var records []*StreamRecord

	// This is a simplified implementation
	// In practice, you'd poll from the actual storage system
	for _, topic := range sc.topics {
		// Get records from storage
		partitionKey := fmt.Sprintf("%s-0", topic) // Simplified - use partition 0
		offset := sc.offsets[partitionKey]

		result, err := sc.storage.Read(0, offset, 1000) // Read from partition 0
		if err != nil {
			continue // No records available
		}

		for _, msg := range result.Messages {
			record := &StreamRecord{
				Topic:     topic,
				Partition: 0,
				Offset:    msg.Offset,
				Key:       string(msg.Key),
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
				Headers:   msg.Headers,
				Context:   make(map[string]interface{}),
			}
			records = append(records, record)
			sc.offsets[partitionKey] = msg.Offset + 1
		}
	}

	return records, nil
}

// Commit commits current offsets
func (sc *StreamConsumer) Commit() error {
	// In practice, you'd commit offsets to a durable store
	return nil
}

// Close closes the consumer
func (sc *StreamConsumer) Close() error {
	return nil
}

// StreamProducer represents a stream producer
type StreamProducer struct {
	topic   string
	config  *StreamConfig
	storage *storage.LogEngine
}

// NewStreamProducer creates a new stream producer
func NewStreamProducer(topic string, config *StreamConfig, storage *storage.LogEngine) (*StreamProducer, error) {
	return &StreamProducer{
		topic:   topic,
		config:  config,
		storage: storage,
	}, nil
}

// Send sends a record to the topic
func (sp *StreamProducer) Send(record *StreamRecord) error {
	// Convert StreamRecord to Message
	msg := &types.Message{
		Key:       []byte(record.Key),
		Value:     record.Value.([]byte), // Simplified conversion
		Headers:   record.Headers,
		Timestamp: record.Timestamp,
		Topic:     sp.topic,
		Partition: record.Partition,
	}

	// Append to storage
	_, err := sp.storage.Append(record.Partition, []*types.Message{msg})
	return err
}

// Close closes the producer
func (sp *StreamProducer) Close() error {
	return nil
}
