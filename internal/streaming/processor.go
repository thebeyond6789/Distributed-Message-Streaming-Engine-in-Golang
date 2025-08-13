package streaming

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ProcessorType represents different types of stream processors
type ProcessorType string

const (
	ProcessorTypeMap       ProcessorType = "map"
	ProcessorTypeFilter    ProcessorType = "filter"
	ProcessorTypeAggregate ProcessorType = "aggregate"
	ProcessorTypeJoin      ProcessorType = "join"
	ProcessorTypeReduce    ProcessorType = "reduce"
	ProcessorTypeFlatMap   ProcessorType = "flatmap"
	ProcessorTypeWindow    ProcessorType = "window"
	ProcessorTypeCustom    ProcessorType = "custom"
)

// BaseProcessor provides common processor functionality
type BaseProcessor struct {
	name       string
	config     map[string]interface{}
	stateStore StateStore
	metrics    *ProcessorMetrics
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
}

// ProcessorMetrics represents processor metrics
type ProcessorMetrics struct {
	RecordsProcessed int64     `json:"records_processed"`
	RecordsDropped   int64     `json:"records_dropped"`
	ErrorCount       int64     `json:"error_count"`
	ProcessingTime   int64     `json:"processing_time_ns"`
	LastProcessed    time.Time `json:"last_processed"`
}

// NewBaseProcessor creates a new base processor
func NewBaseProcessor(name string, config map[string]interface{}, stateStore StateStore) *BaseProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &BaseProcessor{
		name:       name,
		config:     config,
		stateStore: stateStore,
		metrics:    &ProcessorMetrics{},
		ctx:        ctx,
		cancel:     cancel,
	}
}

// GetState returns the state store
func (bp *BaseProcessor) GetState() StateStore {
	return bp.stateStore
}

// GetMetrics returns processor metrics
func (bp *BaseProcessor) GetMetrics() *ProcessorMetrics {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.metrics
}

// Close closes the processor
func (bp *BaseProcessor) Close() error {
	bp.cancel()
	if bp.stateStore != nil {
		return bp.stateStore.Close()
	}
	return nil
}

func (bp *BaseProcessor) updateMetrics(processed int, dropped int, processingTime time.Duration, hasError bool) {
	atomic.AddInt64(&bp.metrics.RecordsProcessed, int64(processed))
	atomic.AddInt64(&bp.metrics.RecordsDropped, int64(dropped))
	atomic.AddInt64(&bp.metrics.ProcessingTime, int64(processingTime))

	if hasError {
		atomic.AddInt64(&bp.metrics.ErrorCount, 1)
	}

	bp.mu.Lock()
	bp.metrics.LastProcessed = time.Now()
	bp.mu.Unlock()
}

// MapProcessor implements map transformation
type MapProcessor struct {
	*BaseProcessor
	mapFunc MapFunction
}

// NewMapProcessor creates a new map processor
func NewMapProcessor(name string, config map[string]interface{}, mapFunc MapFunction) *MapProcessor {
	return &MapProcessor{
		BaseProcessor: NewBaseProcessor(name, config, nil),
		mapFunc:       mapFunc,
	}
}

// Process applies the map function to the record
func (mp *MapProcessor) Process(record *StreamRecord) ([]*StreamRecord, error) {
	start := time.Now()

	mapped, err := mp.mapFunc(record)
	processingTime := time.Since(start)

	if err != nil {
		mp.updateMetrics(0, 1, processingTime, true)
		return nil, fmt.Errorf("map processing failed: %w", err)
	}

	mp.updateMetrics(1, 0, processingTime, false)

	if mapped == nil {
		return []*StreamRecord{}, nil
	}

	return []*StreamRecord{mapped}, nil
}

// FilterProcessor implements filtering
type FilterProcessor struct {
	*BaseProcessor
	predicate FilterPredicate
}

// NewFilterProcessor creates a new filter processor
func NewFilterProcessor(name string, config map[string]interface{}, predicate FilterPredicate) *FilterProcessor {
	return &FilterProcessor{
		BaseProcessor: NewBaseProcessor(name, config, nil),
		predicate:     predicate,
	}
}

// Process applies the filter predicate to the record
func (fp *FilterProcessor) Process(record *StreamRecord) ([]*StreamRecord, error) {
	start := time.Now()

	passed := fp.predicate(record)
	processingTime := time.Since(start)

	if passed {
		fp.updateMetrics(1, 0, processingTime, false)
		return []*StreamRecord{record}, nil
	}

	fp.updateMetrics(0, 1, processingTime, false)
	return []*StreamRecord{}, nil
}

// AggregateProcessor implements windowed aggregation
type AggregateProcessor struct {
	*BaseProcessor
	windowManager *WindowManager
	windowName    string
	aggregateFunc AggregateFunction
	reduceFunc    ReduceFunction
	windowStore   WindowStore
	trigger       WindowTrigger
}

// NewAggregateProcessor creates a new aggregate processor
func NewAggregateProcessor(name string, config map[string]interface{},
	windowManager *WindowManager, windowName string,
	aggregateFunc AggregateFunction, reduceFunc ReduceFunction,
	windowStore WindowStore, trigger WindowTrigger) *AggregateProcessor {

	return &AggregateProcessor{
		BaseProcessor: NewBaseProcessor(name, config, nil),
		windowManager: windowManager,
		windowName:    windowName,
		aggregateFunc: aggregateFunc,
		reduceFunc:    reduceFunc,
		windowStore:   windowStore,
		trigger:       trigger,
	}
}

// Process aggregates records in windows
func (ap *AggregateProcessor) Process(record *StreamRecord) ([]*StreamRecord, error) {
	start := time.Now()

	// Assign record to windows
	windows, err := ap.windowManager.AssignWindows(ap.windowName, record)
	if err != nil {
		processingTime := time.Since(start)
		ap.updateMetrics(0, 1, processingTime, true)
		return nil, fmt.Errorf("window assignment failed: %w", err)
	}

	var results []*StreamRecord

	for _, window := range windows {
		// Get current aggregate value
		stateKey := fmt.Sprintf("%s:%s", record.Key, window.Start.Format(time.RFC3339))
		currentValue, _ := ap.windowStore.Get(stateKey, window)

		// Apply aggregation
		var newValue interface{}
		switch ap.aggregateFunc {
		case AggregateFunctionSum:
			if currentValue == nil {
				newValue = record.Value
			} else {
				newValue = ap.reduceFunc(currentValue, record.Value)
			}
		case AggregateFunctionCount:
			if currentValue == nil {
				newValue = int64(1)
			} else {
				newValue = currentValue.(int64) + 1
			}
		case AggregateFunctionCustom:
			if ap.reduceFunc != nil {
				newValue = ap.reduceFunc(currentValue, record.Value)
			} else {
				newValue = record.Value
			}
		default:
			newValue = record.Value
		}

		// Store updated aggregate
		if err := ap.windowStore.Put(stateKey, newValue, window); err != nil {
			processingTime := time.Since(start)
			ap.updateMetrics(0, 1, processingTime, true)
			return nil, fmt.Errorf("failed to store aggregate: %w", err)
		}

		// Check if trigger should fire
		count := int64(1) // Simplified - in real implementation, track count
		if ap.trigger != nil && ap.trigger.ShouldFire(window, record, count) {
			// Create result record
			resultRecord := &StreamRecord{
				Topic:     record.Topic + "-aggregated",
				Partition: record.Partition,
				Key:       record.Key,
				Value:     newValue,
				Timestamp: time.Now(),
				Headers:   record.Headers,
				Context: map[string]interface{}{
					"window":          window,
					"aggregate_func":  ap.aggregateFunc,
					"original_record": record,
				},
			}
			results = append(results, resultRecord)
		}
	}

	processingTime := time.Since(start)
	ap.updateMetrics(1, 0, processingTime, false)

	return results, nil
}

// JoinProcessor implements stream joins
type JoinProcessor struct {
	*BaseProcessor
	leftStore     StateStore
	rightStore    StateStore
	joinType      JoinType
	joinCondition *JoinCondition
	windowManager *WindowManager
}

// NewJoinProcessor creates a new join processor
func NewJoinProcessor(name string, config map[string]interface{},
	leftStore, rightStore StateStore, joinType JoinType,
	joinCondition *JoinCondition, windowManager *WindowManager) *JoinProcessor {

	return &JoinProcessor{
		BaseProcessor: NewBaseProcessor(name, config, nil),
		leftStore:     leftStore,
		rightStore:    rightStore,
		joinType:      joinType,
		joinCondition: joinCondition,
		windowManager: windowManager,
	}
}

// Process performs stream join
func (jp *JoinProcessor) Process(record *StreamRecord) ([]*StreamRecord, error) {
	start := time.Now()

	// Determine which side this record belongs to
	var ownStore, otherStore StateStore

	// This is simplified - in practice, you'd need to determine left/right
	// based on topic or other metadata
	isLeft := true // Simplified assumption

	if isLeft {
		ownStore = jp.leftStore
		otherStore = jp.rightStore
	} else {
		ownStore = jp.rightStore
		otherStore = jp.leftStore
	}

	// Store current record
	if err := ownStore.Put(record.Key, record); err != nil {
		processingTime := time.Since(start)
		jp.updateMetrics(0, 1, processingTime, true)
		return nil, fmt.Errorf("failed to store record: %w", err)
	}

	// Look for matching records in other store
	var results []*StreamRecord

	// Get join key value from record
	joinKeyValue := record.Key // Simplified

	// Find matching records within time window
	otherRecord, err := otherStore.Get(joinKeyValue)
	if err != nil {
		// No match found, handle based on join type
		switch jp.joinType {
		case JoinTypeInner:
			// Inner join - no output if no match
		case JoinTypeLeft:
			if isLeft {
				// Left join with left record - output with null right
				joinResult := &StreamRecord{
					Topic:     record.Topic + "-joined",
					Partition: record.Partition,
					Key:       record.Key,
					Value: &JoinResult{
						LeftRecord:  record,
						RightRecord: nil,
						JoinKey:     joinKeyValue,
						Timestamp:   record.Timestamp,
					},
					Timestamp: record.Timestamp,
					Headers:   record.Headers,
				}
				results = append(results, joinResult)
			}
		case JoinTypeRight:
			if !isLeft {
				// Right join with right record - output with null left
				joinResult := &StreamRecord{
					Topic:     record.Topic + "-joined",
					Partition: record.Partition,
					Key:       record.Key,
					Value: &JoinResult{
						LeftRecord:  nil,
						RightRecord: record,
						JoinKey:     joinKeyValue,
						Timestamp:   record.Timestamp,
					},
					Timestamp: record.Timestamp,
					Headers:   record.Headers,
				}
				results = append(results, joinResult)
			}
		case JoinTypeFull:
			// Full outer join - always output
			var leftRec, rightRec *StreamRecord
			if isLeft {
				leftRec = record
			} else {
				rightRec = record
			}

			joinResult := &StreamRecord{
				Topic:     record.Topic + "-joined",
				Partition: record.Partition,
				Key:       record.Key,
				Value: &JoinResult{
					LeftRecord:  leftRec,
					RightRecord: rightRec,
					JoinKey:     joinKeyValue,
					Timestamp:   record.Timestamp,
				},
				Timestamp: record.Timestamp,
				Headers:   record.Headers,
			}
			results = append(results, joinResult)
		}
	} else {
		// Match found - check time window
		otherStreamRecord := otherRecord.(*StreamRecord)
		timeDiff := record.Timestamp.Sub(otherStreamRecord.Timestamp)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		if timeDiff <= jp.joinCondition.TimeWindow {
			// Within time window - create join result
			var leftRec, rightRec *StreamRecord
			if isLeft {
				leftRec = record
				rightRec = otherStreamRecord
			} else {
				leftRec = otherStreamRecord
				rightRec = record
			}

			joinResult := &StreamRecord{
				Topic:     record.Topic + "-joined",
				Partition: record.Partition,
				Key:       record.Key,
				Value: &JoinResult{
					LeftRecord:  leftRec,
					RightRecord: rightRec,
					JoinKey:     joinKeyValue,
					Timestamp:   record.Timestamp,
				},
				Timestamp: record.Timestamp,
				Headers:   record.Headers,
			}
			results = append(results, joinResult)
		}
	}

	processingTime := time.Since(start)
	jp.updateMetrics(1, 0, processingTime, false)

	return results, nil
}

// FlatMapProcessor implements flat map transformation
type FlatMapProcessor struct {
	*BaseProcessor
	flatMapFunc func(*StreamRecord) ([]*StreamRecord, error)
}

// NewFlatMapProcessor creates a new flat map processor
func NewFlatMapProcessor(name string, config map[string]interface{},
	flatMapFunc func(*StreamRecord) ([]*StreamRecord, error)) *FlatMapProcessor {

	return &FlatMapProcessor{
		BaseProcessor: NewBaseProcessor(name, config, nil),
		flatMapFunc:   flatMapFunc,
	}
}

// Process applies the flat map function
func (fmp *FlatMapProcessor) Process(record *StreamRecord) ([]*StreamRecord, error) {
	start := time.Now()

	results, err := fmp.flatMapFunc(record)
	processingTime := time.Since(start)

	if err != nil {
		fmp.updateMetrics(0, 1, processingTime, true)
		return nil, fmt.Errorf("flat map processing failed: %w", err)
	}

	fmp.updateMetrics(len(results), 0, processingTime, false)
	return results, nil
}

// CustomProcessor allows for custom processing logic
type CustomProcessor struct {
	*BaseProcessor
	processFunc ProcessorFunction
}

// NewCustomProcessor creates a new custom processor
func NewCustomProcessor(name string, config map[string]interface{},
	stateStore StateStore, processFunc ProcessorFunction) *CustomProcessor {

	return &CustomProcessor{
		BaseProcessor: NewBaseProcessor(name, config, stateStore),
		processFunc:   processFunc,
	}
}

// Process applies the custom processing function
func (cp *CustomProcessor) Process(record *StreamRecord) ([]*StreamRecord, error) {
	start := time.Now()

	results, err := cp.processFunc(record)
	processingTime := time.Since(start)

	if err != nil {
		cp.updateMetrics(0, 1, processingTime, true)
		return nil, fmt.Errorf("custom processing failed: %w", err)
	}

	cp.updateMetrics(len(results), 0, processingTime, false)
	return results, nil
}

// ProcessorChain represents a chain of processors
type ProcessorChain struct {
	processors []StreamProcessor
	metrics    *ProcessorMetrics
	mu         sync.RWMutex
}

// NewProcessorChain creates a new processor chain
func NewProcessorChain(processors ...StreamProcessor) *ProcessorChain {
	return &ProcessorChain{
		processors: processors,
		metrics:    &ProcessorMetrics{},
	}
}

// Process processes a record through the entire chain
func (pc *ProcessorChain) Process(record *StreamRecord) ([]*StreamRecord, error) {
	start := time.Now()

	current := []*StreamRecord{record}

	for _, processor := range pc.processors {
		var next []*StreamRecord

		for _, rec := range current {
			results, err := processor.Process(rec)
			if err != nil {
				processingTime := time.Since(start)
				pc.updateMetrics(0, 1, processingTime, true)
				return nil, fmt.Errorf("processor chain failed: %w", err)
			}
			next = append(next, results...)
		}

		current = next
	}

	processingTime := time.Since(start)
	pc.updateMetrics(len(current), 0, processingTime, false)

	return current, nil
}

// GetState returns nil for processor chain (no single state store)
func (pc *ProcessorChain) GetState() StateStore {
	return nil
}

// Close closes all processors in the chain
func (pc *ProcessorChain) Close() error {
	var lastErr error
	for _, processor := range pc.processors {
		if err := processor.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// GetMetrics returns chain metrics
func (pc *ProcessorChain) GetMetrics() *ProcessorMetrics {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.metrics
}

func (pc *ProcessorChain) updateMetrics(processed, dropped int, processingTime time.Duration, hasError bool) {
	atomic.AddInt64(&pc.metrics.RecordsProcessed, int64(processed))
	atomic.AddInt64(&pc.metrics.RecordsDropped, int64(dropped))
	atomic.AddInt64(&pc.metrics.ProcessingTime, int64(processingTime))

	if hasError {
		atomic.AddInt64(&pc.metrics.ErrorCount, 1)
	}

	pc.mu.Lock()
	pc.metrics.LastProcessed = time.Now()
	pc.mu.Unlock()
}
