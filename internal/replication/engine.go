package replication

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"streaming-engine/internal/raft"
	"streaming-engine/internal/storage"
	"streaming-engine/pkg/types"
)

// Engine handles message replication across brokers
type Engine struct {
	config *Config
	nodeID types.NodeID

	// Dependencies
	storage  *storage.LogEngine
	raftNode *raft.RaftNode

	// Replication state
	mu              sync.RWMutex
	partitionStates map[string]*PartitionReplicationState

	// Metrics
	metrics *Metrics

	// Control
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running int32

	// Event listeners
	eventListeners []EventListener
	eventMu        sync.RWMutex
}

// Config represents replication engine configuration
type Config struct {
	NodeID                     types.NodeID               `json:"node_id"`
	ReplicationFactor          int32                      `json:"replication_factor"`
	MinISR                     int32                      `json:"min_isr"`
	ReplicaLagTime             time.Duration              `json:"replica_lag_time"`
	ReplicaLagMessages         int64                      `json:"replica_lag_messages"`
	SyncInterval               time.Duration              `json:"sync_interval"`
	HeartbeatInterval          time.Duration              `json:"heartbeat_interval"`
	MaxRetries                 int                        `json:"max_retries"`
	RetryBackoff               time.Duration              `json:"retry_backoff"`
	ConsistencyLevel           types.ConsistencyLevel     `json:"consistency_level"`
	EnableCrossDatacenter      bool                       `json:"enable_cross_datacenter"`
	ConflictResolutionStrategy ConflictResolutionStrategy `json:"conflict_resolution_strategy"`
}

// DefaultConfig returns default replication configuration
func DefaultConfig() *Config {
	return &Config{
		ReplicationFactor:          3,
		MinISR:                     2,
		ReplicaLagTime:             30 * time.Second,
		ReplicaLagMessages:         1000,
		SyncInterval:               time.Second,
		HeartbeatInterval:          5 * time.Second,
		MaxRetries:                 3,
		RetryBackoff:               100 * time.Millisecond,
		ConsistencyLevel:           types.ConsistencyLevelQuorum,
		EnableCrossDatacenter:      false,
		ConflictResolutionStrategy: ConflictResolutionLastWriteWins,
	}
}

// PartitionReplicationState represents replication state for a partition
type PartitionReplicationState struct {
	Topic            string                         `json:"topic"`
	Partition        int32                          `json:"partition"`
	Leader           types.NodeID                   `json:"leader"`
	Replicas         []types.NodeID                 `json:"replicas"`
	ISR              []types.NodeID                 `json:"isr"`
	HighWatermark    int64                          `json:"high_watermark"`
	LogEndOffset     int64                          `json:"log_end_offset"`
	ReplicaStates    map[types.NodeID]*ReplicaState `json:"replica_states"`
	LastISRUpdate    time.Time                      `json:"last_isr_update"`
	ConflictCount    int64                          `json:"conflict_count"`
	LastConflictTime time.Time                      `json:"last_conflict_time"`
}

// ReplicaState represents the state of a single replica
type ReplicaState struct {
	NodeID          types.NodeID  `json:"node_id"`
	LastFetchTime   time.Time     `json:"last_fetch_time"`
	LastFetchOffset int64         `json:"last_fetch_offset"`
	CaughtUpTime    time.Time     `json:"caught_up_time"`
	LagMessages     int64         `json:"lag_messages"`
	LagTime         time.Duration `json:"lag_time"`
	InSync          bool          `json:"in_sync"`
	LastHeartbeat   time.Time     `json:"last_heartbeat"`
}

// Metrics represents replication metrics
type Metrics struct {
	TotalReplications     int64   `json:"total_replications"`
	FailedReplications    int64   `json:"failed_replications"`
	ConflictsResolved     int64   `json:"conflicts_resolved"`
	ISRChanges            int64   `json:"isr_changes"`
	AverageReplicationLag float64 `json:"average_replication_lag"`
	ReplicationThroughput float64 `json:"replication_throughput"`
}

// ConflictResolutionStrategy defines how conflicts are resolved
type ConflictResolutionStrategy string

const (
	ConflictResolutionLastWriteWins  ConflictResolutionStrategy = "last_write_wins"
	ConflictResolutionFirstWriteWins ConflictResolutionStrategy = "first_write_wins"
	ConflictResolutionTimestamp      ConflictResolutionStrategy = "timestamp"
	ConflictResolutionCustom         ConflictResolutionStrategy = "custom"
)

// EventListener listens to replication events
type EventListener interface {
	OnEvent(event Event)
}

// Event represents a replication event
type Event interface {
	Type() string
	Data() interface{}
}

// NewEngine creates a new replication engine
func NewEngine(config *Config, storage *storage.LogEngine, raftNode *raft.RaftNode) *Engine {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		config:          config,
		nodeID:          config.NodeID,
		storage:         storage,
		raftNode:        raftNode,
		partitionStates: make(map[string]*PartitionReplicationState),
		metrics:         &Metrics{},
		ctx:             ctx,
		cancel:          cancel,
		eventListeners:  make([]EventListener, 0),
	}
}

// Start starts the replication engine
func (e *Engine) Start() error {
	if !atomic.CompareAndSwapInt32(&e.running, 0, 1) {
		return fmt.Errorf("replication engine already running")
	}

	// Start background tasks
	e.wg.Add(1)
	go e.syncLoop()

	e.wg.Add(1)
	go e.heartbeatLoop()

	e.wg.Add(1)
	go e.isrUpdateLoop()

	return nil
}

// Stop stops the replication engine
func (e *Engine) Stop() error {
	if !atomic.CompareAndSwapInt32(&e.running, 1, 0) {
		return nil
	}

	e.cancel()
	e.wg.Wait()

	return nil
}

// AddPartition adds a partition for replication
func (e *Engine) AddPartition(topic string, partition int32, leader types.NodeID, replicas []types.NodeID) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)

	state := &PartitionReplicationState{
		Topic:         topic,
		Partition:     partition,
		Leader:        leader,
		Replicas:      replicas,
		ISR:           []types.NodeID{leader}, // Initially only leader is in ISR
		ReplicaStates: make(map[types.NodeID]*ReplicaState),
		LastISRUpdate: time.Now(),
	}

	// Initialize replica states
	for _, replica := range replicas {
		state.ReplicaStates[replica] = &ReplicaState{
			NodeID:        replica,
			InSync:        replica == leader,
			LastHeartbeat: time.Now(),
		}
	}

	e.partitionStates[key] = state

	// Emit event
	e.emitEvent(&PartitionAddedEvent{
		Topic:     topic,
		Partition: partition,
		Leader:    leader,
		Replicas:  replicas,
		Timestamp: time.Now(),
	})

	return nil
}

// RemovePartition removes a partition from replication
func (e *Engine) RemovePartition(topic string, partition int32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	delete(e.partitionStates, key)

	// Emit event
	e.emitEvent(&PartitionRemovedEvent{
		Topic:     topic,
		Partition: partition,
		Timestamp: time.Now(),
	})

	return nil
}

// ReplicateMessage replicates a message to followers
func (e *Engine) ReplicateMessage(topic string, partition int32, messages []*types.Message) error {
	e.mu.RLock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	state, exists := e.partitionStates[key]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("partition %s-%d not found", topic, partition)
	}

	// Only leader can replicate
	if state.Leader != e.nodeID {
		return fmt.Errorf("not leader for partition %s-%d", topic, partition)
	}

	// Append to local storage first
	result, err := e.storage.Append(partition, messages)
	if err != nil {
		atomic.AddInt64(&e.metrics.FailedReplications, 1)
		return fmt.Errorf("failed to append locally: %w", err)
	}

	// Update partition state
	e.mu.Lock()
	state.LogEndOffset = result.LastOffset + 1
	e.mu.Unlock()

	// Replicate to followers
	return e.replicateToFollowers(state, messages, result.BaseOffset)
}

// FetchMessages fetches messages from leader for replication
func (e *Engine) FetchMessages(topic string, partition int32, offset int64, maxBytes int) (*ReplicationResponse, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	e.mu.RLock()
	state, exists := e.partitionStates[key]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("partition %s-%d not found", topic, partition)
	}

	// Read from storage
	result, err := e.storage.Read(partition, offset, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read from storage: %w", err)
	}

	// Update replica state
	e.updateReplicaState(state, e.nodeID, offset)

	return &ReplicationResponse{
		Topic:         topic,
		Partition:     partition,
		Messages:      result.Messages,
		HighWatermark: state.HighWatermark,
		LogEndOffset:  state.LogEndOffset,
		ErrorCode:     result.ErrorCode,
	}, nil
}

// UpdateHighWatermark updates the high watermark for a partition
func (e *Engine) UpdateHighWatermark(topic string, partition int32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	state, exists := e.partitionStates[key]
	if !exists {
		return fmt.Errorf("partition %s-%d not found", topic, partition)
	}

	// Only leader can update high watermark
	if state.Leader != e.nodeID {
		return nil
	}

	// Find minimum offset among ISR replicas
	minOffset := state.LogEndOffset
	for _, replicaID := range state.ISR {
		if replicaState, exists := state.ReplicaStates[replicaID]; exists {
			if replicaState.LastFetchOffset < minOffset {
				minOffset = replicaState.LastFetchOffset
			}
		}
	}

	// Update high watermark
	if minOffset > state.HighWatermark {
		state.HighWatermark = minOffset

		// Emit event
		e.emitEvent(&HighWatermarkUpdatedEvent{
			Topic:         topic,
			Partition:     partition,
			HighWatermark: minOffset,
			Timestamp:     time.Now(),
		})
	}

	return nil
}

// GetPartitionState returns the replication state for a partition
func (e *Engine) GetPartitionState(topic string, partition int32) (*PartitionReplicationState, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	state, exists := e.partitionStates[key]
	if !exists {
		return nil, fmt.Errorf("partition %s-%d not found", topic, partition)
	}

	// Return a copy
	return e.copyPartitionState(state), nil
}

// GetMetrics returns replication metrics
func (e *Engine) GetMetrics() *Metrics {
	// Return a copy
	return &Metrics{
		TotalReplications:     atomic.LoadInt64(&e.metrics.TotalReplications),
		FailedReplications:    atomic.LoadInt64(&e.metrics.FailedReplications),
		ConflictsResolved:     atomic.LoadInt64(&e.metrics.ConflictsResolved),
		ISRChanges:            atomic.LoadInt64(&e.metrics.ISRChanges),
		AverageReplicationLag: e.metrics.AverageReplicationLag,
		ReplicationThroughput: e.metrics.ReplicationThroughput,
	}
}

// AddEventListener adds an event listener
func (e *Engine) AddEventListener(listener EventListener) {
	e.eventMu.Lock()
	defer e.eventMu.Unlock()

	e.eventListeners = append(e.eventListeners, listener)
}

// RemoveEventListener removes an event listener
func (e *Engine) RemoveEventListener(listener EventListener) {
	e.eventMu.Lock()
	defer e.eventMu.Unlock()

	for i, l := range e.eventListeners {
		if l == listener {
			e.eventListeners = append(e.eventListeners[:i], e.eventListeners[i+1:]...)
			break
		}
	}
}

// Private methods

func (e *Engine) syncLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.performSync()
		}
	}
}

func (e *Engine) heartbeatLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.sendHeartbeats()
		}
	}
}

func (e *Engine) isrUpdateLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.updateISRs()
		}
	}
}

func (e *Engine) performSync() {
	e.mu.RLock()
	states := make([]*PartitionReplicationState, 0, len(e.partitionStates))
	for _, state := range e.partitionStates {
		states = append(states, state)
	}
	e.mu.RUnlock()

	for _, state := range states {
		if state.Leader == e.nodeID {
			// As leader, update high watermark
			e.UpdateHighWatermark(state.Topic, state.Partition)
		} else {
			// As follower, fetch from leader
			e.fetchFromLeader(state)
		}
	}
}

func (e *Engine) sendHeartbeats() {
	e.mu.RLock()
	states := make([]*PartitionReplicationState, 0, len(e.partitionStates))
	for _, state := range e.partitionStates {
		states = append(states, state)
	}
	e.mu.RUnlock()

	for _, state := range states {
		// Send heartbeat to all replicas
		for _, replicaID := range state.Replicas {
			if replicaID != e.nodeID {
				go e.sendHeartbeat(replicaID, state.Topic, state.Partition)
			}
		}
	}
}

func (e *Engine) updateISRs() {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	for _, state := range e.partitionStates {
		if state.Leader != e.nodeID {
			continue // Only leader manages ISR
		}

		var newISR []types.NodeID
		isrChanged := false

		for _, replicaID := range state.Replicas {
			replicaState := state.ReplicaStates[replicaID]
			if replicaState == nil {
				continue
			}

			// Check if replica is in sync
			inSync := e.isReplicaInSync(state, replicaState, now)

			if inSync {
				newISR = append(newISR, replicaID)
			}

			// Update replica sync status
			if replicaState.InSync != inSync {
				replicaState.InSync = inSync
				isrChanged = true
			}
		}

		// Update ISR if changed
		if isrChanged || len(newISR) != len(state.ISR) {
			state.ISR = newISR
			state.LastISRUpdate = now
			atomic.AddInt64(&e.metrics.ISRChanges, 1)

			// Emit event
			e.emitEvent(&ISRChangedEvent{
				Topic:     state.Topic,
				Partition: state.Partition,
				NewISR:    newISR,
				Timestamp: now,
			})
		}
	}
}

func (e *Engine) isReplicaInSync(state *PartitionReplicationState, replica *ReplicaState, now time.Time) bool {
	// Check message lag
	if state.LogEndOffset-replica.LastFetchOffset > e.config.ReplicaLagMessages {
		return false
	}

	// Check time lag
	if now.Sub(replica.LastFetchTime) > e.config.ReplicaLagTime {
		return false
	}

	// Check heartbeat
	if now.Sub(replica.LastHeartbeat) > e.config.HeartbeatInterval*2 {
		return false
	}

	return true
}

func (e *Engine) replicateToFollowers(state *PartitionReplicationState, messages []*types.Message, baseOffset int64) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(state.Replicas))

	successCount := int32(1) // Leader counts as 1

	for _, replicaID := range state.Replicas {
		if replicaID == e.nodeID {
			continue // Skip self
		}

		wg.Add(1)
		go func(replica types.NodeID) {
			defer wg.Done()

			if err := e.replicateToFollower(replica, state, messages, baseOffset); err != nil {
				errCh <- err
			} else {
				atomic.AddInt32(&successCount, 1)
			}
		}(replicaID)
	}

	wg.Wait()
	close(errCh)

	// Check consistency level
	switch e.config.ConsistencyLevel {
	case types.ConsistencyLevelOne:
		// Already have leader
	case types.ConsistencyLevelQuorum:
		requiredReplicas := (len(state.Replicas) / 2) + 1
		if int(successCount) < requiredReplicas {
			return fmt.Errorf("insufficient replicas for quorum: %d/%d", successCount, requiredReplicas)
		}
	case types.ConsistencyLevelAll:
		if int(successCount) < len(state.Replicas) {
			return fmt.Errorf("not all replicas acknowledged: %d/%d", successCount, len(state.Replicas))
		}
	}

	atomic.AddInt64(&e.metrics.TotalReplications, 1)
	return nil
}

func (e *Engine) replicateToFollower(replica types.NodeID, state *PartitionReplicationState, messages []*types.Message, baseOffset int64) error {
	// This would send replication request to follower
	// For now, simulate success

	// Update replica state
	e.updateReplicaState(state, replica, baseOffset+int64(len(messages)))

	return nil
}

func (e *Engine) fetchFromLeader(state *PartitionReplicationState) error {
	// Get current offset
	currentOffset := state.ReplicaStates[e.nodeID].LastFetchOffset

	// Fetch from leader
	resp, err := e.FetchMessages(state.Topic, state.Partition, currentOffset, 1024*1024) // 1MB
	if err != nil {
		return err
	}

	// Apply messages locally
	if len(resp.Messages) > 0 {
		_, err := e.storage.Append(state.Partition, resp.Messages)
		if err != nil {
			return err
		}

		// Update local state
		e.updateReplicaState(state, e.nodeID, currentOffset+int64(len(resp.Messages)))
	}

	return nil
}

func (e *Engine) sendHeartbeat(replica types.NodeID, topic string, partition int32) error {
	// Send heartbeat to replica
	// This would be implemented with actual network call

	e.mu.Lock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	if state, exists := e.partitionStates[key]; exists {
		if replicaState, exists := state.ReplicaStates[replica]; exists {
			replicaState.LastHeartbeat = time.Now()
		}
	}
	e.mu.Unlock()

	return nil
}

func (e *Engine) updateReplicaState(state *PartitionReplicationState, replica types.NodeID, offset int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	replicaState := state.ReplicaStates[replica]
	if replicaState == nil {
		replicaState = &ReplicaState{
			NodeID: replica,
		}
		state.ReplicaStates[replica] = replicaState
	}

	replicaState.LastFetchOffset = offset
	replicaState.LastFetchTime = time.Now()

	// Update lag
	replicaState.LagMessages = state.LogEndOffset - offset
	if replicaState.LagMessages == 0 {
		replicaState.CaughtUpTime = time.Now()
	}
}

func (e *Engine) emitEvent(event Event) {
	e.eventMu.RLock()
	listeners := make([]EventListener, len(e.eventListeners))
	copy(listeners, e.eventListeners)
	e.eventMu.RUnlock()

	for _, listener := range listeners {
		go listener.OnEvent(event)
	}
}

func (e *Engine) copyPartitionState(state *PartitionReplicationState) *PartitionReplicationState {
	replicas := make([]types.NodeID, len(state.Replicas))
	copy(replicas, state.Replicas)

	isr := make([]types.NodeID, len(state.ISR))
	copy(isr, state.ISR)

	replicaStates := make(map[types.NodeID]*ReplicaState)
	for nodeID, rs := range state.ReplicaStates {
		replicaStates[nodeID] = &ReplicaState{
			NodeID:          rs.NodeID,
			LastFetchTime:   rs.LastFetchTime,
			LastFetchOffset: rs.LastFetchOffset,
			CaughtUpTime:    rs.CaughtUpTime,
			LagMessages:     rs.LagMessages,
			LagTime:         rs.LagTime,
			InSync:          rs.InSync,
			LastHeartbeat:   rs.LastHeartbeat,
		}
	}

	return &PartitionReplicationState{
		Topic:            state.Topic,
		Partition:        state.Partition,
		Leader:           state.Leader,
		Replicas:         replicas,
		ISR:              isr,
		HighWatermark:    state.HighWatermark,
		LogEndOffset:     state.LogEndOffset,
		ReplicaStates:    replicaStates,
		LastISRUpdate:    state.LastISRUpdate,
		ConflictCount:    state.ConflictCount,
		LastConflictTime: state.LastConflictTime,
	}
}

// ReplicationResponse represents a response to replication fetch
type ReplicationResponse struct {
	Topic         string           `json:"topic"`
	Partition     int32            `json:"partition"`
	Messages      []*types.Message `json:"messages"`
	HighWatermark int64            `json:"high_watermark"`
	LogEndOffset  int64            `json:"log_end_offset"`
	ErrorCode     types.ErrorCode  `json:"error_code"`
}
