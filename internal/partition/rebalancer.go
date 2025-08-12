package partition

import (
	"fmt"
	"sync"
	"time"
)

// Rebalancer handles consumer group rebalancing
type Rebalancer struct {
	manager *Manager
	config  *ManagerConfig

	// State
	mu             sync.RWMutex
	rebalanceQueue chan RebalanceRequest
	running        bool

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// RebalanceRequest represents a rebalance request
type RebalanceRequest struct {
	GroupID   string    `json:"group_id"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}

// RebalanceResult represents the result of a rebalance operation
type RebalanceResult struct {
	GroupID       string                           `json:"group_id"`
	Success       bool                             `json:"success"`
	Error         error                            `json:"error"`
	OldAssignment map[string][]PartitionAssignment `json:"old_assignment"`
	NewAssignment map[string][]PartitionAssignment `json:"new_assignment"`
	Duration      time.Duration                    `json:"duration"`
	Reason        string                           `json:"reason"`
	Generation    int32                            `json:"generation"`
	Timestamp     time.Time                        `json:"timestamp"`
}

// NewRebalancer creates a new rebalancer
func NewRebalancer(manager *Manager, config *ManagerConfig) *Rebalancer {
	return &Rebalancer{
		manager:        manager,
		config:         config,
		rebalanceQueue: make(chan RebalanceRequest, 100),
		stopCh:         make(chan struct{}),
	}
}

// Start starts the rebalancer
func (r *Rebalancer) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return fmt.Errorf("rebalancer already running")
	}

	r.running = true

	// Start rebalance processor
	r.wg.Add(1)
	go r.processRebalanceRequests()

	// Start periodic rebalancer if enabled
	if r.config.EnableAutoRebalance {
		r.wg.Add(1)
		go r.periodicRebalance()
	}

	return nil
}

// Stop stops the rebalancer
func (r *Rebalancer) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	r.running = false
	close(r.stopCh)
	r.wg.Wait()

	return nil
}

// TriggerRebalance triggers a rebalance for all affected groups
func (r *Rebalancer) TriggerRebalance(reason string) {
	if !r.running {
		return
	}

	// Get all consumer groups that might be affected
	groups := r.manager.ListConsumerGroups()

	for groupID := range groups {
		select {
		case r.rebalanceQueue <- RebalanceRequest{
			GroupID:   groupID,
			Reason:    reason,
			Timestamp: time.Now(),
		}:
		default:
			// Queue is full, skip this rebalance
		}
	}
}

// RebalanceGroup triggers rebalance for a specific group
func (r *Rebalancer) RebalanceGroup(groupID, reason string) {
	if !r.running {
		return
	}

	select {
	case r.rebalanceQueue <- RebalanceRequest{
		GroupID:   groupID,
		Reason:    reason,
		Timestamp: time.Now(),
	}:
	default:
		// Queue is full, skip this rebalance
	}
}

// Private methods

func (r *Rebalancer) processRebalanceRequests() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		case req := <-r.rebalanceQueue:
			r.executeRebalance(req)
		}
	}
}

func (r *Rebalancer) periodicRebalance() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.RebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.checkAndTriggerRebalance()
		}
	}
}

func (r *Rebalancer) checkAndTriggerRebalance() {
	// Check for groups that need rebalancing
	groups := r.manager.ListConsumerGroups()

	for groupID, group := range groups {
		needsRebalance := false
		reason := ""

		// Check for expired members
		now := time.Now()
		for memberID, member := range group.Members {
			if now.Sub(member.LastHeartbeat) > r.config.SessionTimeout {
				r.manager.LeaveConsumerGroup(groupID, memberID)
				needsRebalance = true
				reason = "member_expired"
			}
		}

		// Check for unbalanced assignment
		if !needsRebalance && r.isAssignmentUnbalanced(group) {
			needsRebalance = true
			reason = "unbalanced_assignment"
		}

		if needsRebalance {
			r.RebalanceGroup(groupID, reason)
		}
	}
}

func (r *Rebalancer) executeRebalance(req RebalanceRequest) {
	startTime := time.Now()

	result := &RebalanceResult{
		GroupID:   req.GroupID,
		Reason:    req.Reason,
		Timestamp: startTime,
	}

	// Get consumer group
	group, err := r.manager.GetConsumerGroup(req.GroupID)
	if err != nil {
		result.Error = fmt.Errorf("failed to get consumer group: %w", err)
		r.emitRebalanceResult(result)
		return
	}

	// Skip rebalance if group is empty or already rebalancing
	if group.State == ConsumerGroupStateEmpty || group.State == ConsumerGroupStateRebalancing {
		result.Success = true
		result.Duration = time.Since(startTime)
		r.emitRebalanceResult(result)
		return
	}

	// Set group state to rebalancing
	r.setGroupState(group, ConsumerGroupStateRebalancing)

	// Store old assignment
	result.OldAssignment = make(map[string][]PartitionAssignment)
	for memberID, assignments := range group.Assignment {
		result.OldAssignment[memberID] = make([]PartitionAssignment, len(assignments))
		copy(result.OldAssignment[memberID], assignments)
	}

	// Get subscribed topics
	topics := r.getSubscribedTopics(group)

	// Convert members to slice
	var members []*ConsumerMember
	for _, member := range group.Members {
		members = append(members, member)
	}

	// Generate new assignment
	newAssignment, err := r.manager.assignmentPolicy.Assign(topics, members)
	if err != nil {
		result.Error = fmt.Errorf("failed to generate assignment: %w", err)
		r.setGroupState(group, ConsumerGroupStateStable)
		r.emitRebalanceResult(result)
		return
	}

	// Validate assignment
	if err := ValidateAssignment(newAssignment, topics, members); err != nil {
		result.Error = fmt.Errorf("invalid assignment: %w", err)
		r.setGroupState(group, ConsumerGroupStateStable)
		r.emitRebalanceResult(result)
		return
	}

	// Apply new assignment
	r.applyAssignment(group, newAssignment)

	// Update group state
	group.Generation++
	group.LastRebalance = time.Now()
	group.RebalanceReason = req.Reason
	r.setGroupState(group, ConsumerGroupStateStable)

	// Store result
	result.Success = true
	result.NewAssignment = newAssignment
	result.Generation = group.Generation
	result.Duration = time.Since(startTime)

	r.emitRebalanceResult(result)
}

func (r *Rebalancer) getSubscribedTopics(group *ConsumerGroup) map[string]*TopicMetadata {
	topicSet := make(map[string]bool)

	// Collect all subscribed topics
	for _, member := range group.Members {
		for _, topic := range member.Subscription {
			topicSet[topic] = true
		}
	}

	// Get topic metadata
	topics := make(map[string]*TopicMetadata)
	allTopics := r.manager.ListTopics()

	for topic := range topicSet {
		if topicMeta, exists := allTopics[topic]; exists {
			topics[topic] = topicMeta
		}
	}

	return topics
}

func (r *Rebalancer) applyAssignment(group *ConsumerGroup, assignment map[string][]PartitionAssignment) {
	// Update group assignment
	group.Assignment = make(map[string][]PartitionAssignment)
	for memberID, partitions := range assignment {
		group.Assignment[memberID] = make([]PartitionAssignment, len(partitions))
		copy(group.Assignment[memberID], partitions)
	}

	// Update member assignments
	for memberID, member := range group.Members {
		if partitions, exists := assignment[memberID]; exists {
			member.Assignment = make([]PartitionAssignment, len(partitions))
			copy(member.Assignment, partitions)
		} else {
			member.Assignment = make([]PartitionAssignment, 0)
		}
	}
}

func (r *Rebalancer) setGroupState(group *ConsumerGroup, state ConsumerGroupState) {
	oldState := group.State
	group.State = state

	// Emit state change event
	r.manager.emitEvent(&ConsumerGroupStateChangedEvent{
		GroupID:   group.GroupID,
		OldState:  oldState,
		NewState:  state,
		Timestamp: time.Now(),
	})
}

func (r *Rebalancer) isAssignmentUnbalanced(group *ConsumerGroup) bool {
	if len(group.Members) <= 1 {
		return false
	}

	// Calculate assignment counts
	var counts []int
	for _, assignments := range group.Assignment {
		counts = append(counts, len(assignments))
	}

	if len(counts) == 0 {
		return false
	}

	// Find min and max
	min, max := counts[0], counts[0]
	for _, count := range counts[1:] {
		if count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}

	// Consider unbalanced if difference is more than 1
	return (max - min) > 1
}

func (r *Rebalancer) emitRebalanceResult(result *RebalanceResult) {
	// Emit rebalance completed event
	r.manager.emitEvent(&RebalanceCompletedEvent{
		GroupID:       result.GroupID,
		Success:       result.Success,
		Error:         result.Error,
		OldAssignment: result.OldAssignment,
		NewAssignment: result.NewAssignment,
		Duration:      result.Duration,
		Reason:        result.Reason,
		Generation:    result.Generation,
		Timestamp:     result.Timestamp,
	})
}

// Event types for partition management

// TopicCreatedEvent represents a topic creation event
type TopicCreatedEvent struct {
	TopicName  string    `json:"topic_name"`
	Partitions int32     `json:"partitions"`
	Timestamp  time.Time `json:"timestamp"`
}

func (e *TopicCreatedEvent) Type() string {
	return "topic_created"
}

func (e *TopicCreatedEvent) Data() interface{} {
	return e
}

// TopicDeletedEvent represents a topic deletion event
type TopicDeletedEvent struct {
	TopicName string    `json:"topic_name"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *TopicDeletedEvent) Type() string {
	return "topic_deleted"
}

func (e *TopicDeletedEvent) Data() interface{} {
	return e
}

// BrokerRegisteredEvent represents a broker registration event
type BrokerRegisteredEvent struct {
	NodeID    string    `json:"node_id"`
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *BrokerRegisteredEvent) Type() string {
	return "broker_registered"
}

func (e *BrokerRegisteredEvent) Data() interface{} {
	return e
}

// BrokerUnregisteredEvent represents a broker unregistration event
type BrokerUnregisteredEvent struct {
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *BrokerUnregisteredEvent) Type() string {
	return "broker_unregistered"
}

func (e *BrokerUnregisteredEvent) Data() interface{} {
	return e
}

// ConsumerJoinedEvent represents a consumer joining event
type ConsumerJoinedEvent struct {
	GroupID   string    `json:"group_id"`
	MemberID  string    `json:"member_id"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *ConsumerJoinedEvent) Type() string {
	return "consumer_joined"
}

func (e *ConsumerJoinedEvent) Data() interface{} {
	return e
}

// ConsumerLeftEvent represents a consumer leaving event
type ConsumerLeftEvent struct {
	GroupID   string    `json:"group_id"`
	MemberID  string    `json:"member_id"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *ConsumerLeftEvent) Type() string {
	return "consumer_left"
}

func (e *ConsumerLeftEvent) Data() interface{} {
	return e
}

// ConsumerGroupStateChangedEvent represents a consumer group state change event
type ConsumerGroupStateChangedEvent struct {
	GroupID   string             `json:"group_id"`
	OldState  ConsumerGroupState `json:"old_state"`
	NewState  ConsumerGroupState `json:"new_state"`
	Timestamp time.Time          `json:"timestamp"`
}

func (e *ConsumerGroupStateChangedEvent) Type() string {
	return "consumer_group_state_changed"
}

func (e *ConsumerGroupStateChangedEvent) Data() interface{} {
	return e
}

// RebalanceCompletedEvent represents a rebalance completion event
type RebalanceCompletedEvent struct {
	GroupID       string                           `json:"group_id"`
	Success       bool                             `json:"success"`
	Error         error                            `json:"error"`
	OldAssignment map[string][]PartitionAssignment `json:"old_assignment"`
	NewAssignment map[string][]PartitionAssignment `json:"new_assignment"`
	Duration      time.Duration                    `json:"duration"`
	Reason        string                           `json:"reason"`
	Generation    int32                            `json:"generation"`
	Timestamp     time.Time                        `json:"timestamp"`
}

func (e *RebalanceCompletedEvent) Type() string {
	return "rebalance_completed"
}

func (e *RebalanceCompletedEvent) Data() interface{} {
	return e
}

// PartitionReassignmentEvent represents a partition reassignment event
type PartitionReassignmentEvent struct {
	Topic       string    `json:"topic"`
	Partition   int32     `json:"partition"`
	OldReplicas []string  `json:"old_replicas"`
	NewReplicas []string  `json:"new_replicas"`
	OldLeader   string    `json:"old_leader"`
	NewLeader   string    `json:"new_leader"`
	Timestamp   time.Time `json:"timestamp"`
}

func (e *PartitionReassignmentEvent) Type() string {
	return "partition_reassignment"
}

func (e *PartitionReassignmentEvent) Data() interface{} {
	return e
}
