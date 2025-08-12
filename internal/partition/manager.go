package partition

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"time"

	"streaming-engine/pkg/types"
)

// Manager handles partition assignment and management
type Manager struct {
	mu               sync.RWMutex
	topics           map[string]*TopicMetadata
	brokers          map[types.NodeID]*BrokerMetadata
	consumerGroups   map[string]*ConsumerGroup
	assignmentPolicy AssignmentPolicy
	rebalancer       *Rebalancer

	// Configuration
	config *ManagerConfig

	// Events
	eventListeners []EventListener
	eventMu        sync.RWMutex
}

// ManagerConfig represents partition manager configuration
type ManagerConfig struct {
	RebalanceInterval        time.Duration `json:"rebalance_interval"`
	SessionTimeout           time.Duration `json:"session_timeout"`
	HeartbeatInterval        time.Duration `json:"heartbeat_interval"`
	MaxRebalanceTime         time.Duration `json:"max_rebalance_time"`
	DefaultReplicationFactor int32         `json:"default_replication_factor"`
	AssignmentStrategy       string        `json:"assignment_strategy"`
	EnableAutoRebalance      bool          `json:"enable_auto_rebalance"`
}

// DefaultManagerConfig returns default configuration
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		RebalanceInterval:        30 * time.Second,
		SessionTimeout:           30 * time.Second,
		HeartbeatInterval:        10 * time.Second,
		MaxRebalanceTime:         5 * time.Minute,
		DefaultReplicationFactor: 3,
		AssignmentStrategy:       "RoundRobin",
		EnableAutoRebalance:      true,
	}
}

// TopicMetadata represents metadata for a topic
type TopicMetadata struct {
	Name              string                   `json:"name"`
	Partitions        int32                    `json:"partitions"`
	ReplicationFactor int32                    `json:"replication_factor"`
	PartitionMap      map[int32]*PartitionInfo `json:"partition_map"`
	Created           time.Time                `json:"created"`
	LastModified      time.Time                `json:"last_modified"`
}

// PartitionInfo represents information about a partition
type PartitionInfo struct {
	Topic          string         `json:"topic"`
	Partition      int32          `json:"partition"`
	Leader         types.NodeID   `json:"leader"`
	Replicas       []types.NodeID `json:"replicas"`
	ISR            []types.NodeID `json:"isr"` // In-Sync Replicas
	HighWatermark  int64          `json:"high_watermark"`
	LogStartOffset int64          `json:"log_start_offset"`
	LastUpdated    time.Time      `json:"last_updated"`
}

// BrokerMetadata represents metadata for a broker
type BrokerMetadata struct {
	NodeID     types.NodeID   `json:"node_id"`
	Host       string         `json:"host"`
	Port       int            `json:"port"`
	Rack       string         `json:"rack"`
	Online     bool           `json:"online"`
	LastSeen   time.Time      `json:"last_seen"`
	LoadScore  float64        `json:"load_score"`
	Partitions []PartitionRef `json:"partitions"`
}

// PartitionRef represents a reference to a partition
type PartitionRef struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Role      string `json:"role"` // "leader" or "follower"
}

// ConsumerGroup represents a consumer group
type ConsumerGroup struct {
	GroupID         string                           `json:"group_id"`
	Members         map[string]*ConsumerMember       `json:"members"`
	State           ConsumerGroupState               `json:"state"`
	Coordinator     types.NodeID                     `json:"coordinator"`
	Generation      int32                            `json:"generation"`
	Assignment      map[string][]PartitionAssignment `json:"assignment"`
	Offsets         map[string]map[int32]int64       `json:"offsets"` // topic -> partition -> offset
	LastRebalance   time.Time                        `json:"last_rebalance"`
	RebalanceReason string                           `json:"rebalance_reason"`
}

// ConsumerMember represents a consumer group member
type ConsumerMember struct {
	MemberID       string                `json:"member_id"`
	ClientID       string                `json:"client_id"`
	Host           string                `json:"host"`
	SessionTimeout time.Duration         `json:"session_timeout"`
	Subscription   []string              `json:"subscription"`
	Assignment     []PartitionAssignment `json:"assignment"`
	LastHeartbeat  time.Time             `json:"last_heartbeat"`
	Metadata       map[string]string     `json:"metadata"`
}

// PartitionAssignment represents assignment of a partition to a consumer
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

// AssignmentPolicy defines how partitions are assigned
type AssignmentPolicy interface {
	Assign(topics map[string]*TopicMetadata, members []*ConsumerMember) (map[string][]PartitionAssignment, error)
	Name() string
}

// EventListener listens to partition events
type EventListener interface {
	OnEvent(event Event)
}

// Event represents a partition event
type Event interface {
	Type() string
	Data() interface{}
}

// NewManager creates a new partition manager
func NewManager(config *ManagerConfig) *Manager {
	if config == nil {
		config = DefaultManagerConfig()
	}

	manager := &Manager{
		topics:         make(map[string]*TopicMetadata),
		brokers:        make(map[types.NodeID]*BrokerMetadata),
		consumerGroups: make(map[string]*ConsumerGroup),
		config:         config,
		eventListeners: make([]EventListener, 0),
	}

	// Set assignment policy
	switch config.AssignmentStrategy {
	case "RoundRobin":
		manager.assignmentPolicy = NewRoundRobinAssignmentPolicy()
	case "Range":
		manager.assignmentPolicy = NewRangeAssignmentPolicy()
	case "Sticky":
		manager.assignmentPolicy = NewStickyAssignmentPolicy()
	default:
		manager.assignmentPolicy = NewRoundRobinAssignmentPolicy()
	}

	// Initialize rebalancer
	manager.rebalancer = NewRebalancer(manager, config)

	return manager
}

// Topic Management

// CreateTopic creates a new topic with partitions
func (m *Manager) CreateTopic(name string, partitions int32, replicationFactor int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	if replicationFactor == 0 {
		replicationFactor = m.config.DefaultReplicationFactor
	}

	// Validate parameters
	if partitions <= 0 {
		return fmt.Errorf("partitions must be positive")
	}

	if replicationFactor <= 0 {
		return fmt.Errorf("replication factor must be positive")
	}

	availableBrokers := m.getOnlineBrokers()
	if int32(len(availableBrokers)) < replicationFactor {
		return fmt.Errorf("not enough brokers for replication factor %d", replicationFactor)
	}

	// Create topic metadata
	topic := &TopicMetadata{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		PartitionMap:      make(map[int32]*PartitionInfo),
		Created:           time.Now(),
		LastModified:      time.Now(),
	}

	// Assign partitions to brokers
	if err := m.assignPartitions(topic, availableBrokers); err != nil {
		return fmt.Errorf("failed to assign partitions: %w", err)
	}

	m.topics[name] = topic

	// Emit event
	m.emitEvent(&TopicCreatedEvent{
		TopicName:  name,
		Partitions: partitions,
		Timestamp:  time.Now(),
	})

	return nil
}

// DeleteTopic deletes a topic
func (m *Manager) DeleteTopic(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic, exists := m.topics[name]
	if !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	// Remove partition assignments from brokers
	for _, partition := range topic.PartitionMap {
		for _, replica := range partition.Replicas {
			if broker, exists := m.brokers[replica]; exists {
				m.removePartitionFromBroker(broker, name, partition.Partition)
			}
		}
	}

	delete(m.topics, name)

	// Emit event
	m.emitEvent(&TopicDeletedEvent{
		TopicName: name,
		Timestamp: time.Now(),
	})

	return nil
}

// GetTopic returns topic metadata
func (m *Manager) GetTopic(name string) (*TopicMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topic, exists := m.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	// Return a copy to prevent external modification
	return m.copyTopicMetadata(topic), nil
}

// ListTopics returns all topics
func (m *Manager) ListTopics() map[string]*TopicMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*TopicMetadata)
	for name, topic := range m.topics {
		result[name] = m.copyTopicMetadata(topic)
	}

	return result
}

// Broker Management

// RegisterBroker registers a new broker
func (m *Manager) RegisterBroker(nodeID types.NodeID, host string, port int, rack string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	broker := &BrokerMetadata{
		NodeID:     nodeID,
		Host:       host,
		Port:       port,
		Rack:       rack,
		Online:     true,
		LastSeen:   time.Now(),
		LoadScore:  0.0,
		Partitions: make([]PartitionRef, 0),
	}

	m.brokers[nodeID] = broker

	// Emit event
	m.emitEvent(&BrokerRegisteredEvent{
		NodeID:    nodeID,
		Host:      host,
		Port:      port,
		Timestamp: time.Now(),
	})

	return nil
}

// UnregisterBroker unregisters a broker
func (m *Manager) UnregisterBroker(nodeID types.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	broker, exists := m.brokers[nodeID]
	if !exists {
		return fmt.Errorf("broker %s not found", nodeID)
	}

	broker.Online = false

	// Trigger rebalance if this broker had partitions
	if len(broker.Partitions) > 0 && m.config.EnableAutoRebalance {
		go m.rebalancer.TriggerRebalance("broker_offline")
	}

	// Emit event
	m.emitEvent(&BrokerUnregisteredEvent{
		NodeID:    nodeID,
		Timestamp: time.Now(),
	})

	return nil
}

// UpdateBrokerHeartbeat updates broker heartbeat
func (m *Manager) UpdateBrokerHeartbeat(nodeID types.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	broker, exists := m.brokers[nodeID]
	if !exists {
		return fmt.Errorf("broker %s not found", nodeID)
	}

	broker.LastSeen = time.Now()
	broker.Online = true

	return nil
}

// GetBroker returns broker metadata
func (m *Manager) GetBroker(nodeID types.NodeID) (*BrokerMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	broker, exists := m.brokers[nodeID]
	if !exists {
		return nil, fmt.Errorf("broker %s not found", nodeID)
	}

	// Return a copy
	return m.copyBrokerMetadata(broker), nil
}

// ListBrokers returns all brokers
func (m *Manager) ListBrokers() map[types.NodeID]*BrokerMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[types.NodeID]*BrokerMetadata)
	for nodeID, broker := range m.brokers {
		result[nodeID] = m.copyBrokerMetadata(broker)
	}

	return result
}

// Consumer Group Management

// JoinConsumerGroup adds a consumer to a group
func (m *Manager) JoinConsumerGroup(groupID, memberID, clientID, host string,
	sessionTimeout time.Duration, subscription []string, metadata map[string]string) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.consumerGroups[groupID]
	if !exists {
		group = &ConsumerGroup{
			GroupID:    groupID,
			Members:    make(map[string]*ConsumerMember),
			State:      ConsumerGroupStateEmpty,
			Generation: 0,
			Assignment: make(map[string][]PartitionAssignment),
			Offsets:    make(map[string]map[int32]int64),
		}
		m.consumerGroups[groupID] = group
	}

	member := &ConsumerMember{
		MemberID:       memberID,
		ClientID:       clientID,
		Host:           host,
		SessionTimeout: sessionTimeout,
		Subscription:   subscription,
		LastHeartbeat:  time.Now(),
		Metadata:       metadata,
	}

	group.Members[memberID] = member

	// Trigger rebalance
	if m.config.EnableAutoRebalance {
		go m.rebalanceConsumerGroup(group, "member_joined")
	}

	// Emit event
	m.emitEvent(&ConsumerJoinedEvent{
		GroupID:   groupID,
		MemberID:  memberID,
		Timestamp: time.Now(),
	})

	return nil
}

// LeaveConsumerGroup removes a consumer from a group
func (m *Manager) LeaveConsumerGroup(groupID, memberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.consumerGroups[groupID]
	if !exists {
		return fmt.Errorf("consumer group %s not found", groupID)
	}

	if _, exists := group.Members[memberID]; !exists {
		return fmt.Errorf("member %s not found in group %s", memberID, groupID)
	}

	delete(group.Members, memberID)
	delete(group.Assignment, memberID)

	// Update group state
	if len(group.Members) == 0 {
		group.State = ConsumerGroupStateEmpty
	} else if m.config.EnableAutoRebalance {
		// Trigger rebalance
		go m.rebalanceConsumerGroup(group, "member_left")
	}

	// Emit event
	m.emitEvent(&ConsumerLeftEvent{
		GroupID:   groupID,
		MemberID:  memberID,
		Timestamp: time.Now(),
	})

	return nil
}

// Heartbeat updates consumer heartbeat
func (m *Manager) Heartbeat(groupID, memberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.consumerGroups[groupID]
	if !exists {
		return fmt.Errorf("consumer group %s not found", groupID)
	}

	member, exists := group.Members[memberID]
	if !exists {
		return fmt.Errorf("member %s not found in group %s", memberID, groupID)
	}

	member.LastHeartbeat = time.Now()

	return nil
}

// CommitOffset commits an offset for a consumer group
func (m *Manager) CommitOffset(groupID, topic string, partition int32, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.consumerGroups[groupID]
	if !exists {
		return fmt.Errorf("consumer group %s not found", groupID)
	}

	if group.Offsets[topic] == nil {
		group.Offsets[topic] = make(map[int32]int64)
	}

	group.Offsets[topic][partition] = offset

	return nil
}

// GetOffset returns the committed offset for a consumer group
func (m *Manager) GetOffset(groupID, topic string, partition int32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.consumerGroups[groupID]
	if !exists {
		return 0, fmt.Errorf("consumer group %s not found", groupID)
	}

	if topicOffsets, exists := group.Offsets[topic]; exists {
		if offset, exists := topicOffsets[partition]; exists {
			return offset, nil
		}
	}

	return 0, nil // Return 0 if no offset committed
}

// GetConsumerGroup returns consumer group metadata
func (m *Manager) GetConsumerGroup(groupID string) (*ConsumerGroup, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.consumerGroups[groupID]
	if !exists {
		return nil, fmt.Errorf("consumer group %s not found", groupID)
	}

	// Return a copy
	return m.copyConsumerGroup(group), nil
}

// ListConsumerGroups returns all consumer groups
func (m *Manager) ListConsumerGroups() map[string]*ConsumerGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*ConsumerGroup)
	for groupID, group := range m.consumerGroups {
		result[groupID] = m.copyConsumerGroup(group)
	}

	return result
}

// Partition Assignment

// GetPartitionAssignment returns current partition assignment
func (m *Manager) GetPartitionAssignment(topic string, partition int32) (*PartitionInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topicMeta, exists := m.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	partitionInfo, exists := topicMeta.PartitionMap[partition]
	if !exists {
		return nil, fmt.Errorf("partition %d not found in topic %s", partition, topic)
	}

	// Return a copy
	return m.copyPartitionInfo(partitionInfo), nil
}

// GetPartitionLeader returns the leader for a partition
func (m *Manager) GetPartitionLeader(topic string, partition int32) (types.NodeID, error) {
	partitionInfo, err := m.GetPartitionAssignment(topic, partition)
	if err != nil {
		return "", err
	}

	return partitionInfo.Leader, nil
}

// ChoosePartition chooses a partition for a key using consistent hashing
func (m *Manager) ChoosePartition(topic string, key []byte) (int32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topicMeta, exists := m.topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic %s not found", topic)
	}

	if len(key) == 0 {
		// No key provided, use round-robin or random selection
		// For simplicity, return partition 0
		return 0, nil
	}

	// Use CRC32 hash for consistent partitioning
	hash := crc32.ChecksumIEEE(key)
	partition := int32(hash % uint32(topicMeta.Partitions))

	return partition, nil
}

// Event Management

// AddEventListener adds an event listener
func (m *Manager) AddEventListener(listener EventListener) {
	m.eventMu.Lock()
	defer m.eventMu.Unlock()

	m.eventListeners = append(m.eventListeners, listener)
}

// RemoveEventListener removes an event listener
func (m *Manager) RemoveEventListener(listener EventListener) {
	m.eventMu.Lock()
	defer m.eventMu.Unlock()

	for i, l := range m.eventListeners {
		if l == listener {
			m.eventListeners = append(m.eventListeners[:i], m.eventListeners[i+1:]...)
			break
		}
	}
}

// Private methods

func (m *Manager) getOnlineBrokers() []types.NodeID {
	var onlineBrokers []types.NodeID
	for nodeID, broker := range m.brokers {
		if broker.Online {
			onlineBrokers = append(onlineBrokers, nodeID)
		}
	}
	return onlineBrokers
}

func (m *Manager) assignPartitions(topic *TopicMetadata, brokers []types.NodeID) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers available")
	}

	// Sort brokers for deterministic assignment
	sort.Slice(brokers, func(i, j int) bool {
		return string(brokers[i]) < string(brokers[j])
	})

	// Assign each partition
	for i := int32(0); i < topic.Partitions; i++ {
		replicas := make([]types.NodeID, 0, topic.ReplicationFactor)

		// Choose replicas using round-robin with offset
		for j := int32(0); j < topic.ReplicationFactor; j++ {
			brokerIndex := (int(i) + int(j)) % len(brokers)
			replicas = append(replicas, brokers[brokerIndex])
		}

		// First replica is the leader
		leader := replicas[0]

		partitionInfo := &PartitionInfo{
			Topic:          topic.Name,
			Partition:      i,
			Leader:         leader,
			Replicas:       replicas,
			ISR:            replicas, // Initially all replicas are in-sync
			HighWatermark:  0,
			LogStartOffset: 0,
			LastUpdated:    time.Now(),
		}

		topic.PartitionMap[i] = partitionInfo

		// Update broker partition assignments
		for j, replica := range replicas {
			if broker, exists := m.brokers[replica]; exists {
				role := "follower"
				if j == 0 {
					role = "leader"
				}

				broker.Partitions = append(broker.Partitions, PartitionRef{
					Topic:     topic.Name,
					Partition: i,
					Role:      role,
				})
			}
		}
	}

	return nil
}

func (m *Manager) removePartitionFromBroker(broker *BrokerMetadata, topic string, partition int32) {
	for i, partRef := range broker.Partitions {
		if partRef.Topic == topic && partRef.Partition == partition {
			broker.Partitions = append(broker.Partitions[:i], broker.Partitions[i+1:]...)
			break
		}
	}
}

func (m *Manager) rebalanceConsumerGroup(group *ConsumerGroup, reason string) {
	m.rebalancer.RebalanceGroup(group.GroupID, reason)
}

func (m *Manager) emitEvent(event Event) {
	m.eventMu.RLock()
	listeners := make([]EventListener, len(m.eventListeners))
	copy(listeners, m.eventListeners)
	m.eventMu.RUnlock()

	for _, listener := range listeners {
		go listener.OnEvent(event)
	}
}

// Copy methods to prevent external modification

func (m *Manager) copyTopicMetadata(topic *TopicMetadata) *TopicMetadata {
	copy := &TopicMetadata{
		Name:              topic.Name,
		Partitions:        topic.Partitions,
		ReplicationFactor: topic.ReplicationFactor,
		Created:           topic.Created,
		LastModified:      topic.LastModified,
		PartitionMap:      make(map[int32]*PartitionInfo),
	}

	for partition, info := range topic.PartitionMap {
		copy.PartitionMap[partition] = m.copyPartitionInfo(info)
	}

	return copy
}

func (m *Manager) copyPartitionInfo(info *PartitionInfo) *PartitionInfo {
	replicas := make([]types.NodeID, len(info.Replicas))
	copy(replicas, info.Replicas)

	isr := make([]types.NodeID, len(info.ISR))
	copy(isr, info.ISR)

	return &PartitionInfo{
		Topic:          info.Topic,
		Partition:      info.Partition,
		Leader:         info.Leader,
		Replicas:       replicas,
		ISR:            isr,
		HighWatermark:  info.HighWatermark,
		LogStartOffset: info.LogStartOffset,
		LastUpdated:    info.LastUpdated,
	}
}

func (m *Manager) copyBrokerMetadata(broker *BrokerMetadata) *BrokerMetadata {
	partitions := make([]PartitionRef, len(broker.Partitions))
	copy(partitions, broker.Partitions)

	return &BrokerMetadata{
		NodeID:     broker.NodeID,
		Host:       broker.Host,
		Port:       broker.Port,
		Rack:       broker.Rack,
		Online:     broker.Online,
		LastSeen:   broker.LastSeen,
		LoadScore:  broker.LoadScore,
		Partitions: partitions,
	}
}

func (m *Manager) copyConsumerGroup(group *ConsumerGroup) *ConsumerGroup {
	members := make(map[string]*ConsumerMember)
	for memberID, member := range group.Members {
		members[memberID] = m.copyConsumerMember(member)
	}

	assignment := make(map[string][]PartitionAssignment)
	for memberID, assignments := range group.Assignment {
		memberAssignments := make([]PartitionAssignment, len(assignments))
		copy(memberAssignments, assignments)
		assignment[memberID] = memberAssignments
	}

	offsets := make(map[string]map[int32]int64)
	for topic, topicOffsets := range group.Offsets {
		offsets[topic] = make(map[int32]int64)
		for partition, offset := range topicOffsets {
			offsets[topic][partition] = offset
		}
	}

	return &ConsumerGroup{
		GroupID:         group.GroupID,
		Members:         members,
		State:           group.State,
		Coordinator:     group.Coordinator,
		Generation:      group.Generation,
		Assignment:      assignment,
		Offsets:         offsets,
		LastRebalance:   group.LastRebalance,
		RebalanceReason: group.RebalanceReason,
	}
}

func (m *Manager) copyConsumerMember(member *ConsumerMember) *ConsumerMember {
	subscription := make([]string, len(member.Subscription))
	copy(subscription, member.Subscription)

	assignment := make([]PartitionAssignment, len(member.Assignment))
	copy(assignment, member.Assignment)

	metadata := make(map[string]string)
	for k, v := range member.Metadata {
		metadata[k] = v
	}

	return &ConsumerMember{
		MemberID:       member.MemberID,
		ClientID:       member.ClientID,
		Host:           member.Host,
		SessionTimeout: member.SessionTimeout,
		Subscription:   subscription,
		Assignment:     assignment,
		LastHeartbeat:  member.LastHeartbeat,
		Metadata:       metadata,
	}
}
