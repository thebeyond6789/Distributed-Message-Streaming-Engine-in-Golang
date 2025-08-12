package raft

import (
	"encoding/json"
	"fmt"
	"time"

	"streaming-engine/pkg/types"
)

// NoopCommand represents a no-op command
type NoopCommand struct{}

func (c *NoopCommand) Type() CommandType {
	return CommandTypeNoop
}

func (c *NoopCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *NoopCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *NoopCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	return nil, nil
}

// SetKeyCommand represents a set key command
type SetKeyCommand struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (c *SetKeyCommand) Type() CommandType {
	return CommandTypeSetKey
}

func (c *SetKeyCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *SetKeyCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *SetKeyCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	if sm, ok := stateMachine.(*KeyValueStateMachine); ok {
		return sm.Set(c.Key, c.Value), nil
	}
	return nil, fmt.Errorf("unsupported state machine type")
}

// DeleteKeyCommand represents a delete key command
type DeleteKeyCommand struct {
	Key string `json:"key"`
}

func (c *DeleteKeyCommand) Type() CommandType {
	return CommandTypeDeleteKey
}

func (c *DeleteKeyCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *DeleteKeyCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *DeleteKeyCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	if sm, ok := stateMachine.(*KeyValueStateMachine); ok {
		return sm.Delete(c.Key), nil
	}
	return nil, fmt.Errorf("unsupported state machine type")
}

// CreateTopicCommand represents a create topic command
type CreateTopicCommand struct {
	Topic types.TopicConfig `json:"topic"`
}

func (c *CreateTopicCommand) Type() CommandType {
	return CommandTypeCreateTopic
}

func (c *CreateTopicCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *CreateTopicCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *CreateTopicCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	if sm, ok := stateMachine.(*StreamingStateMachine); ok {
		return sm.CreateTopic(&c.Topic), nil
	}
	return nil, fmt.Errorf("unsupported state machine type")
}

// DeleteTopicCommand represents a delete topic command
type DeleteTopicCommand struct {
	TopicName string `json:"topic_name"`
}

func (c *DeleteTopicCommand) Type() CommandType {
	return CommandTypeDeleteTopic
}

func (c *DeleteTopicCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *DeleteTopicCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *DeleteTopicCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	if sm, ok := stateMachine.(*StreamingStateMachine); ok {
		return sm.DeleteTopic(c.TopicName), nil
	}
	return nil, fmt.Errorf("unsupported state machine type")
}

// JoinClusterCommand represents a join cluster command
type JoinClusterCommand struct {
	NodeID   types.NodeID      `json:"node_id"`
	Address  string            `json:"address"`
	Voting   bool              `json:"voting"`
	Metadata map[string]string `json:"metadata"`
}

func (c *JoinClusterCommand) Type() CommandType {
	return CommandTypeJoinCluster
}

func (c *JoinClusterCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *JoinClusterCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *JoinClusterCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	if sm, ok := stateMachine.(*ClusterStateMachine); ok {
		return sm.JoinNode(c.NodeID, c.Address, c.Voting, c.Metadata), nil
	}
	return nil, fmt.Errorf("unsupported state machine type")
}

// LeaveClusterCommand represents a leave cluster command
type LeaveClusterCommand struct {
	NodeID types.NodeID `json:"node_id"`
}

func (c *LeaveClusterCommand) Type() CommandType {
	return CommandTypeLeaveCluster
}

func (c *LeaveClusterCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *LeaveClusterCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *LeaveClusterCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	if sm, ok := stateMachine.(*ClusterStateMachine); ok {
		return sm.LeaveNode(c.NodeID), nil
	}
	return nil, fmt.Errorf("unsupported state machine type")
}

// ConfigCommand represents a configuration command
type ConfigCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (c *ConfigCommand) Type() CommandType {
	return CommandTypeConfig
}

func (c *ConfigCommand) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *ConfigCommand) Decode(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *ConfigCommand) Apply(stateMachine StateMachine) (interface{}, error) {
	if sm, ok := stateMachine.(*ConfigStateMachine); ok {
		return sm.SetConfig(c.Key, c.Value), nil
	}
	return nil, fmt.Errorf("unsupported state machine type")
}

// CommandFactory creates commands from type and data
func CommandFactory(cmdType CommandType, data []byte) (Command, error) {
	var cmd Command

	switch cmdType {
	case CommandTypeNoop:
		cmd = &NoopCommand{}
	case CommandTypeSetKey:
		cmd = &SetKeyCommand{}
	case CommandTypeDeleteKey:
		cmd = &DeleteKeyCommand{}
	case CommandTypeCreateTopic:
		cmd = &CreateTopicCommand{}
	case CommandTypeDeleteTopic:
		cmd = &DeleteTopicCommand{}
	case CommandTypeJoinCluster:
		cmd = &JoinClusterCommand{}
	case CommandTypeLeaveCluster:
		cmd = &LeaveClusterCommand{}
	case CommandTypeConfig:
		cmd = &ConfigCommand{}
	default:
		return nil, fmt.Errorf("unknown command type: %d", cmdType)
	}

	if err := cmd.Decode(data); err != nil {
		return nil, fmt.Errorf("failed to decode command: %w", err)
	}

	return cmd, nil
}

// KeyValueStateMachine implements a simple key-value state machine
type KeyValueStateMachine struct {
	data map[string][]byte
}

// NewKeyValueStateMachine creates a new key-value state machine
func NewKeyValueStateMachine() *KeyValueStateMachine {
	return &KeyValueStateMachine{
		data: make(map[string][]byte),
	}
}

// Apply applies a command to the state machine
func (sm *KeyValueStateMachine) Apply(command Command) (interface{}, error) {
	return command.Apply(sm)
}

// Get retrieves a value by key
func (sm *KeyValueStateMachine) Get(key string) []byte {
	return sm.data[key]
}

// Set sets a key-value pair
func (sm *KeyValueStateMachine) Set(key string, value []byte) interface{} {
	sm.data[key] = value
	return fmt.Sprintf("Set %s", key)
}

// Delete deletes a key
func (sm *KeyValueStateMachine) Delete(key string) interface{} {
	delete(sm.data, key)
	return fmt.Sprintf("Deleted %s", key)
}

// Snapshot creates a snapshot of the state machine
func (sm *KeyValueStateMachine) Snapshot() ([]byte, error) {
	return json.Marshal(sm.data)
}

// Restore restores the state machine from a snapshot
func (sm *KeyValueStateMachine) Restore(snapshot []byte) error {
	return json.Unmarshal(snapshot, &sm.data)
}

// GetState returns the current state
func (sm *KeyValueStateMachine) GetState() interface{} {
	return sm.data
}

// StreamingStateMachine implements a state machine for streaming metadata
type StreamingStateMachine struct {
	topics     map[string]*types.TopicConfig
	partitions map[string]map[int32]*types.PartitionInfo
}

// NewStreamingStateMachine creates a new streaming state machine
func NewStreamingStateMachine() *StreamingStateMachine {
	return &StreamingStateMachine{
		topics:     make(map[string]*types.TopicConfig),
		partitions: make(map[string]map[int32]*types.PartitionInfo),
	}
}

// Apply applies a command to the state machine
func (sm *StreamingStateMachine) Apply(command Command) (interface{}, error) {
	return command.Apply(sm)
}

// CreateTopic creates a new topic
func (sm *StreamingStateMachine) CreateTopic(config *types.TopicConfig) interface{} {
	sm.topics[config.Name] = config
	sm.partitions[config.Name] = make(map[int32]*types.PartitionInfo)

	// Create partition info
	for i := int32(0); i < config.Partitions; i++ {
		sm.partitions[config.Name][i] = &types.PartitionInfo{
			Topic:     config.Name,
			Partition: i,
			Replicas:  []types.NodeID{},
			ISR:       []types.NodeID{},
		}
	}

	return fmt.Sprintf("Created topic %s", config.Name)
}

// DeleteTopic deletes a topic
func (sm *StreamingStateMachine) DeleteTopic(topicName string) interface{} {
	delete(sm.topics, topicName)
	delete(sm.partitions, topicName)
	return fmt.Sprintf("Deleted topic %s", topicName)
}

// GetTopic retrieves topic configuration
func (sm *StreamingStateMachine) GetTopic(name string) *types.TopicConfig {
	return sm.topics[name]
}

// GetTopics retrieves all topics
func (sm *StreamingStateMachine) GetTopics() map[string]*types.TopicConfig {
	return sm.topics
}

// GetPartitions retrieves partitions for a topic
func (sm *StreamingStateMachine) GetPartitions(topic string) map[int32]*types.PartitionInfo {
	return sm.partitions[topic]
}

// Snapshot creates a snapshot of the state machine
func (sm *StreamingStateMachine) Snapshot() ([]byte, error) {
	state := struct {
		Topics     map[string]*types.TopicConfig             `json:"topics"`
		Partitions map[string]map[int32]*types.PartitionInfo `json:"partitions"`
	}{
		Topics:     sm.topics,
		Partitions: sm.partitions,
	}
	return json.Marshal(state)
}

// Restore restores the state machine from a snapshot
func (sm *StreamingStateMachine) Restore(snapshot []byte) error {
	var state struct {
		Topics     map[string]*types.TopicConfig             `json:"topics"`
		Partitions map[string]map[int32]*types.PartitionInfo `json:"partitions"`
	}

	if err := json.Unmarshal(snapshot, &state); err != nil {
		return err
	}

	sm.topics = state.Topics
	sm.partitions = state.Partitions

	return nil
}

// GetState returns the current state
func (sm *StreamingStateMachine) GetState() interface{} {
	return struct {
		Topics     map[string]*types.TopicConfig             `json:"topics"`
		Partitions map[string]map[int32]*types.PartitionInfo `json:"partitions"`
	}{
		Topics:     sm.topics,
		Partitions: sm.partitions,
	}
}

// ClusterStateMachine implements a state machine for cluster membership
type ClusterStateMachine struct {
	servers map[types.NodeID]*ServerInfo
	version int64
}

// NewClusterStateMachine creates a new cluster state machine
func NewClusterStateMachine() *ClusterStateMachine {
	return &ClusterStateMachine{
		servers: make(map[types.NodeID]*ServerInfo),
		version: 0,
	}
}

// Apply applies a command to the state machine
func (sm *ClusterStateMachine) Apply(command Command) (interface{}, error) {
	return command.Apply(sm)
}

// JoinNode adds a node to the cluster
func (sm *ClusterStateMachine) JoinNode(nodeID types.NodeID, address string, voting bool, metadata map[string]string) interface{} {
	sm.servers[nodeID] = &ServerInfo{
		NodeID:   nodeID,
		Address:  address,
		Voting:   voting,
		Metadata: metadata,
	}
	sm.version++
	return fmt.Sprintf("Node %s joined cluster", nodeID)
}

// LeaveNode removes a node from the cluster
func (sm *ClusterStateMachine) LeaveNode(nodeID types.NodeID) interface{} {
	delete(sm.servers, nodeID)
	sm.version++
	return fmt.Sprintf("Node %s left cluster", nodeID)
}

// GetConfiguration returns the current cluster configuration
func (sm *ClusterStateMachine) GetConfiguration() *Configuration {
	return &Configuration{
		Servers: sm.servers,
		Version: sm.version,
	}
}

// Snapshot creates a snapshot of the state machine
func (sm *ClusterStateMachine) Snapshot() ([]byte, error) {
	return json.Marshal(sm.GetConfiguration())
}

// Restore restores the state machine from a snapshot
func (sm *ClusterStateMachine) Restore(snapshot []byte) error {
	var config Configuration
	if err := json.Unmarshal(snapshot, &config); err != nil {
		return err
	}

	sm.servers = config.Servers
	sm.version = config.Version

	return nil
}

// GetState returns the current state
func (sm *ClusterStateMachine) GetState() interface{} {
	return sm.GetConfiguration()
}

// ConfigStateMachine implements a state machine for configuration
type ConfigStateMachine struct {
	config    map[string]string
	timestamp time.Time
}

// NewConfigStateMachine creates a new configuration state machine
func NewConfigStateMachine() *ConfigStateMachine {
	return &ConfigStateMachine{
		config:    make(map[string]string),
		timestamp: time.Now(),
	}
}

// Apply applies a command to the state machine
func (sm *ConfigStateMachine) Apply(command Command) (interface{}, error) {
	return command.Apply(sm)
}

// SetConfig sets a configuration value
func (sm *ConfigStateMachine) SetConfig(key, value string) interface{} {
	sm.config[key] = value
	sm.timestamp = time.Now()
	return fmt.Sprintf("Set config %s=%s", key, value)
}

// GetConfig retrieves a configuration value
func (sm *ConfigStateMachine) GetConfig(key string) string {
	return sm.config[key]
}

// GetAllConfig retrieves all configuration
func (sm *ConfigStateMachine) GetAllConfig() map[string]string {
	return sm.config
}

// Snapshot creates a snapshot of the state machine
func (sm *ConfigStateMachine) Snapshot() ([]byte, error) {
	state := struct {
		Config    map[string]string `json:"config"`
		Timestamp time.Time         `json:"timestamp"`
	}{
		Config:    sm.config,
		Timestamp: sm.timestamp,
	}
	return json.Marshal(state)
}

// Restore restores the state machine from a snapshot
func (sm *ConfigStateMachine) Restore(snapshot []byte) error {
	var state struct {
		Config    map[string]string `json:"config"`
		Timestamp time.Time         `json:"timestamp"`
	}

	if err := json.Unmarshal(snapshot, &state); err != nil {
		return err
	}

	sm.config = state.Config
	sm.timestamp = state.Timestamp

	return nil
}

// GetState returns the current state
func (sm *ConfigStateMachine) GetState() interface{} {
	return struct {
		Config    map[string]string `json:"config"`
		Timestamp time.Time         `json:"timestamp"`
	}{
		Config:    sm.config,
		Timestamp: sm.timestamp,
	}
}

// CompositeStateMachine combines multiple state machines
type CompositeStateMachine struct {
	keyValue  *KeyValueStateMachine
	streaming *StreamingStateMachine
	cluster   *ClusterStateMachine
	config    *ConfigStateMachine
}

// NewCompositeStateMachine creates a new composite state machine
func NewCompositeStateMachine() *CompositeStateMachine {
	return &CompositeStateMachine{
		keyValue:  NewKeyValueStateMachine(),
		streaming: NewStreamingStateMachine(),
		cluster:   NewClusterStateMachine(),
		config:    NewConfigStateMachine(),
	}
}

// Apply applies a command to the appropriate state machine
func (sm *CompositeStateMachine) Apply(command Command) (interface{}, error) {
	switch command.Type() {
	case CommandTypeSetKey, CommandTypeDeleteKey:
		return sm.keyValue.Apply(command)
	case CommandTypeCreateTopic, CommandTypeDeleteTopic, CommandTypeCreatePartition, CommandTypeDeletePartition:
		return sm.streaming.Apply(command)
	case CommandTypeJoinCluster, CommandTypeLeaveCluster:
		return sm.cluster.Apply(command)
	case CommandTypeConfig:
		return sm.config.Apply(command)
	case CommandTypeNoop:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown command type: %d", command.Type())
	}
}

// Snapshot creates a snapshot of all state machines
func (sm *CompositeStateMachine) Snapshot() ([]byte, error) {
	kvSnapshot, err := sm.keyValue.Snapshot()
	if err != nil {
		return nil, err
	}

	streamingSnapshot, err := sm.streaming.Snapshot()
	if err != nil {
		return nil, err
	}

	clusterSnapshot, err := sm.cluster.Snapshot()
	if err != nil {
		return nil, err
	}

	configSnapshot, err := sm.config.Snapshot()
	if err != nil {
		return nil, err
	}

	composite := struct {
		KeyValue  json.RawMessage `json:"key_value"`
		Streaming json.RawMessage `json:"streaming"`
		Cluster   json.RawMessage `json:"cluster"`
		Config    json.RawMessage `json:"config"`
	}{
		KeyValue:  kvSnapshot,
		Streaming: streamingSnapshot,
		Cluster:   clusterSnapshot,
		Config:    configSnapshot,
	}

	return json.Marshal(composite)
}

// Restore restores all state machines from a snapshot
func (sm *CompositeStateMachine) Restore(snapshot []byte) error {
	var composite struct {
		KeyValue  json.RawMessage `json:"key_value"`
		Streaming json.RawMessage `json:"streaming"`
		Cluster   json.RawMessage `json:"cluster"`
		Config    json.RawMessage `json:"config"`
	}

	if err := json.Unmarshal(snapshot, &composite); err != nil {
		return err
	}

	if err := sm.keyValue.Restore(composite.KeyValue); err != nil {
		return err
	}

	if err := sm.streaming.Restore(composite.Streaming); err != nil {
		return err
	}

	if err := sm.cluster.Restore(composite.Cluster); err != nil {
		return err
	}

	if err := sm.config.Restore(composite.Config); err != nil {
		return err
	}

	return nil
}

// GetState returns the combined state
func (sm *CompositeStateMachine) GetState() interface{} {
	return struct {
		KeyValue  interface{} `json:"key_value"`
		Streaming interface{} `json:"streaming"`
		Cluster   interface{} `json:"cluster"`
		Config    interface{} `json:"config"`
	}{
		KeyValue:  sm.keyValue.GetState(),
		Streaming: sm.streaming.GetState(),
		Cluster:   sm.cluster.GetState(),
		Config:    sm.config.GetState(),
	}
}

// GetKeyValueStateMachine returns the key-value state machine
func (sm *CompositeStateMachine) GetKeyValueStateMachine() *KeyValueStateMachine {
	return sm.keyValue
}

// GetStreamingStateMachine returns the streaming state machine
func (sm *CompositeStateMachine) GetStreamingStateMachine() *StreamingStateMachine {
	return sm.streaming
}

// GetClusterStateMachine returns the cluster state machine
func (sm *CompositeStateMachine) GetClusterStateMachine() *ClusterStateMachine {
	return sm.cluster
}

// GetConfigStateMachine returns the config state machine
func (sm *CompositeStateMachine) GetConfigStateMachine() *ConfigStateMachine {
	return sm.config
}
