package network

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"streaming-engine/api/proto"
	"streaming-engine/internal/raft"
	"streaming-engine/internal/storage"
	"streaming-engine/pkg/types"
)

// GRPCServer implements the streaming gRPC service
type GRPCServer struct {
	proto.UnimplementedStreamingServiceServer

	config   *GRPCServerConfig
	server   *grpc.Server
	listener net.Listener

	// Dependencies
	logEngine    *storage.LogEngine
	raftNode     *raft.RaftNode
	stateMachine *raft.CompositeStateMachine

	// State
	mu      sync.RWMutex
	running bool
}

// GRPCServerConfig represents gRPC server configuration
type GRPCServerConfig struct {
	Address               string        `json:"address"`
	Port                  int           `json:"port"`
	MaxReceiveMessageSize int           `json:"max_receive_message_size"`
	MaxSendMessageSize    int           `json:"max_send_message_size"`
	ConnectionTimeout     time.Duration `json:"connection_timeout"`
	EnableReflection      bool          `json:"enable_reflection"`
	EnableHealthCheck     bool          `json:"enable_health_check"`
}

// DefaultGRPCServerConfig returns default gRPC server configuration
func DefaultGRPCServerConfig() *GRPCServerConfig {
	return &GRPCServerConfig{
		Address:               "0.0.0.0",
		Port:                  9093,
		MaxReceiveMessageSize: 16 * 1024 * 1024, // 16MB
		MaxSendMessageSize:    16 * 1024 * 1024, // 16MB
		ConnectionTimeout:     30 * time.Second,
		EnableReflection:      true,
		EnableHealthCheck:     true,
	}
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(config *GRPCServerConfig) *GRPCServer {
	if config == nil {
		config = DefaultGRPCServerConfig()
	}

	return &GRPCServer{
		config: config,
	}
}

// SetDependencies sets the required dependencies
func (s *GRPCServer) SetDependencies(logEngine *storage.LogEngine, raftNode *raft.RaftNode, stateMachine *raft.CompositeStateMachine) {
	s.logEngine = logEngine
	s.raftNode = raftNode
	s.stateMachine = stateMachine
}

// Start starts the gRPC server
func (s *GRPCServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("gRPC server already running")
	}

	// Create listener
	addr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener

	// Create gRPC server with options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.config.MaxReceiveMessageSize),
		grpc.MaxSendMsgSize(s.config.MaxSendMessageSize),
		grpc.ConnectionTimeout(s.config.ConnectionTimeout),
	}

	s.server = grpc.NewServer(opts...)

	// Register service
	proto.RegisterStreamingServiceServer(s.server, s)

	// Enable reflection if configured
	if s.config.EnableReflection {
		// reflection.Register(s.server) // Would need to import reflection package
	}

	s.running = true

	// Start serving
	go func() {
		if err := s.server.Serve(listener); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	fmt.Printf("gRPC server listening on %s\n", addr)
	return nil
}

// Stop stops the gRPC server
func (s *GRPCServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	// Graceful stop
	if s.server != nil {
		s.server.GracefulStop()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	s.running = false
	return nil
}

// Topic Management Methods

// CreateTopic creates a new topic
func (s *GRPCServer) CreateTopic(ctx context.Context, req *proto.CreateTopicRequest) (*proto.CreateTopicResponse, error) {
	if s.raftNode == nil {
		return nil, status.Error(codes.Unavailable, "raft node not available")
	}

	// Convert proto config to internal type
	topicConfig := &types.TopicConfig{
		Name:              req.Config.Name,
		Partitions:        req.Config.Partitions,
		ReplicationFactor: req.Config.ReplicationFactor,
		RetentionTime:     time.Duration(req.Config.RetentionTimeMs) * time.Millisecond,
		RetentionSize:     req.Config.RetentionSize,
		CompactionPolicy:  req.Config.CompactionPolicy,
		CompressionType:   types.CompressionType(req.Config.CompressionType),
		MaxMessageSize:    req.Config.MaxMessageSize,
	}

	// Create command
	cmd := &raft.CreateTopicCommand{
		Topic: *topicConfig,
	}

	// Submit to Raft
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	result, err := s.raftNode.Apply(cmd, timeout)
	if err != nil {
		return &proto.CreateTopicResponse{
			Success:      false,
			ErrorMessage: err.Error(),
			ErrorCode:    int32(types.ErrUnknown),
		}, nil
	}

	_ = result // Use result as needed

	return &proto.CreateTopicResponse{
		Success:   true,
		ErrorCode: int32(types.ErrNoError),
	}, nil
}

// DeleteTopic deletes a topic
func (s *GRPCServer) DeleteTopic(ctx context.Context, req *proto.DeleteTopicRequest) (*proto.DeleteTopicResponse, error) {
	if s.raftNode == nil {
		return nil, status.Error(codes.Unavailable, "raft node not available")
	}

	// Create command
	cmd := &raft.DeleteTopicCommand{
		TopicName: req.TopicName,
	}

	// Submit to Raft
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	_, err := s.raftNode.Apply(cmd, timeout)
	if err != nil {
		return &proto.DeleteTopicResponse{
			Success:      false,
			ErrorMessage: err.Error(),
			ErrorCode:    int32(types.ErrUnknown),
		}, nil
	}

	return &proto.DeleteTopicResponse{
		Success:   true,
		ErrorCode: int32(types.ErrNoError),
	}, nil
}

// ListTopics lists all topics
func (s *GRPCServer) ListTopics(ctx context.Context, req *proto.ListTopicsRequest) (*proto.ListTopicsResponse, error) {
	if s.stateMachine == nil {
		return nil, status.Error(codes.Unavailable, "state machine not available")
	}

	streamingSM := s.stateMachine.GetStreamingStateMachine()
	topics := streamingSM.GetTopics()

	response := &proto.ListTopicsResponse{
		TopicNames:   make([]string, 0, len(topics)),
		TopicConfigs: make([]*proto.TopicConfig, 0, len(topics)),
	}

	for name, config := range topics {
		response.TopicNames = append(response.TopicNames, name)

		protoConfig := &proto.TopicConfig{
			Name:              config.Name,
			Partitions:        config.Partitions,
			ReplicationFactor: config.ReplicationFactor,
			RetentionTimeMs:   int64(config.RetentionTime / time.Millisecond),
			RetentionSize:     config.RetentionSize,
			CompactionPolicy:  config.CompactionPolicy,
			CompressionType:   string(config.CompressionType),
			MaxMessageSize:    config.MaxMessageSize,
		}

		response.TopicConfigs = append(response.TopicConfigs, protoConfig)
	}

	return response, nil
}

// GetTopicMetadata gets metadata for a specific topic
func (s *GRPCServer) GetTopicMetadata(ctx context.Context, req *proto.GetTopicMetadataRequest) (*proto.GetTopicMetadataResponse, error) {
	if s.stateMachine == nil {
		return nil, status.Error(codes.Unavailable, "state machine not available")
	}

	streamingSM := s.stateMachine.GetStreamingStateMachine()
	config := streamingSM.GetTopic(req.TopicName)
	if config == nil {
		return nil, status.Error(codes.NotFound, "topic not found")
	}

	partitions := streamingSM.GetPartitions(req.TopicName)

	protoConfig := &proto.TopicConfig{
		Name:              config.Name,
		Partitions:        config.Partitions,
		ReplicationFactor: config.ReplicationFactor,
		RetentionTimeMs:   int64(config.RetentionTime / time.Millisecond),
		RetentionSize:     config.RetentionSize,
		CompactionPolicy:  config.CompactionPolicy,
		CompressionType:   string(config.CompressionType),
		MaxMessageSize:    config.MaxMessageSize,
	}

	protoPartitions := make([]*proto.PartitionInfo, 0, len(partitions))
	for _, partition := range partitions {
		protoPartition := &proto.PartitionInfo{
			Topic:     partition.Topic,
			Partition: partition.Partition,
			Leader:    string(partition.Leader),
			Replicas:  make([]string, len(partition.Replicas)),
			Isr:       make([]string, len(partition.ISR)),
			Offset:    partition.Offset,
			Size:      partition.Size,
		}

		for i, replica := range partition.Replicas {
			protoPartition.Replicas[i] = string(replica)
		}

		for i, isr := range partition.ISR {
			protoPartition.Isr[i] = string(isr)
		}

		protoPartitions = append(protoPartitions, protoPartition)
	}

	return &proto.GetTopicMetadataResponse{
		Config:     protoConfig,
		Partitions: protoPartitions,
	}, nil
}

// Cluster Management Methods

// GetClusterInfo gets cluster information
func (s *GRPCServer) GetClusterInfo(ctx context.Context, req *emptypb.Empty) (*proto.ClusterInfoResponse, error) {
	if s.stateMachine == nil {
		return nil, status.Error(codes.Unavailable, "state machine not available")
	}

	clusterSM := s.stateMachine.GetClusterStateMachine()
	configuration := clusterSM.GetConfiguration()

	protoBrokers := make([]*proto.BrokerInfo, 0, len(configuration.Servers))
	for _, server := range configuration.Servers {
		broker := &proto.BrokerInfo{
			NodeId:    string(server.NodeID),
			Host:      server.Address, // Would need to parse host from address
			Port:      9092,           // Would need to parse port from address
			Metadata:  server.Metadata,
			Timestamp: timestamppb.Now(),
		}
		protoBrokers = append(protoBrokers, broker)
	}

	var leader string
	if s.raftNode != nil {
		leader = string(s.raftNode.GetLeader())
	}

	return &proto.ClusterInfoResponse{
		ClusterId: "streaming-cluster",
		Brokers:   protoBrokers,
		Leader:    leader,
		Version:   "1.0.0",
		Timestamp: timestamppb.Now(),
	}, nil
}

// GetBrokerInfo gets information about a specific broker
func (s *GRPCServer) GetBrokerInfo(ctx context.Context, req *proto.GetBrokerInfoRequest) (*proto.GetBrokerInfoResponse, error) {
	if s.stateMachine == nil {
		return nil, status.Error(codes.Unavailable, "state machine not available")
	}

	clusterSM := s.stateMachine.GetClusterStateMachine()
	configuration := clusterSM.GetConfiguration()

	server, exists := configuration.Servers[types.NodeID(req.NodeId)]
	if !exists {
		return nil, status.Error(codes.NotFound, "broker not found")
	}

	broker := &proto.BrokerInfo{
		NodeId:    string(server.NodeID),
		Host:      server.Address, // Would need to parse host from address
		Port:      9092,           // Would need to parse port from address
		Metadata:  server.Metadata,
		Timestamp: timestamppb.Now(),
	}

	return &proto.GetBrokerInfoResponse{
		Broker: broker,
	}, nil
}

// Consumer Group Management Methods

// ListConsumerGroups lists all consumer groups
func (s *GRPCServer) ListConsumerGroups(ctx context.Context, req *proto.ListConsumerGroupsRequest) (*proto.ListConsumerGroupsResponse, error) {
	// Implementation would depend on consumer group storage
	return &proto.ListConsumerGroupsResponse{
		GroupIds: []string{},
		Groups:   []*proto.ConsumerGroupInfo{},
	}, nil
}

// GetConsumerGroupInfo gets information about a consumer group
func (s *GRPCServer) GetConsumerGroupInfo(ctx context.Context, req *proto.GetConsumerGroupInfoRequest) (*proto.GetConsumerGroupInfoResponse, error) {
	// Implementation would depend on consumer group storage
	return &proto.GetConsumerGroupInfoResponse{
		Group: &proto.ConsumerGroupInfo{
			GroupId: req.GroupId,
			Members: []*proto.ConsumerMemberInfo{},
		},
	}, nil
}

// ResetConsumerGroupOffsets resets consumer group offsets
func (s *GRPCServer) ResetConsumerGroupOffsets(ctx context.Context, req *proto.ResetConsumerGroupOffsetsRequest) (*proto.ResetConsumerGroupOffsetsResponse, error) {
	// Implementation would depend on consumer group storage
	return &proto.ResetConsumerGroupOffsetsResponse{
		Success:        true,
		UpdatedOffsets: req.PartitionOffsets,
	}, nil
}

// Metrics and Monitoring Methods

// GetMetrics gets system metrics
func (s *GRPCServer) GetMetrics(ctx context.Context, req *proto.GetMetricsRequest) (*proto.GetMetricsResponse, error) {
	metrics := make([]*proto.Metric, 0)

	// Add Raft metrics
	if s.raftNode != nil {
		raftMetrics := s.raftNode.GetMetrics()

		metrics = append(metrics, &proto.Metric{
			Name:      "raft_term",
			Type:      "gauge",
			Value:     float64(raftMetrics.Term),
			Timestamp: timestamppb.Now(),
			Help:      "Current Raft term",
		})

		metrics = append(metrics, &proto.Metric{
			Name:      "raft_commit_index",
			Type:      "gauge",
			Value:     float64(raftMetrics.CommitIndex),
			Timestamp: timestamppb.Now(),
			Help:      "Raft commit index",
		})

		metrics = append(metrics, &proto.Metric{
			Name:      "raft_last_applied",
			Type:      "gauge",
			Value:     float64(raftMetrics.LastApplied),
			Timestamp: timestamppb.Now(),
			Help:      "Raft last applied index",
		})
	}

	// Add storage metrics
	if s.logEngine != nil {
		partitions := s.logEngine.GetPartitions()

		metrics = append(metrics, &proto.Metric{
			Name:      "total_partitions",
			Type:      "gauge",
			Value:     float64(len(partitions)),
			Timestamp: timestamppb.Now(),
			Help:      "Total number of partitions",
		})
	}

	return &proto.GetMetricsResponse{
		Metrics:   metrics,
		Timestamp: timestamppb.Now(),
	}, nil
}

// GetHealth gets system health status
func (s *GRPCServer) GetHealth(ctx context.Context, req *emptypb.Empty) (*proto.HealthResponse, error) {
	healthy := true
	status := "OK"
	checks := make(map[string]string)

	// Check Raft node health
	if s.raftNode != nil {
		state, term, isLeader := s.raftNode.GetState()
		checks["raft_state"] = state.String()
		checks["raft_term"] = fmt.Sprintf("%d", term)
		checks["raft_is_leader"] = fmt.Sprintf("%t", isLeader)
	} else {
		healthy = false
		status = "Raft node not available"
		checks["raft"] = "unavailable"
	}

	// Check log engine health
	if s.logEngine != nil {
		checks["log_engine"] = "ok"
	} else {
		healthy = false
		status = "Log engine not available"
		checks["log_engine"] = "unavailable"
	}

	return &proto.HealthResponse{
		Healthy:   healthy,
		Status:    status,
		Checks:    checks,
		Timestamp: timestamppb.Now(),
	}, nil
}

// Stream Processing Methods

// CreateStream creates a new stream processing job
func (s *GRPCServer) CreateStream(ctx context.Context, req *proto.CreateStreamRequest) (*proto.CreateStreamResponse, error) {
	// Implementation would depend on stream processing engine
	return &proto.CreateStreamResponse{
		Success:  true,
		StreamId: "stream-" + req.Config.Name,
	}, nil
}

// DeleteStream deletes a stream processing job
func (s *GRPCServer) DeleteStream(ctx context.Context, req *proto.DeleteStreamRequest) (*proto.DeleteStreamResponse, error) {
	// Implementation would depend on stream processing engine
	return &proto.DeleteStreamResponse{
		Success: true,
	}, nil
}

// ListStreams lists all stream processing jobs
func (s *GRPCServer) ListStreams(ctx context.Context, req *proto.ListStreamsRequest) (*proto.ListStreamsResponse, error) {
	// Implementation would depend on stream processing engine
	return &proto.ListStreamsResponse{
		Streams: []*proto.StreamInfo{},
	}, nil
}
