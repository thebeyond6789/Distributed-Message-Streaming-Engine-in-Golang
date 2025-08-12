package network

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"streaming-engine/internal/raft"
	"streaming-engine/pkg/protocol"
	"streaming-engine/pkg/types"
)

// Server represents the main network server
type Server struct {
	config      *ServerConfig
	listener    net.Listener
	connections map[string]*Connection
	connMu      sync.RWMutex
	handlers    map[protocol.MessageType]MessageHandler
	handlerMu   sync.RWMutex

	// Statistics
	stats *ServerStats

	// Control
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running int32

	// Dependencies
	raftNode *raft.RaftNode

	// Connection pooling
	connPool *ConnectionPool

	// Rate limiting
	rateLimiter *RateLimiter
}

// ServerConfig represents server configuration
type ServerConfig struct {
	Address            string        `json:"address"`
	Port               int           `json:"port"`
	MaxConnections     int           `json:"max_connections"`
	ReadTimeout        time.Duration `json:"read_timeout"`
	WriteTimeout       time.Duration `json:"write_timeout"`
	KeepAlive          time.Duration `json:"keep_alive"`
	MaxMessageSize     int32         `json:"max_message_size"`
	BufferSize         int           `json:"buffer_size"`
	EnableCompression  bool          `json:"enable_compression"`
	EnableTLS          bool          `json:"enable_tls"`
	TLSCertFile        string        `json:"tls_cert_file"`
	TLSKeyFile         string        `json:"tls_key_file"`
	RateLimitRequests  int           `json:"rate_limit_requests"`
	RateLimitWindow    time.Duration `json:"rate_limit_window"`
	ConnectionPoolSize int           `json:"connection_pool_size"`
	WorkerPoolSize     int           `json:"worker_pool_size"`
}

// DefaultServerConfig returns default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Address:            "0.0.0.0",
		Port:               9092,
		MaxConnections:     1000,
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		KeepAlive:          60 * time.Second,
		MaxMessageSize:     16 * 1024 * 1024, // 16MB
		BufferSize:         64 * 1024,        // 64KB
		EnableCompression:  false,
		EnableTLS:          false,
		RateLimitRequests:  1000,
		RateLimitWindow:    time.Minute,
		ConnectionPoolSize: 100,
		WorkerPoolSize:     10,
	}
}

// MessageHandler represents a message handler function
type MessageHandler func(ctx context.Context, conn *Connection, msg *protocol.Message) error

// ServerStats represents server statistics
type ServerStats struct {
	ConnectionsTotal  int64     `json:"connections_total"`
	ConnectionsActive int64     `json:"connections_active"`
	MessagesReceived  int64     `json:"messages_received"`
	MessagesSent      int64     `json:"messages_sent"`
	BytesReceived     int64     `json:"bytes_received"`
	BytesSent         int64     `json:"bytes_sent"`
	ErrorsTotal       int64     `json:"errors_total"`
	RequestsPerSecond float64   `json:"requests_per_second"`
	AverageLatency    float64   `json:"average_latency"`
	LastUpdated       time.Time `json:"last_updated"`
}

// NewServer creates a new network server
func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	server := &Server{
		config:      config,
		connections: make(map[string]*Connection),
		handlers:    make(map[protocol.MessageType]MessageHandler),
		stats:       &ServerStats{},
		ctx:         ctx,
		cancel:      cancel,
	}

	// Initialize connection pool
	server.connPool = NewConnectionPool(config.ConnectionPoolSize)

	// Initialize rate limiter
	server.rateLimiter = NewRateLimiter(config.RateLimitRequests, config.RateLimitWindow)

	// Register default handlers
	server.registerDefaultHandlers()

	return server, nil
}

// SetRaftNode sets the Raft node for handling consensus
func (s *Server) SetRaftNode(node *raft.RaftNode) {
	s.raftNode = node
}

// Start starts the server
func (s *Server) Start() error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return fmt.Errorf("server already running")
	}

	// Create listener
	addr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener

	// Start accepting connections
	s.wg.Add(1)
	go s.acceptConnections()

	// Start statistics updater
	s.wg.Add(1)
	go s.updateStats()

	fmt.Printf("Server listening on %s\n", addr)
	return nil
}

// Stop stops the server
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil // Already stopped
	}

	// Cancel context
	s.cancel()

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.connMu.Lock()
	for _, conn := range s.connections {
		conn.Close()
	}
	s.connMu.Unlock()

	// Wait for goroutines
	s.wg.Wait()

	// Close connection pool
	s.connPool.Close()

	return nil
}

// RegisterHandler registers a message handler
func (s *Server) RegisterHandler(msgType protocol.MessageType, handler MessageHandler) {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()

	s.handlers[msgType] = handler
}

// GetStats returns server statistics
func (s *Server) GetStats() *ServerStats {
	stats := *s.stats
	stats.LastUpdated = time.Now()
	return &stats
}

// SendMessage sends a message to a specific connection
func (s *Server) SendMessage(connID string, msg *protocol.Message) error {
	s.connMu.RLock()
	conn, exists := s.connections[connID]
	s.connMu.RUnlock()

	if !exists {
		return fmt.Errorf("connection not found: %s", connID)
	}

	return conn.SendMessage(msg)
}

// BroadcastMessage broadcasts a message to all connections
func (s *Server) BroadcastMessage(msg *protocol.Message) error {
	s.connMu.RLock()
	connections := make([]*Connection, 0, len(s.connections))
	for _, conn := range s.connections {
		connections = append(connections, conn)
	}
	s.connMu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(connections))

	for _, conn := range connections {
		wg.Add(1)
		go func(c *Connection) {
			defer wg.Done()
			if err := c.SendMessage(msg); err != nil {
				errCh <- err
			}
		}(conn)
	}

	wg.Wait()
	close(errCh)

	// Return first error if any
	for err := range errCh {
		return err
	}

	return nil
}

// Private methods

func (s *Server) acceptConnections() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				atomic.AddInt64(&s.stats.ErrorsTotal, 1)
				continue
			}
		}

		// Check connection limit
		s.connMu.RLock()
		connCount := len(s.connections)
		s.connMu.RUnlock()

		if connCount >= s.config.MaxConnections {
			conn.Close()
			atomic.AddInt64(&s.stats.ErrorsTotal, 1)
			continue
		}

		// Handle connection
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(netConn net.Conn) {
	defer s.wg.Done()

	// Create connection wrapper
	conn := NewConnection(netConn, s.config)
	connID := conn.ID()

	// Add to connections map
	s.connMu.Lock()
	s.connections[connID] = conn
	s.connMu.Unlock()

	// Update stats
	atomic.AddInt64(&s.stats.ConnectionsTotal, 1)
	atomic.AddInt64(&s.stats.ConnectionsActive, 1)

	defer func() {
		// Remove from connections map
		s.connMu.Lock()
		delete(s.connections, connID)
		s.connMu.Unlock()

		// Update stats
		atomic.AddInt64(&s.stats.ConnectionsActive, -1)

		conn.Close()
	}()

	// Handle messages
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Check rate limit
		if !s.rateLimiter.Allow() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Read message
		msg, err := conn.ReadMessage()
		if err != nil {
			if !isConnectionClosed(err) {
				atomic.AddInt64(&s.stats.ErrorsTotal, 1)
			}
			return
		}

		// Update stats
		atomic.AddInt64(&s.stats.MessagesReceived, 1)
		atomic.AddInt64(&s.stats.BytesReceived, int64(len(msg.Payload)))

		// Handle message
		if err := s.handleMessage(conn, msg); err != nil {
			atomic.AddInt64(&s.stats.ErrorsTotal, 1)
			// Continue handling other messages
		}
	}
}

func (s *Server) handleMessage(conn *Connection, msg *protocol.Message) error {
	ctx, cancel := context.WithTimeout(s.ctx, s.config.ReadTimeout)
	defer cancel()

	s.handlerMu.RLock()
	handler, exists := s.handlers[msg.Header.MessageType]
	s.handlerMu.RUnlock()

	if !exists {
		return fmt.Errorf("no handler for message type: %d", msg.Header.MessageType)
	}

	return handler(ctx, conn, msg)
}

func (s *Server) registerDefaultHandlers() {
	// Produce request handler
	s.RegisterHandler(protocol.MessageTypeProduceRequest, s.handleProduceRequest)

	// Fetch request handler
	s.RegisterHandler(protocol.MessageTypeFetchRequest, s.handleFetchRequest)

	// Metadata request handler
	s.RegisterHandler(protocol.MessageTypeMetadataRequest, s.handleMetadataRequest)

	// Heartbeat request handler
	s.RegisterHandler(protocol.MessageTypeHeartbeatRequest, s.handleHeartbeatRequest)

	// Raft message handlers
	s.RegisterHandler(protocol.MessageTypeRaftAppendEntries, s.handleRaftAppendEntries)
	s.RegisterHandler(protocol.MessageTypeRaftRequestVote, s.handleRaftRequestVote)
	s.RegisterHandler(protocol.MessageTypeRaftInstallSnapshot, s.handleRaftInstallSnapshot)
}

func (s *Server) handleProduceRequest(ctx context.Context, conn *Connection, msg *protocol.Message) error {
	decoder := protocol.NewDecoder()
	req, err := decoder.DecodeProduceRequest(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to decode produce request: %w", err)
	}

	// Process produce request
	response := &protocol.ProduceResponse{
		Responses: make(map[string]map[int32]*protocol.PartitionResponse),
		Timestamp: time.Now(),
	}

	// Send response
	encoder := protocol.NewEncoder()
	respData, err := encoder.EncodeMessage(protocol.MessageTypeProduceResponse,
		mustMarshal(response), msg.Header.RequestID)
	if err != nil {
		return fmt.Errorf("failed to encode response: %w", err)
	}

	respMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       msg.Header.Magic,
			Version:     msg.Header.Version,
			MessageType: protocol.MessageTypeProduceResponse,
			RequestID:   msg.Header.RequestID,
		},
		Payload: respData,
	}

	return conn.SendMessage(respMsg)
}

func (s *Server) handleFetchRequest(ctx context.Context, conn *Connection, msg *protocol.Message) error {
	decoder := protocol.NewDecoder()
	req, err := decoder.DecodeFetchRequest(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to decode fetch request: %w", err)
	}

	// Process fetch request
	response := &protocol.FetchResponse{
		ThrottleTime: 0,
		ErrorCode:    types.ErrNoError,
		Responses:    make(map[string]map[int32]*protocol.FetchResponsePartition),
	}

	// Send response
	encoder := protocol.NewEncoder()
	respData, err := encoder.EncodeMessage(protocol.MessageTypeFetchResponse,
		mustMarshal(response), msg.Header.RequestID)
	if err != nil {
		return fmt.Errorf("failed to encode response: %w", err)
	}

	respMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       msg.Header.Magic,
			Version:     msg.Header.Version,
			MessageType: protocol.MessageTypeFetchResponse,
			RequestID:   msg.Header.RequestID,
		},
		Payload: respData,
	}

	return conn.SendMessage(respMsg)
}

func (s *Server) handleMetadataRequest(ctx context.Context, conn *Connection, msg *protocol.Message) error {
	// Process metadata request
	response := &protocol.MetadataResponse{
		ThrottleTime:  0,
		Brokers:       []types.BrokerInfo{},
		ClusterID:     "streaming-cluster",
		ControllerID:  "node-1",
		TopicMetadata: []protocol.TopicMetadata{},
	}

	// Send response
	encoder := protocol.NewEncoder()
	respData, err := encoder.EncodeMessage(protocol.MessageTypeMetadataResponse,
		mustMarshal(response), msg.Header.RequestID)
	if err != nil {
		return fmt.Errorf("failed to encode response: %w", err)
	}

	respMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       msg.Header.Magic,
			Version:     msg.Header.Version,
			MessageType: protocol.MessageTypeMetadataResponse,
			RequestID:   msg.Header.RequestID,
		},
		Payload: respData,
	}

	return conn.SendMessage(respMsg)
}

func (s *Server) handleHeartbeatRequest(ctx context.Context, conn *Connection, msg *protocol.Message) error {
	// Process heartbeat request
	response := &protocol.HeartbeatResponse{
		ThrottleTime: 0,
		ErrorCode:    types.ErrNoError,
	}

	// Send response
	encoder := protocol.NewEncoder()
	respData, err := encoder.EncodeMessage(protocol.MessageTypeHeartbeatResponse,
		mustMarshal(response), msg.Header.RequestID)
	if err != nil {
		return fmt.Errorf("failed to encode response: %w", err)
	}

	respMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       msg.Header.Magic,
			Version:     msg.Header.Version,
			MessageType: protocol.MessageTypeHeartbeatResponse,
			RequestID:   msg.Header.RequestID,
		},
		Payload: respData,
	}

	return conn.SendMessage(respMsg)
}

func (s *Server) handleRaftAppendEntries(ctx context.Context, conn *Connection, msg *protocol.Message) error {
	if s.raftNode == nil {
		return fmt.Errorf("raft node not configured")
	}

	// Decode append entries request
	var req raft.AppendEntriesRequest
	if err := mustUnmarshal(msg.Payload, &req); err != nil {
		return fmt.Errorf("failed to decode append entries request: %w", err)
	}

	// Process with Raft node
	resp, err := s.raftNode.AppendEntries(&req)
	if err != nil {
		return fmt.Errorf("failed to process append entries: %w", err)
	}

	// Send response
	respData := mustMarshal(resp)
	respMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       msg.Header.Magic,
			Version:     msg.Header.Version,
			MessageType: protocol.MessageTypeRaftAppendEntries,
			RequestID:   msg.Header.RequestID,
		},
		Payload: respData,
	}

	return conn.SendMessage(respMsg)
}

func (s *Server) handleRaftRequestVote(ctx context.Context, conn *Connection, msg *protocol.Message) error {
	if s.raftNode == nil {
		return fmt.Errorf("raft node not configured")
	}

	// Decode vote request
	var req raft.VoteRequest
	if err := mustUnmarshal(msg.Payload, &req); err != nil {
		return fmt.Errorf("failed to decode vote request: %w", err)
	}

	// Process with Raft node
	resp, err := s.raftNode.RequestVote(&req)
	if err != nil {
		return fmt.Errorf("failed to process vote request: %w", err)
	}

	// Send response
	respData := mustMarshal(resp)
	respMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       msg.Header.Magic,
			Version:     msg.Header.Version,
			MessageType: protocol.MessageTypeRaftRequestVote,
			RequestID:   msg.Header.RequestID,
		},
		Payload: respData,
	}

	return conn.SendMessage(respMsg)
}

func (s *Server) handleRaftInstallSnapshot(ctx context.Context, conn *Connection, msg *protocol.Message) error {
	if s.raftNode == nil {
		return fmt.Errorf("raft node not configured")
	}

	// Decode install snapshot request
	var req raft.InstallSnapshotRequest
	if err := mustUnmarshal(msg.Payload, &req); err != nil {
		return fmt.Errorf("failed to decode install snapshot request: %w", err)
	}

	// Process with Raft node
	resp, err := s.raftNode.InstallSnapshot(&req)
	if err != nil {
		return fmt.Errorf("failed to process install snapshot: %w", err)
	}

	// Send response
	respData := mustMarshal(resp)
	respMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       msg.Header.Magic,
			Version:     msg.Header.Version,
			MessageType: protocol.MessageTypeRaftInstallSnapshot,
			RequestID:   msg.Header.RequestID,
		},
		Payload: respData,
	}

	return conn.SendMessage(respMsg)
}

func (s *Server) updateStats() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastRequests int64
	var lastTime time.Time = time.Now()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			currentRequests := atomic.LoadInt64(&s.stats.MessagesReceived)

			if !lastTime.IsZero() {
				duration := now.Sub(lastTime).Seconds()
				requestsDiff := currentRequests - lastRequests
				s.stats.RequestsPerSecond = float64(requestsDiff) / duration
			}

			lastRequests = currentRequests
			lastTime = now
		}
	}
}

// Utility functions
func isConnectionClosed(err error) bool {
	if err == nil {
		return false
	}

	// Check for common connection closed errors
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}

	errStr := err.Error()
	return errStr == "EOF" ||
		errStr == "connection reset by peer" ||
		errStr == "use of closed network connection"
}

func mustMarshal(v interface{}) []byte {
	// This would use proper JSON marshaling in a real implementation
	return []byte{}
}

func mustUnmarshal(data []byte, v interface{}) error {
	// This would use proper JSON unmarshaling in a real implementation
	return nil
}
