package network

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"streaming-engine/pkg/protocol"
)

// Connection represents a network connection wrapper
type Connection struct {
	// Network connection
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	// Connection info
	id         string
	localAddr  string
	remoteAddr string

	// Configuration
	config *ServerConfig

	// State
	mu        sync.RWMutex
	closed    int32
	lastRead  time.Time
	lastWrite time.Time

	// Statistics
	stats *ConnectionStats

	// Protocol
	decoder *protocol.Decoder
	encoder *protocol.Encoder

	// Buffering
	sendBuffer   chan *protocol.Message
	sendBufferMu sync.Mutex

	// Context for operations
	ctx    context.Context
	cancel context.CancelFunc
}

// ConnectionStats represents connection statistics
type ConnectionStats struct {
	MessagesReceived int64     `json:"messages_received"`
	MessagesSent     int64     `json:"messages_sent"`
	BytesReceived    int64     `json:"bytes_received"`
	BytesSent        int64     `json:"bytes_sent"`
	Errors           int64     `json:"errors"`
	Connected        time.Time `json:"connected"`
	LastActivity     time.Time `json:"last_activity"`
}

// NewConnection creates a new connection wrapper
func NewConnection(netConn net.Conn, config *ServerConfig) *Connection {
	// Generate connection ID
	idBytes := make([]byte, 16)
	rand.Read(idBytes)
	id := hex.EncodeToString(idBytes)

	ctx, cancel := context.WithCancel(context.Background())

	conn := &Connection{
		conn:       netConn,
		reader:     bufio.NewReaderSize(netConn, config.BufferSize),
		writer:     bufio.NewWriterSize(netConn, config.BufferSize),
		id:         id,
		localAddr:  netConn.LocalAddr().String(),
		remoteAddr: netConn.RemoteAddr().String(),
		config:     config,
		stats:      &ConnectionStats{Connected: time.Now()},
		decoder:    protocol.NewDecoder(),
		encoder:    protocol.NewEncoder(),
		sendBuffer: make(chan *protocol.Message, 1000),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Set connection timeouts
	if config.KeepAlive > 0 {
		if tcpConn, ok := netConn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(config.KeepAlive)
		}
	}

	// Start send buffer processor
	go conn.processSendBuffer()

	return conn
}

// ID returns the connection ID
func (c *Connection) ID() string {
	return c.id
}

// LocalAddr returns the local address
func (c *Connection) LocalAddr() string {
	return c.localAddr
}

// RemoteAddr returns the remote address
func (c *Connection) RemoteAddr() string {
	return c.remoteAddr
}

// ReadMessage reads a message from the connection
func (c *Connection) ReadMessage() (*protocol.Message, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, fmt.Errorf("connection closed")
	}

	// Set read timeout
	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}

	// Read message header first
	headerBytes := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(c.reader, headerBytes); err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Read payload length from header
	header, err := c.decoder.DecodeMessage(headerBytes)
	if err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return nil, fmt.Errorf("failed to decode header: %w", err)
	}

	// Validate message size
	if header.Header.Length > uint32(c.config.MaxMessageSize) {
		atomic.AddInt64(&c.stats.Errors, 1)
		return nil, fmt.Errorf("message too large: %d bytes", header.Header.Length)
	}

	// Read payload
	payloadBytes := make([]byte, header.Header.Length)
	if _, err := io.ReadFull(c.reader, payloadBytes); err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	// Combine header and payload
	messageBytes := append(headerBytes, payloadBytes...)

	// Decode complete message
	msg, err := c.decoder.DecodeMessage(messageBytes)
	if err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return nil, fmt.Errorf("failed to decode message: %w", err)
	}

	// Update statistics
	atomic.AddInt64(&c.stats.MessagesReceived, 1)
	atomic.AddInt64(&c.stats.BytesReceived, int64(len(messageBytes)))

	c.mu.Lock()
	c.lastRead = time.Now()
	c.stats.LastActivity = c.lastRead
	c.mu.Unlock()

	return msg, nil
}

// SendMessage sends a message to the connection
func (c *Connection) SendMessage(msg *protocol.Message) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("connection closed")
	}

	// Use buffered sending for better performance
	select {
	case c.sendBuffer <- msg:
		return nil
	case <-c.ctx.Done():
		return fmt.Errorf("connection context cancelled")
	default:
		// Buffer full, send directly
		return c.sendMessageDirect(msg)
	}
}

// SendMessageSync sends a message synchronously
func (c *Connection) SendMessageSync(msg *protocol.Message) error {
	return c.sendMessageDirect(msg)
}

// WriteRaw writes raw data to the connection
func (c *Connection) WriteRaw(data []byte) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("connection closed")
	}

	// Set write timeout
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.writer.Write(data); err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := c.writer.Flush(); err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return fmt.Errorf("failed to flush data: %w", err)
	}

	// Update statistics
	atomic.AddInt64(&c.stats.BytesSent, int64(len(data)))
	c.lastWrite = time.Now()
	c.stats.LastActivity = c.lastWrite

	return nil
}

// ReadRaw reads raw data from the connection
func (c *Connection) ReadRaw(buffer []byte) (int, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return 0, fmt.Errorf("connection closed")
	}

	// Set read timeout
	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}

	n, err := c.reader.Read(buffer)
	if err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return n, err
	}

	// Update statistics
	atomic.AddInt64(&c.stats.BytesReceived, int64(n))

	c.mu.Lock()
	c.lastRead = time.Now()
	c.stats.LastActivity = c.lastRead
	c.mu.Unlock()

	return n, nil
}

// IsHealthy checks if the connection is healthy
func (c *Connection) IsHealthy() bool {
	if atomic.LoadInt32(&c.closed) == 1 {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if connection is too old without activity
	if time.Since(c.stats.LastActivity) > c.config.KeepAlive*2 {
		return false
	}

	return true
}

// GetStats returns connection statistics
func (c *Connection) GetStats() *ConnectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return a copy to prevent data races
	stats := *c.stats
	return &stats
}

// Close closes the connection
func (c *Connection) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil // Already closed
	}

	// Cancel context
	c.cancel()

	// Close send buffer
	close(c.sendBuffer)

	// Close underlying connection
	return c.conn.Close()
}

// IsClosed returns whether the connection is closed
func (c *Connection) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// Ping sends a ping message to test connectivity
func (c *Connection) Ping() error {
	// Create a simple ping message
	pingMsg := &protocol.Message{
		Header: protocol.MessageHeader{
			Magic:       protocol.MagicByte,
			Version:     protocol.ProtocolVersion,
			MessageType: protocol.MessageTypeHeartbeatRequest,
			RequestID:   uint32(time.Now().Unix()),
		},
		Payload: []byte("ping"),
	}

	return c.SendMessage(pingMsg)
}

// SetReadTimeout sets the read timeout for this connection
func (c *Connection) SetReadTimeout(timeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.ReadTimeout = timeout
}

// SetWriteTimeout sets the write timeout for this connection
func (c *Connection) SetWriteTimeout(timeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.WriteTimeout = timeout
}

// Private methods

func (c *Connection) sendMessageDirect(msg *protocol.Message) error {
	// Encode message
	data, err := c.encoder.EncodeMessage(msg.Header.MessageType, msg.Payload, msg.Header.RequestID)
	if err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return fmt.Errorf("failed to encode message: %w", err)
	}

	// Set write timeout
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Write data
	if _, err := c.writer.Write(data); err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return fmt.Errorf("failed to write message: %w", err)
	}

	// Flush immediately for synchronous sends
	if err := c.writer.Flush(); err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return fmt.Errorf("failed to flush message: %w", err)
	}

	// Update statistics
	atomic.AddInt64(&c.stats.MessagesSent, 1)
	atomic.AddInt64(&c.stats.BytesSent, int64(len(data)))
	c.lastWrite = time.Now()
	c.stats.LastActivity = c.lastWrite

	return nil
}

func (c *Connection) processSendBuffer() {
	defer func() {
		// Drain remaining messages
		for range c.sendBuffer {
			// Discard messages
		}
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var batch []*protocol.Message

	for {
		select {
		case <-c.ctx.Done():
			// Flush remaining batch
			if len(batch) > 0 {
				c.flushBatch(batch)
			}
			return

		case msg, ok := <-c.sendBuffer:
			if !ok {
				// Channel closed, flush remaining batch
				if len(batch) > 0 {
					c.flushBatch(batch)
				}
				return
			}

			batch = append(batch, msg)

			// Flush when batch is full
			if len(batch) >= 10 {
				c.flushBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			// Flush batch periodically
			if len(batch) > 0 {
				c.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (c *Connection) flushBatch(batch []*protocol.Message) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return
	}

	// Set write timeout
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var totalBytes int64

	for _, msg := range batch {
		// Encode message
		data, err := c.encoder.EncodeMessage(msg.Header.MessageType, msg.Payload, msg.Header.RequestID)
		if err != nil {
			atomic.AddInt64(&c.stats.Errors, 1)
			continue
		}

		// Write to buffer
		if _, err := c.writer.Write(data); err != nil {
			atomic.AddInt64(&c.stats.Errors, 1)
			continue
		}

		totalBytes += int64(len(data))
		atomic.AddInt64(&c.stats.MessagesSent, 1)
	}

	// Flush all messages at once
	if err := c.writer.Flush(); err != nil {
		atomic.AddInt64(&c.stats.Errors, 1)
		return
	}

	// Update statistics
	atomic.AddInt64(&c.stats.BytesSent, totalBytes)
	c.lastWrite = time.Now()
	c.stats.LastActivity = c.lastWrite
}

// ConnectionPool manages a pool of reusable connections
type ConnectionPool struct {
	pool    chan *Connection
	maxSize int
	mu      sync.RWMutex
	active  map[string]*Connection
	closed  bool
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxSize int) *ConnectionPool {
	return &ConnectionPool{
		pool:    make(chan *Connection, maxSize),
		maxSize: maxSize,
		active:  make(map[string]*Connection),
	}
}

// Get retrieves a connection from the pool or creates a new one
func (cp *ConnectionPool) Get(address string, config *ServerConfig) (*Connection, error) {
	cp.mu.RLock()
	if cp.closed {
		cp.mu.RUnlock()
		return nil, fmt.Errorf("connection pool closed")
	}

	// Check if we have an active connection to this address
	if conn, exists := cp.active[address]; exists && conn.IsHealthy() {
		cp.mu.RUnlock()
		return conn, nil
	}
	cp.mu.RUnlock()

	// Try to get a connection from the pool
	select {
	case conn := <-cp.pool:
		if conn.IsHealthy() {
			cp.mu.Lock()
			cp.active[address] = conn
			cp.mu.Unlock()
			return conn, nil
		}
		// Connection is not healthy, close it
		conn.Close()
	default:
		// Pool is empty, create new connection
	}

	// Create new connection
	netConn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	conn := NewConnection(netConn, config)

	cp.mu.Lock()
	cp.active[address] = conn
	cp.mu.Unlock()

	return conn, nil
}

// Put returns a connection to the pool
func (cp *ConnectionPool) Put(conn *Connection) {
	if conn == nil || conn.IsClosed() || !conn.IsHealthy() {
		if conn != nil {
			conn.Close()
		}
		return
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		conn.Close()
		return
	}

	// Remove from active connections
	for addr, c := range cp.active {
		if c == conn {
			delete(cp.active, addr)
			break
		}
	}

	// Try to put back in pool
	select {
	case cp.pool <- conn:
		// Successfully returned to pool
	default:
		// Pool is full, close the connection
		conn.Close()
	}
}

// Close closes all connections in the pool
func (cp *ConnectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return
	}

	cp.closed = true

	// Close all active connections
	for _, conn := range cp.active {
		conn.Close()
	}

	// Close all pooled connections
	close(cp.pool)
	for conn := range cp.pool {
		conn.Close()
	}
}

// Stats returns pool statistics
func (cp *ConnectionPool) Stats() map[string]interface{} {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return map[string]interface{}{
		"active_connections": len(cp.active),
		"pooled_connections": len(cp.pool),
		"max_size":           cp.maxSize,
		"closed":             cp.closed,
	}
}
