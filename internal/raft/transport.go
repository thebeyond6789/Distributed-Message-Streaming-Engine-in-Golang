package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"streaming-engine/pkg/types"
)

// TCPTransport implements Transport interface using TCP
type TCPTransport struct {
	nodeID   types.NodeID
	listener net.Listener
	peers    map[types.NodeID]string
	conns    map[types.NodeID]net.Conn
	mu       sync.RWMutex

	// Request handlers
	node *RaftNode

	// Configuration
	timeout time.Duration
	retries int

	// Control
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

// NewTCPTransport creates a new TCP transport
func NewTCPTransport(nodeID types.NodeID, addr string) (*TCPTransport, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	transport := &TCPTransport{
		nodeID:     nodeID,
		listener:   listener,
		peers:      make(map[types.NodeID]string),
		conns:      make(map[types.NodeID]net.Conn),
		timeout:    5 * time.Second,
		retries:    3,
		shutdownCh: make(chan struct{}),
	}

	// Start accepting connections
	transport.wg.Add(1)
	go transport.acceptConnections()

	return transport, nil
}

// SetNode sets the Raft node for handling requests
func (t *TCPTransport) SetNode(node *RaftNode) {
	t.node = node
}

// SetPeers sets the peer addresses
func (t *TCPTransport) SetPeers(peers map[types.NodeID]string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Close old connections
	for nodeID, conn := range t.conns {
		if _, exists := peers[nodeID]; !exists {
			conn.Close()
			delete(t.conns, nodeID)
		}
	}

	t.peers = make(map[types.NodeID]string)
	for nodeID, addr := range peers {
		t.peers[nodeID] = addr
	}
}

// RequestVote sends a vote request to a peer
func (t *TCPTransport) RequestVote(target types.NodeID, req *VoteRequest) (*VoteResponse, error) {
	return t.sendRPC(target, "RequestVote", req, &VoteResponse{})
}

// AppendEntries sends an append entries request to a peer
func (t *TCPTransport) AppendEntries(target types.NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return t.sendRPC(target, "AppendEntries", req, &AppendEntriesResponse{})
}

// InstallSnapshot sends an install snapshot request to a peer
func (t *TCPTransport) InstallSnapshot(target types.NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return t.sendRPC(target, "InstallSnapshot", req, &InstallSnapshotResponse{})
}

// Close closes the transport
func (t *TCPTransport) Close() error {
	close(t.shutdownCh)

	if t.listener != nil {
		t.listener.Close()
	}

	t.mu.Lock()
	for _, conn := range t.conns {
		conn.Close()
	}
	t.mu.Unlock()

	t.wg.Wait()
	return nil
}

// Private methods

func (t *TCPTransport) sendRPC(target types.NodeID, method string, req interface{}, resp interface{}) (interface{}, error) {
	conn, err := t.getConnection(target)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection to %s: %w", target, err)
	}

	rpcReq := &RPCRequest{
		Method: method,
		Params: req,
	}

	// Send request
	if err := t.sendRequest(conn, rpcReq); err != nil {
		// Close bad connection
		t.mu.Lock()
		delete(t.conns, target)
		t.mu.Unlock()
		conn.Close()
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Receive response
	rpcResp, err := t.receiveResponse(conn)
	if err != nil {
		// Close bad connection
		t.mu.Lock()
		delete(t.conns, target)
		t.mu.Unlock()
		conn.Close()
		return nil, fmt.Errorf("failed to receive response: %w", err)
	}

	if rpcResp.Error != "" {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error)
	}

	// Decode response
	responseData, err := json.Marshal(rpcResp.Result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	if err := json.Unmarshal(responseData, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return resp, nil
}

func (t *TCPTransport) getConnection(target types.NodeID) (net.Conn, error) {
	t.mu.RLock()
	if conn, exists := t.conns[target]; exists {
		t.mu.RUnlock()
		return conn, nil
	}

	addr, exists := t.peers[target]
	if !exists {
		t.mu.RUnlock()
		return nil, fmt.Errorf("no address for peer %s", target)
	}
	t.mu.RUnlock()

	// Create new connection
	conn, err := net.DialTimeout("tcp", addr, t.timeout)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.conns[target] = conn
	t.mu.Unlock()

	return conn, nil
}

func (t *TCPTransport) sendRequest(conn net.Conn, req *RPCRequest) error {
	conn.SetWriteDeadline(time.Now().Add(t.timeout))

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	// Write length prefix
	length := len(data)
	if err := writeInt32(conn, int32(length)); err != nil {
		return err
	}

	// Write data
	_, err = conn.Write(data)
	return err
}

func (t *TCPTransport) receiveResponse(conn net.Conn) (*RPCResponse, error) {
	conn.SetReadDeadline(time.Now().Add(t.timeout))

	// Read length prefix
	length, err := readInt32(conn)
	if err != nil {
		return nil, err
	}

	// Read data
	data := make([]byte, length)
	if _, err := conn.Read(data); err != nil {
		return nil, err
	}

	var resp RPCResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (t *TCPTransport) acceptConnections() {
	defer t.wg.Done()

	for {
		select {
		case <-t.shutdownCh:
			return
		default:
		}

		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.shutdownCh:
				return
			default:
				continue
			}
		}

		t.wg.Add(1)
		go t.handleConnection(conn)
	}
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()

	for {
		select {
		case <-t.shutdownCh:
			return
		default:
		}

		// Read request
		req, err := t.receiveRequest(conn)
		if err != nil {
			return
		}

		// Handle request
		resp := t.handleRequest(req)

		// Send response
		if err := t.sendResponse(conn, resp); err != nil {
			return
		}
	}
}

func (t *TCPTransport) receiveRequest(conn net.Conn) (*RPCRequest, error) {
	conn.SetReadDeadline(time.Now().Add(t.timeout))

	// Read length prefix
	length, err := readInt32(conn)
	if err != nil {
		return nil, err
	}

	// Read data
	data := make([]byte, length)
	if _, err := conn.Read(data); err != nil {
		return nil, err
	}

	var req RPCRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}

	return &req, nil
}

func (t *TCPTransport) sendResponse(conn net.Conn, resp *RPCResponse) error {
	conn.SetWriteDeadline(time.Now().Add(t.timeout))

	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	// Write length prefix
	length := len(data)
	if err := writeInt32(conn, int32(length)); err != nil {
		return err
	}

	// Write data
	_, err = conn.Write(data)
	return err
}

func (t *TCPTransport) handleRequest(req *RPCRequest) *RPCResponse {
	if t.node == nil {
		return &RPCResponse{
			Error: "node not set",
		}
	}

	switch req.Method {
	case "RequestVote":
		return t.handleRequestVote(req)
	case "AppendEntries":
		return t.handleAppendEntries(req)
	case "InstallSnapshot":
		return t.handleInstallSnapshot(req)
	default:
		return &RPCResponse{
			Error: fmt.Sprintf("unknown method: %s", req.Method),
		}
	}
}

func (t *TCPTransport) handleRequestVote(req *RPCRequest) *RPCResponse {
	var voteReq VoteRequest
	if err := t.decodeParams(req.Params, &voteReq); err != nil {
		return &RPCResponse{
			Error: fmt.Sprintf("failed to decode request: %v", err),
		}
	}

	resp, err := t.node.RequestVote(&voteReq)
	if err != nil {
		return &RPCResponse{
			Error: err.Error(),
		}
	}

	return &RPCResponse{
		Result: resp,
	}
}

func (t *TCPTransport) handleAppendEntries(req *RPCRequest) *RPCResponse {
	var appendReq AppendEntriesRequest
	if err := t.decodeParams(req.Params, &appendReq); err != nil {
		return &RPCResponse{
			Error: fmt.Sprintf("failed to decode request: %v", err),
		}
	}

	resp, err := t.node.AppendEntries(&appendReq)
	if err != nil {
		return &RPCResponse{
			Error: err.Error(),
		}
	}

	return &RPCResponse{
		Result: resp,
	}
}

func (t *TCPTransport) handleInstallSnapshot(req *RPCRequest) *RPCResponse {
	var snapshotReq InstallSnapshotRequest
	if err := t.decodeParams(req.Params, &snapshotReq); err != nil {
		return &RPCResponse{
			Error: fmt.Sprintf("failed to decode request: %v", err),
		}
	}

	resp, err := t.node.InstallSnapshot(&snapshotReq)
	if err != nil {
		return &RPCResponse{
			Error: err.Error(),
		}
	}

	return &RPCResponse{
		Result: resp,
	}
}

func (t *TCPTransport) decodeParams(params interface{}, target interface{}) error {
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, target)
}

// RPC message types
type RPCRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
}

type RPCResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// Utility functions for reading/writing integers
func writeInt32(conn net.Conn, value int32) error {
	bytes := make([]byte, 4)
	bytes[0] = byte(value >> 24)
	bytes[1] = byte(value >> 16)
	bytes[2] = byte(value >> 8)
	bytes[3] = byte(value)

	_, err := conn.Write(bytes)
	return err
}

func readInt32(conn net.Conn) (int32, error) {
	bytes := make([]byte, 4)
	if _, err := conn.Read(bytes); err != nil {
		return 0, err
	}

	value := int32(bytes[0])<<24 | int32(bytes[1])<<16 | int32(bytes[2])<<8 | int32(bytes[3])
	return value, nil
}

// MockTransport implements Transport interface for testing
type MockTransport struct {
	nodeID types.NodeID
	peers  map[types.NodeID]*MockTransport
	node   *RaftNode

	// Message queues
	voteRequests     chan *VoteRequest
	appendRequests   chan *AppendEntriesRequest
	snapshotRequests chan *InstallSnapshotRequest

	// Response handlers
	voteResponses     map[string]*VoteResponse
	appendResponses   map[string]*AppendEntriesResponse
	snapshotResponses map[string]*InstallSnapshotResponse

	mu sync.RWMutex
}

// NewMockTransport creates a new mock transport for testing
func NewMockTransport(nodeID types.NodeID) *MockTransport {
	return &MockTransport{
		nodeID:            nodeID,
		peers:             make(map[types.NodeID]*MockTransport),
		voteRequests:      make(chan *VoteRequest, 100),
		appendRequests:    make(chan *AppendEntriesRequest, 100),
		snapshotRequests:  make(chan *InstallSnapshotRequest, 100),
		voteResponses:     make(map[string]*VoteResponse),
		appendResponses:   make(map[string]*AppendEntriesResponse),
		snapshotResponses: make(map[string]*InstallSnapshotResponse),
	}
}

// ConnectPeer connects this transport to a peer
func (mt *MockTransport) ConnectPeer(peer *MockTransport) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.peers[peer.nodeID] = peer
}

// SetNode sets the Raft node
func (mt *MockTransport) SetNode(node *RaftNode) {
	mt.node = node
}

// SetPeers sets the peer addresses (no-op for mock)
func (mt *MockTransport) SetPeers(peers map[types.NodeID]string) {
	// No-op for mock transport
}

// RequestVote sends a vote request
func (mt *MockTransport) RequestVote(target types.NodeID, req *VoteRequest) (*VoteResponse, error) {
	mt.mu.RLock()
	peer, exists := mt.peers[target]
	mt.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("peer not found: %s", target)
	}

	// Send request to peer
	select {
	case peer.voteRequests <- req:
	default:
		return nil, fmt.Errorf("peer %s queue full", target)
	}

	// Process request if peer has a node
	if peer.node != nil {
		resp, err := peer.node.RequestVote(req)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}

	return &VoteResponse{VoteGranted: false}, nil
}

// AppendEntries sends an append entries request
func (mt *MockTransport) AppendEntries(target types.NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	mt.mu.RLock()
	peer, exists := mt.peers[target]
	mt.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("peer not found: %s", target)
	}

	// Send request to peer
	select {
	case peer.appendRequests <- req:
	default:
		return nil, fmt.Errorf("peer %s queue full", target)
	}

	// Process request if peer has a node
	if peer.node != nil {
		resp, err := peer.node.AppendEntries(req)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}

	return &AppendEntriesResponse{Success: false}, nil
}

// InstallSnapshot sends an install snapshot request
func (mt *MockTransport) InstallSnapshot(target types.NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	mt.mu.RLock()
	peer, exists := mt.peers[target]
	mt.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("peer not found: %s", target)
	}

	// Send request to peer
	select {
	case peer.snapshotRequests <- req:
	default:
		return nil, fmt.Errorf("peer %s queue full", target)
	}

	// Process request if peer has a node
	if peer.node != nil {
		resp, err := peer.node.InstallSnapshot(req)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}

	return &InstallSnapshotResponse{}, nil
}

// Close closes the transport
func (mt *MockTransport) Close() error {
	return nil
}
