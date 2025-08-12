package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"streaming-engine/pkg/types"
)

// RaftNode represents a Raft consensus node
type RaftNode struct {
	// Configuration
	config *RaftConfig
	nodeID types.NodeID

	// State
	mu          sync.RWMutex
	state       NodeState
	currentTerm int64
	votedFor    *types.NodeID
	leader      types.NodeID

	// Log
	log         []*LogEntry
	commitIndex int64
	lastApplied int64

	// Leader state
	nextIndex  map[types.NodeID]int64
	matchIndex map[types.NodeID]int64

	// Timing
	electionTimer   *time.Timer
	heartbeatTimer  *time.Timer
	electionTimeout time.Duration

	// Configuration
	configuration *Configuration

	// Dependencies
	transport    Transport
	storage      Storage
	stateMachine StateMachine

	// Channels and goroutines
	applyCh      chan *LogEntry
	shutdownCh   chan struct{}
	routineGroup sync.WaitGroup

	// Metrics
	metrics atomic.Value // *RaftMetrics

	// Event listeners
	eventListeners []EventListener
	eventMu        sync.RWMutex

	// Snapshot
	snapshotIndex int64
	snapshotTerm  int64

	// Control
	running  int32
	shutdown int32
}

// NewRaftNode creates a new Raft node
func NewRaftNode(config *RaftConfig, transport Transport, storage Storage, stateMachine StateMachine) (*RaftNode, error) {
	if config == nil {
		config = DefaultRaftConfig()
	}

	node := &RaftNode{
		config:          config,
		nodeID:          config.NodeID,
		state:           StateFollower,
		currentTerm:     0,
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make(map[types.NodeID]int64),
		matchIndex:      make(map[types.NodeID]int64),
		electionTimeout: config.ElectionTimeout,
		transport:       transport,
		storage:         storage,
		stateMachine:    stateMachine,
		applyCh:         make(chan *LogEntry, 1000),
		shutdownCh:      make(chan struct{}),
		configuration:   &Configuration{Servers: make(map[types.NodeID]*ServerInfo)},
		eventListeners:  make([]EventListener, 0),
	}

	// Load persistent state
	if err := node.loadState(); err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	// Initialize metrics
	node.updateMetrics()

	return node, nil
}

// Start starts the Raft node
func (n *RaftNode) Start() error {
	if !atomic.CompareAndSwapInt32(&n.running, 0, 1) {
		return errors.New("node already running")
	}

	// Start apply goroutine
	n.routineGroup.Add(1)
	go n.applyEntries()

	// Start snapshot goroutine
	n.routineGroup.Add(1)
	go n.snapshotRoutine()

	// Reset election timer
	n.resetElectionTimer()

	return nil
}

// Stop stops the Raft node
func (n *RaftNode) Stop() error {
	if !atomic.CompareAndSwapInt32(&n.shutdown, 0, 1) {
		return nil // Already stopped
	}

	// Stop timers
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}

	// Signal shutdown
	close(n.shutdownCh)

	// Wait for goroutines
	n.routineGroup.Wait()

	// Close dependencies
	if err := n.storage.Close(); err != nil {
		return fmt.Errorf("failed to close storage: %w", err)
	}

	if err := n.transport.Close(); err != nil {
		return fmt.Errorf("failed to close transport: %w", err)
	}

	atomic.StoreInt32(&n.running, 0)

	return nil
}

// Apply submits a command to be applied to the state machine
func (n *RaftNode) Apply(command Command, timeout time.Duration) (*ApplyResult, error) {
	if atomic.LoadInt32(&n.shutdown) == 1 {
		return nil, errors.New("node is shut down")
	}

	n.mu.RLock()
	if n.state != StateLeader {
		leader := n.leader
		n.mu.RUnlock()

		if leader == "" {
			return nil, errors.New("no leader available")
		}

		if !n.config.DisableProposalForwarding {
			// Forward to leader (implementation would depend on transport)
			return nil, errors.New("proposal forwarding not implemented")
		}

		return nil, fmt.Errorf("not leader, current leader: %s", leader)
	}

	// Create log entry
	entry := &LogEntry{
		Term:    n.currentTerm,
		Index:   n.getLastLogIndex() + 1,
		Type:    command.Type(),
		Command: command,
	}

	// Encode command data
	data, err := command.Encode()
	if err != nil {
		n.mu.RUnlock()
		return nil, fmt.Errorf("failed to encode command: %w", err)
	}
	entry.Data = data

	// Append to log
	n.log = append(n.log, entry)
	n.mu.RUnlock()

	// Persist log entry
	if err := n.storage.AppendLogEntries([]*LogEntry{entry}); err != nil {
		return nil, fmt.Errorf("failed to persist log entry: %w", err)
	}

	// Send append entries to followers
	n.sendAppendEntries()

	// Wait for commit with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, errors.New("timeout waiting for commit")
		case <-time.After(10 * time.Millisecond):
			n.mu.RLock()
			if n.commitIndex >= entry.Index {
				n.mu.RUnlock()

				// Wait for apply
				for n.lastApplied < entry.Index {
					time.Sleep(time.Millisecond)
				}

				// Get result (this would be stored during apply)
				result := &ApplyResult{
					Index: entry.Index,
					Term:  entry.Term,
				}

				return result, nil
			}
			n.mu.RUnlock()
		}
	}
}

// RequestVote handles vote request RPC
func (n *RaftNode) RequestVote(req *VoteRequest) (*VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := &VoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// If term is outdated, reject
	if req.Term < n.currentTerm {
		return response, nil
	}

	// If term is newer, update our term and convert to follower
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = nil
		n.state = StateFollower
		n.leader = ""
		n.persistState()
	}

	response.Term = n.currentTerm

	// Check if we can vote for this candidate
	canVote := (n.votedFor == nil || *n.votedFor == req.CandidateID)
	if !canVote {
		return response, nil
	}

	// Check if candidate's log is at least as up-to-date as ours
	lastLogIndex := n.getLastLogIndex()
	lastLogTerm := n.getLastLogTerm()

	logOk := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if logOk {
		response.VoteGranted = true
		n.votedFor = &req.CandidateID
		n.resetElectionTimer()
		n.persistState()
	}

	return response, nil
}

// AppendEntries handles append entries RPC
func (n *RaftNode) AppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := &AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// If term is outdated, reject
	if req.Term < n.currentTerm {
		return response, nil
	}

	// If term is newer or equal, update our term and convert to follower
	if req.Term >= n.currentTerm {
		n.currentTerm = req.Term
		n.state = StateFollower
		n.leader = req.LeaderID
		n.votedFor = nil
		n.resetElectionTimer()
		n.persistState()
	}

	response.Term = n.currentTerm

	// Check if we have the previous log entry
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > n.getLastLogIndex() {
			// We don't have the previous entry
			response.ConflictIndex = n.getLastLogIndex() + 1
			return response, nil
		}

		prevEntry := n.getLogEntry(req.PrevLogIndex)
		if prevEntry == nil || prevEntry.Term != req.PrevLogTerm {
			// Previous entry term doesn't match
			if prevEntry != nil {
				response.ConflictTerm = prevEntry.Term
				response.ConflictIndex = n.findFirstIndexOfTerm(prevEntry.Term)
			} else {
				response.ConflictIndex = req.PrevLogIndex
			}
			return response, nil
		}
	}

	// Append new entries
	if len(req.Entries) > 0 {
		// Delete conflicting entries
		insertIndex := req.PrevLogIndex + 1
		if insertIndex <= n.getLastLogIndex() {
			// Check for conflicts
			for i, entry := range req.Entries {
				logIndex := insertIndex + int64(i)
				if logIndex <= n.getLastLogIndex() {
					existingEntry := n.getLogEntry(logIndex)
					if existingEntry.Term != entry.Term {
						// Conflict found, delete from here
						n.log = n.log[:logIndex-n.getFirstLogIndex()]
						n.storage.DeleteLogEntries(logIndex)
						break
					}
				}
			}
		}

		// Append new entries
		for i, entry := range req.Entries {
			logIndex := insertIndex + int64(i)
			if logIndex > n.getLastLogIndex() {
				n.log = append(n.log, entry)
			}
		}

		// Persist new entries
		n.storage.AppendLogEntries(req.Entries)
	}

	// Update commit index
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.getLastLogIndex())
		n.signalApply()
	}

	response.Success = true
	return response, nil
}

// InstallSnapshot handles install snapshot RPC
func (n *RaftNode) InstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := &InstallSnapshotResponse{
		Term: n.currentTerm,
	}

	// If term is outdated, reject
	if req.Term < n.currentTerm {
		return response, nil
	}

	// Update term and convert to follower
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = nil
		n.persistState()
	}

	n.state = StateFollower
	n.leader = req.LeaderID
	n.resetElectionTimer()

	response.Term = n.currentTerm

	// Install snapshot
	snapshot := &Snapshot{
		Index: req.LastIncludedIndex,
		Term:  req.LastIncludedTerm,
		Data:  req.Data,
		Size:  int64(len(req.Data)),
	}

	// Apply snapshot to state machine
	if err := n.stateMachine.Restore(snapshot.Data); err != nil {
		return response, err
	}

	// Update log
	if req.LastIncludedIndex > n.getLastLogIndex() {
		n.log = []*LogEntry{}
	} else {
		// Keep log entries after snapshot
		cutIndex := req.LastIncludedIndex - n.getFirstLogIndex() + 1
		if cutIndex < int64(len(n.log)) {
			n.log = n.log[cutIndex:]
		} else {
			n.log = []*LogEntry{}
		}
	}

	// Update state
	n.snapshotIndex = req.LastIncludedIndex
	n.snapshotTerm = req.LastIncludedTerm
	n.lastApplied = req.LastIncludedIndex
	n.commitIndex = req.LastIncludedIndex

	// Persist snapshot
	n.storage.SetSnapshot(snapshot)
	n.storage.DeleteLogEntries(0) // Clear all log entries

	return response, nil
}

// GetState returns the current state of the node
func (n *RaftNode) GetState() (NodeState, int64, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.state, n.currentTerm, n.state == StateLeader
}

// GetLeader returns the current leader
func (n *RaftNode) GetLeader() types.NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leader
}

// GetMetrics returns current metrics
func (n *RaftNode) GetMetrics() *RaftMetrics {
	if metrics := n.metrics.Load(); metrics != nil {
		return metrics.(*RaftMetrics)
	}
	return &RaftMetrics{}
}

// AddEventListener adds an event listener
func (n *RaftNode) AddEventListener(listener EventListener) {
	n.eventMu.Lock()
	defer n.eventMu.Unlock()

	n.eventListeners = append(n.eventListeners, listener)
}

// RemoveEventListener removes an event listener
func (n *RaftNode) RemoveEventListener(listener EventListener) {
	n.eventMu.Lock()
	defer n.eventMu.Unlock()

	for i, l := range n.eventListeners {
		if l == listener {
			n.eventListeners = append(n.eventListeners[:i], n.eventListeners[i+1:]...)
			break
		}
	}
}

// Private methods

func (n *RaftNode) loadState() error {
	// Load current term
	term, err := n.storage.GetCurrentTerm()
	if err != nil {
		return err
	}
	n.currentTerm = term

	// Load voted for
	votedFor, err := n.storage.GetVotedFor()
	if err != nil {
		return err
	}
	n.votedFor = votedFor

	// Load log entries
	firstIndex, err := n.storage.GetFirstLogIndex()
	if err != nil {
		return err
	}

	lastIndex, err := n.storage.GetLastLogIndex()
	if err != nil {
		return err
	}

	if lastIndex >= firstIndex {
		entries, err := n.storage.GetLogEntries(firstIndex, lastIndex+1)
		if err != nil {
			return err
		}
		n.log = entries
	}

	// Load snapshot
	snapshot, err := n.storage.GetSnapshot()
	if err == nil && snapshot != nil {
		n.snapshotIndex = snapshot.Index
		n.snapshotTerm = snapshot.Term
		n.lastApplied = snapshot.Index
		n.commitIndex = snapshot.Index

		// Restore state machine
		if err := n.stateMachine.Restore(snapshot.Data); err != nil {
			return err
		}
	}

	return nil
}

func (n *RaftNode) persistState() error {
	if err := n.storage.SetCurrentTerm(n.currentTerm); err != nil {
		return err
	}

	if err := n.storage.SetVotedFor(n.votedFor); err != nil {
		return err
	}

	return n.storage.Sync()
}

func (n *RaftNode) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Randomize election timeout
	timeout := n.electionTimeout + time.Duration(rand.Intn(int(n.electionTimeout)))

	n.electionTimer = time.AfterFunc(timeout, func() {
		n.startElection()
	})
}

func (n *RaftNode) resetHeartbeatTimer() {
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}

	n.heartbeatTimer = time.AfterFunc(n.config.HeartbeatInterval, func() {
		n.sendHeartbeat()
	})
}

func (n *RaftNode) startElection() {
	n.mu.Lock()

	if n.state == StateLeader {
		n.mu.Unlock()
		return
	}

	// Convert to candidate
	n.state = StateCandidate
	n.currentTerm++
	n.votedFor = &n.nodeID
	n.leader = ""

	term := n.currentTerm
	lastLogIndex := n.getLastLogIndex()
	lastLogTerm := n.getLastLogTerm()

	n.persistState()
	n.resetElectionTimer()
	n.mu.Unlock()

	// Send vote requests
	votes := 1 // Vote for ourselves
	voters := len(n.configuration.Servers)

	if voters == 1 {
		// Single node cluster
		n.becomeLeader()
		return
	}

	for nodeID, server := range n.configuration.Servers {
		if nodeID == n.nodeID || !server.Voting {
			continue
		}

		go func(target types.NodeID) {
			req := &VoteRequest{
				Term:         term,
				CandidateID:  n.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			resp, err := n.transport.RequestVote(target, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Check if we're still a candidate for this term
			if n.state != StateCandidate || n.currentTerm != term {
				return
			}

			// Handle newer term
			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = StateFollower
				n.votedFor = nil
				n.leader = ""
				n.persistState()
				n.resetElectionTimer()
				return
			}

			// Count vote
			if resp.VoteGranted {
				votes++
				if votes > voters/2 {
					n.becomeLeader()
				}
			}
		}(nodeID)
	}
}

func (n *RaftNode) becomeLeader() {
	if n.state != StateCandidate {
		return
	}

	oldState := n.state
	n.state = StateLeader
	n.leader = n.nodeID

	// Initialize leader state
	lastLogIndex := n.getLastLogIndex()
	for nodeID := range n.configuration.Servers {
		if nodeID != n.nodeID {
			n.nextIndex[nodeID] = lastLogIndex + 1
			n.matchIndex[nodeID] = 0
		}
	}

	// Stop election timer
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Send initial heartbeat
	n.sendHeartbeat()

	// Emit event
	n.emitEvent(&StateChangeEvent{
		OldState: oldState,
		NewState: n.state,
		Term:     n.currentTerm,
	})

	n.emitEvent(&LeadershipChangeEvent{
		NewLeader: n.nodeID,
		Term:      n.currentTerm,
	})
}

func (n *RaftNode) sendHeartbeat() {
	n.mu.RLock()
	if n.state != StateLeader {
		n.mu.RUnlock()
		return
	}
	n.mu.RUnlock()

	n.sendAppendEntries()
	n.resetHeartbeatTimer()
}

func (n *RaftNode) sendAppendEntries() {
	n.mu.RLock()
	if n.state != StateLeader {
		n.mu.RUnlock()
		return
	}

	for nodeID, server := range n.configuration.Servers {
		if nodeID == n.nodeID || !server.Voting {
			continue
		}

		go n.sendAppendEntriesToPeer(nodeID)
	}
	n.mu.RUnlock()
}

func (n *RaftNode) sendAppendEntriesToPeer(target types.NodeID) {
	n.mu.Lock()

	if n.state != StateLeader {
		n.mu.Unlock()
		return
	}

	nextIndex := n.nextIndex[target]
	prevLogIndex := nextIndex - 1
	var prevLogTerm int64

	if prevLogIndex > 0 {
		if prevLogIndex == n.snapshotIndex {
			prevLogTerm = n.snapshotTerm
		} else {
			prevEntry := n.getLogEntry(prevLogIndex)
			if prevEntry == nil {
				// Need to send snapshot
				n.mu.Unlock()
				n.sendSnapshot(target)
				return
			}
			prevLogTerm = prevEntry.Term
		}
	}

	// Get entries to send
	var entries []*LogEntry
	lastLogIndex := n.getLastLogIndex()
	if nextIndex <= lastLogIndex {
		maxEntries := min(n.config.MaxEntriesPerMessage, int(lastLogIndex-nextIndex+1))
		for i := 0; i < maxEntries; i++ {
			entry := n.getLogEntry(nextIndex + int64(i))
			if entry != nil {
				entries = append(entries, entry)
			}
		}
	}

	req := &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	n.mu.Unlock()

	resp, err := n.transport.AppendEntries(target, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we're still leader
	if n.state != StateLeader {
		return
	}

	// Handle newer term
	if resp.Term > n.currentTerm {
		n.currentTerm = resp.Term
		n.state = StateFollower
		n.votedFor = nil
		n.leader = ""
		n.persistState()
		n.resetElectionTimer()
		return
	}

	if resp.Success {
		// Update next and match index
		n.nextIndex[target] = prevLogIndex + int64(len(entries)) + 1
		n.matchIndex[target] = prevLogIndex + int64(len(entries))

		// Update commit index
		n.updateCommitIndex()
	} else {
		// Decrement nextIndex and retry
		if resp.ConflictIndex > 0 {
			n.nextIndex[target] = resp.ConflictIndex
		} else {
			n.nextIndex[target] = max(1, n.nextIndex[target]-1)
		}
	}
}

func (n *RaftNode) sendSnapshot(target types.NodeID) {
	// Implementation for sending snapshots
	// This would be more complex in a real implementation
}

func (n *RaftNode) updateCommitIndex() {
	// Find the highest index replicated on majority
	indices := make([]int64, 0, len(n.matchIndex)+1)
	indices = append(indices, n.getLastLogIndex()) // Leader's index

	for _, index := range n.matchIndex {
		indices = append(indices, index)
	}

	// Sort indices
	for i := 0; i < len(indices)-1; i++ {
		for j := i + 1; j < len(indices); j++ {
			if indices[i] > indices[j] {
				indices[i], indices[j] = indices[j], indices[i]
			}
		}
	}

	// Find majority index
	majorityIndex := indices[len(indices)/2]

	// Only commit entries from current term
	if majorityIndex > n.commitIndex && n.getLogEntry(majorityIndex).Term == n.currentTerm {
		n.commitIndex = majorityIndex
		n.signalApply()
	}
}

func (n *RaftNode) applyEntries() {
	defer n.routineGroup.Done()

	for {
		select {
		case <-n.shutdownCh:
			return
		case <-n.applyCh:
			n.processApply()
		case <-time.After(n.config.ApplyTimeout):
			n.processApply()
		}
	}
}

func (n *RaftNode) processApply() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.getLogEntry(n.lastApplied)
		if entry == nil {
			continue
		}

		// Apply command to state machine
		if entry.Command != nil {
			result, err := entry.Command.Apply(n.stateMachine)

			// Emit event
			n.emitEvent(&CommandAppliedEvent{
				Command: entry.Command,
				Index:   entry.Index,
				Term:    entry.Term,
				Result:  result,
			})

			_ = err // Handle error as needed
		}
	}
}

func (n *RaftNode) signalApply() {
	select {
	case n.applyCh <- nil:
	default:
	}
}

func (n *RaftNode) snapshotRoutine() {
	defer n.routineGroup.Done()

	ticker := time.NewTicker(n.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.maybeSnapshot()
		}
	}
}

func (n *RaftNode) maybeSnapshot() {
	n.mu.Lock()

	logSize := len(n.log)
	if logSize < n.config.SnapshotThreshold {
		n.mu.Unlock()
		return
	}

	snapshotIndex := n.lastApplied
	snapshotTerm := n.getLogEntry(snapshotIndex).Term

	n.mu.Unlock()

	// Create snapshot
	data, err := n.stateMachine.Snapshot()
	if err != nil {
		return
	}

	snapshot := &Snapshot{
		Index: snapshotIndex,
		Term:  snapshotTerm,
		Data:  data,
		Size:  int64(len(data)),
	}

	// Persist snapshot
	if err := n.storage.SetSnapshot(snapshot); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Trim log
	cutIndex := snapshotIndex - n.getFirstLogIndex()
	if cutIndex > 0 && cutIndex < int64(len(n.log)) {
		n.log = n.log[cutIndex:]
		n.storage.DeleteLogEntries(snapshotIndex)
	}

	n.snapshotIndex = snapshotIndex
	n.snapshotTerm = snapshotTerm

	// Emit event
	n.emitEvent(&SnapshotCreatedEvent{
		Index: snapshotIndex,
		Term:  snapshotTerm,
		Size:  snapshot.Size,
	})
}

func (n *RaftNode) getLogEntry(index int64) *LogEntry {
	if index <= 0 {
		return nil
	}

	if index == n.snapshotIndex {
		return &LogEntry{
			Term:  n.snapshotTerm,
			Index: n.snapshotIndex,
		}
	}

	firstIndex := n.getFirstLogIndex()
	if index < firstIndex || index > n.getLastLogIndex() {
		return nil
	}

	return n.log[index-firstIndex]
}

func (n *RaftNode) getFirstLogIndex() int64 {
	if len(n.log) == 0 {
		return n.snapshotIndex + 1
	}
	return n.log[0].Index
}

func (n *RaftNode) getLastLogIndex() int64 {
	if len(n.log) == 0 {
		return n.snapshotIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) getLastLogTerm() int64 {
	if len(n.log) == 0 {
		return n.snapshotTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *RaftNode) findFirstIndexOfTerm(term int64) int64 {
	for _, entry := range n.log {
		if entry.Term == term {
			return entry.Index
		}
	}
	return 0
}

func (n *RaftNode) updateMetrics() {
	metrics := &RaftMetrics{
		State:           n.state,
		Term:            n.currentTerm,
		Leader:          n.leader,
		CommitIndex:     n.commitIndex,
		LastApplied:     n.lastApplied,
		LastLogIndex:    n.getLastLogIndex(),
		LastLogTerm:     n.getLastLogTerm(),
		ElectionTimeout: n.electionTimeout,
		LastHeartbeat:   time.Now(),
		SnapshotIndex:   n.snapshotIndex,
		SnapshotTerm:    n.snapshotTerm,
	}

	n.metrics.Store(metrics)
}

func (n *RaftNode) emitEvent(event Event) {
	n.eventMu.RLock()
	listeners := make([]EventListener, len(n.eventListeners))
	copy(listeners, n.eventListeners)
	n.eventMu.RUnlock()

	for _, listener := range listeners {
		go listener.OnEvent(event)
	}
}

// Utility functions
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
