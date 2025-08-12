package raft

import (
	"streaming-engine/pkg/types"
	"time"
)

// NodeState represents the state of a Raft node
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

// String returns string representation of NodeState
func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int64       `json:"term"`
	Index   int64       `json:"index"`
	Type    CommandType `json:"type"`
	Command Command     `json:"command"`
	Data    []byte      `json:"data"`
}

// CommandType represents different types of commands
type CommandType int

const (
	CommandTypeNoop CommandType = iota
	CommandTypeSetKey
	CommandTypeDeleteKey
	CommandTypeCreateTopic
	CommandTypeDeleteTopic
	CommandTypeCreatePartition
	CommandTypeDeletePartition
	CommandTypeJoinCluster
	CommandTypeLeaveCluster
	CommandTypeConfig
)

// Command represents a command to be applied to the state machine
type Command interface {
	Type() CommandType
	Encode() ([]byte, error)
	Decode([]byte) error
	Apply(stateMachine StateMachine) (interface{}, error)
}

// StateMachine represents the application state machine
type StateMachine interface {
	Apply(command Command) (interface{}, error)
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
	GetState() interface{}
}

// VoteRequest represents a request vote RPC
type VoteRequest struct {
	Term         int64        `json:"term"`
	CandidateID  types.NodeID `json:"candidate_id"`
	LastLogIndex int64        `json:"last_log_index"`
	LastLogTerm  int64        `json:"last_log_term"`
}

// VoteResponse represents a response to vote request
type VoteResponse struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

// AppendEntriesRequest represents an append entries RPC
type AppendEntriesRequest struct {
	Term         int64        `json:"term"`
	LeaderID     types.NodeID `json:"leader_id"`
	PrevLogIndex int64        `json:"prev_log_index"`
	PrevLogTerm  int64        `json:"prev_log_term"`
	Entries      []*LogEntry  `json:"entries"`
	LeaderCommit int64        `json:"leader_commit"`
}

// AppendEntriesResponse represents a response to append entries
type AppendEntriesResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
	// Optimization: include conflict info for faster log repair
	ConflictIndex int64 `json:"conflict_index,omitempty"`
	ConflictTerm  int64 `json:"conflict_term,omitempty"`
}

// InstallSnapshotRequest represents an install snapshot RPC
type InstallSnapshotRequest struct {
	Term              int64        `json:"term"`
	LeaderID          types.NodeID `json:"leader_id"`
	LastIncludedIndex int64        `json:"last_included_index"`
	LastIncludedTerm  int64        `json:"last_included_term"`
	Offset            int64        `json:"offset"`
	Data              []byte       `json:"data"`
	Done              bool         `json:"done"`
}

// InstallSnapshotResponse represents a response to install snapshot
type InstallSnapshotResponse struct {
	Term int64 `json:"term"`
}

// PersistentState represents state that must be persisted
type PersistentState struct {
	CurrentTerm int64         `json:"current_term"`
	VotedFor    *types.NodeID `json:"voted_for"`
	Log         []*LogEntry   `json:"log"`
}

// VolatileState represents volatile state
type VolatileState struct {
	CommitIndex int64 `json:"commit_index"`
	LastApplied int64 `json:"last_applied"`
}

// LeaderState represents leader-specific volatile state
type LeaderState struct {
	NextIndex  map[types.NodeID]int64 `json:"next_index"`
	MatchIndex map[types.NodeID]int64 `json:"match_index"`
}

// Configuration represents cluster configuration
type Configuration struct {
	Servers map[types.NodeID]*ServerInfo `json:"servers"`
	Version int64                        `json:"version"`
}

// ServerInfo represents information about a server
type ServerInfo struct {
	NodeID   types.NodeID      `json:"node_id"`
	Address  string            `json:"address"`
	Voting   bool              `json:"voting"`
	Metadata map[string]string `json:"metadata"`
}

// Snapshot represents a state machine snapshot
type Snapshot struct {
	Index int64  `json:"index"`
	Term  int64  `json:"term"`
	Data  []byte `json:"data"`
	Size  int64  `json:"size"`
}

// RaftConfig represents Raft configuration
type RaftConfig struct {
	NodeID                    types.NodeID  `json:"node_id"`
	ElectionTimeout           time.Duration `json:"election_timeout"`
	HeartbeatInterval         time.Duration `json:"heartbeat_interval"`
	ApplyTimeout              time.Duration `json:"apply_timeout"`
	MaxEntriesPerMessage      int           `json:"max_entries_per_message"`
	SnapshotThreshold         int           `json:"snapshot_threshold"`
	SnapshotInterval          time.Duration `json:"snapshot_interval"`
	LogCacheSize              int           `json:"log_cache_size"`
	PreVote                   bool          `json:"pre_vote"`
	DisableProposalForwarding bool          `json:"disable_proposal_forwarding"`
}

// DefaultRaftConfig returns a default Raft configuration
func DefaultRaftConfig() *RaftConfig {
	return &RaftConfig{
		ElectionTimeout:           150 * time.Millisecond,
		HeartbeatInterval:         50 * time.Millisecond,
		ApplyTimeout:              100 * time.Millisecond,
		MaxEntriesPerMessage:      100,
		SnapshotThreshold:         1000,
		SnapshotInterval:          5 * time.Minute,
		LogCacheSize:              1000,
		PreVote:                   true,
		DisableProposalForwarding: false,
	}
}

// RaftMetrics represents Raft metrics
type RaftMetrics struct {
	State           NodeState     `json:"state"`
	Term            int64         `json:"term"`
	Leader          types.NodeID  `json:"leader"`
	CommitIndex     int64         `json:"commit_index"`
	LastApplied     int64         `json:"last_applied"`
	LastLogIndex    int64         `json:"last_log_index"`
	LastLogTerm     int64         `json:"last_log_term"`
	ElectionTimeout time.Duration `json:"election_timeout"`
	LastHeartbeat   time.Time     `json:"last_heartbeat"`
	AppliedCommands int64         `json:"applied_commands"`
	FailedAppends   int64         `json:"failed_appends"`
	SnapshotIndex   int64         `json:"snapshot_index"`
	SnapshotTerm    int64         `json:"snapshot_term"`
}

// ApplyResult represents the result of applying a command
type ApplyResult struct {
	Response interface{} `json:"response"`
	Error    error       `json:"error"`
	Index    int64       `json:"index"`
	Term     int64       `json:"term"`
}

// Transport represents the network transport for Raft
type Transport interface {
	// RPC methods
	RequestVote(target types.NodeID, req *VoteRequest) (*VoteResponse, error)
	AppendEntries(target types.NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(target types.NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)

	// Connection management
	SetPeers(peers map[types.NodeID]string)
	Close() error
}

// Storage represents persistent storage for Raft
type Storage interface {
	// Persistent state
	GetCurrentTerm() (int64, error)
	SetCurrentTerm(term int64) error
	GetVotedFor() (*types.NodeID, error)
	SetVotedFor(nodeID *types.NodeID) error

	// Log operations
	GetLogEntry(index int64) (*LogEntry, error)
	GetLogEntries(start, end int64) ([]*LogEntry, error)
	AppendLogEntries(entries []*LogEntry) error
	DeleteLogEntries(start int64) error
	GetLastLogIndex() (int64, error)
	GetLastLogTerm() (int64, error)
	GetFirstLogIndex() (int64, error)

	// Snapshot operations
	GetSnapshot() (*Snapshot, error)
	SetSnapshot(snapshot *Snapshot) error

	// Persistence
	Sync() error
	Close() error
}

// Event represents events that can be emitted by Raft
type Event interface {
	Type() string
	Data() interface{}
}

// EventListener represents a listener for Raft events
type EventListener interface {
	OnEvent(event Event)
}

// LeadershipChangeEvent represents a leadership change event
type LeadershipChangeEvent struct {
	OldLeader types.NodeID `json:"old_leader"`
	NewLeader types.NodeID `json:"new_leader"`
	Term      int64        `json:"term"`
}

func (e *LeadershipChangeEvent) Type() string {
	return "leadership_change"
}

func (e *LeadershipChangeEvent) Data() interface{} {
	return e
}

// StateChangeEvent represents a state change event
type StateChangeEvent struct {
	OldState NodeState `json:"old_state"`
	NewState NodeState `json:"new_state"`
	Term     int64     `json:"term"`
}

func (e *StateChangeEvent) Type() string {
	return "state_change"
}

func (e *StateChangeEvent) Data() interface{} {
	return e
}

// CommandAppliedEvent represents a command applied event
type CommandAppliedEvent struct {
	Command Command     `json:"command"`
	Index   int64       `json:"index"`
	Term    int64       `json:"term"`
	Result  interface{} `json:"result"`
}

func (e *CommandAppliedEvent) Type() string {
	return "command_applied"
}

func (e *CommandAppliedEvent) Data() interface{} {
	return e
}

// SnapshotCreatedEvent represents a snapshot created event
type SnapshotCreatedEvent struct {
	Index int64 `json:"index"`
	Term  int64 `json:"term"`
	Size  int64 `json:"size"`
}

func (e *SnapshotCreatedEvent) Type() string {
	return "snapshot_created"
}

func (e *SnapshotCreatedEvent) Data() interface{} {
	return e
}

// ConfigurationChangeEvent represents a configuration change event
type ConfigurationChangeEvent struct {
	OldConfiguration *Configuration `json:"old_configuration"`
	NewConfiguration *Configuration `json:"new_configuration"`
	Index            int64          `json:"index"`
	Term             int64          `json:"term"`
}

func (e *ConfigurationChangeEvent) Type() string {
	return "configuration_change"
}

func (e *ConfigurationChangeEvent) Data() interface{} {
	return e
}
