package replication

import (
	"fmt"
	"time"

	"streaming-engine/pkg/types"
)

// ConflictResolver handles conflict resolution between replicas
type ConflictResolver struct {
	strategy ConflictResolutionStrategy
	metrics  *ConflictMetrics
}

// ConflictMetrics tracks conflict resolution statistics
type ConflictMetrics struct {
	TotalConflicts      int64     `json:"total_conflicts"`
	ResolvedConflicts   int64     `json:"resolved_conflicts"`
	UnresolvedConflicts int64     `json:"unresolved_conflicts"`
	LastConflictTime    time.Time `json:"last_conflict_time"`
}

// Conflict represents a conflict between message versions
type Conflict struct {
	Topic         string                 `json:"topic"`
	Partition     int32                  `json:"partition"`
	Offset        int64                  `json:"offset"`
	LocalMessage  *types.Message         `json:"local_message"`
	RemoteMessage *types.Message         `json:"remote_message"`
	ConflictType  ConflictType           `json:"conflict_type"`
	DetectedAt    time.Time              `json:"detected_at"`
	Context       map[string]interface{} `json:"context"`
}

// ConflictType represents different types of conflicts
type ConflictType string

const (
	ConflictTypeValueDifference  ConflictType = "value_difference"
	ConflictTypeTimestamp        ConflictType = "timestamp"
	ConflictTypeKeyMismatch      ConflictType = "key_mismatch"
	ConflictTypeHeaderDifference ConflictType = "header_difference"
	ConflictTypeOrderingIssue    ConflictType = "ordering_issue"
)

// Resolution represents the result of conflict resolution
type Resolution struct {
	ResolvedMessage *types.Message             `json:"resolved_message"`
	Strategy        ConflictResolutionStrategy `json:"strategy"`
	Reason          string                     `json:"reason"`
	Confidence      float64                    `json:"confidence"`
	ResolvedAt      time.Time                  `json:"resolved_at"`
	Metadata        map[string]interface{}     `json:"metadata"`
}

// NewConflictResolver creates a new conflict resolver
func NewConflictResolver(strategy ConflictResolutionStrategy) *ConflictResolver {
	return &ConflictResolver{
		strategy: strategy,
		metrics:  &ConflictMetrics{},
	}
}

// DetectConflict detects conflicts between local and remote messages
func (cr *ConflictResolver) DetectConflict(topic string, partition int32, offset int64, local, remote *types.Message) *Conflict {
	if local == nil || remote == nil {
		return nil
	}

	// Check for various types of conflicts
	var conflictType ConflictType
	var detected bool

	// Value difference
	if !bytesEqual(local.Value, remote.Value) {
		conflictType = ConflictTypeValueDifference
		detected = true
	} else if !bytesEqual(local.Key, remote.Key) {
		// Key mismatch
		conflictType = ConflictTypeKeyMismatch
		detected = true
	} else if !local.Timestamp.Equal(remote.Timestamp) {
		// Timestamp difference
		conflictType = ConflictTypeTimestamp
		detected = true
	} else if !headersEqual(local.Headers, remote.Headers) {
		// Header difference
		conflictType = ConflictTypeHeaderDifference
		detected = true
	}

	if !detected {
		return nil
	}

	conflict := &Conflict{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		LocalMessage:  local,
		RemoteMessage: remote,
		ConflictType:  conflictType,
		DetectedAt:    time.Now(),
		Context:       make(map[string]interface{}),
	}

	cr.metrics.TotalConflicts++
	cr.metrics.LastConflictTime = time.Now()

	return conflict
}

// ResolveConflict resolves a conflict using the configured strategy
func (cr *ConflictResolver) ResolveConflict(conflict *Conflict) (*Resolution, error) {
	if conflict == nil {
		return nil, fmt.Errorf("no conflict to resolve")
	}

	resolution, err := cr.applyStrategy(conflict)
	if err != nil {
		cr.metrics.UnresolvedConflicts++
		return nil, fmt.Errorf("failed to resolve conflict: %w", err)
	}

	cr.metrics.ResolvedConflicts++
	return resolution, nil
}

// GetMetrics returns conflict resolution metrics
func (cr *ConflictResolver) GetMetrics() *ConflictMetrics {
	return &ConflictMetrics{
		TotalConflicts:      cr.metrics.TotalConflicts,
		ResolvedConflicts:   cr.metrics.ResolvedConflicts,
		UnresolvedConflicts: cr.metrics.UnresolvedConflicts,
		LastConflictTime:    cr.metrics.LastConflictTime,
	}
}

// SetStrategy updates the conflict resolution strategy
func (cr *ConflictResolver) SetStrategy(strategy ConflictResolutionStrategy) {
	cr.strategy = strategy
}

// Private methods

func (cr *ConflictResolver) applyStrategy(conflict *Conflict) (*Resolution, error) {
	switch cr.strategy {
	case ConflictResolutionLastWriteWins:
		return cr.lastWriteWins(conflict)
	case ConflictResolutionFirstWriteWins:
		return cr.firstWriteWins(conflict)
	case ConflictResolutionTimestamp:
		return cr.timestampBased(conflict)
	case ConflictResolutionCustom:
		return cr.customResolution(conflict)
	default:
		return nil, fmt.Errorf("unknown conflict resolution strategy: %s", cr.strategy)
	}
}

func (cr *ConflictResolver) lastWriteWins(conflict *Conflict) (*Resolution, error) {
	local := conflict.LocalMessage
	remote := conflict.RemoteMessage

	var winner *types.Message
	var reason string

	// Compare timestamps
	if remote.Timestamp.After(local.Timestamp) {
		winner = remote
		reason = "remote message has later timestamp"
	} else if local.Timestamp.After(remote.Timestamp) {
		winner = local
		reason = "local message has later timestamp"
	} else {
		// Same timestamp, use offset as tiebreaker
		if remote.Offset > local.Offset {
			winner = remote
			reason = "remote message has higher offset"
		} else {
			winner = local
			reason = "local message has higher offset"
		}
	}

	return &Resolution{
		ResolvedMessage: winner,
		Strategy:        ConflictResolutionLastWriteWins,
		Reason:          reason,
		Confidence:      0.8,
		ResolvedAt:      time.Now(),
		Metadata: map[string]interface{}{
			"local_timestamp":  local.Timestamp,
			"remote_timestamp": remote.Timestamp,
			"local_offset":     local.Offset,
			"remote_offset":    remote.Offset,
		},
	}, nil
}

func (cr *ConflictResolver) firstWriteWins(conflict *Conflict) (*Resolution, error) {
	local := conflict.LocalMessage
	remote := conflict.RemoteMessage

	var winner *types.Message
	var reason string

	// Compare timestamps (opposite of last write wins)
	if local.Timestamp.Before(remote.Timestamp) {
		winner = local
		reason = "local message has earlier timestamp"
	} else if remote.Timestamp.Before(local.Timestamp) {
		winner = remote
		reason = "remote message has earlier timestamp"
	} else {
		// Same timestamp, use offset as tiebreaker
		if local.Offset < remote.Offset {
			winner = local
			reason = "local message has lower offset"
		} else {
			winner = remote
			reason = "remote message has lower offset"
		}
	}

	return &Resolution{
		ResolvedMessage: winner,
		Strategy:        ConflictResolutionFirstWriteWins,
		Reason:          reason,
		Confidence:      0.8,
		ResolvedAt:      time.Now(),
		Metadata: map[string]interface{}{
			"local_timestamp":  local.Timestamp,
			"remote_timestamp": remote.Timestamp,
			"local_offset":     local.Offset,
			"remote_offset":    remote.Offset,
		},
	}, nil
}

func (cr *ConflictResolver) timestampBased(conflict *Conflict) (*Resolution, error) {
	local := conflict.LocalMessage
	remote := conflict.RemoteMessage

	// For timestamp-based resolution, we need to look at message creation time
	// This could be stored in headers or derived from message content

	localCreationTime := extractCreationTime(local)
	remoteCreationTime := extractCreationTime(remote)

	var winner *types.Message
	var reason string
	confidence := 0.9

	if remoteCreationTime.After(localCreationTime) {
		winner = remote
		reason = "remote message was created later"
	} else if localCreationTime.After(remoteCreationTime) {
		winner = local
		reason = "local message was created later"
	} else {
		// Same creation time, fall back to message timestamp
		if remote.Timestamp.After(local.Timestamp) {
			winner = remote
			reason = "remote message has later processing timestamp"
		} else {
			winner = local
			reason = "local message has later processing timestamp"
		}
		confidence = 0.6 // Lower confidence for fallback
	}

	return &Resolution{
		ResolvedMessage: winner,
		Strategy:        ConflictResolutionTimestamp,
		Reason:          reason,
		Confidence:      confidence,
		ResolvedAt:      time.Now(),
		Metadata: map[string]interface{}{
			"local_creation_time":  localCreationTime,
			"remote_creation_time": remoteCreationTime,
			"local_timestamp":      local.Timestamp,
			"remote_timestamp":     remote.Timestamp,
		},
	}, nil
}

func (cr *ConflictResolver) customResolution(conflict *Conflict) (*Resolution, error) {
	// Custom resolution logic can be implemented here
	// This is a placeholder for application-specific conflict resolution

	local := conflict.LocalMessage
	remote := conflict.RemoteMessage

	// Example: Prefer messages with specific header values
	if localPriority := getPriority(local); localPriority > 0 {
		if remotePriority := getPriority(remote); remotePriority > 0 {
			if localPriority > remotePriority {
				return &Resolution{
					ResolvedMessage: local,
					Strategy:        ConflictResolutionCustom,
					Reason:          fmt.Sprintf("local message has higher priority (%d vs %d)", localPriority, remotePriority),
					Confidence:      0.95,
					ResolvedAt:      time.Now(),
					Metadata: map[string]interface{}{
						"local_priority":  localPriority,
						"remote_priority": remotePriority,
					},
				}, nil
			} else {
				return &Resolution{
					ResolvedMessage: remote,
					Strategy:        ConflictResolutionCustom,
					Reason:          fmt.Sprintf("remote message has higher priority (%d vs %d)", remotePriority, localPriority),
					Confidence:      0.95,
					ResolvedAt:      time.Now(),
					Metadata: map[string]interface{}{
						"local_priority":  localPriority,
						"remote_priority": remotePriority,
					},
				}, nil
			}
		}
	}

	// Fall back to last write wins
	return cr.lastWriteWins(conflict)
}

// Vector clock-based conflict resolution
func (cr *ConflictResolver) vectorClockResolution(conflict *Conflict) (*Resolution, error) {
	local := conflict.LocalMessage
	remote := conflict.RemoteMessage

	localClock := extractVectorClock(local)
	remoteClock := extractVectorClock(remote)

	// Compare vector clocks
	comparison := compareVectorClocks(localClock, remoteClock)

	var winner *types.Message
	var reason string
	confidence := 0.9

	switch comparison {
	case VectorClockBefore:
		winner = remote
		reason = "remote message happens-after local message (vector clock)"
	case VectorClockAfter:
		winner = local
		reason = "local message happens-after remote message (vector clock)"
	case VectorClockConcurrent:
		// Concurrent events, need to break tie
		winner = remote
		reason = "concurrent messages, chose remote arbitrarily"
		confidence = 0.5
	default:
		return nil, fmt.Errorf("unable to compare vector clocks")
	}

	return &Resolution{
		ResolvedMessage: winner,
		Strategy:        ConflictResolutionCustom,
		Reason:          reason,
		Confidence:      confidence,
		ResolvedAt:      time.Now(),
		Metadata: map[string]interface{}{
			"local_vector_clock":  localClock,
			"remote_vector_clock": remoteClock,
			"comparison":          comparison,
		},
	}, nil
}

// Helper functions

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func headersEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func extractCreationTime(msg *types.Message) time.Time {
	if creationTimeStr, exists := msg.Headers["creation_time"]; exists {
		if creationTime, err := time.Parse(time.RFC3339, creationTimeStr); err == nil {
			return creationTime
		}
	}
	// Fall back to message timestamp
	return msg.Timestamp
}

func getPriority(msg *types.Message) int {
	if priorityStr, exists := msg.Headers["priority"]; exists {
		if priority := parsePriority(priorityStr); priority > 0 {
			return priority
		}
	}
	return 0
}

func parsePriority(priorityStr string) int {
	switch priorityStr {
	case "high":
		return 3
	case "medium":
		return 2
	case "low":
		return 1
	default:
		return 0
	}
}

// Vector clock types and functions
type VectorClock map[string]int64

type VectorClockComparison int

const (
	VectorClockBefore VectorClockComparison = iota
	VectorClockAfter
	VectorClockConcurrent
	VectorClockEqual
)

func extractVectorClock(msg *types.Message) VectorClock {
	clock := make(VectorClock)

	// Extract vector clock from headers
	for key, value := range msg.Headers {
		if len(key) > 3 && key[:3] == "vc_" {
			nodeID := key[3:]
			if timestamp, err := time.Parse(time.RFC3339, value); err == nil {
				clock[nodeID] = timestamp.UnixNano()
			}
		}
	}

	return clock
}

func compareVectorClocks(a, b VectorClock) VectorClockComparison {
	if len(a) == 0 && len(b) == 0 {
		return VectorClockEqual
	}

	// Get all nodes
	nodes := make(map[string]bool)
	for node := range a {
		nodes[node] = true
	}
	for node := range b {
		nodes[node] = true
	}

	aLessB := false
	bLessA := false

	for node := range nodes {
		aTime := a[node]
		bTime := b[node]

		if aTime < bTime {
			aLessB = true
		} else if aTime > bTime {
			bLessA = true
		}
	}

	if aLessB && !bLessA {
		return VectorClockBefore
	} else if bLessA && !aLessB {
		return VectorClockAfter
	} else if !aLessB && !bLessA {
		return VectorClockEqual
	} else {
		return VectorClockConcurrent
	}
}

// Operational Transform-based conflict resolution
type OperationalTransform struct {
	operations []Operation
}

type Operation interface {
	Apply(message *types.Message) (*types.Message, error)
	Transform(other Operation) (Operation, Operation, error)
}

// Example operation: Insert operation
type InsertOperation struct {
	Position int    `json:"position"`
	Content  string `json:"content"`
}

func (op *InsertOperation) Apply(message *types.Message) (*types.Message, error) {
	// Apply insert operation to message value
	value := string(message.Value)
	if op.Position < 0 || op.Position > len(value) {
		return nil, fmt.Errorf("invalid position for insert: %d", op.Position)
	}

	newValue := value[:op.Position] + op.Content + value[op.Position:]

	// Create new message with modified value
	newMessage := *message
	newMessage.Value = []byte(newValue)

	return &newMessage, nil
}

func (op *InsertOperation) Transform(other Operation) (Operation, Operation, error) {
	switch otherOp := other.(type) {
	case *InsertOperation:
		// Transform two insert operations
		if op.Position <= otherOp.Position {
			// Other operation position needs to be adjusted
			return op, &InsertOperation{
				Position: otherOp.Position + len(op.Content),
				Content:  otherOp.Content,
			}, nil
		} else {
			// This operation position needs to be adjusted
			return &InsertOperation{
				Position: op.Position + len(otherOp.Content),
				Content:  op.Content,
			}, otherOp, nil
		}
	default:
		return nil, nil, fmt.Errorf("unsupported operation transformation")
	}
}

// Delete operation
type DeleteOperation struct {
	Position int `json:"position"`
	Length   int `json:"length"`
}

func (op *DeleteOperation) Apply(message *types.Message) (*types.Message, error) {
	value := string(message.Value)
	if op.Position < 0 || op.Position+op.Length > len(value) {
		return nil, fmt.Errorf("invalid position/length for delete: %d/%d", op.Position, op.Length)
	}

	newValue := value[:op.Position] + value[op.Position+op.Length:]

	newMessage := *message
	newMessage.Value = []byte(newValue)

	return &newMessage, nil
}

func (op *DeleteOperation) Transform(other Operation) (Operation, Operation, error) {
	// Implementation would handle transformation between delete and other operations
	return nil, nil, fmt.Errorf("delete operation transformation not implemented")
}
