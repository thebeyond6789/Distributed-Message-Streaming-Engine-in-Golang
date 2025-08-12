package partition

import (
	"fmt"
	"sort"
)

// RoundRobinAssignmentPolicy implements round-robin assignment
type RoundRobinAssignmentPolicy struct{}

// NewRoundRobinAssignmentPolicy creates a new round-robin assignment policy
func NewRoundRobinAssignmentPolicy() *RoundRobinAssignmentPolicy {
	return &RoundRobinAssignmentPolicy{}
}

// Name returns the policy name
func (p *RoundRobinAssignmentPolicy) Name() string {
	return "RoundRobin"
}

// Assign assigns partitions using round-robin strategy
func (p *RoundRobinAssignmentPolicy) Assign(topics map[string]*TopicMetadata, members []*ConsumerMember) (map[string][]PartitionAssignment, error) {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment), nil
	}

	// Collect all partitions from subscribed topics
	var allPartitions []PartitionAssignment
	memberSubscriptions := make(map[string]map[string]bool)

	// Build subscription map for each member
	for _, member := range members {
		memberSubscriptions[member.MemberID] = make(map[string]bool)
		for _, topic := range member.Subscription {
			memberSubscriptions[member.MemberID][topic] = true

			// Add partitions from this topic
			if topicMeta, exists := topics[topic]; exists {
				for i := int32(0); i < topicMeta.Partitions; i++ {
					allPartitions = append(allPartitions, PartitionAssignment{
						Topic:     topic,
						Partition: i,
					})
				}
			}
		}
	}

	// Sort partitions for deterministic assignment
	sort.Slice(allPartitions, func(i, j int) bool {
		if allPartitions[i].Topic != allPartitions[j].Topic {
			return allPartitions[i].Topic < allPartitions[j].Topic
		}
		return allPartitions[i].Partition < allPartitions[j].Partition
	})

	// Sort members for deterministic assignment
	sort.Slice(members, func(i, j int) bool {
		return members[i].MemberID < members[j].MemberID
	})

	// Initialize assignment map
	assignment := make(map[string][]PartitionAssignment)
	for _, member := range members {
		assignment[member.MemberID] = make([]PartitionAssignment, 0)
	}

	// Assign partitions in round-robin fashion
	memberIndex := 0
	for _, partition := range allPartitions {
		// Find next member who is subscribed to this topic
		for attempts := 0; attempts < len(members); attempts++ {
			member := members[memberIndex]
			if memberSubscriptions[member.MemberID][partition.Topic] {
				assignment[member.MemberID] = append(assignment[member.MemberID], partition)
				break
			}
			memberIndex = (memberIndex + 1) % len(members)
		}
		memberIndex = (memberIndex + 1) % len(members)
	}

	return assignment, nil
}

// RangeAssignmentPolicy implements range assignment
type RangeAssignmentPolicy struct{}

// NewRangeAssignmentPolicy creates a new range assignment policy
func NewRangeAssignmentPolicy() *RangeAssignmentPolicy {
	return &RangeAssignmentPolicy{}
}

// Name returns the policy name
func (p *RangeAssignmentPolicy) Name() string {
	return "Range"
}

// Assign assigns partitions using range strategy
func (p *RangeAssignmentPolicy) Assign(topics map[string]*TopicMetadata, members []*ConsumerMember) (map[string][]PartitionAssignment, error) {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment), nil
	}

	// Sort members for deterministic assignment
	sort.Slice(members, func(i, j int) bool {
		return members[i].MemberID < members[j].MemberID
	})

	// Initialize assignment map
	assignment := make(map[string][]PartitionAssignment)
	for _, member := range members {
		assignment[member.MemberID] = make([]PartitionAssignment, 0)
	}

	// Get all topics and sort them
	var allTopics []string
	for topic := range topics {
		allTopics = append(allTopics, topic)
	}
	sort.Strings(allTopics)

	// Assign each topic separately using range assignment
	for _, topic := range allTopics {
		topicMeta := topics[topic]

		// Find members subscribed to this topic
		var subscribedMembers []*ConsumerMember
		for _, member := range members {
			for _, subscribedTopic := range member.Subscription {
				if subscribedTopic == topic {
					subscribedMembers = append(subscribedMembers, member)
					break
				}
			}
		}

		if len(subscribedMembers) == 0 {
			continue
		}

		// Calculate partition ranges
		partitionsPerMember := int(topicMeta.Partitions) / len(subscribedMembers)
		extraPartitions := int(topicMeta.Partitions) % len(subscribedMembers)

		currentPartition := int32(0)
		for i, member := range subscribedMembers {
			// Calculate how many partitions this member gets
			memberPartitions := partitionsPerMember
			if i < extraPartitions {
				memberPartitions++
			}

			// Assign partitions to this member
			for j := 0; j < memberPartitions; j++ {
				assignment[member.MemberID] = append(assignment[member.MemberID], PartitionAssignment{
					Topic:     topic,
					Partition: currentPartition,
				})
				currentPartition++
			}
		}
	}

	return assignment, nil
}

// StickyAssignmentPolicy implements sticky assignment
type StickyAssignmentPolicy struct {
	previousAssignment map[string][]PartitionAssignment
}

// NewStickyAssignmentPolicy creates a new sticky assignment policy
func NewStickyAssignmentPolicy() *StickyAssignmentPolicy {
	return &StickyAssignmentPolicy{
		previousAssignment: make(map[string][]PartitionAssignment),
	}
}

// Name returns the policy name
func (p *StickyAssignmentPolicy) Name() string {
	return "Sticky"
}

// Assign assigns partitions using sticky strategy
func (p *StickyAssignmentPolicy) Assign(topics map[string]*TopicMetadata, members []*ConsumerMember) (map[string][]PartitionAssignment, error) {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment), nil
	}

	// Collect all partitions from subscribed topics
	var allPartitions []PartitionAssignment
	memberSubscriptions := make(map[string]map[string]bool)

	// Build subscription map for each member
	for _, member := range members {
		memberSubscriptions[member.MemberID] = make(map[string]bool)
		for _, topic := range member.Subscription {
			memberSubscriptions[member.MemberID][topic] = true

			// Add partitions from this topic
			if topicMeta, exists := topics[topic]; exists {
				for i := int32(0); i < topicMeta.Partitions; i++ {
					allPartitions = append(allPartitions, PartitionAssignment{
						Topic:     topic,
						Partition: i,
					})
				}
			}
		}
	}

	// Initialize assignment map
	assignment := make(map[string][]PartitionAssignment)
	for _, member := range members {
		assignment[member.MemberID] = make([]PartitionAssignment, 0)
	}

	// Start with previous assignments for existing members
	assignedPartitions := make(map[PartitionAssignment]bool)
	for memberID, partitions := range p.previousAssignment {
		// Check if member still exists and is subscribed to these partitions
		memberExists := false
		for _, member := range members {
			if member.MemberID == memberID {
				memberExists = true
				break
			}
		}

		if memberExists {
			for _, partition := range partitions {
				if memberSubscriptions[memberID][partition.Topic] {
					assignment[memberID] = append(assignment[memberID], partition)
					assignedPartitions[partition] = true
				}
			}
		}
	}

	// Assign remaining partitions using round-robin
	var unassignedPartitions []PartitionAssignment
	for _, partition := range allPartitions {
		if !assignedPartitions[partition] {
			unassignedPartitions = append(unassignedPartitions, partition)
		}
	}

	// Sort members by current assignment count for balancing
	sort.Slice(members, func(i, j int) bool {
		countI := len(assignment[members[i].MemberID])
		countJ := len(assignment[members[j].MemberID])
		if countI != countJ {
			return countI < countJ
		}
		return members[i].MemberID < members[j].MemberID
	})

	// Assign unassigned partitions
	for _, partition := range unassignedPartitions {
		// Find member with least partitions who is subscribed to this topic
		var selectedMember *ConsumerMember
		minPartitions := int(^uint(0) >> 1) // Max int

		for _, member := range members {
			if memberSubscriptions[member.MemberID][partition.Topic] {
				currentCount := len(assignment[member.MemberID])
				if currentCount < minPartitions {
					minPartitions = currentCount
					selectedMember = member
				}
			}
		}

		if selectedMember != nil {
			assignment[selectedMember.MemberID] = append(assignment[selectedMember.MemberID], partition)
		}
	}

	// Store current assignment for next time
	p.previousAssignment = make(map[string][]PartitionAssignment)
	for memberID, partitions := range assignment {
		p.previousAssignment[memberID] = make([]PartitionAssignment, len(partitions))
		copy(p.previousAssignment[memberID], partitions)
	}

	return assignment, nil
}

// CooperativeStickyAssignmentPolicy implements cooperative sticky assignment
type CooperativeStickyAssignmentPolicy struct {
	previousAssignment map[string][]PartitionAssignment
}

// NewCooperativeStickyAssignmentPolicy creates a new cooperative sticky assignment policy
func NewCooperativeStickyAssignmentPolicy() *CooperativeStickyAssignmentPolicy {
	return &CooperativeStickyAssignmentPolicy{
		previousAssignment: make(map[string][]PartitionAssignment),
	}
}

// Name returns the policy name
func (p *CooperativeStickyAssignmentPolicy) Name() string {
	return "CooperativeSticky"
}

// Assign assigns partitions using cooperative sticky strategy
func (p *CooperativeStickyAssignmentPolicy) Assign(topics map[string]*TopicMetadata, members []*ConsumerMember) (map[string][]PartitionAssignment, error) {
	if len(members) == 0 {
		return make(map[string][]PartitionAssignment), nil
	}

	// This is a simplified version of cooperative sticky assignment
	// In a full implementation, this would be done in multiple phases
	// to minimize partition movement

	return p.assignPhaseOne(topics, members)
}

func (p *CooperativeStickyAssignmentPolicy) assignPhaseOne(topics map[string]*TopicMetadata, members []*ConsumerMember) (map[string][]PartitionAssignment, error) {
	// Collect all partitions
	var allPartitions []PartitionAssignment
	memberSubscriptions := make(map[string]map[string]bool)

	for _, member := range members {
		memberSubscriptions[member.MemberID] = make(map[string]bool)
		for _, topic := range member.Subscription {
			memberSubscriptions[member.MemberID][topic] = true

			if topicMeta, exists := topics[topic]; exists {
				for i := int32(0); i < topicMeta.Partitions; i++ {
					partition := PartitionAssignment{
						Topic:     topic,
						Partition: i,
					}

					// Only add if not already in list
					found := false
					for _, existing := range allPartitions {
						if existing.Topic == partition.Topic && existing.Partition == partition.Partition {
							found = true
							break
						}
					}
					if !found {
						allPartitions = append(allPartitions, partition)
					}
				}
			}
		}
	}

	// Calculate target assignment counts
	totalPartitions := len(allPartitions)
	totalMembers := len(members)
	minPartitionsPerMember := totalPartitions / totalMembers
	membersWithExtraPartition := totalPartitions % totalMembers

	// Initialize assignment
	assignment := make(map[string][]PartitionAssignment)
	for _, member := range members {
		assignment[member.MemberID] = make([]PartitionAssignment, 0)
	}

	// Start with current assignments for existing members
	assignedPartitions := make(map[PartitionAssignment]bool)
	memberCounts := make(map[string]int)

	for memberID, partitions := range p.previousAssignment {
		memberExists := false
		for _, member := range members {
			if member.MemberID == memberID {
				memberExists = true
				break
			}
		}

		if memberExists {
			for _, partition := range partitions {
				if memberSubscriptions[memberID][partition.Topic] {
					assignment[memberID] = append(assignment[memberID], partition)
					assignedPartitions[partition] = true
					memberCounts[memberID]++
				}
			}
		}
	}

	// Calculate over-assigned members and partitions to revoke
	var partitionsToRevoke []PartitionAssignment
	for _, member := range members {
		memberID := member.MemberID
		currentCount := memberCounts[memberID]

		targetCount := minPartitionsPerMember
		memberIndex := 0
		for i, m := range members {
			if m.MemberID == memberID {
				memberIndex = i
				break
			}
		}
		if memberIndex < membersWithExtraPartition {
			targetCount++
		}

		if currentCount > targetCount {
			// Revoke excess partitions
			excess := currentCount - targetCount
			for i := len(assignment[memberID]) - 1; i >= 0 && excess > 0; i-- {
				partition := assignment[memberID][i]
				partitionsToRevoke = append(partitionsToRevoke, partition)
				assignment[memberID] = append(assignment[memberID][:i], assignment[memberID][i+1:]...)
				delete(assignedPartitions, partition)
				excess--
			}
		}
	}

	// Assign unassigned partitions and revoked partitions
	var unassignedPartitions []PartitionAssignment
	for _, partition := range allPartitions {
		if !assignedPartitions[partition] {
			unassignedPartitions = append(unassignedPartitions, partition)
		}
	}
	unassignedPartitions = append(unassignedPartitions, partitionsToRevoke...)

	// Assign to under-assigned members
	for _, partition := range unassignedPartitions {
		// Find member who needs partitions and is subscribed to this topic
		var selectedMember *ConsumerMember

		for i, member := range members {
			if memberSubscriptions[member.MemberID][partition.Topic] {
				currentCount := len(assignment[member.MemberID])
				targetCount := minPartitionsPerMember
				if i < membersWithExtraPartition {
					targetCount++
				}

				if currentCount < targetCount {
					selectedMember = member
					break
				}
			}
		}

		if selectedMember != nil {
			assignment[selectedMember.MemberID] = append(assignment[selectedMember.MemberID], partition)
		}
	}

	// Store assignment for next time
	p.previousAssignment = make(map[string][]PartitionAssignment)
	for memberID, partitions := range assignment {
		p.previousAssignment[memberID] = make([]PartitionAssignment, len(partitions))
		copy(p.previousAssignment[memberID], partitions)
	}

	return assignment, nil
}

// AssignmentBalanceScore calculates how balanced an assignment is
func AssignmentBalanceScore(assignment map[string][]PartitionAssignment) float64 {
	if len(assignment) == 0 {
		return 1.0
	}

	// Calculate statistics
	var counts []int
	totalPartitions := 0

	for _, partitions := range assignment {
		count := len(partitions)
		counts = append(counts, count)
		totalPartitions += count
	}

	if totalPartitions == 0 {
		return 1.0
	}

	// Calculate mean
	mean := float64(totalPartitions) / float64(len(assignment))

	// Calculate variance
	variance := 0.0
	for _, count := range counts {
		diff := float64(count) - mean
		variance += diff * diff
	}
	variance /= float64(len(assignment))

	// Balance score is inverse of coefficient of variation
	// Perfect balance (variance = 0) gives score = 1.0
	// Higher variance gives lower score
	if variance == 0 {
		return 1.0
	}

	stdDev := variance
	cv := stdDev / mean
	return 1.0 / (1.0 + cv)
}

// ValidateAssignment validates that an assignment is correct
func ValidateAssignment(assignment map[string][]PartitionAssignment, topics map[string]*TopicMetadata, members []*ConsumerMember) error {
	// Build subscription map
	memberSubscriptions := make(map[string]map[string]bool)
	for _, member := range members {
		memberSubscriptions[member.MemberID] = make(map[string]bool)
		for _, topic := range member.Subscription {
			memberSubscriptions[member.MemberID][topic] = true
		}
	}

	// Check that all assigned partitions exist and members are subscribed
	assignedPartitions := make(map[string]map[int32]bool)
	for memberID, partitions := range assignment {
		// Check member exists
		memberExists := false
		for _, member := range members {
			if member.MemberID == memberID {
				memberExists = true
				break
			}
		}
		if !memberExists {
			return fmt.Errorf("assignment contains unknown member: %s", memberID)
		}

		for _, partition := range partitions {
			// Check topic exists
			if _, exists := topics[partition.Topic]; !exists {
				return fmt.Errorf("assignment contains unknown topic: %s", partition.Topic)
			}

			// Check partition exists
			topicMeta := topics[partition.Topic]
			if partition.Partition >= topicMeta.Partitions {
				return fmt.Errorf("assignment contains invalid partition %d for topic %s", partition.Partition, partition.Topic)
			}

			// Check member is subscribed to topic
			if !memberSubscriptions[memberID][partition.Topic] {
				return fmt.Errorf("member %s assigned to unsubscribed topic %s", memberID, partition.Topic)
			}

			// Check for duplicate assignments
			if assignedPartitions[partition.Topic] == nil {
				assignedPartitions[partition.Topic] = make(map[int32]bool)
			}
			if assignedPartitions[partition.Topic][partition.Partition] {
				return fmt.Errorf("partition %s-%d assigned to multiple members", partition.Topic, partition.Partition)
			}
			assignedPartitions[partition.Topic][partition.Partition] = true
		}
	}

	return nil
}
