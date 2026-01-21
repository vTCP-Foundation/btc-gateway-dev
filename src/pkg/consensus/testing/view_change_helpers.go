package testing

import (
	"fmt"
	"time"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/mocks"
	"btc-gateway/pkg/consensus/types"
)

// ViewChangeTestHelpers provides utilities for view change testing
type ViewChangeTestHelpers struct {
	nodes          map[types.NodeID]*integration.Node
	networkManager *integration.NetworkManager
	tracer         *mocks.ConsensusEventTracer
	config         *types.ConsensusConfig
	storages       map[types.NodeID]*mocks.MockStorage // Optional - set via SetStorages for state sync
}

// NewViewChangeTestHelpers creates a new instance of view change test helpers
func NewViewChangeTestHelpers(nodes map[types.NodeID]*integration.Node, networkManager *integration.NetworkManager, tracer *mocks.ConsensusEventTracer, config *types.ConsensusConfig) *ViewChangeTestHelpers {
	return &ViewChangeTestHelpers{
		nodes:          nodes,
		networkManager: networkManager,
		tracer:         tracer,
		config:         config,
	}
}

// BlockNode blocks a node from network communication by partitioning it
func (h *ViewChangeTestHelpers) BlockNode(nodeID types.NodeID) error {
	node, exists := h.nodes[nodeID]
	if !exists {
		return NewNodeNotFoundError(nodeID)
	}

	// Get the network from the node and set it as partitioned
	network := node.GetNetwork()
	if mockNetwork, ok := network.(interface{ SetPartitioned(bool) }); ok {
		mockNetwork.SetPartitioned(true)
		return nil
	}

	return NewNetworkControlError("unable to partition node - network doesn't support partitioning")
}

// UnblockNode restores network communication for a previously blocked node
func (h *ViewChangeTestHelpers) UnblockNode(nodeID types.NodeID) error {
	node, exists := h.nodes[nodeID]
	if !exists {
		return NewNodeNotFoundError(nodeID)
	}

	// Get the network from the node and remove partitioning
	network := node.GetNetwork()
	if mockNetwork, ok := network.(interface{ SetPartitioned(bool) }); ok {
		mockNetwork.SetPartitioned(false)
		return nil
	}

	return NewNetworkControlError("unable to unpartition node - network doesn't support partitioning")
}

// GetViewTimeout calculates the timeout for a given view using consensus config
func (h *ViewChangeTestHelpers) GetViewTimeout(view types.ViewNumber) time.Duration {
	return h.config.GetTimeoutForView(view)
}

// WaitForTimeout waits for the timeout duration for a specific view plus buffer
func (h *ViewChangeTestHelpers) WaitForTimeout(view types.ViewNumber) {
	timeout := h.GetViewTimeout(view)
	time.Sleep(timeout + 500*time.Millisecond)
}


// ValidateEventExists checks if an event type exists in the collected events
func (h *ViewChangeTestHelpers) ValidateEventExists(eventType events.EventType) bool {
	allEvents := h.tracer.GetEvents()
	for _, event := range allEvents {
		if event.EventType == eventType {
			return true
		}
	}
	return false
}

// FilterEventsByType returns all events of a specific type
func (h *ViewChangeTestHelpers) FilterEventsByType(eventType events.EventType) []events.ConsensusEvent {
	allEvents := h.tracer.GetEvents()
	var filtered []events.ConsensusEvent
	
	for _, event := range allEvents {
		if event.EventType == eventType {
			filtered = append(filtered, event)
		}
	}
	
	return filtered
}

// FilterEventsByNode returns all events from a specific node
func (h *ViewChangeTestHelpers) FilterEventsByNode(nodeID types.NodeID) []events.ConsensusEvent {
	allEvents := h.tracer.GetEvents()
	var filtered []events.ConsensusEvent
	
	for _, event := range allEvents {
		if event.NodeID == uint16(nodeID) {
			filtered = append(filtered, event)
		}
	}
	
	return filtered
}

// FilterEventsByView returns all events for a specific view
func (h *ViewChangeTestHelpers) FilterEventsByView(view uint64) []events.ConsensusEvent {
	allEvents := h.tracer.GetEvents()
	var filtered []events.ConsensusEvent
	
	for _, event := range allEvents {
		if viewPayload, exists := event.Payload["view"]; exists {
			// Handle both uint64 and types.ViewNumber
			var eventView uint64
			var viewOk bool
			
			if v, ok := viewPayload.(uint64); ok {
				eventView = v
				viewOk = true
			} else if v, ok := viewPayload.(types.ViewNumber); ok {
				eventView = uint64(v)
				viewOk = true
			}
			
			if viewOk && eventView == view {
				filtered = append(filtered, event)
			}
		}
	}
	
	return filtered
}

// ValidateEventOrder validates that events occur in the expected temporal order
func (h *ViewChangeTestHelpers) ValidateEventOrder(firstEventType, secondEventType events.EventType) bool {
	allEvents := h.tracer.GetEvents()
	
	var firstEventTime, secondEventTime *time.Time
	
	for _, event := range allEvents {
		if event.EventType == firstEventType && firstEventTime == nil {
			firstEventTime = &event.Timestamp
		}
		if event.EventType == secondEventType && secondEventTime == nil {
			secondEventTime = &event.Timestamp
		}
	}
	
	if firstEventTime == nil || secondEventTime == nil {
		return false // One or both events not found
	}
	
	return firstEventTime.Before(*secondEventTime) || firstEventTime.Equal(*secondEventTime)
}

// GetEventsCount returns the count of events of a specific type
func (h *ViewChangeTestHelpers) GetEventsCount(eventType events.EventType) int {
	return len(h.FilterEventsByType(eventType))
}

// FindEventWithPayload finds the first event of a type with specific payload content
func (h *ViewChangeTestHelpers) FindEventWithPayload(eventType events.EventType, payloadKey string, payloadValue interface{}) *events.ConsensusEvent {
	filtered := h.FilterEventsByType(eventType)
	
	for _, event := range filtered {
		if value, exists := event.Payload[payloadKey]; exists && value == payloadValue {
			return &event
		}
	}
	
	return nil
}

// WaitForEvent waits for a specific event to occur within timeout duration
func (h *ViewChangeTestHelpers) WaitForEvent(eventType events.EventType, maxWait time.Duration) bool {
	start := time.Now()
	checkInterval := 50 * time.Millisecond
	
	for time.Since(start) < maxWait {
		if h.ValidateEventExists(eventType) {
			return true
		}
		time.Sleep(checkInterval)
	}
	
	return false // Timeout reached
}

// WaitForEventFromNode waits for a specific event from a specific node
func (h *ViewChangeTestHelpers) WaitForEventFromNode(nodeID types.NodeID, eventType events.EventType, maxWait time.Duration) bool {
	start := time.Now()
	checkInterval := 50 * time.Millisecond

	for time.Since(start) < maxWait {
		nodeEvents := h.FilterEventsByNode(nodeID)
		for _, event := range nodeEvents {
			if event.EventType == eventType {
				return true
			}
		}
		time.Sleep(checkInterval)
	}

	return false
}

// WaitForCondition polls until condition returns true or timeout
// Returns true if condition was met, false on timeout
func (h *ViewChangeTestHelpers) WaitForCondition(condition func() bool, maxWait time.Duration) bool {
	deadline := time.Now().Add(maxWait)
	checkInterval := 50 * time.Millisecond

	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(checkInterval)
	}

	return false
}

// WaitForEventCount waits until at least minCount events of eventType occur
func (h *ViewChangeTestHelpers) WaitForEventCount(eventType events.EventType, minCount int, maxWait time.Duration) bool {
	return h.WaitForCondition(func() bool {
		return h.GetEventsCount(eventType) >= minCount
	}, maxWait)
}

// WaitForConsensusCompletion waits until at least one BlockCommitted event occurs
// Use this after ProposeBlock to wait for consensus to complete
func (h *ViewChangeTestHelpers) WaitForConsensusCompletion(maxWait time.Duration) bool {
	return h.WaitForEventCount(events.EventBlockCommitted, 1, maxWait)
}

// WaitForNodeView waits until a specific node reaches at least the target view
func (h *ViewChangeTestHelpers) WaitForNodeView(nodeID types.NodeID, targetView types.ViewNumber, maxWait time.Duration) bool {
	node, exists := h.nodes[nodeID]
	if !exists {
		return false
	}

	return h.WaitForCondition(func() bool {
		return node.GetCoordinator().GetCurrentView() >= targetView
	}, maxWait)
}

// WaitForAllNodesView waits until all nodes reach at least the target view
func (h *ViewChangeTestHelpers) WaitForAllNodesView(targetView types.ViewNumber, maxWait time.Duration) bool {
	return h.WaitForCondition(func() bool {
		for _, node := range h.nodes {
			if node.GetCoordinator().GetCurrentView() < targetView {
				return false
			}
		}
		return true
	}, maxWait)
}

// WaitForProposalProcessed waits until at least one ProposalReceived event occurs
// Use this after ProposeBlock to wait for proposal distribution
func (h *ViewChangeTestHelpers) WaitForProposalProcessed(maxWait time.Duration) bool {
	return h.WaitForEventCount(events.EventProposalReceived, 1, maxWait)
}

// WaitForQuorumCommits waits until quorum number of BlockCommitted events occur
// Quorum is calculated as (2n/3 + 1) where n is the number of nodes
func (h *ViewChangeTestHelpers) WaitForQuorumCommits(maxWait time.Duration) bool {
	quorum := (len(h.nodes)*2)/3 + 1
	return h.WaitForEventCount(events.EventBlockCommitted, quorum, maxWait)
}

// VerifyAllNodesHaveLeader checks that all nodes have the same view
func (h *ViewChangeTestHelpers) VerifyAllNodesHaveView(expectedView types.ViewNumber) error {
	for nodeID, node := range h.nodes {
		coordinator := node.GetCoordinator()
		currentView := coordinator.GetCurrentView()
		
		// Check if node is at the expected view
		if currentView != expectedView {
			return fmt.Errorf("node %d is at view %d, expected view %d", nodeID, currentView, expectedView)
		}
	}
	
	return nil // All nodes have correct view
}

// BlockNodeAfterEvent blocks a node after waiting for a specific event
func (h *ViewChangeTestHelpers) BlockNodeAfterEvent(nodeID types.NodeID, waitForEvent events.EventType, maxWait time.Duration) error {
	// Wait for the event to occur
	if !h.WaitForEvent(waitForEvent, maxWait) {
		return fmt.Errorf("timeout waiting for event %s before blocking node %d", waitForEvent, nodeID)
	}
	
	// Add small delay to ensure event processing is complete
	time.Sleep(100 * time.Millisecond)
	
	// Block the node
	return h.BlockNode(nodeID)
}

// ValidateCompleteConsensusRound verifies that a complete consensus round occurs
func (h *ViewChangeTestHelpers) ValidateCompleteConsensusRound(expectedView types.ViewNumber, proposer types.NodeID, maxWait time.Duration) error {
	start := time.Now()
	checkInterval := 200 * time.Millisecond
	
	requiredEvents := []events.EventType{
		events.EventProposalCreated,
		events.EventProposalBroadcasted,
		events.EventBlockCommitted, 
	}
	
	foundEvents := make(map[events.EventType]bool)
	
	for time.Since(start) < maxWait {
		// Check for all required events
		for _, eventType := range requiredEvents {
			if !foundEvents[eventType] {
				filteredEvents := h.FilterEventsByType(eventType)
				for _, event := range filteredEvents {
					// Check if event is from expected view and proposer
					if viewPayload, exists := event.Payload["view"]; exists {
						if eventView, ok := viewPayload.(uint64); ok && eventView == uint64(expectedView) {
							if event.NodeID == uint16(proposer) || eventType == events.EventBlockCommitted {
								foundEvents[eventType] = true
								break
							}
						}
					}
				}
			}
		}
		
		// Check if all events found
		allFound := true
		for _, eventType := range requiredEvents {
			if !foundEvents[eventType] {
				allFound = false
				break
			}
		}
		
		if allFound {
			return nil // Success
		}
		
		time.Sleep(checkInterval)
	}
	
	// Report missing events
	var missing []events.EventType
	for _, eventType := range requiredEvents {
		if !foundEvents[eventType] {
			missing = append(missing, eventType)
		}
	}
	
	return fmt.Errorf("timeout waiting for complete consensus round - missing events: %v", missing)
}

// Error types for view change testing

// NodeNotFoundError represents an error when a requested node doesn't exist
type NodeNotFoundError struct {
	NodeID types.NodeID
}

func NewNodeNotFoundError(nodeID types.NodeID) *NodeNotFoundError {
	return &NodeNotFoundError{NodeID: nodeID}
}

func (e *NodeNotFoundError) Error() string {
	return "node " + string(rune(e.NodeID)) + " not found in test network"
}

// NetworkControlError represents an error in network control operations
type NetworkControlError struct {
	Message string
}

func NewNetworkControlError(message string) *NetworkControlError {
	return &NetworkControlError{Message: message}
}

func (e *NetworkControlError) Error() string {
	return e.Message
}