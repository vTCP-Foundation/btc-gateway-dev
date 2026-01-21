// Package testing provides comprehensive event validation for consensus protocol testing.
// This package implements rule-based validation to ensure protocol compliance.
package testing

import (
	"fmt"
	"time"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/types"
)

// ExpectedEventSequence defines a single expected event in the consensus protocol sequence
type ExpectedEventSequence struct {
	SequenceNumber int              // Order in which this event should occur
	Phase          string           // Consensus phase (NewView, Proposal, Prepare, PreCommit, Commit, Decide)
	SubPhase       string           // Sub-phase for detailed tracking (Creation, Broadcast, Reception, etc.)
	NodeRole       string           // Expected node role (Leader, Validator, All)
	NodeID         *int             // Specific node ID if applicable (nil means any node of the role)
	EventType      events.EventType // The specific event type expected
	Required       bool             // Whether this event must occur
	MinOccurrences int              // Minimum number of times this event should occur
	MaxOccurrences int              // Maximum number of times this event should occur (0 = unlimited)
	MaxDelay       time.Duration    // Maximum allowed delay for this event
	WindowStart    events.EventType // Event that starts the time window for this event
	WindowDuration time.Duration    // Duration of the time window from WindowStart
	DependsOn      []int            // Sequence numbers this event depends on
	EnablesEvents  []int            // Sequence numbers this event enables
	Description    string           // Human-readable description
	FailureHint    string           // Hint for debugging when this event fails
}

// ValidationResult contains the results of validating an event sequence
type ValidationResult struct {
	TotalExpected    int                           // Total number of expected events
	TotalFound       int                           // Total number of matching events found
	PassedEvents     []ExpectedEventSequence       // Events that passed validation
	FailedEvents     []ExpectedEventSequence       // Events that failed validation
	UnexpectedEvents []events.ConsensusEvent       // Events that were not expected
	Summary          string                        // Human-readable summary
	Success          bool                          // Whether all validations passed
}

// ValidateSequentialEvents validates that the actual events match the expected sequence with role constraints.
// nodeCount is required to properly calculate leader rotation (leader = view % nodeCount).
func ValidateSequentialEvents(expectedSequence []ExpectedEventSequence, actualEvents []events.ConsensusEvent, nodeCount int) ValidationResult {
	result := ValidationResult{
		TotalExpected:    len(expectedSequence),
		TotalFound:       0,
		PassedEvents:     []ExpectedEventSequence{},
		FailedEvents:     []ExpectedEventSequence{},
		UnexpectedEvents: []events.ConsensusEvent{},
		Success:          true,
	}

	// Build index of actual events by type
	eventIndex := make(map[events.EventType][]events.ConsensusEvent)
	for _, event := range actualEvents {
		eventIndex[event.EventType] = append(eventIndex[event.EventType], event)
	}

	// Validate each expected event
	for _, expected := range expectedSequence {
		eventsOfType := eventIndex[expected.EventType]

		// Filter events by role and node constraints
		matchingEvents := filterEventsByRoleConstraints(eventsOfType, expected, nodeCount)
		
		if len(matchingEvents) == 0 {
			// No events matching role constraints found
			if expected.Required {
				if len(eventsOfType) > 0 {
					expected.FailureHint = fmt.Sprintf("Found %d events of type %s but none matched role constraint (NodeRole: %s, NodeID: %v)", 
						len(eventsOfType), expected.EventType, expected.NodeRole, expected.NodeID)
				} else {
					expected.FailureHint = fmt.Sprintf("Required event %s not found", expected.EventType)
				}
				result.FailedEvents = append(result.FailedEvents, expected)
				result.Success = false
			}
			continue
		}

		// Check occurrence count
		if len(matchingEvents) < expected.MinOccurrences {
			expected.FailureHint = fmt.Sprintf("Expected at least %d occurrences, found %d (with role constraints NodeRole: %s, NodeID: %v)", 
				expected.MinOccurrences, len(matchingEvents), expected.NodeRole, expected.NodeID)
			result.FailedEvents = append(result.FailedEvents, expected)
			result.Success = false
			continue
		}

		if expected.MaxOccurrences > 0 && len(matchingEvents) > expected.MaxOccurrences {
			expected.FailureHint = fmt.Sprintf("Expected at most %d occurrences, found %d (with role constraints NodeRole: %s, NodeID: %v)", 
				expected.MaxOccurrences, len(matchingEvents), expected.NodeRole, expected.NodeID)
			result.FailedEvents = append(result.FailedEvents, expected)
			result.Success = false
			continue
		}

		// Event found and count is correct
		result.PassedEvents = append(result.PassedEvents, expected)
		result.TotalFound += len(matchingEvents)
	}

	// Create summary
	if result.Success {
		result.Summary = fmt.Sprintf("✅ All %d expected events validated successfully", result.TotalExpected)
	} else {
		result.Summary = fmt.Sprintf("❌ %d/%d events failed validation", len(result.FailedEvents), result.TotalExpected)
	}

	return result
}

// filterEventsByRoleConstraints filters events based on NodeRole and NodeID constraints.
// nodeCount is required for proper leader calculation.
func filterEventsByRoleConstraints(eventList []events.ConsensusEvent, expected ExpectedEventSequence, nodeCount int) []events.ConsensusEvent {
	if len(eventList) == 0 {
		return eventList
	}

	var filtered []events.ConsensusEvent

	for _, event := range eventList {
		// Check NodeID constraint first (most specific)
		if expected.NodeID != nil {
			if int(event.NodeID) == *expected.NodeID {
				filtered = append(filtered, event)
			}
			continue // Skip role check if NodeID is specified
		}

		// Check NodeRole constraint
		switch expected.NodeRole {
		case "Leader":
			// For leader events, determine if this node was the leader for this view
			// Uses round-robin: leader = view % nodeCount
			if isNodeLeaderForEvent(event, nodeCount) {
				filtered = append(filtered, event)
			}
		case "Validator":
			// For validator events, exclude the leader
			if !isNodeLeaderForEvent(event, nodeCount) {
				filtered = append(filtered, event)
			}
		case "All":
			// All nodes should emit this event
			filtered = append(filtered, event)
		default:
			// No role constraint or unknown role - include all
			filtered = append(filtered, event)
		}
	}

	return filtered
}

// isNodeLeaderForEvent determines if the node that emitted this event was the leader
// for the view in which the event occurred. Uses round-robin leader rotation.
//
// STRICT: All consensus events MUST include "view" in their payload.
// This ensures event emission is correct and prevents silent drift.
func isNodeLeaderForEvent(event events.ConsensusEvent, nodeCount int) bool {
	// Extract view from event payload - strictly required
	viewRaw, hasView := event.Payload["view"]
	if !hasView {
		panic(fmt.Sprintf(
			"EVENT VALIDATION ERROR: event %s from node %d missing 'view' in payload - "+
				"all consensus events must include view for proper leader/validator role detection. "+
				"Payload keys: %v",
			event.EventType, event.NodeID, getPayloadKeys(event.Payload)))
	}

	// Handle different view number representations
	var view types.ViewNumber
	switch v := viewRaw.(type) {
	case types.ViewNumber:
		view = v
	case uint64:
		view = types.ViewNumber(v)
	case int:
		view = types.ViewNumber(v)
	case int64:
		view = types.ViewNumber(v)
	default:
		panic(fmt.Sprintf(
			"EVENT VALIDATION ERROR: event %s from node %d has 'view' with unexpected type %T (value: %v) - "+
				"expected types.ViewNumber or numeric type",
			event.EventType, event.NodeID, viewRaw, viewRaw))
	}

	// Calculate leader for this view using round-robin rotation
	leader := int(view) % nodeCount
	return int(event.NodeID) == leader
}

// getPayloadKeys returns the keys present in an event payload for debugging.
func getPayloadKeys(payload map[string]interface{}) []string {
	keys := make([]string, 0, len(payload))
	for k := range payload {
		keys = append(keys, k)
	}
	return keys
}