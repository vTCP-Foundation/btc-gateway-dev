// Package events provides error definitions for event tracing operations.
package events

import "fmt"

// EventErrorType represents different categories of event tracing errors
type EventErrorType int

const (
	ErrorTypeInvalidEvent EventErrorType = iota
	ErrorTypeStorageFull
	ErrorTypeCorruptedEvent
	ErrorTypeInvalidPayload
)

// EventError represents an error that occurred during event tracing operations
type EventError struct {
	Type    EventErrorType
	Message string
	NodeID  uint16
	Event   *ConsensusEvent
}

// Error implements the error interface
func (e *EventError) Error() string {
	return fmt.Sprintf("event error [node %d]: %s", e.NodeID, e.Message)
}

// NewEventError creates a new event error
func NewEventError(errorType EventErrorType, message string) *EventError {
	return &EventError{
		Type:    errorType,
		Message: message,
	}
}

// NewEventErrorWithNode creates a new event error with node context
func NewEventErrorWithNode(errorType EventErrorType, message string, nodeID uint16) *EventError {
	return &EventError{
		Type:    errorType,
		Message: message,
		NodeID:  nodeID,
	}
}

// NewEventErrorWithEvent creates a new event error with full event context
func NewEventErrorWithEvent(errorType EventErrorType, message string, nodeID uint16, event *ConsensusEvent) *EventError {
	return &EventError{
		Type:    errorType,
		Message: message,
		NodeID:  nodeID,
		Event:   event,
	}
}