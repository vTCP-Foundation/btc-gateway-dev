// Package mocks provides mock implementations of consensus interfaces for testing.
package mocks

import (
	"sync"
	"time"

	"btc-gateway/pkg/consensus/events"
)


// ConsensusEventTracer provides centralized event collection for testing.
// This implementation is thread-safe and collects all events for later analysis.
type ConsensusEventTracer struct {
	events []events.ConsensusEvent
	mutex  sync.RWMutex
}

// NewConsensusEventTracer creates a new event tracer for testing
func NewConsensusEventTracer() *ConsensusEventTracer {
	return &ConsensusEventTracer{
		events: make([]events.ConsensusEvent, 0, 1000), // Pre-allocate for better performance
	}
}

// RecordEvent records a consensus event with timestamp
func (t *ConsensusEventTracer) RecordEvent(nodeID uint16, eventType events.EventType, payload events.EventPayload) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	
	event := events.ConsensusEvent{
		NodeID:    nodeID,
		EventType: eventType,
		Payload:   payload,
		Timestamp: time.Now(),
	}
	
	t.events = append(t.events, event)
}

// RecordTransition records a state transition with context
func (t *ConsensusEventTracer) RecordTransition(nodeID uint16, from, to events.State, trigger string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	
	event := events.ConsensusEvent{
		NodeID:    nodeID,
		EventType: events.EventStateTransition,
		FromState: from,
		ToState:   to,
		Trigger:   trigger,
		Timestamp: time.Now(),
		Payload:   events.EventPayload{
			"from_state": string(from),
			"to_state":   string(to),
			"trigger":    trigger,
		},
	}
	
	t.events = append(t.events, event)
}

// RecordMessage records message sending/receiving
func (t *ConsensusEventTracer) RecordMessage(nodeID uint16, direction events.MessageDirection, msgType string, payload events.EventPayload) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	
	event := events.ConsensusEvent{
		NodeID:      nodeID,
		EventType:   events.EventType("message_" + string(direction)),
		Direction:   direction,
		MessageType: msgType,
		Payload:     payload,
		Timestamp:   time.Now(),
	}
	
	t.events = append(t.events, event)
}

// GetEvents returns a copy of all recorded events
func (t *ConsensusEventTracer) GetEvents() []events.ConsensusEvent {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	// Return a copy to prevent external modification
	eventsCopy := make([]events.ConsensusEvent, len(t.events))
	copy(eventsCopy, t.events)
	return eventsCopy
}

// GetEventsByType returns all events of a specific type
func (t *ConsensusEventTracer) GetEventsByType(eventType events.EventType) []events.ConsensusEvent {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	var filtered []events.ConsensusEvent
	for _, event := range t.events {
		if event.EventType == eventType {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// GetEventsByNode returns all events for a specific node
func (t *ConsensusEventTracer) GetEventsByNode(nodeID uint16) []events.ConsensusEvent {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	var filtered []events.ConsensusEvent
	for _, event := range t.events {
		if event.NodeID == nodeID {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// GetEventsByNodeAndType returns events for a specific node and type
func (t *ConsensusEventTracer) GetEventsByNodeAndType(nodeID uint16, eventType events.EventType) []events.ConsensusEvent {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	
	var filtered []events.ConsensusEvent
	for _, event := range t.events {
		if event.NodeID == nodeID && event.EventType == eventType {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// Reset clears all recorded events (useful for multi-round testing)
func (t *ConsensusEventTracer) Reset() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.events = t.events[:0] // Clear slice but keep capacity
}

// GetEventCount returns the total number of recorded events
func (t *ConsensusEventTracer) GetEventCount() int {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return len(t.events)
}

// GetEventTimeline returns events sorted by timestamp
func (t *ConsensusEventTracer) GetEventTimeline() []events.ConsensusEvent {
	events := t.GetEvents()
	
	// Events are already in chronological order since we append with time.Now()
	// But we could add sorting here if needed for multi-threaded scenarios
	return events
}