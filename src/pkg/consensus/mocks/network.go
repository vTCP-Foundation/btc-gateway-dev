// Package mocks provides mock implementations of consensus interfaces for testing.
package mocks

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/messages"
	"btc-gateway/pkg/consensus/network"
	"btc-gateway/pkg/consensus/types"
)

// NetworkConfig contains configuration parameters for MockNetwork behavior.
type NetworkConfig struct {
	// BaseDelay is the base delay for message delivery
	BaseDelay time.Duration
	// DelayVariation is the random variation added to base delay
	DelayVariation time.Duration
	// PacketLossRate is the probability (0.0-1.0) of messages being dropped
	PacketLossRate float64
	// DuplicationRate is the probability (0.0-1.0) of messages being duplicated
	DuplicationRate float64
	// MessageQueueSize is the buffer size for the message queue
	MessageQueueSize int
}

// DefaultNetworkConfig returns a configuration suitable for most tests.
func DefaultNetworkConfig() NetworkConfig {
	return NetworkConfig{
		BaseDelay:        10 * time.Millisecond,
		DelayVariation:   5 * time.Millisecond,
		PacketLossRate:   0.0,
		DuplicationRate:  0.0,
		MessageQueueSize: 100,
	}
}

// NetworkFailureConfig contains failure injection parameters.
type NetworkFailureConfig struct {
	// PartitionedNodes contains node IDs that are isolated from the network
	PartitionedNodes map[types.NodeID]bool
	// FailingSendRate is the probability of Send operations failing
	FailingSendRate float64
	// FailingBroadcastRate is the probability of Broadcast operations failing
	FailingBroadcastRate float64
	// MessageCorruptionRate is the probability of messages being corrupted
	MessageCorruptionRate float64
}

// DefaultNetworkFailureConfig returns a failure configuration with no failures.
func DefaultNetworkFailureConfig() NetworkFailureConfig {
	return NetworkFailureConfig{
		PartitionedNodes:      make(map[types.NodeID]bool),
		FailingSendRate:       0.0,
		FailingBroadcastRate:  0.0,
		MessageCorruptionRate: 0.0,
	}
}

// MockNetwork implements NetworkInterface for testing with configurable behavior.
type MockNetwork struct {
	nodeID      types.NodeID
	nodes       map[types.NodeID]*MockNetwork
	msgQueue    chan network.ReceivedMessage
	config      NetworkConfig
	failures    NetworkFailureConfig
	eventTracer events.EventTracer
	mu          sync.RWMutex
	rand        *rand.Rand
	randMu      sync.Mutex // protects rand (rand.Rand is not thread-safe)
	stopped     bool
}

// NewMockNetwork creates a new MockNetwork instance.
func NewMockNetwork(nodeID types.NodeID, config NetworkConfig, failures NetworkFailureConfig) *MockNetwork {
	return &MockNetwork{
		nodeID:      nodeID,
		nodes:       make(map[types.NodeID]*MockNetwork),
		msgQueue:    make(chan network.ReceivedMessage, config.MessageQueueSize),
		config:      config,
		failures:    failures,
		eventTracer: &events.NoOpEventTracer{}, // Default to no-op for production safety
		rand:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(nodeID))),
	}
}

// SetEventTracer sets the event tracer for this network instance
func (mn *MockNetwork) SetEventTracer(tracer events.EventTracer) {
	mn.mu.Lock()
	defer mn.mu.Unlock()
	if tracer != nil {
		mn.eventTracer = tracer
	} else {
		mn.eventTracer = &events.NoOpEventTracer{}
	}
}

// SetPeers establishes connections to other mock network nodes.
func (mn *MockNetwork) SetPeers(peers map[types.NodeID]*MockNetwork) {
	mn.mu.Lock()
	defer mn.mu.Unlock()
	
	// Clear existing peers and set new ones
	mn.nodes = make(map[types.NodeID]*MockNetwork)
	for id, peer := range peers {
		if id != mn.nodeID {
			mn.nodes[id] = peer
		}
	}
}

// Send delivers a message to a specific node.
func (mn *MockNetwork) Send(ctx context.Context, nodeID types.NodeID, message messages.ConsensusMessage) error {
	mn.mu.RLock()
	defer mn.mu.RUnlock()
	
	// Record outbound message event
	if mn.eventTracer != nil {
		mn.eventTracer.RecordMessage(uint16(mn.nodeID), events.MessageOutbound, 
			reflect.TypeOf(message).Elem().Name(), events.EventPayload{
				"destination":   nodeID,
				"message_type":  reflect.TypeOf(message).Elem().Name(),
			})
	}
	
	if mn.stopped {
		return network.NewNetworkError(network.ErrorTypeConnection, "network is stopped")
	}
	
	// Check if this node is partitioned
	if mn.failures.PartitionedNodes[mn.nodeID] {
		return network.NewNetworkError(network.ErrorTypeConnection, "node is partitioned")
	}
	
	// Check if target node is partitioned
	if mn.failures.PartitionedNodes[nodeID] {
		return network.NewNetworkError(network.ErrorTypeNodeNotFound, "target node is partitioned")
	}
	
	// Simulate random Send failures
	if mn.randomFloat64() < mn.failures.FailingSendRate {
		return network.NewNetworkError(network.ErrorTypeMessageDelivery, "simulated send failure")
	}
	
	// Find target node
	targetNode, exists := mn.nodes[nodeID]
	if !exists {
		return network.NewNetworkError(network.ErrorTypeNodeNotFound, fmt.Sprintf("node %d not found", nodeID))
	}
	
	// Simulate packet loss
	if mn.randomFloat64() < mn.config.PacketLossRate {
		// Message dropped, but not an error from sender perspective
		return nil
	}

	// Deliver message with delay
	go mn.deliverMessage(ctx, targetNode, message, false)

	// Simulate duplication
	if mn.randomFloat64() < mn.config.DuplicationRate {
		go mn.deliverMessage(ctx, targetNode, message, true)
	}
	
	return nil
}

// Broadcast delivers a message to all connected nodes.
func (mn *MockNetwork) Broadcast(ctx context.Context, message messages.ConsensusMessage) error {
	mn.mu.RLock()
	defer mn.mu.RUnlock()
	
	// Record broadcast message event
	if mn.eventTracer != nil {
		mn.eventTracer.RecordMessage(uint16(mn.nodeID), events.MessageOutbound, 
			reflect.TypeOf(message).Elem().Name(), events.EventPayload{
				"destination":   "broadcast",
				"message_type":  reflect.TypeOf(message).Elem().Name(),
				"peer_count":    len(mn.nodes),
			})
	}
	
	if mn.stopped {
		return network.NewNetworkError(network.ErrorTypeConnection, "network is stopped")
	}
	
	// Check if this node is partitioned
	if mn.failures.PartitionedNodes[mn.nodeID] {
		return network.NewNetworkError(network.ErrorTypeConnection, "node is partitioned")
	}
	
	// Simulate random Broadcast failures
	if mn.randomFloat64() < mn.failures.FailingBroadcastRate {
		return network.NewNetworkError(network.ErrorTypeMessageDelivery, "simulated broadcast failure")
	}
	
	// Send to all peers
	for _, targetNode := range mn.nodes {
		// Check if target is partitioned
		if mn.failures.PartitionedNodes[targetNode.nodeID] {
			continue // Skip partitioned nodes
		}
		
		// Simulate packet loss per recipient
		if mn.randomFloat64() < mn.config.PacketLossRate {
			continue // Message dropped to this recipient
		}

		// Deliver message with delay
		go mn.deliverMessage(ctx, targetNode, message, false)

		// Simulate duplication per recipient
		if mn.randomFloat64() < mn.config.DuplicationRate {
			go mn.deliverMessage(ctx, targetNode, message, true)
		}
	}
	
	return nil
}

// Receive returns the channel for receiving messages.
func (mn *MockNetwork) Receive() <-chan network.ReceivedMessage {
	return mn.msgQueue
}

// Stop stops the mock network, preventing further message delivery.
func (mn *MockNetwork) Stop() {
	mn.mu.Lock()
	defer mn.mu.Unlock()
	
	mn.stopped = true
	close(mn.msgQueue)
}

// IsPartitioned returns true if this node is currently partitioned.
func (mn *MockNetwork) IsPartitioned() bool {
	mn.mu.RLock()
	defer mn.mu.RUnlock()
	return mn.failures.PartitionedNodes[mn.nodeID]
}

// SetPartitioned sets the partition status for this node.
func (mn *MockNetwork) SetPartitioned(partitioned bool) {
	mn.mu.Lock()
	defer mn.mu.Unlock()
	
	if partitioned {
		mn.failures.PartitionedNodes[mn.nodeID] = true
	} else {
		delete(mn.failures.PartitionedNodes, mn.nodeID)
	}
}

// UpdateFailures updates the failure configuration.
func (mn *MockNetwork) UpdateFailures(failures NetworkFailureConfig) {
	mn.mu.Lock()
	defer mn.mu.Unlock()
	mn.failures = failures
}

// GetStats returns network statistics for monitoring.
func (mn *MockNetwork) GetStats() NetworkStats {
	mn.mu.RLock()
	defer mn.mu.RUnlock()
	
	return NetworkStats{
		NodeID:           mn.nodeID,
		ConnectedPeers:   len(mn.nodes),
		QueueSize:        len(mn.msgQueue),
		QueueCapacity:    cap(mn.msgQueue),
		IsPartitioned:    mn.failures.PartitionedNodes[mn.nodeID],
		IsStopped:        mn.stopped,
	}
}

// NetworkStats contains runtime statistics for MockNetwork.
type NetworkStats struct {
	NodeID           types.NodeID
	ConnectedPeers   int
	QueueSize        int
	QueueCapacity    int
	IsPartitioned    bool
	IsStopped        bool
}

// randomFloat64 returns a thread-safe random float64 in [0.0, 1.0).
func (mn *MockNetwork) randomFloat64() float64 {
	mn.randMu.Lock()
	defer mn.randMu.Unlock()
	return mn.rand.Float64()
}

// randomInt63n returns a thread-safe random int64 in [0, n).
func (mn *MockNetwork) randomInt63n(n int64) int64 {
	mn.randMu.Lock()
	defer mn.randMu.Unlock()
	return mn.rand.Int63n(n)
}

// deliverMessage handles message delivery with configured delays and corruption.
func (mn *MockNetwork) deliverMessage(ctx context.Context, target *MockNetwork, message messages.ConsensusMessage, isDuplicate bool) {
	// Calculate delivery delay
	delay := mn.config.BaseDelay
	if mn.config.DelayVariation > 0 {
		variation := time.Duration(mn.randomInt63n(int64(mn.config.DelayVariation)))
		delay += variation
	}
	
	// Wait for delay or context cancellation
	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
		// Continue with delivery
	}
	
	// Create received message
	receivedMsg := network.ReceivedMessage{
		Message:    message,
		Sender:     mn.nodeID,
		ReceivedAt: time.Now(),
	}
	
	// Simulate message corruption
	if mn.randomFloat64() < mn.failures.MessageCorruptionRate {
		// For simplicity, corruption means creating a timeout message as corrupted data
		// In a real implementation, this would modify the message bytes
		corruptedTimeout := messages.NewTimeoutMsg(0, nil, mn.nodeID, []byte("corrupted"))
		receivedMsg.Message = corruptedTimeout
	}
	
	// Try to deliver to target
	target.mu.RLock()
	stopped := target.stopped
	target.mu.RUnlock()
	
	if !stopped {
		select {
		case target.msgQueue <- receivedMsg:
			// Record inbound message event on target node
			if target.eventTracer != nil {
				target.eventTracer.RecordMessage(uint16(target.nodeID), events.MessageInbound, 
					reflect.TypeOf(message).Elem().Name(), events.EventPayload{
						"sender":       mn.nodeID,
						"message_type": reflect.TypeOf(message).Elem().Name(),
						"delay":        time.Since(receivedMsg.ReceivedAt),
					})
			}
		case <-ctx.Done():
			// Context cancelled during delivery
		default:
			// Queue is full, message dropped
			if target.eventTracer != nil {
				target.eventTracer.RecordEvent(uint16(target.nodeID), "message_dropped", 
					events.EventPayload{
						"sender":       mn.nodeID,
						"message_type": reflect.TypeOf(message).Elem().Name(),
						"reason":       "queue_full",
					})
			}
		}
	}
}