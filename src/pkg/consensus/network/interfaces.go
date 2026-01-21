// Package network defines the network abstraction interfaces for the HotStuff consensus protocol.
// These interfaces allow the consensus engine to operate independently of specific network implementations.
package network

import (
	"context"
	"time"

	"btc-gateway/pkg/consensus/messages"
	"btc-gateway/pkg/consensus/types"
)

// NetworkInterface provides network communication abstraction for consensus nodes.
// Implementations should be provided by a NetworkProvider that manages lifecycle.
type NetworkInterface interface {
	Send(ctx context.Context, nodeID types.NodeID, message messages.ConsensusMessage) error
	Broadcast(ctx context.Context, message messages.ConsensusMessage) error
	Receive() <-chan ReceivedMessage
}

// ReceivedMessage represents a consensus message received from the network.
type ReceivedMessage struct {
	Message    messages.ConsensusMessage
	Sender     types.NodeID
	ReceivedAt time.Time
}