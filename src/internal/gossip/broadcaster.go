// Package gossip provides transaction gossip functionality for the mempool system.
package gossip

import (
	"btc-gateway/internal/statemachine"
)

// TxBroadcaster defines the interface for broadcasting transactions to the network.
type TxBroadcaster interface {
	// Broadcast sends a transaction to the gossip network.
	// This is fire-and-forget semantics - errors indicate local failures only.
	Broadcast(tx *statemachine.Transaction) error

	// Close shuts down the broadcaster and releases resources.
	Close() error
}
