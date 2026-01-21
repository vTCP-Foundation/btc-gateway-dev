// Package crypto defines the cryptographic abstraction interfaces for the HotStuff consensus protocol.
// These interfaces allow the consensus engine to operate independently of specific cryptographic implementations.
package crypto

import (
	"btc-gateway/pkg/consensus/types"
)

// CryptoInterface provides cryptographic operations for consensus.
// Implementations should be provided by a CryptoProvider that manages keys and lifecycle.
// Intentionally excludes signature aggregation for post-quantum compatibility.
type CryptoInterface interface {
	Sign(data []byte) ([]byte, error)
	Verify(data []byte, signature []byte, nodeID types.NodeID) error
	Hash(data []byte) types.BlockHash
}