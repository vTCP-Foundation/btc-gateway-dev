// Package storage defines the storage abstraction interfaces for the HotStuff consensus protocol.
// These interfaces allow the consensus engine to persist state independently of specific database implementations.
package storage

import (
	"btc-gateway/pkg/consensus/types"
)

// StorageInterface provides consensus state persistence abstraction.
// Implementations should be provided by a StorageProvider that manages lifecycle.
type StorageInterface interface {
	StoreBlock(block *types.Block) error
	GetBlock(hash types.BlockHash) (*types.Block, error)
	StoreQC(qc *types.QuorumCertificate) error
	GetQC(blockHash types.BlockHash, phase types.ConsensusPhase) (*types.QuorumCertificate, error)
	StoreView(view types.ViewNumber) error
	GetCurrentView() (types.ViewNumber, error)
	GetBlocksByHeight(height types.Height) ([]*types.Block, error)
	GetHighestQC() (*types.QuorumCertificate, error)
}