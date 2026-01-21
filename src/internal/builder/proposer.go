// Package builder provides block building functionality for the mempool system.
package builder

import (
	"btc-gateway/internal/statemachine"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/types"
)

// ConsensusProposer defines the interface for proposing blocks to consensus.
type ConsensusProposer interface {
	// ProposeBlock proposes a transaction batch as a new block.
	ProposeBlock(batch *statemachine.TransactionBatch) error

	// IsLeader returns true if this node is the current leader.
	IsLeader() bool

	// GetCurrentView returns the current consensus view.
	GetCurrentView() types.ViewNumber
}

// NodeProposer adapts integration.Node to the ConsensusProposer interface.
type NodeProposer struct {
	node   *integration.Node
	config *types.ConsensusConfig
	nodeID types.NodeID
}

// NewNodeProposer creates a new NodeProposer adapter.
func NewNodeProposer(node *integration.Node, config *types.ConsensusConfig, nodeID types.NodeID) *NodeProposer {
	return &NodeProposer{
		node:   node,
		config: config,
		nodeID: nodeID,
	}
}

// ProposeBlock proposes a transaction batch as a new block.
func (p *NodeProposer) ProposeBlock(batch *statemachine.TransactionBatch) error {
	// Serialize the batch
	payload, err := batch.Serialize()
	if err != nil {
		return err
	}

	// Propose via the node
	return p.node.ProposeBlock(payload)
}

// IsLeader returns true if this node is the current leader.
func (p *NodeProposer) IsLeader() bool {
	currentView := p.GetCurrentView()
	leader, _ := p.config.GetLeaderForView(currentView)
	return leader == p.nodeID
}

// GetCurrentView returns the current consensus view.
func (p *NodeProposer) GetCurrentView() types.ViewNumber {
	return p.node.GetCoordinator().GetCurrentView()
}

// Ensure NodeProposer implements ConsensusProposer.
var _ ConsensusProposer = (*NodeProposer)(nil)
