// Package engine implements the core HotStuff consensus protocol state machine.
package engine

import (
	"fmt"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/types"
)

// HotStuffConsensus represents the main coordinator for the HotStuff consensus protocol.
// It orchestrates the interaction between block tree management, safety rules, and voting.
type HotStuffConsensus struct {
	// nodeID is this node's identifier in the consensus network
	nodeID types.NodeID
	// view is the current consensus view number
	view types.ViewNumber
	// config contains the consensus network configuration
	config *types.ConsensusConfig
	// eventTracer for recording consensus events
	eventTracer events.EventTracer
	
	// blockTree manages the block chain and forks
	blockTree *BlockTree
	// safetyRules enforces safety properties of the consensus
	safetyRules *SafetyRules
	// votingRule handles vote collection and quorum formation
	votingRule *VotingRule
	
	// lockedQC is the highest quorum certificate that this node has locked on
	lockedQC *types.QuorumCertificate
	// prepareQC is the highest prepare quorum certificate received
	prepareQC *types.QuorumCertificate
	// preCommitQC is the highest pre-commit quorum certificate received
	preCommitQC *types.QuorumCertificate
}

// NewHotStuffConsensus creates a new HotStuff consensus coordinator with a default genesis block.
func NewHotStuffConsensus(nodeID types.NodeID, config *types.ConsensusConfig, eventTracer events.EventTracer) (*HotStuffConsensus, error) {
	// Create default genesis block
	genesisBlock := types.NewBlock(
		types.BlockHash{}, // No parent
		0,                 // Height 0
		0,                 // View 0
		0,                // Genesis always proposed by Node 0
		[]byte("genesis"), // Genesis payload
	)
	return NewHotStuffConsensusWithGenesis(nodeID, config, genesisBlock, eventTracer)
}

// NewHotStuffConsensusWithGenesis creates a new HotStuff consensus coordinator with a shared genesis block.
func NewHotStuffConsensusWithGenesis(nodeID types.NodeID, config *types.ConsensusConfig, genesisBlock *types.Block, eventTracer events.EventTracer) (*HotStuffConsensus, error) {
	if config == nil {
		return nil, fmt.Errorf("consensus configuration cannot be nil")
	}
	
	if !config.IsValidNodeID(nodeID) {
		return nil, fmt.Errorf("invalid node ID: %d", nodeID)
	}
	
	if genesisBlock == nil {
		return nil, fmt.Errorf("genesis block cannot be nil")
	}
	
	if !genesisBlock.IsGenesis() {
		return nil, fmt.Errorf("provided block is not a valid genesis block")
	}
	
	blockTree := NewBlockTree(genesisBlock)
	safetyRules := NewSafetyRules()
	votingRule := NewVotingRule(config.QuorumThreshold())
	
	return &HotStuffConsensus{
		nodeID:      nodeID,
		view:        0,
		config:      config,
		eventTracer: eventTracer,
		blockTree:   blockTree,
		safetyRules: safetyRules,
		votingRule:  votingRule,
		lockedQC:    nil,
		prepareQC:   nil,
	}, nil
}

// GetNodeID returns this node's identifier.
func (hc *HotStuffConsensus) GetNodeID() types.NodeID {
	return hc.nodeID
}

// GetCurrentView returns the current consensus view number.
func (hc *HotStuffConsensus) GetCurrentView() types.ViewNumber {
	return hc.view
}

// GetBlockTree returns the block tree manager.
func (hc *HotStuffConsensus) GetBlockTree() *BlockTree {
	return hc.blockTree
}

// GetLockedQC returns the current locked quorum certificate.
func (hc *HotStuffConsensus) GetLockedQC() *types.QuorumCertificate {
	return hc.lockedQC
}

// GetPrepareQC returns the current prepare quorum certificate.
func (hc *HotStuffConsensus) GetPrepareQC() *types.QuorumCertificate {
	return hc.prepareQC
}

// ProcessProposal processes a block proposal message according to HotStuff protocol.
// This handles the prepare phase of the consensus.
func (hc *HotStuffConsensus) ProcessProposal(block *types.Block) (*types.Vote, error) {
	if block == nil {
		return nil, fmt.Errorf("block cannot be nil")
	}
	
	// Validate block structure
	if err := block.Validate(hc.config); err != nil {
		// Record validation failure event
		if hc.eventTracer != nil {
			hc.eventTracer.RecordEvent(uint16(hc.nodeID), events.EventProposalRejected, events.EventPayload{
				"block_hash": block.Hash,
				"view":       block.View,
				"reason":     "validation_failed",
				"error":      err.Error(),
			})
		}
		return nil, fmt.Errorf("invalid block: %w", err)
	}
	
	// Check if we're in the correct view
	if block.View != hc.view {
		return nil, fmt.Errorf("block view %d does not match current view %d", block.View, hc.view)
	}
	
	// Verify the proposer is the expected leader for this view
	expectedLeader, err := hc.config.GetLeaderForView(hc.view)
	if err != nil {
		return nil, fmt.Errorf("failed to get leader for view %d: %w", hc.view, err)
	}
	if block.Proposer != expectedLeader {
		return nil, fmt.Errorf("block proposer %d is not the expected leader %d for view %d", 
			block.Proposer, expectedLeader, hc.view)
	}
	
	// Add block to the block tree
	if err := hc.blockTree.AddBlock(block); err != nil {
		return nil, fmt.Errorf("failed to add block to tree: %w", err)
	}
	
	// Block addition event is now handled by coordinator
	
	// Check safety rules before voting
	canVote, err := hc.safetyRules.CanVote(block, hc.lockedQC, hc.blockTree)
	if err != nil {
		return nil, fmt.Errorf("safety rule check failed: %w", err)
	}
	if !canVote {
		// Record safety rule rejection event
		if hc.eventTracer != nil {
			hc.eventTracer.RecordEvent(uint16(hc.nodeID), events.EventProposalRejected, events.EventPayload{
				"block_hash": block.Hash,
				"view":       block.View,
				"reason":     "safety_rules",
			})
		}
		return nil, fmt.Errorf("safety rules prevent voting for block %x", block.Hash)
	}
	
	// SafeNode predicate event is now handled by coordinator
	
	// Create and return prepare vote (unsigned - coordinator will sign it)
	vote := types.NewVote(
		block.Hash,
		hc.view,
		types.PhasePrepare,
		hc.nodeID,
		nil, // Unsigned - coordinator will handle signing
	)
	
	// Record vote creation event at consensus engine level
	if hc.eventTracer != nil {
		hc.eventTracer.RecordEvent(uint16(hc.nodeID), events.EventBlockValidated, events.EventPayload{
			"block_hash":   block.Hash,
			"view":         block.View,
			"vote_created": true,
			"phase":        types.PhasePrepare,
		})
	}
	
	// Record that we voted in this view to prevent double voting
	hc.safetyRules.RecordVote(hc.view)
	
	return vote, nil
}

// ProcessVote processes a vote message and potentially forms a quorum certificate.
func (hc *HotStuffConsensus) ProcessVote(vote *types.Vote) (*types.QuorumCertificate, error) {
	if vote == nil {
		return nil, fmt.Errorf("vote cannot be nil")
	}
	
	// Validate vote
	if err := vote.Validate(hc.config); err != nil {
		return nil, fmt.Errorf("invalid vote: %w", err)
	}
	
	// Add vote to voting rule
	qc, err := hc.votingRule.AddVote(vote, hc.config)
	if err != nil {
		return nil, fmt.Errorf("failed to add vote: %w", err)
	}
	
	// If we formed a quorum certificate, process it
	if qc != nil {
		if err := hc.processQuorumCertificate(qc); err != nil {
			return nil, fmt.Errorf("failed to process quorum certificate: %w", err)
		}
	}
	
	return qc, nil
}

// ProcessQC processes a received quorum certificate directly.
// This is used when a QC is received from another node rather than being formed locally.
func (hc *HotStuffConsensus) ProcessQC(qc *types.QuorumCertificate) error {
	if qc == nil {
		return fmt.Errorf("quorum certificate cannot be nil")
	}
	
	return hc.processQuorumCertificate(qc)
}

// ProcessTimeout handles view timeout and advances to the next view.
func (hc *HotStuffConsensus) ProcessTimeout() error {
	// Advance to next view
	hc.view++
	
	// Clear view-specific state
	hc.votingRule.ClearView(hc.view - 1)
	
	return nil
}

// AdvanceView advances the consensus to the next view.
func (hc *HotStuffConsensus) AdvanceView() {
	hc.view++
	hc.votingRule.ClearView(hc.view - 1)
}

// processQuorumCertificate processes a newly formed quorum certificate according to HotStuff rules.
func (hc *HotStuffConsensus) processQuorumCertificate(qc *types.QuorumCertificate) error {
	switch qc.Phase {
	case types.PhasePrepare:
		return hc.processPrepareQC(qc)
	case types.PhasePreCommit:
		return hc.processPreCommitQC(qc)
	case types.PhaseCommit:
		return hc.processCommitQC(qc)
	default:
		return fmt.Errorf("unknown consensus phase: %s", qc.Phase)
	}
}

// processPrepareQC handles a prepare phase quorum certificate.
func (hc *HotStuffConsensus) processPrepareQC(qc *types.QuorumCertificate) error {
	// Update prepare QC
	hc.prepareQC = qc
	
	// Update safety rules
	if err := hc.safetyRules.UpdatePrepareQC(qc); err != nil {
		return fmt.Errorf("failed to update prepare QC in safety rules: %w", err)
	}
	
	return nil
}

// processPreCommitQC handles a pre-commit phase quorum certificate.
// SAFETY FIX: PreCommit QC processing should NOT update lockedQC
func (hc *HotStuffConsensus) processPreCommitQC(qc *types.QuorumCertificate) error {
	// Store PreCommitQC for later use, but DO NOT update lockedQC
	hc.preCommitQC = qc
	
	// Update safety rules with PreCommit QC (but not locked QC)
	if err := hc.safetyRules.UpdatePreCommitQC(qc); err != nil {
		return fmt.Errorf("failed to update precommit QC in safety rules: %w", err)
	}
	
	return nil
}

// processCommitQC handles a commit phase quorum certificate.
func (hc *HotStuffConsensus) processCommitQC(qc *types.QuorumCertificate) error {
	// Get the block being committed
	block, err := hc.blockTree.GetBlock(qc.BlockHash)
	if err != nil {
		return fmt.Errorf("failed to get block %x from tree: %w", qc.BlockHash, err)
	}
	
	// Commit the block
	if err := hc.blockTree.CommitBlock(block); err != nil {
		return fmt.Errorf("failed to commit block %x: %w", qc.BlockHash, err)
	}
	
	return nil
}

// UpdateLockedQC updates the locked QC with a PreCommitQC (HotStuff Algorithm 2 line 25).
// This should only be called during the Commit phase after receiving a PreCommitQC.
func (hc *HotStuffConsensus) UpdateLockedQC(preCommitQC *types.QuorumCertificate) error {
	if preCommitQC == nil {
		return fmt.Errorf("precommit QC cannot be nil")
	}
	
	if preCommitQC.Phase != types.PhasePreCommit {
		return fmt.Errorf("can only update lockedQC with PreCommit QC, got %s", preCommitQC.Phase)
	}
	
	// Update the locked QC (HotStuff Algorithm 2 line 25)
	hc.lockedQC = preCommitQC
	
	// Update safety rules
	if err := hc.safetyRules.UpdateLockedQC(preCommitQC); err != nil {
		return fmt.Errorf("failed to update locked QC in safety rules: %w", err)
	}
	
	return nil
}

// String returns a string representation of the consensus state for debugging.
func (hc *HotStuffConsensus) String() string {
	lockedQCStr := "nil"
	if hc.lockedQC != nil {
		lockedQCStr = hc.lockedQC.String()
	}
	
	prepareQCStr := "nil"
	if hc.prepareQC != nil {
		prepareQCStr = hc.prepareQC.String()
	}
	
	return fmt.Sprintf("HotStuffConsensus{NodeID: %d, View: %d, LockedQC: %s, PrepareQC: %s}", 
		hc.nodeID, hc.view, lockedQCStr, prepareQCStr)
}