package engine

import (
	"fmt"

	"btc-gateway/pkg/consensus/types"
)

// SafetyRules implements the safety properties of the HotStuff consensus protocol.
// It prevents conflicting votes and ensures that the consensus maintains safety invariants.
type SafetyRules struct {
	// lockedQC is the quorum certificate that this node is locked on
	lockedQC *types.QuorumCertificate
	
	// prepareQC is the highest prepare quorum certificate seen
	prepareQC *types.QuorumCertificate
	
	// preCommitQC is the highest pre-commit quorum certificate seen
	preCommitQC *types.QuorumCertificate
	
	// lastVotedView tracks the last view in which this node voted to prevent double voting
	lastVotedView types.ViewNumber
	
	// hasVoted tracks whether we have voted before (to handle initial view 0)
	hasVoted bool
}

// NewSafetyRules creates a new SafetyRules instance with empty state.
func NewSafetyRules() *SafetyRules {
	return &SafetyRules{
		lockedQC:      nil,
		prepareQC:     nil,
		lastVotedView: 0,
		hasVoted:      false,
	}
}

// CanVote determines whether it's safe to vote for a given block according to HotStuff safety rules.
// The safety rules prevent voting for conflicting blocks that could lead to safety violations.
func (sr *SafetyRules) CanVote(block *types.Block, currentLockedQC *types.QuorumCertificate, blockTree *BlockTree) (bool, error) {
	if block == nil {
		return false, fmt.Errorf("block cannot be nil")
	}
	
	if blockTree == nil {
		return false, fmt.Errorf("blockTree cannot be nil")
	}
	
	// Rule 1: Don't vote twice in the same view (prevent equivocation)
	if sr.hasVoted && block.View <= sr.lastVotedView {
		return false, nil
	}
	
	// Rule 2: If we have a locked QC, only vote for blocks that extend the locked chain
	// SAFENODE predicate: (block extends from lockedQC.block) OR (justify.view > lockedQC.view)
	if sr.lockedQC != nil {
		// The block must either:
		// a) Be the same block we're locked on, or
		// b) Have the locked block as an ancestor (proper ancestry check)
		if block.Hash != sr.lockedQC.BlockHash {
			// Proper SAFENODE check: verify block extends from locked block
			isDescendant, err := blockTree.IsAncestor(sr.lockedQC.BlockHash, block.Hash)
			if err != nil {
				return false, fmt.Errorf("failed to verify block ancestry for safety check: %w", err)
			}
			if !isDescendant {
				return false, nil // Block doesn't extend from locked block
			}
		}
	}
	
	// Rule 3: Additional safety check - if we have a current locked QC parameter,
	// ensure consistency with our internal state
	if currentLockedQC != nil && sr.lockedQC != nil {
		if currentLockedQC.View < sr.lockedQC.View {
			return false, fmt.Errorf("provided locked QC is older than our internal locked QC")
		}
	}
	
	return true, nil
}

// UpdateLockedQC updates the locked quorum certificate when we receive a prepare or pre-commit QC.
// This represents a commitment to a specific chain that we cannot abandon.
// For non-pipelined HotStuff, locking happens on PrepareQC as per the data flow specification.
func (sr *SafetyRules) UpdateLockedQC(qc *types.QuorumCertificate) error {
	if qc == nil {
		return fmt.Errorf("quorum certificate cannot be nil")
	}
	
	// ARCHITECTURAL FIX: Non-pipelined HotStuff locks on PrepareQC per data flow diagram
	// Standard pipelined HotStuff locks on PreCommit, but this is Simple HotStuff
	if qc.Phase != types.PhasePrepare && qc.Phase != types.PhasePreCommit {
		return fmt.Errorf("only prepare or pre-commit QCs can update locked QC, got %s", qc.Phase)
	}
	
	// Only update if the new QC is from a higher or equal view
	if sr.lockedQC == nil || qc.View >= sr.lockedQC.View {
		sr.lockedQC = qc
	}
	
	return nil
}

// UpdatePrepareQC updates the prepare quorum certificate when we receive a prepare QC.
// This helps with liveness by tracking the highest prepare QC seen.
func (sr *SafetyRules) UpdatePrepareQC(qc *types.QuorumCertificate) error {
	if qc == nil {
		return fmt.Errorf("quorum certificate cannot be nil")
	}
	
	if qc.Phase != types.PhasePrepare {
		return fmt.Errorf("only prepare QCs can update prepare QC, got %s", qc.Phase)
	}
	
	// Only update if the new QC is from a higher or equal view
	if sr.prepareQC == nil || qc.View >= sr.prepareQC.View {
		sr.prepareQC = qc
	}
	
	return nil
}

// UpdatePreCommitQC updates the pre-commit quorum certificate when we receive a precommit QC.
// This is tracked for protocol completeness but does NOT update lockedQC.
func (sr *SafetyRules) UpdatePreCommitQC(qc *types.QuorumCertificate) error {
	if qc == nil {
		return fmt.Errorf("quorum certificate cannot be nil")
	}
	
	if qc.Phase != types.PhasePreCommit {
		return fmt.Errorf("expected pre-commit QC, got %s", qc.Phase)
	}
	
	// Store for tracking purposes (no safety rule implications)
	sr.preCommitQC = qc
	
	return nil
}

// ValidateProposal validates that a block proposal is safe to consider.
// This is called before even attempting to vote on a proposal.
func (sr *SafetyRules) ValidateProposal(block *types.Block) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}
	
	// Check that the proposal doesn't conflict with our locked state
	if sr.lockedQC != nil {
		// If we're locked on a different block at the same or higher view,
		// this proposal might conflict
		if block.View <= sr.lockedQC.View && block.Hash != sr.lockedQC.BlockHash {
			// In a complete implementation, we would do a full ancestry check
			// For now, we use a simplified heuristic
			return fmt.Errorf("proposal may conflict with locked QC")
		}
	}
	
	return nil
}

// RecordVote records that we voted in a particular view to prevent double voting.
func (sr *SafetyRules) RecordVote(view types.ViewNumber) {
	sr.hasVoted = true
	if view > sr.lastVotedView {
		sr.lastVotedView = view
	}
}

// GetLockedQC returns the current locked quorum certificate.
func (sr *SafetyRules) GetLockedQC() *types.QuorumCertificate {
	return sr.lockedQC
}

// GetPrepareQC returns the current prepare quorum certificate.
func (sr *SafetyRules) GetPrepareQC() *types.QuorumCertificate {
	return sr.prepareQC
}

// GetLastVotedView returns the last view in which this node voted.
func (sr *SafetyRules) GetLastVotedView() types.ViewNumber {
	return sr.lastVotedView
}

// IsLocked returns true if this node is locked on a specific block.
func (sr *SafetyRules) IsLocked() bool {
	return sr.lockedQC != nil
}

// GetLockedBlock returns the hash of the block we're locked on, if any.
func (sr *SafetyRules) GetLockedBlock() (types.BlockHash, bool) {
	if sr.lockedQC == nil {
		return types.BlockHash{}, false
	}
	return sr.lockedQC.BlockHash, true
}

// Reset clears all safety state. This should only be used in testing or restart scenarios.
func (sr *SafetyRules) Reset() {
	sr.lockedQC = nil
	sr.prepareQC = nil
	sr.lastVotedView = 0
	sr.hasVoted = false
}

// ValidateQCTransition validates that transitioning from one QC to another is safe.
func (sr *SafetyRules) ValidateQCTransition(fromQC, toQC *types.QuorumCertificate) error {
	if fromQC == nil || toQC == nil {
		return fmt.Errorf("both QCs must be non-nil for transition validation")
	}
	
	// View must advance or stay the same
	if toQC.View < fromQC.View {
		return fmt.Errorf("QC transition must not go backwards in view: %d -> %d", 
			fromQC.View, toQC.View)
	}
	
	// Phase transitions must follow the protocol
	switch fromQC.Phase {
	case types.PhasePrepare:
		if toQC.Phase != types.PhasePreCommit && toQC.Phase != types.PhasePrepare {
			return fmt.Errorf("invalid phase transition from Prepare to %s", toQC.Phase)
		}
	case types.PhasePreCommit:
		if toQC.Phase != types.PhaseCommit && toQC.Phase != types.PhasePrepare {
			return fmt.Errorf("invalid phase transition from PreCommit to %s", toQC.Phase)
		}
	case types.PhaseCommit:
		if toQC.Phase != types.PhasePrepare {
			return fmt.Errorf("invalid phase transition from Commit to %s", toQC.Phase)
		}
	}
	
	return nil
}

// String returns a string representation of the safety rules state for debugging.
func (sr *SafetyRules) String() string {
	lockedStr := "nil"
	if sr.lockedQC != nil {
		lockedStr = sr.lockedQC.String()
	}
	
	prepareStr := "nil"
	if sr.prepareQC != nil {
		prepareStr = sr.prepareQC.String()
	}
	
	return fmt.Sprintf("SafetyRules{LockedQC: %s, PrepareQC: %s, LastVoted: %d}", 
		lockedStr, prepareStr, sr.lastVotedView)
}