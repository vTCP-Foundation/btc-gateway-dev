// Package messages defines the message types used in the HotStuff consensus protocol.
package messages

import (
	"btc-gateway/pkg/consensus/types"
	"fmt"
)

// MessageType represents the different types of consensus messages.
type MessageType uint8

const (
	// MsgTypeProposal represents a block proposal message
	MsgTypeProposal MessageType = iota
	// MsgTypeVote represents a vote message
	MsgTypeVote
	// MsgTypePrepareQC represents a prepare quorum certificate message
	MsgTypePrepareQC
	// MsgTypePreCommitQC represents a pre-commit quorum certificate message
	MsgTypePreCommitQC
	// MsgTypeCommitQC represents a commit quorum certificate message
	MsgTypeCommitQC
	// MsgTypeTimeout represents a timeout message for view changes
	MsgTypeTimeout
	// MsgTypeNewView represents a new view announcement message
	MsgTypeNewView
)

// String returns a human-readable representation of the message type.
func (mt MessageType) String() string {
	switch mt {
	case MsgTypeProposal:
		return "Proposal"
	case MsgTypeVote:
		return "Vote"
	case MsgTypePrepareQC:
		return "PrepareQC"
	case MsgTypePreCommitQC:
		return "PreCommitQC"
	case MsgTypeCommitQC:
		return "CommitQC"
	case MsgTypeTimeout:
		return "Timeout"
	case MsgTypeNewView:
		return "NewView"
	default:
		return "Unknown"
	}
}

// ConsensusMessage defines the common interface for all consensus messages.
type ConsensusMessage interface {
	// Type returns the message type
	Type() MessageType
	// View returns the consensus view number for this message
	View() types.ViewNumber
	// Sender returns the node that sent this message
	Sender() types.NodeID
	// Validate performs basic validation on the message
	Validate(config *types.ConsensusConfig) error
}

// ProposalMsg represents a block proposal message sent by the leader.
type ProposalMsg struct {
	// Block is the proposed block
	Block *types.Block
	// HighQC is the highest quorum certificate known to the proposer
	HighQC *types.QuorumCertificate
	// ViewNumber is the current view
	ViewNumber types.ViewNumber
	// Proposer is the node proposing this block
	Proposer types.NodeID
}

// NewProposalMsg creates a new block proposal message.
func NewProposalMsg(block *types.Block, highQC *types.QuorumCertificate, view types.ViewNumber, proposer types.NodeID) *ProposalMsg {
	return &ProposalMsg{
		Block:      block,
		HighQC:     highQC,
		ViewNumber: view,
		Proposer:   proposer,
	}
}

// Type returns the message type.
func (pm *ProposalMsg) Type() MessageType {
	return MsgTypeProposal
}

// View returns the consensus view number.
func (pm *ProposalMsg) View() types.ViewNumber {
	return pm.ViewNumber
}

// Sender returns the node that sent this message.
func (pm *ProposalMsg) Sender() types.NodeID {
	return pm.Proposer
}

// Validate performs basic validation on the proposal message.
func (pm *ProposalMsg) Validate(config *types.ConsensusConfig) error {
	if pm.Block == nil {
		return fmt.Errorf("proposal block cannot be nil")
	}

	if err := pm.Block.Validate(config); err != nil {
		return fmt.Errorf("invalid proposal block: %w", err)
	}

	if pm.Block.View != pm.ViewNumber {
		return fmt.Errorf("block view %d does not match message view %d", pm.Block.View, pm.ViewNumber)
	}

	if pm.Block.Proposer != pm.Proposer {
		return fmt.Errorf("block proposer %d does not match message proposer %d", pm.Block.Proposer, pm.Proposer)
	}

	if config != nil && !config.IsValidNodeID(pm.Proposer) {
		return fmt.Errorf("invalid proposer node ID: %d", pm.Proposer)
	}

	// HighQC can be nil for genesis blocks
	if pm.HighQC != nil {
		if err := pm.HighQC.Validate(config); err != nil {
			return fmt.Errorf("invalid high QC: %w", err)
		}
	}

	return nil
}

// VoteMsg represents a vote message sent by consensus participants.
type VoteMsg struct {
	// Vote is the actual vote being cast
	Vote *types.Vote
	// Voter is the node casting this vote
	Voter types.NodeID
}

// NewVoteMsg creates a new vote message.
func NewVoteMsg(vote *types.Vote, voter types.NodeID) *VoteMsg {
	return &VoteMsg{
		Vote:  vote,
		Voter: voter,
	}
}

// Type returns the message type.
func (vm *VoteMsg) Type() MessageType {
	return MsgTypeVote
}

// View returns the consensus view number.
func (vm *VoteMsg) View() types.ViewNumber {
	if vm.Vote == nil {
		return 0
	}
	return vm.Vote.View
}

// Sender returns the node that sent this message.
func (vm *VoteMsg) Sender() types.NodeID {
	return vm.Voter
}

// Validate performs basic validation on the vote message.
func (vm *VoteMsg) Validate(config *types.ConsensusConfig) error {
	if vm.Vote == nil {
		return fmt.Errorf("vote cannot be nil")
	}

	if err := vm.Vote.Validate(config); err != nil {
		return fmt.Errorf("invalid vote: %w", err)
	}

	if vm.Vote.Voter != vm.Voter {
		return fmt.Errorf("vote voter %d does not match message voter %d", vm.Vote.Voter, vm.Voter)
	}

	if config != nil && !config.IsValidNodeID(vm.Voter) {
		return fmt.Errorf("invalid voter node ID: %d", vm.Voter)
	}

	return nil
}

// TimeoutMsg represents a timeout message sent when a view times out.
type TimeoutMsg struct {
	// ViewNumber is the view that timed out
	ViewNumber types.ViewNumber
	// HighQC is the highest QC known to the sender
	HighQC *types.QuorumCertificate
	// SenderID is the node that detected the timeout
	SenderID types.NodeID
	// Signature is the signature of this timeout message
	Signature []byte
}

// NewTimeoutMsg creates a new timeout message.
func NewTimeoutMsg(view types.ViewNumber, highQC *types.QuorumCertificate, sender types.NodeID, signature []byte) *TimeoutMsg {
	return &TimeoutMsg{
		ViewNumber: view,
		HighQC:     highQC,
		SenderID:   sender,
		Signature:  signature,
	}
}

// Type returns the message type.
func (tm *TimeoutMsg) Type() MessageType {
	return MsgTypeTimeout
}

// View returns the consensus view number.
func (tm *TimeoutMsg) View() types.ViewNumber {
	return tm.ViewNumber
}

// Sender returns the node that sent this message.
func (tm *TimeoutMsg) Sender() types.NodeID {
	return tm.SenderID
}

// Validate performs basic validation on the timeout message.
func (tm *TimeoutMsg) Validate(config *types.ConsensusConfig) error {
	if config != nil && !config.IsValidNodeID(tm.SenderID) {
		return fmt.Errorf("invalid sender node ID: %d", tm.SenderID)
	}

	if len(tm.Signature) == 0 {
		return fmt.Errorf("timeout signature cannot be empty")
	}

	// HighQC can be nil
	if tm.HighQC != nil {
		if err := tm.HighQC.Validate(config); err != nil {
			return fmt.Errorf("invalid high QC: %w", err)
		}
	}

	return nil
}

// NewViewMsg represents a NewView message sent by a validator to the leader.
type NewViewMsg struct {
    // ViewNumber is the new view number
    ViewNumber types.ViewNumber
    // HighQC is the highest QC that justifies this view change
    HighQC *types.QuorumCertificate
    // TimeoutCerts contains timeout certificates that justify the view change
    TimeoutCerts []TimeoutMsg
    // SenderID is the validator sending this NewView to the leader
    SenderID types.NodeID
    // Signature is the cryptographic signature of this NewView message
    Signature []byte
}

// NewNewViewMsg creates a new view message.
func NewNewViewMsg(view types.ViewNumber, highQC *types.QuorumCertificate, timeouts []TimeoutMsg, sender types.NodeID, signature []byte) *NewViewMsg {
    return &NewViewMsg{
        ViewNumber:   view,
        HighQC:       highQC,
        TimeoutCerts: timeouts,
        SenderID:     sender,
        Signature:    signature,
    }
}

// Type returns the message type.
func (nvm *NewViewMsg) Type() MessageType {
	return MsgTypeNewView
}

// View returns the consensus view number.
func (nvm *NewViewMsg) View() types.ViewNumber {
    return nvm.ViewNumber
}

// Sender returns the node that sent this message.
func (nvm *NewViewMsg) Sender() types.NodeID {
    return nvm.SenderID
}

// Validate performs basic validation on the new view message.
func (nvm *NewViewMsg) Validate(config *types.ConsensusConfig) error {
    if config != nil && !config.IsValidNodeID(nvm.SenderID) {
        return fmt.Errorf("invalid sender node ID: %d", nvm.SenderID)
    }

    // HighQC can be nil for view 0
    if nvm.HighQC != nil {
        if err := nvm.HighQC.Validate(config); err != nil {
            return fmt.Errorf("invalid high QC: %w", err)
        }
    }

	// Validate timeout certificates
	for i, timeout := range nvm.TimeoutCerts {
		if err := timeout.Validate(config); err != nil {
			return fmt.Errorf("invalid timeout certificate at index %d: %w", i, err)
		}

		// All timeouts should be for the previous view
		if timeout.ViewNumber != nvm.ViewNumber-1 {
			return fmt.Errorf("timeout certificate %d has view %d, expected %d",
				i, timeout.ViewNumber, nvm.ViewNumber-1)
		}
	}

    // Signature must be present (cryptographic verification done separately)
    if len(nvm.Signature) == 0 {
        return fmt.Errorf("NewView message signature cannot be empty")
    }

    return nil
}

// QCMsg represents a quorum certificate message for broadcasting QCs (lines 88, 111, 135, 158)
type QCMsg struct {
	// QC is the quorum certificate being broadcast
	QC *types.QuorumCertificate
	// QCType indicates which phase this QC is for
	QCType MessageType
	// SenderID is the node broadcasting this QC
	SenderID types.NodeID
}

// NewPrepareQCMsg creates a new PrepareQC broadcast message (line 88-89)
func NewPrepareQCMsg(qc *types.QuorumCertificate, sender types.NodeID) *QCMsg {
	return &QCMsg{
		QC:       qc,
		QCType:   MsgTypePrepareQC,
		SenderID: sender,
	}
}

// NewPreCommitQCMsg creates a new PreCommitQC broadcast message (line 135-136)
func NewPreCommitQCMsg(qc *types.QuorumCertificate, sender types.NodeID) *QCMsg {
	return &QCMsg{
		QC:       qc,
		QCType:   MsgTypePreCommitQC,
		SenderID: sender,
	}
}

// NewCommitQCMsg creates a new CommitQC broadcast message (line 158-159)
func NewCommitQCMsg(qc *types.QuorumCertificate, sender types.NodeID) *QCMsg {
	return &QCMsg{
		QC:       qc,
		QCType:   MsgTypeCommitQC,
		SenderID: sender,
	}
}

// Type returns the message type.
func (qm *QCMsg) Type() MessageType {
	return qm.QCType
}

// View returns the consensus view number.
func (qm *QCMsg) View() types.ViewNumber {
	if qm.QC == nil {
		return 0
	}
	return qm.QC.View
}

// Sender returns the node that sent this message.
func (qm *QCMsg) Sender() types.NodeID {
	return qm.SenderID
}

// Validate performs basic validation on the QC message.
func (qm *QCMsg) Validate(config *types.ConsensusConfig) error {
	if qm.QC == nil {
		return fmt.Errorf("QC cannot be nil")
	}

	if err := qm.QC.Validate(config); err != nil {
		return fmt.Errorf("invalid QC: %w", err)
	}

	if config != nil && !config.IsValidNodeID(qm.SenderID) {
		return fmt.Errorf("invalid sender node ID: %d", qm.SenderID)
	}

	// Validate QC type matches message type
	var expectedPhase types.ConsensusPhase
	switch qm.QCType {
	case MsgTypePrepareQC:
		expectedPhase = types.PhasePrepare
	case MsgTypePreCommitQC:
		expectedPhase = types.PhasePreCommit
	case MsgTypeCommitQC:
		expectedPhase = types.PhaseCommit
	default:
		return fmt.Errorf("invalid QC message type: %s", qm.QCType)
	}

	if qm.QC.Phase != expectedPhase {
		return fmt.Errorf("QC phase %s does not match message type %s", qm.QC.Phase, qm.QCType)
	}

	return nil
}
