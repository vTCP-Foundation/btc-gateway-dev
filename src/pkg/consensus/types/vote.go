package types

import (
	"fmt"
)

// Vote represents a vote cast by a consensus participant for a specific block in a given phase.
type Vote struct {
	// BlockHash is the hash of the block being voted on
	BlockHash BlockHash
	// View is the consensus view number for this vote
	View ViewNumber
	// Phase indicates which consensus phase this vote belongs to
	Phase ConsensusPhase
	// Voter is the node that cast this vote
	Voter NodeID
	// Signature is the cryptographic signature of this vote (abstract, no aggregation)
	Signature []byte
}

// NewVote creates a new vote for the specified block, view, phase, and voter.
// The signature parameter should be provided by the cryptographic layer.
func NewVote(blockHash BlockHash, view ViewNumber, phase ConsensusPhase, voter NodeID, signature []byte) *Vote {
	return &Vote{
		BlockHash: blockHash,
		View:      view,
		Phase:     phase,
		Voter:     voter,
		Signature: signature,
	}
}

// Validate performs basic validation on the vote structure.
func (v *Vote) Validate(config *ConsensusConfig) error {
	if config == nil {
		return fmt.Errorf("consensus configuration cannot be nil")
	}

	if !config.IsValidNodeID(v.Voter) {
		return fmt.Errorf("invalid voter node ID: %d", v.Voter)
	}

	if !v.Phase.IsValid() {
		return fmt.Errorf("invalid consensus phase: %d", v.Phase)
	}

	if v.Phase == PhaseNone {
		return fmt.Errorf("vote cannot be in PhaseNone")
	}

	if v.BlockHash == (BlockHash{}) {
		return fmt.Errorf("vote block hash cannot be empty")
	}

	// todo: add strict validation rule
	// when the cryptography layer will be implemented.
	// for now, we only check if the signature is empty.
	if len(v.Signature) == 0 {
		return fmt.Errorf("vote signature cannot be empty")
	}

	return nil
}

// String returns a string representation of the vote for debugging.
func (v *Vote) String() string {
	return fmt.Sprintf("Vote{BlockHash: %x, View: %d, Phase: %s, Voter: %d}",
		v.BlockHash[:8], v.View, v.Phase, v.Voter)
}

// Equals returns true if two votes are identical in all fields.
func (v *Vote) Equals(other *Vote) bool {
	if other == nil {
		return false
	}

	return v.BlockHash == other.BlockHash &&
		v.View == other.View &&
		v.Phase == other.Phase &&
		v.Voter == other.Voter &&
		len(v.Signature) == len(other.Signature) &&
		string(v.Signature) == string(other.Signature)
}
