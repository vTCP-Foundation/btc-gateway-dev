package types

import (
	"fmt"
)

// QuorumCertificate represents a collection of votes that form a quorum for a specific block and phase.
// It serves as proof that enough validators have voted for a particular block.
type QuorumCertificate struct {
	// BlockHash is the hash of the block this QC certifies
	BlockHash BlockHash
	// View is the consensus view number
	View ViewNumber
	// Phase indicates which consensus phase this QC belongs to
	Phase ConsensusPhase
	// Votes contains the individual votes from participants (no signature aggregation)
	Votes []Vote
	// VoterBitmap represents which validators participated as a bitmap
	VoterBitmap []byte
}

// NewQuorumCertificate creates a new QuorumCertificate from a collection of votes.
func NewQuorumCertificate(blockHash BlockHash, view ViewNumber, phase ConsensusPhase, votes []Vote, config *ConsensusConfig) (*QuorumCertificate, error) {
	qc := &QuorumCertificate{
		BlockHash: blockHash,
		View:      view,
		Phase:     phase,
		Votes:     make([]Vote, len(votes)),
	}

	copy(qc.Votes, votes)

	if config != nil {
		voters := make([]NodeID, len(votes))
		for i, vote := range votes {
			voters[i] = vote.Voter
		}

		bitmap, err := config.CreateVoterBitmap(voters)
		if err != nil {
			return nil, fmt.Errorf("failed to create voter bitmap: %w", err)
		}
		qc.VoterBitmap = bitmap
	}

	return qc, nil
}

// Validate performs validation on the QuorumCertificate structure.
func (qc *QuorumCertificate) Validate(config *ConsensusConfig) error {
	if qc.BlockHash == (BlockHash{}) {
		return fmt.Errorf("QC block hash cannot be empty")
	}

	if !qc.Phase.IsValid() {
		return fmt.Errorf("invalid consensus phase: %d", qc.Phase)
	}

	if qc.Phase == PhaseNone {
		return fmt.Errorf("QC cannot be in PhaseNone")
	}

	if len(qc.Votes) == 0 {
		return fmt.Errorf("QC must contain at least one vote")
	}

	// Check quorum threshold if config is provided
	if config != nil && !config.HasQuorum(len(qc.Votes)) {
		return fmt.Errorf("QC does not meet quorum threshold: got %d votes, need %d",
			len(qc.Votes), config.QuorumThreshold())
	}

	// Validate all votes are for the same block, view, and phase
	for i, vote := range qc.Votes {
		if err := vote.Validate(config); err != nil {
			return fmt.Errorf("invalid vote at index %d: %w", i, err)
		}

		if vote.BlockHash != qc.BlockHash {
			return fmt.Errorf("vote %d has different block hash", i)
		}

		if vote.View != qc.View {
			return fmt.Errorf("vote %d has different view", i)
		}

		if vote.Phase != qc.Phase {
			return fmt.Errorf("vote %d has different phase", i)
		}
	}

	// Check for duplicate voters
	voterMap := make(map[NodeID]bool)
	for i, vote := range qc.Votes {
		if voterMap[vote.Voter] {
			return fmt.Errorf("duplicate voter %d at index %d", vote.Voter, i)
		}
		voterMap[vote.Voter] = true
	}

	return nil
}

// HasQuorum returns true if the QC contains enough votes to form a quorum using the provided config.
func (qc *QuorumCertificate) HasQuorum(config *ConsensusConfig) bool {
	if config == nil {
		return len(qc.Votes) > 0 // Fallback to any votes if no config
	}
	return config.HasQuorum(len(qc.Votes))
}

// GetVoters returns the list of node IDs that voted in this QC.
func (qc *QuorumCertificate) GetVoters() []NodeID {
	voters := make([]NodeID, len(qc.Votes))
	for i, vote := range qc.Votes {
		voters[i] = vote.Voter
	}
	return voters
}

// GetVotersFromBitmap returns the list of voters represented in the bitmap using the config.
func (qc *QuorumCertificate) GetVotersFromBitmap(config *ConsensusConfig) []NodeID {
	if config == nil {
		return qc.GetVoters() // Fallback to vote list
	}
	return config.ParseVoterBitmap(qc.VoterBitmap)
}

// String returns a string representation of the QC for debugging.
func (qc *QuorumCertificate) String() string {
	return fmt.Sprintf("QC{BlockHash: %x, View: %d, Phase: %s, Votes: %d}",
		qc.BlockHash[:8], qc.View, qc.Phase, len(qc.Votes))
}
