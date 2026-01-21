// Package types defines the fundamental data types used throughout the HotStuff consensus protocol.
package types

import (
	"fmt"
	"time"
)

// ViewNumber represents a consensus view identifier.
// Each view corresponds to a potential leadership period.
type ViewNumber uint64

// NodeID represents a unique identifier for a consensus participant.
// It corresponds to the node's index in the consensus configuration.
type NodeID uint16

// String returns a string representation of the NodeID.
func (n NodeID) String() string {
	return fmt.Sprintf("%d", n)
}

// BlockHash represents a SHA-256 hash of a block.
type BlockHash [32]byte

// Height represents the position of a block in the blockchain.
type Height uint64

// Timestamp represents the time when a block was created.
type Timestamp time.Time

// MarshalJSON implements json.Marshaler for Timestamp.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return time.Time(t).MarshalJSON()
}

// UnmarshalJSON implements json.Unmarshaler for Timestamp.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var ts time.Time
	if err := ts.UnmarshalJSON(data); err != nil {
		return err
	}
	*t = Timestamp(ts)
	return nil
}

// ConsensusPhase represents the current phase of the HotStuff consensus protocol.
type ConsensusPhase uint8

const (
	// PhaseNone indicates no active consensus phase
	PhaseNone ConsensusPhase = iota
	// PhasePrepare is the first phase where nodes vote on block proposals
	PhasePrepare
	// PhasePreCommit is the second phase where nodes commit to the proposal
	PhasePreCommit
	// PhaseCommit is the final phase where the block is committed to the chain
	PhaseCommit
)

// String returns a human-readable representation of the consensus phase.
func (p ConsensusPhase) String() string {
	switch p {
	case PhaseNone:
		return "None"
	case PhasePrepare:
		return "Prepare"
	case PhasePreCommit:
		return "PreCommit"
	case PhaseCommit:
		return "Commit"
	default:
		return "Unknown"
	}
}

// IsValid returns true if the consensus phase is valid.
func (p ConsensusPhase) IsValid() bool {
	return p >= PhaseNone && p <= PhaseCommit
}
