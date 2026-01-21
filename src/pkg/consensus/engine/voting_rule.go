package engine

import (
	"fmt"

	"btc-gateway/pkg/consensus/types"
)

// VoteKey represents a unique identifier for a vote collection.
// It combines block hash, view, and phase to uniquely identify a voting context.
type VoteKey struct {
	BlockHash types.BlockHash
	View      types.ViewNumber
	Phase     types.ConsensusPhase
}

// VotingRule manages vote collection and quorum certificate formation for the HotStuff protocol.
// It tracks votes across different views and phases and determines when quorums are reached.
type VotingRule struct {
	// threshold is the minimum number of votes required for a quorum (2f+1)
	threshold int

	// pendingVotes stores votes indexed by their unique key (block, view, phase)
	pendingVotes map[VoteKey]map[types.NodeID]*types.Vote
}

// NewVotingRule creates a new VotingRule instance with the specified quorum threshold.
func NewVotingRule(threshold int) *VotingRule {
	if threshold < 1 {
		threshold = 1 // Minimum threshold
	}

	return &VotingRule{
		threshold:    threshold,
		pendingVotes: make(map[VoteKey]map[types.NodeID]*types.Vote),
	}
}

// AddVote adds a vote to the collection and checks if a quorum is reached.
// Returns a QuorumCertificate if a quorum is formed, nil otherwise.
func (vr *VotingRule) AddVote(vote *types.Vote, config *types.ConsensusConfig) (*types.QuorumCertificate, error) {
	if vote == nil {
		return nil, fmt.Errorf("vote cannot be nil")
	}

	if config == nil {
		return nil, fmt.Errorf("consensus configuration cannot be nil")
	}

	// Validate the vote
	if err := vote.Validate(config); err != nil {
		return nil, fmt.Errorf("invalid vote: %w", err)
	}

	// Create vote key for indexing
	key := VoteKey{
		BlockHash: vote.BlockHash,
		View:      vote.View,
		Phase:     vote.Phase,
	}

	// Initialize vote map for this key if it doesn't exist
	if vr.pendingVotes[key] == nil {
		vr.pendingVotes[key] = make(map[types.NodeID]*types.Vote)
	}

	// Check for duplicate vote from the same node
	if existingVote, exists := vr.pendingVotes[key][vote.Voter]; exists {
		if !vote.Equals(existingVote) {
			return nil, fmt.Errorf("conflicting vote from node %d: existing vote %s, new vote %s",
				vote.Voter, existingVote.String(), vote.String())
		}
		// Same vote, ignore duplicate
		return nil, nil
	}

	// Add the vote
	vr.pendingVotes[key][vote.Voter] = vote

	// Check if we have reached quorum
	if len(vr.pendingVotes[key]) >= vr.threshold {
		return vr.formQuorumCertificate(key, config)
	}

	return nil, nil
}

// CheckQuorum checks if a quorum exists for the specified block, view, and phase.
func (vr *VotingRule) CheckQuorum(blockHash types.BlockHash, view types.ViewNumber, phase types.ConsensusPhase) bool {
	key := VoteKey{
		BlockHash: blockHash,
		View:      view,
		Phase:     phase,
	}

	votes, exists := vr.pendingVotes[key]
	return exists && len(votes) >= vr.threshold
}

// FormQC attempts to form a quorum certificate for the specified block, view, and phase.
// Returns nil if no quorum exists.
func (vr *VotingRule) FormQC(blockHash types.BlockHash, view types.ViewNumber, phase types.ConsensusPhase, config *types.ConsensusConfig) (*types.QuorumCertificate, error) {
	if config == nil {
		return nil, fmt.Errorf("consensus configuration cannot be nil")
	}

	key := VoteKey{
		BlockHash: blockHash,
		View:      view,
		Phase:     phase,
	}

	if !vr.CheckQuorum(blockHash, view, phase) {
		return nil, fmt.Errorf("no quorum available for block %x, view %d, phase %s",
			blockHash, view, phase)
	}

	return vr.formQuorumCertificate(key, config)
}

// formQuorumCertificate creates a QuorumCertificate from the votes for the given key.
func (vr *VotingRule) formQuorumCertificate(key VoteKey, config *types.ConsensusConfig) (*types.QuorumCertificate, error) {
	votes := vr.pendingVotes[key]
	if len(votes) < vr.threshold {
		return nil, fmt.Errorf("insufficient votes for quorum: have %d, need %d",
			len(votes), vr.threshold)
	}

	// Convert vote map to slice
	voteSlice := make([]types.Vote, 0, len(votes))
	for _, vote := range votes {
		voteSlice = append(voteSlice, *vote)
	}

	// Create the quorum certificate
	qc, err := types.NewQuorumCertificate(key.BlockHash, key.View, key.Phase, voteSlice, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create quorum certificate: %w", err)
	}

	return qc, nil
}

// ValidateVote performs additional validation on a vote before accepting it.
func (vr *VotingRule) ValidateVote(vote *types.Vote, config *types.ConsensusConfig) error {
	if vote == nil {
		return fmt.Errorf("vote cannot be nil")
	}

	if config == nil {
		return fmt.Errorf("consensus configuration cannot be nil")
	}

	// Basic vote validation
	if err := vote.Validate(config); err != nil {
		return err
	}

	// Check if this vote would conflict with existing votes from the same node
	key := VoteKey{
		BlockHash: vote.BlockHash,
		View:      vote.View,
		Phase:     vote.Phase,
	}

	if existingVote, exists := vr.pendingVotes[key][vote.Voter]; exists {
		if !vote.Equals(existingVote) {
			return fmt.Errorf("conflicting vote detected from node %d", vote.Voter)
		}
	}

	return nil
}

// GetVotes returns all votes for a specific block, view, and phase.
func (vr *VotingRule) GetVotes(blockHash types.BlockHash, view types.ViewNumber, phase types.ConsensusPhase) []*types.Vote {
	key := VoteKey{
		BlockHash: blockHash,
		View:      view,
		Phase:     phase,
	}

	votes := vr.pendingVotes[key]
	if votes == nil {
		return nil
	}

	result := make([]*types.Vote, 0, len(votes))
	for _, vote := range votes {
		result = append(result, vote)
	}

	return result
}

// GetVoteCount returns the number of votes for a specific block, view, and phase.
func (vr *VotingRule) GetVoteCount(blockHash types.BlockHash, view types.ViewNumber, phase types.ConsensusPhase) int {
	key := VoteKey{
		BlockHash: blockHash,
		View:      view,
		Phase:     phase,
	}

	votes := vr.pendingVotes[key]
	if votes == nil {
		return 0
	}

	return len(votes)
}

// HasVoted returns true if the specified node has voted for the given block, view, and phase.
func (vr *VotingRule) HasVoted(nodeID types.NodeID, blockHash types.BlockHash, view types.ViewNumber, phase types.ConsensusPhase) bool {
	key := VoteKey{
		BlockHash: blockHash,
		View:      view,
		Phase:     phase,
	}

	votes := vr.pendingVotes[key]
	if votes == nil {
		return false
	}

	_, exists := votes[nodeID]
	return exists
}

// ClearView removes all votes for a specific view to free memory.
// This should be called when a view is no longer relevant.
func (vr *VotingRule) ClearView(view types.ViewNumber) {
	keysToDelete := make([]VoteKey, 0)

	for key := range vr.pendingVotes {
		if key.View == view {
			keysToDelete = append(keysToDelete, key)
		}
	}

	for _, key := range keysToDelete {
		delete(vr.pendingVotes, key)
	}
}

// ClearOldViews removes all votes for views older than the specified view.
func (vr *VotingRule) ClearOldViews(currentView types.ViewNumber) {
	keysToDelete := make([]VoteKey, 0)

	for key := range vr.pendingVotes {
		if key.View < currentView {
			keysToDelete = append(keysToDelete, key)
		}
	}

	for _, key := range keysToDelete {
		delete(vr.pendingVotes, key)
	}
}

// GetThreshold returns the quorum threshold for this voting rule.
func (vr *VotingRule) GetThreshold() int {
	return vr.threshold
}

// SetThreshold updates the quorum threshold. This should only be used when the
// consensus configuration changes.
func (vr *VotingRule) SetThreshold(threshold int) {
	if threshold < 1 {
		threshold = 1
	}
	vr.threshold = threshold
}

// GetActiveViews returns a list of views that have pending votes.
func (vr *VotingRule) GetActiveViews() []types.ViewNumber {
	viewSet := make(map[types.ViewNumber]bool)

	for key := range vr.pendingVotes {
		viewSet[key.View] = true
	}

	views := make([]types.ViewNumber, 0, len(viewSet))
	for view := range viewSet {
		views = append(views, view)
	}

	return views
}

// GetPendingVoteKeys returns all vote keys that currently have pending votes.
func (vr *VotingRule) GetPendingVoteKeys() []VoteKey {
	keys := make([]VoteKey, 0, len(vr.pendingVotes))
	for key := range vr.pendingVotes {
		keys = append(keys, key)
	}
	return keys
}

// Reset clears all pending votes. This should only be used in testing or restart scenarios.
func (vr *VotingRule) Reset() {
	vr.pendingVotes = make(map[VoteKey]map[types.NodeID]*types.Vote)
}

// String returns a string representation of the voting rule state for debugging.
func (vr *VotingRule) String() string {
	activeViews := vr.GetActiveViews()
	return fmt.Sprintf("VotingRule{Threshold: %d, ActiveViews: %v, PendingVotes: %d}",
		vr.threshold, activeViews, len(vr.pendingVotes))
}
