package integration

import (
	"fmt"
	"sync"

	"btc-gateway/pkg/consensus/types"
)

// LeaderElection implements round-robin leader selection mechanism.
// It provides deterministic leader selection based on view numbers and validator lists.
type LeaderElection struct {
	// List of validator nodes participating in consensus
	validators []types.NodeID
	
	// Current view for leader calculation (cached for efficiency)
	currentView types.ViewNumber
	
	// Cached leader for current view
	currentLeader types.NodeID
	
	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// NewLeaderElection creates a new leader election mechanism with the given validators.
func NewLeaderElection(validators []types.NodeID) (*LeaderElection, error) {
	if len(validators) == 0 {
		return nil, fmt.Errorf("validator list cannot be empty")
	}
	
	// Validate all node IDs are unique
	seen := make(map[types.NodeID]bool)
	for _, nodeID := range validators {
		if seen[nodeID] {
			return nil, fmt.Errorf("duplicate validator node ID: %d", nodeID)
		}
		seen[nodeID] = true
	}
	
	le := &LeaderElection{
		validators: make([]types.NodeID, len(validators)),
	}
	
	// Copy validators to prevent external modification
	copy(le.validators, validators)
	
	// Initialize with view 0
	le.currentView = 0
	le.currentLeader = le.calculateLeader(0)
	
	return le, nil
}

// GetLeader returns the leader for a specific view using round-robin selection.
func (le *LeaderElection) GetLeader(view types.ViewNumber) types.NodeID {
	le.mu.RLock()
	
	// Check if we have cached leader for this view
	if view == le.currentView {
		leader := le.currentLeader
		le.mu.RUnlock()
		return leader
	}
	
	le.mu.RUnlock()
	
	// Calculate and cache the leader for this view
	le.mu.Lock()
	defer le.mu.Unlock()
	
	// Double-check after acquiring write lock
	if view == le.currentView {
		return le.currentLeader
	}
	
	leader := le.calculateLeader(view)
	le.currentView = view
	le.currentLeader = leader
	
	return leader
}

// IsValidLeader checks if a given node is the valid leader for a specific view.
func (le *LeaderElection) IsValidLeader(nodeID types.NodeID, view types.ViewNumber) bool {
	expectedLeader := le.GetLeader(view)
	return nodeID == expectedLeader
}

// GetNextLeader returns the next leader in the rotation after the current leader.
func (le *LeaderElection) GetNextLeader(currentLeader types.NodeID) (types.NodeID, error) {
	le.mu.RLock()
	defer le.mu.RUnlock()
	
	// Find the index of the current leader
	currentIndex := -1
	for i, nodeID := range le.validators {
		if nodeID == currentLeader {
			currentIndex = i
			break
		}
	}
	
	if currentIndex == -1 {
		return 0, fmt.Errorf("node %d is not a valid validator", currentLeader)
	}
	
	// Calculate next leader index with wrap-around
	nextIndex := (currentIndex + 1) % len(le.validators)
	return le.validators[nextIndex], nil
}

// GetValidators returns a copy of the validator list.
func (le *LeaderElection) GetValidators() []types.NodeID {
	le.mu.RLock()
	defer le.mu.RUnlock()
	
	validators := make([]types.NodeID, len(le.validators))
	copy(validators, le.validators)
	return validators
}

// IsValidator checks if a node is a valid validator.
func (le *LeaderElection) IsValidator(nodeID types.NodeID) bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	
	for _, validator := range le.validators {
		if validator == nodeID {
			return true
		}
	}
	return false
}

// GetValidatorCount returns the number of validators.
func (le *LeaderElection) GetValidatorCount() int {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return len(le.validators)
}

// GetLeaderIndex returns the index of the leader for a given view.
func (le *LeaderElection) GetLeaderIndex(view types.ViewNumber) int {
	le.mu.RLock()
	defer le.mu.RUnlock()
	
	if len(le.validators) == 0 {
		return -1
	}
	
	return int(view) % len(le.validators)
}

// UpdateValidators updates the validator list (use with caution in production).
func (le *LeaderElection) UpdateValidators(newValidators []types.NodeID) error {
	if len(newValidators) == 0 {
		return fmt.Errorf("new validator list cannot be empty")
	}
	
	// Validate all node IDs are unique
	seen := make(map[types.NodeID]bool)
	for _, nodeID := range newValidators {
		if seen[nodeID] {
			return fmt.Errorf("duplicate validator node ID: %d", nodeID)
		}
		seen[nodeID] = true
	}
	
	le.mu.Lock()
	defer le.mu.Unlock()
	
	// Update validators
	le.validators = make([]types.NodeID, len(newValidators))
	copy(le.validators, newValidators)
	
	// Recalculate current leader
	le.currentLeader = le.calculateLeader(le.currentView)
	
	return nil
}

// GetLeaderHistory returns the leaders for a range of views.
func (le *LeaderElection) GetLeaderHistory(startView, endView types.ViewNumber) map[types.ViewNumber]types.NodeID {
	if startView > endView {
		return make(map[types.ViewNumber]types.NodeID)
	}
	
	le.mu.RLock()
	defer le.mu.RUnlock()
	
	history := make(map[types.ViewNumber]types.NodeID)
	
	for view := startView; view <= endView; view++ {
		history[view] = le.calculateLeader(view)
	}
	
	return history
}

// calculateLeader calculates the leader for a specific view using round-robin (must be called with lock held).
func (le *LeaderElection) calculateLeader(view types.ViewNumber) types.NodeID {
	if len(le.validators) == 0 {
		return 0 // Invalid node ID
	}
	
	// Round-robin selection: leader = validators[view % len(validators)]
	leaderIndex := int(view) % len(le.validators)
	return le.validators[leaderIndex]
}

// String returns a string representation of the leader election state.
func (le *LeaderElection) String() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	
	return fmt.Sprintf("LeaderElection{Validators: %v, CurrentView: %d, CurrentLeader: %d}", 
		le.validators, le.currentView, le.currentLeader)
}

// RotationInfo provides information about the current rotation state.
type RotationInfo struct {
	CurrentView   types.ViewNumber
	CurrentLeader types.NodeID
	LeaderIndex   int
	TotalNodes    int
}

// GetRotationInfo returns current rotation information.
func (le *LeaderElection) GetRotationInfo() RotationInfo {
	le.mu.RLock()
	defer le.mu.RUnlock()
	
	leaderIndex := le.GetLeaderIndex(le.currentView)
	
	return RotationInfo{
		CurrentView:   le.currentView,
		CurrentLeader: le.currentLeader,
		LeaderIndex:   leaderIndex,
		TotalNodes:    len(le.validators),
	}
}