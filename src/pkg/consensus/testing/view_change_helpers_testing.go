//go:build consensus_testing

package testing

import (
	"fmt"
	"time"

	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/mocks"
	"btc-gateway/pkg/consensus/types"
)

// SetStorages sets the storages map for state sync functionality.
// This must be called before using SyncNodeState.
func (h *ViewChangeTestHelpers) SetStorages(storages map[types.NodeID]*mocks.MockStorage) {
	h.storages = storages
}

// SyncNodeState synchronizes a partitioned/behind node with the majority.
// This copies storage state from available source nodes and updates the
// coordinator's internal state via ForceRecoverState.
//
// This method is only available in test builds (//go:build testing).
func (h *ViewChangeTestHelpers) SyncNodeState(nodeID types.NodeID) error {
	if h.storages == nil {
		return fmt.Errorf("storages not set - call SetStorages first")
	}

	// Get all non-blocked nodes as sources
	var sources []types.NodeID
	for id := range h.nodes {
		if id != nodeID && !h.IsNodeBlocked(id) {
			sources = append(sources, id)
		}
	}

	if len(sources) == 0 {
		return fmt.Errorf("no available source nodes for sync")
	}

	// Create mock state sync and perform sync
	stateSync := NewMockStateSync(h.nodes, h.storages)
	return stateSync.SyncNode(nodeID, sources)
}

// IsNodeBlocked checks if a node is currently partitioned/blocked
func (h *ViewChangeTestHelpers) IsNodeBlocked(nodeID types.NodeID) bool {
	node, exists := h.nodes[nodeID]
	if !exists {
		return false
	}

	network := node.GetNetwork()
	if mockNetwork, ok := network.(interface{ IsPartitioned() bool }); ok {
		return mockNetwork.IsPartitioned()
	}

	return false
}

// =============================================================================
// View Divergence Testing Helpers
// =============================================================================

// SetupDivergentViews sets each node to a different view number to simulate
// the post-restart divergence scenario. Returns error if any node setup fails.
func (h *ViewChangeTestHelpers) SetupDivergentViews(views []types.ViewNumber) error {
	if len(views) != len(h.nodes) {
		return fmt.Errorf("views slice length (%d) must match node count (%d)", len(views), len(h.nodes))
	}

	for nodeID, node := range h.nodes {
		if int(nodeID) >= len(views) {
			return fmt.Errorf("node ID %d exceeds views slice length", nodeID)
		}
		targetView := views[nodeID]

		// Get coordinator and cast to testable interface
		coordinator := node.GetCoordinator()
		if testable, ok := interface{}(coordinator).(integration.CoordinatorTestable); ok {
			testable.ForceSetView(targetView)
		} else {
			return fmt.Errorf("coordinator for node %d does not implement CoordinatorTestable", nodeID)
		}
	}

	return nil
}

// ForceNodeToView sets a specific node's view number directly.
func (h *ViewChangeTestHelpers) ForceNodeToView(nodeID types.NodeID, view types.ViewNumber) error {
	node, exists := h.nodes[nodeID]
	if !exists {
		return NewNodeNotFoundError(nodeID)
	}

	coordinator := node.GetCoordinator()
	if testable, ok := interface{}(coordinator).(integration.CoordinatorTestable); ok {
		testable.ForceSetView(view)
		return nil
	}

	return fmt.Errorf("coordinator for node %d does not implement CoordinatorTestable", nodeID)
}

// WaitForViewConvergence waits until all nodes reach the same view.
// Returns true if convergence is achieved, false on timeout.
func (h *ViewChangeTestHelpers) WaitForViewConvergence(maxWait time.Duration) bool {
	return h.WaitForCondition(func() bool {
		views := make(map[types.ViewNumber]int)
		for _, node := range h.nodes {
			views[node.GetCoordinator().GetCurrentView()]++
		}
		// Check if all nodes are at the same view
		for _, count := range views {
			if count == len(h.nodes) {
				return true
			}
		}
		return false
	}, maxWait)
}

// WaitForQuorumAtView waits until at least quorum nodes are at or above a given view.
func (h *ViewChangeTestHelpers) WaitForQuorumAtView(targetView types.ViewNumber, maxWait time.Duration) bool {
	quorum := h.config.QuorumThreshold()
	return h.WaitForCondition(func() bool {
		count := 0
		for _, node := range h.nodes {
			if node.GetCoordinator().GetCurrentView() >= targetView {
				count++
			}
		}
		return count >= quorum
	}, maxWait)
}

// GetMaxCurrentView returns the highest current view among all nodes.
func (h *ViewChangeTestHelpers) GetMaxCurrentView() types.ViewNumber {
	var maxView types.ViewNumber
	for _, node := range h.nodes {
		view := node.GetCoordinator().GetCurrentView()
		if view > maxView {
			maxView = view
		}
	}
	return maxView
}

// GetNodeViews returns a map of node ID to current view for debugging.
func (h *ViewChangeTestHelpers) GetNodeViews() map[types.NodeID]types.ViewNumber {
	views := make(map[types.NodeID]types.ViewNumber)
	for nodeID, node := range h.nodes {
		views[nodeID] = node.GetCoordinator().GetCurrentView()
	}
	return views
}

// TriggerTimeoutsForAllNodes triggers view timeout on all nodes.
// This is useful for forcing timeout message broadcasts.
func (h *ViewChangeTestHelpers) TriggerTimeoutsForAllNodes() {
	for _, node := range h.nodes {
		coordinator := node.GetCoordinator()
		if testable, ok := interface{}(coordinator).(integration.CoordinatorTestable); ok {
			testable.TriggerViewTimeout()
		}
	}
}

// GetLockedQC returns the lockedQC for a specific node.
func (h *ViewChangeTestHelpers) GetLockedQC(nodeID types.NodeID) *types.QuorumCertificate {
	node, exists := h.nodes[nodeID]
	if !exists {
		return nil
	}

	coordinator := node.GetCoordinator()
	if testable, ok := interface{}(coordinator).(integration.CoordinatorTestable); ok {
		return testable.GetLockedQC()
	}

	return nil
}
