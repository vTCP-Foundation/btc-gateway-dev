//go:build consensus_testing

package testing

import (
	"fmt"

	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/mocks"
	"btc-gateway/pkg/consensus/types"
)

// MockStateSync provides state synchronization for testing.
// It copies storage state directly and uses the CoordinatorTestable interface
// to update coordinator state (only available in test builds).
type MockStateSync struct {
	nodes    map[types.NodeID]*integration.Node
	storages map[types.NodeID]*mocks.MockStorage
}

// NewMockStateSync creates a new mock state sync helper.
func NewMockStateSync(
	nodes map[types.NodeID]*integration.Node,
	storages map[types.NodeID]*mocks.MockStorage,
) *MockStateSync {
	return &MockStateSync{
		nodes:    nodes,
		storages: storages,
	}
}

// SyncNode synchronizes a behind node from available source nodes.
// It copies blocks/QCs from storage and uses ForceRecoverState to update
// the coordinator's internal state.
func (m *MockStateSync) SyncNode(behindNodeID types.NodeID, sourceNodeIDs []types.NodeID) error {
	if len(sourceNodeIDs) == 0 {
		return fmt.Errorf("no source nodes provided for sync")
	}

	// Find best source (highest view)
	var bestSource types.NodeID
	var bestView types.ViewNumber
	var found bool
	for _, sourceID := range sourceNodeIDs {
		node, ok := m.nodes[sourceID]
		if !ok {
			continue
		}
		view := node.GetCoordinator().GetCurrentView()
		if !found || view > bestView {
			bestView = view
			bestSource = sourceID
			found = true
		}
	}

	if !found {
		return fmt.Errorf("no valid source nodes found")
	}

	// Copy storage state (blocks and QCs)
	sourceStorage, ok := m.storages[bestSource]
	if !ok {
		return fmt.Errorf("source storage not found for node %d", bestSource)
	}

	targetStorage, ok := m.storages[behindNodeID]
	if !ok {
		return fmt.Errorf("target storage not found for node %d", behindNodeID)
	}

	blocks := sourceStorage.GetAllBlocks()
	qcs := sourceStorage.GetAllQCs()
	if err := targetStorage.ImportState(blocks, qcs); err != nil {
		return fmt.Errorf("failed to import storage state: %w", err)
	}

	// Get coordinator as CoordinatorTestable interface
	targetNode, ok := m.nodes[behindNodeID]
	if !ok {
		return fmt.Errorf("target node not found: %d", behindNodeID)
	}

	targetCoord := targetNode.GetCoordinator()
	testable, ok := interface{}(targetCoord).(integration.CoordinatorTestable)
	if !ok {
		return fmt.Errorf("coordinator does not implement CoordinatorTestable (missing build tag?)")
	}

	// Get state from source
	sourceNode := m.nodes[bestSource]
	sourceCoord := sourceNode.GetCoordinator()
	highestQC := sourceCoord.GetHighestQC()
	lockedQC := sourceNode.GetConsensus().GetLockedQC()

	// Update coordinator state via interface - pass blocks for tree rebuild
	return testable.ForceRecoverState(bestView, highestQC, lockedQC, blocks)
}

// SyncNodeFromAll synchronizes a node from all other available nodes.
func (m *MockStateSync) SyncNodeFromAll(behindNodeID types.NodeID) error {
	var sources []types.NodeID
	for nodeID := range m.nodes {
		if nodeID != behindNodeID {
			sources = append(sources, nodeID)
		}
	}
	return m.SyncNode(behindNodeID, sources)
}

// GetNodesBehind returns nodes that are behind the majority.
// A node is considered behind if its view is less than the maximum view.
func (m *MockStateSync) GetNodesBehind() []types.NodeID {
	// Find max view
	var maxView types.ViewNumber
	for _, node := range m.nodes {
		view := node.GetCoordinator().GetCurrentView()
		if view > maxView {
			maxView = view
		}
	}

	// Find nodes behind
	var behind []types.NodeID
	for nodeID, node := range m.nodes {
		view := node.GetCoordinator().GetCurrentView()
		if view < maxView {
			behind = append(behind, nodeID)
		}
	}

	return behind
}
