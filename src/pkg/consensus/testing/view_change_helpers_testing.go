//go:build consensus_testing

package testing

import (
	"fmt"

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
