//go:build consensus_testing

package engine

import (
	"fmt"

	"btc-gateway/pkg/consensus/types"
)

// ForceSetLockedQC directly sets the lockedQC without phase validation.
// UNSAFE: This bypasses HotStuff protocol safety rules.
// Only use for testing state synchronization scenarios.
func (hc *HotStuffConsensus) ForceSetLockedQC(qc *types.QuorumCertificate) {
	hc.lockedQC = qc
	if hc.safetyRules != nil {
		hc.safetyRules.ForceSetLockedQC(qc)
	}
}

// ForceRebuildBlockTree rebuilds the block tree from imported blocks.
// Blocks are added in dependency order (parent must exist before child).
// Returns error if any block cannot be added (missing parent chain).
// UNSAFE: This bypasses normal block validation. Only use for testing.
func (hc *HotStuffConsensus) ForceRebuildBlockTree(blocks map[types.BlockHash]*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	added := make(map[types.BlockHash]bool)
	rootHash := hc.blockTree.GetRoot().Hash

	// Mark genesis/root as already present
	added[rootHash] = true
	for hash, block := range blocks {
		if block.IsGenesis() || hash == rootHash {
			added[hash] = true
		}
	}

	// Keep adding blocks whose parents are already added
	for {
		progress := false
		for hash, block := range blocks {
			if added[hash] {
				continue
			}
			// Parent must be in tree (either already added or is root)
			if added[block.ParentHash] {
				// Check if already in tree (idempotent)
				if hc.blockTree.HasBlock(hash) {
					added[hash] = true
					progress = true
					continue
				}
				if err := hc.blockTree.AddBlock(block); err != nil {
					return fmt.Errorf("failed to add block %x: %w", hash[:8], err)
				}
				added[hash] = true
				progress = true
			}
		}
		if !progress {
			break
		}
	}

	// Verify all blocks were added
	for hash := range blocks {
		if !added[hash] {
			return fmt.Errorf("block %x could not be added (missing parent chain)", hash[:8])
		}
	}

	return nil
}

// GetSafetyRules returns the internal safety rules for test verification.
// This allows tests to directly verify SAFENODE predicate behavior.
func (hc *HotStuffConsensus) GetSafetyRules() *SafetyRules {
	return hc.safetyRules
}

// Verify HotStuffConsensus implements EngineTestable at compile time
var _ EngineTestable = (*HotStuffConsensus)(nil)
