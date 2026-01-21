//go:build consensus_testing

package engine

import "btc-gateway/pkg/consensus/types"

// EngineTestable provides DANGEROUS test-only state manipulation for HotStuffConsensus.
// This interface is implemented by HotStuffConsensus only when built
// with the "consensus_testing" tag (see consensus_testing.go).
//
// WARNING: These methods bypass HotStuff protocol safety rules and must
// NEVER be used in production code. They exist solely for testing state
// synchronization scenarios where we need to set lockedQC without
// phase validation.
type EngineTestable interface {
	// ForceSetLockedQC directly sets the lockedQC without phase validation.
	// UNSAFE: This bypasses HotStuff protocol safety rules.
	// Only use for testing state synchronization scenarios.
	ForceSetLockedQC(qc *types.QuorumCertificate)

	// ForceRebuildBlockTree rebuilds the block tree from imported blocks.
	// Blocks are added in dependency order (parent must exist before child).
	// Returns error if any block cannot be added (missing parent chain).
	// UNSAFE: This bypasses normal block validation. Only use for testing.
	ForceRebuildBlockTree(blocks map[types.BlockHash]*types.Block) error

	// GetSafetyRules returns the internal safety rules for test verification.
	// This allows tests to directly verify SAFENODE predicate behavior.
	GetSafetyRules() *SafetyRules
}
