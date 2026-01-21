//go:build consensus_testing

package integration

import "btc-gateway/pkg/consensus/types"

// CoordinatorTestable provides DANGEROUS test-only state manipulation for HotStuffCoordinator.
// This interface is implemented by HotStuffCoordinator only when built
// with the "consensus_testing" tag (see coordinator_testing.go).
//
// WARNING: These methods bypass HotStuff protocol safety rules and must
// NEVER be used in production code. They exist solely for testing state
// synchronization scenarios.
type CoordinatorTestable interface {
	// ForceRecoverState updates coordinator state to simulate state sync recovery.
	// It sets the current view, updates highestQC and lockedQC, clears stale vote
	// collections, and restarts the view timer.
	//
	// If blocks is provided (non-nil, non-empty), it rebuilds the block tree first.
	// The method validates that lockedQC and highestQC reference blocks that exist
	// in the tree before updating coordinator state (fail fast).
	//
	// UNSAFE: Bypasses normal state sync protocol.
	// Only available in test builds (//go:build consensus_testing).
	ForceRecoverState(
		targetView types.ViewNumber,
		highestQC *types.QuorumCertificate,
		lockedQC *types.QuorumCertificate,
		blocks map[types.BlockHash]*types.Block, // optional, can be nil
	) error
}
