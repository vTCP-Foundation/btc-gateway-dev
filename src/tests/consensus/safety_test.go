//go:build consensus_testing

package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"btc-gateway/pkg/consensus/engine"
	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/mocks"
	testinglib "btc-gateway/pkg/consensus/testing"
	"btc-gateway/pkg/consensus/types"
)

// =============================================================================
// Helper Functions
// =============================================================================

// createConflictingBlock creates a block that doesn't extend the locked chain.
// It adds the block to the tree so ancestry checks can run.
func createConflictingBlock(t *testing.T, lockedQC *types.QuorumCertificate, blockTree *engine.BlockTree) *types.Block {
	t.Helper()
	genesis := blockTree.GetRoot()

	// Use NewBlock which handles Timestamp and Hash calculation
	block := types.NewBlock(
		genesis.Hash,             // Points to genesis, NOT the locked block's chain
		1,                        // Height
		lockedQC.View+1,          // View
		0,                        // Proposer
		[]byte("conflicting_block"),
	)

	// Add to tree so IsAncestor can find it
	err := blockTree.AddBlock(block)
	require.NoError(t, err, "Should be able to add conflicting block to tree")

	return block
}

// committedBlockInfo holds info about a committed block from events.
type committedBlockInfo struct {
	Hash   types.BlockHash
	Height types.Height
}

// collectCommittedBlocksFromTracer extracts committed block info from event tracer.
func collectCommittedBlocksFromTracer(tracer *mocks.ConsensusEventTracer) []committedBlockInfo {
	var committed []committedBlockInfo
	allEvents := tracer.GetEventsByType(events.EventBlockCommitted)

	for _, event := range allEvents {
		payload := event.Payload
		if hash, ok := payload["block_hash"].(types.BlockHash); ok {
			height, _ := payload["height"].(types.Height)
			committed = append(committed, committedBlockInfo{
				Hash:   hash,
				Height: height,
			})
		}
	}
	return committed
}

// findNodeWithLockedQC returns the first node that has a lockedQC set.
func findNodeWithLockedQC(nodes map[types.NodeID]*integration.Node) (*integration.Node, types.NodeID, *types.QuorumCertificate) {
	for nodeID, node := range nodes {
		lockedQC := node.GetConsensus().GetLockedQC()
		if lockedQC != nil {
			return node, nodeID, lockedQC
		}
	}
	return nil, 0, nil
}

// getSafetyRules gets the SafetyRules from a node using the EngineTestable interface.
func getSafetyRules(t *testing.T, node *integration.Node) *engine.SafetyRules {
	t.Helper()
	consensus := node.GetConsensus()
	testable, ok := interface{}(consensus).(engine.EngineTestable)
	require.True(t, ok, "Consensus engine must implement EngineTestable (check build tags)")
	return testable.GetSafetyRules()
}

// =============================================================================
// Safety Property Tests
// =============================================================================

// TestSafeNodePredicateBlocksConflictingVotes verifies that a node with lockedQC
// refuses to vote for blocks not extending the locked chain.
// This tests the SAFENODE predicate from the HotStuff paper.
func TestSafeNodePredicateBlocksConflictingVotes(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Safety Test: SAFENODE Predicate Blocks Conflicting Votes ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err)
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	config := result.Config

	// Run consensus to establish lockedQC
	leader, err := config.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Running consensus round with leader Node%d to establish lockedQC", leader)

	err = nodes[leader].ProposeBlock([]byte("establish_lock"))
	require.NoError(t, err)

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, config)
	helpers.WaitForQuorumCommits(5 * time.Second)

	// Find node with lockedQC
	targetNode, targetID, lockedQC := findNodeWithLockedQC(nodes)
	require.NotNil(t, lockedQC, "Should have established lockedQC after consensus round")
	t.Logf("Node%d has lockedQC at view %d for block %x", targetID, lockedQC.View, lockedQC.BlockHash[:8])

	// Get the block tree and safety rules
	blockTree := targetNode.GetConsensus().GetBlockTree()
	safetyRules := getSafetyRules(t, targetNode)

	// Create conflicting block: points to genesis, NOT the locked chain
	conflictingBlock := createConflictingBlock(t, lockedQC, blockTree)
	t.Logf("Created conflicting block %x at height %d (parent: genesis, not locked chain)",
		conflictingBlock.Hash[:8], conflictingBlock.Height)

	// Verify SafetyRules rejects this block
	canVote, err := safetyRules.CanVote(conflictingBlock, lockedQC, blockTree)
	assert.NoError(t, err, "CanVote should not return error for valid block structure")
	assert.False(t, canVote, "SAFETY VIOLATION: Node should reject vote for block not extending locked chain")

	t.Log("=== SAFENODE Predicate Test PASSED: Conflicting block correctly rejected ===")
}

// TestSyncedNodeEnforcesLockedQC verifies that after state sync, the synced node
// enforces lockedQC (not just copies the value).
func TestSyncedNodeEnforcesLockedQC(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Safety Test: Synced Node Enforces LockedQC ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err)
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	config := result.Config

	// Run consensus to establish lockedQC
	leader, err := config.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Running consensus round with leader Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("establish_lock_for_sync"))
	require.NoError(t, err)

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, config)
	helpers.WaitForQuorumCommits(5 * time.Second)

	// Find source node with lockedQC
	_, sourceID, sourceLockedQC := findNodeWithLockedQC(nodes)
	require.NotNil(t, sourceLockedQC, "Should have established lockedQC")
	t.Logf("Source Node%d has lockedQC at view %d", sourceID, sourceLockedQC.View)

	// Pick a different target node
	var targetID types.NodeID
	for i := 0; i < totalNodes; i++ {
		if types.NodeID(i) != sourceID {
			targetID = types.NodeID(i)
			break
		}
	}
	t.Logf("Will sync Node%d from Node%d", targetID, sourceID)

	// Sync target node from source
	stateSync := testinglib.NewMockStateSync(nodes, result.Storages)
	err = stateSync.SyncNode(targetID, []types.NodeID{sourceID})
	require.NoError(t, err)
	t.Logf("State sync completed")

	// Verify synced node has lockedQC
	syncedLockedQC := nodes[targetID].GetConsensus().GetLockedQC()
	require.NotNil(t, syncedLockedQC, "Synced node must have lockedQC after sync")
	assert.Equal(t, sourceLockedQC.View, syncedLockedQC.View, "Synced lockedQC view should match source")
	t.Logf("Synced Node%d has lockedQC at view %d", targetID, syncedLockedQC.View)

	// Create conflicting block
	blockTree := nodes[targetID].GetConsensus().GetBlockTree()
	conflictingBlock := createConflictingBlock(t, syncedLockedQC, blockTree)

	// Verify synced node enforces safety
	safetyRules := getSafetyRules(t, nodes[targetID])
	canVote, err := safetyRules.CanVote(conflictingBlock, syncedLockedQC, blockTree)
	assert.NoError(t, err)
	assert.False(t, canVote, "SAFETY VIOLATION: Synced node should enforce lockedQC and reject conflicting block")

	t.Log("=== Synced Node Safety Enforcement Test PASSED ===")
}

// TestNoConflictingCommits proves that all nodes commit the same block for the same height.
// This is a core safety property of BFT consensus.
func TestNoConflictingCommits(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Safety Test: No Conflicting Commits ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err)
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	config := result.Config

	// Run a consensus round - leader proposes, all nodes should commit same block
	leader, err := config.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader Node%d proposing block", leader)

	err = nodes[leader].ProposeBlock([]byte("safety_test_block"))
	require.NoError(t, err)

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, config)
	helpers.WaitForQuorumCommits(5 * time.Second)

	// Collect committed blocks from all nodes via tracer events
	committedBlocks := collectCommittedBlocksFromTracer(tracer)
	t.Logf("Found %d total commit events across all nodes", len(committedBlocks))

	// We should have commits from all 5 nodes
	require.GreaterOrEqual(t, len(committedBlocks), totalNodes,
		"Expected at least %d commit events (one per node)", totalNodes)

	// Group by height and verify only one unique block per height
	heightToHashes := make(map[types.Height]map[types.BlockHash]bool)
	for _, block := range committedBlocks {
		if heightToHashes[block.Height] == nil {
			heightToHashes[block.Height] = make(map[types.BlockHash]bool)
		}
		heightToHashes[block.Height][block.Hash] = true
	}

	// Assert: No height has multiple different blocks (safety property)
	for height, hashes := range heightToHashes {
		hashCount := len(hashes)
		if hashCount > 1 {
			t.Logf("SAFETY VIOLATION: Height %d has %d different committed blocks:", height, hashCount)
			for hash := range hashes {
				t.Logf("  - Block %x", hash[:8])
			}
		}
		assert.Equal(t, 1, hashCount,
			"SAFETY VIOLATION: Multiple different blocks committed at height %d", height)
	}

	// Verify we committed the proposed block at height 1 (height 0 is genesis)
	require.Contains(t, heightToHashes, types.Height(1),
		"Should have committed a block at height 1")
	t.Logf("Height 1 has %d unique block(s) committed across %d nodes",
		len(heightToHashes[types.Height(1)]), totalNodes)

	t.Log("=== No Conflicting Commits Test PASSED ===")
}

// TestLockedQCPreventsEquivocationAcrossViews verifies lockedQC prevents voting
// for conflicting blocks even in higher views.
func TestLockedQCPreventsEquivocationAcrossViews(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Safety Test: LockedQC Prevents Equivocation Across Views ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err)
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	config := result.Config

	// Run consensus to establish lockedQC
	leader, err := config.GetLeaderForView(0)
	require.NoError(t, err)
	err = nodes[leader].ProposeBlock([]byte("establish_lock_cross_view"))
	require.NoError(t, err)

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, config)
	helpers.WaitForQuorumCommits(5 * time.Second)

	// Find node with lockedQC
	targetNode, targetID, lockedQC := findNodeWithLockedQC(nodes)
	require.NotNil(t, lockedQC, "Should have established lockedQC")
	t.Logf("Node%d has lockedQC at view %d", targetID, lockedQC.View)

	// Advance the node's view significantly
	consensus := targetNode.GetConsensus()
	initialView := consensus.GetCurrentView()
	for consensus.GetCurrentView() <= lockedQC.View+3 {
		consensus.AdvanceView()
	}
	newView := consensus.GetCurrentView()
	t.Logf("Advanced Node%d from view %d to view %d (lockedQC is at view %d)",
		targetID, initialView, newView, lockedQC.View)

	// Create conflicting block in higher view that doesn't extend locked chain
	blockTree := targetNode.GetConsensus().GetBlockTree()
	genesis := blockTree.GetRoot()
	conflictingBlock := types.NewBlock(
		genesis.Hash,                       // NOT extending locked chain
		1,                                  // Height
		newView,                            // Higher than lock
		0,                                  // Proposer
		[]byte("cross_view_conflict"),
	)
	err = blockTree.AddBlock(conflictingBlock)
	require.NoError(t, err)
	t.Logf("Created conflicting block at view %d (parent: genesis)", conflictingBlock.View)

	// Verify safety rules still reject it despite higher view
	safetyRules := getSafetyRules(t, targetNode)
	canVote, err := safetyRules.CanVote(conflictingBlock, lockedQC, blockTree)
	assert.NoError(t, err)
	assert.False(t, canVote,
		"SAFETY VIOLATION: Higher view should NOT bypass locked chain ancestry requirement")

	t.Log("=== Cross-View Equivocation Prevention Test PASSED ===")
}

// TestQCPhaseTransitionValidation verifies ValidateQCTransition enforces correct phase progressions.
func TestQCPhaseTransitionValidation(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Safety Test: QC Phase Transition Validation ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err)
	defer result.NetworkManager.StopAll()

	// Use SafetyRules from actual node
	safetyRules := getSafetyRules(t, result.Nodes[0])

	// Create test QCs with different phases
	blockHash := types.BlockHash{1, 2, 3, 4, 5, 6, 7, 8}
	prepareQC := &types.QuorumCertificate{Phase: types.PhasePrepare, View: 1, BlockHash: blockHash}
	precommitQC := &types.QuorumCertificate{Phase: types.PhasePreCommit, View: 1, BlockHash: blockHash}
	commitQC := &types.QuorumCertificate{Phase: types.PhaseCommit, View: 1, BlockHash: blockHash}

	// Test valid transitions
	t.Log("Testing valid phase transitions...")

	// Valid: Prepare -> PreCommit
	err = safetyRules.ValidateQCTransition(prepareQC, precommitQC)
	assert.NoError(t, err, "Prepare->PreCommit should be valid")
	t.Log("  Prepare->PreCommit: VALID")

	// Valid: PreCommit -> Commit
	err = safetyRules.ValidateQCTransition(precommitQC, commitQC)
	assert.NoError(t, err, "PreCommit->Commit should be valid")
	t.Log("  PreCommit->Commit: VALID")

	// Valid: Commit -> Prepare (new round)
	newPrepareQC := &types.QuorumCertificate{Phase: types.PhasePrepare, View: 2, BlockHash: blockHash}
	err = safetyRules.ValidateQCTransition(commitQC, newPrepareQC)
	assert.NoError(t, err, "Commit->Prepare (new view) should be valid")
	t.Log("  Commit->Prepare (new view): VALID")

	// Test invalid transitions
	t.Log("Testing invalid phase transitions...")

	// Invalid: Prepare -> Commit (skips PreCommit)
	err = safetyRules.ValidateQCTransition(prepareQC, commitQC)
	assert.Error(t, err, "Prepare->Commit should be invalid (skips PreCommit)")
	t.Logf("  Prepare->Commit: INVALID (as expected: %v)", err)

	// Invalid: View regression
	oldPrepare := &types.QuorumCertificate{Phase: types.PhasePrepare, View: 5, BlockHash: blockHash}
	newPrecommitLowerView := &types.QuorumCertificate{Phase: types.PhasePreCommit, View: 3, BlockHash: blockHash}
	err = safetyRules.ValidateQCTransition(oldPrepare, newPrecommitLowerView)
	assert.Error(t, err, "View regression should be invalid")
	t.Logf("  View regression (5->3): INVALID (as expected: %v)", err)

	t.Log("=== QC Phase Transition Validation Test PASSED ===")
}

// TestSplitBrainSafety verifies the critical split-brain safety properties:
// 1. No commits occur in either partition when neither has quorum
// 2. All nodes converge to the same committed chain after partition heals
func TestSplitBrainSafety(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Safety Test: Split-Brain Safety Properties ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err)
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	config := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, config)
	helpers.SetStorages(result.Storages)

	// Step 1: Run initial consensus to establish a committed block
	leader, err := config.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Running initial consensus with leader Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("pre_partition_block"))
	require.NoError(t, err)
	helpers.WaitForQuorumCommits(5 * time.Second)

	// Record committed blocks BEFORE partition
	committedBefore := collectCommittedBlocksFromTracer(tracer)
	commitCountBefore := len(committedBefore)
	t.Logf("Commits before partition: %d events", commitCountBefore)

	// Collect unique block hashes at each height before partition
	heightToHashBefore := make(map[types.Height]types.BlockHash)
	for _, block := range committedBefore {
		if _, exists := heightToHashBefore[block.Height]; !exists {
			heightToHashBefore[block.Height] = block.Hash
		}
	}
	t.Logf("Committed heights before partition: %v", getHeights(heightToHashBefore))

	// Step 2: Create a TRUE split-brain partition
	// With n=5 and quorum=4, split into:
	// - Group A: Nodes 0, 1, 2 (3 nodes - cannot reach quorum)
	// - Group B: Nodes 3, 4 (2 nodes - cannot reach quorum)
	// NEITHER group can commit!
	groupB := []types.NodeID{types.NodeID(3), types.NodeID(4)}

	t.Log("Creating split-brain partition:")
	t.Logf("  Group A: Nodes 0, 1, 2 (3 nodes, need 4 for quorum)")
	t.Logf("  Group B: Nodes 3, 4 (2 nodes, need 4 for quorum)")

	for _, nodeID := range groupB {
		err = helpers.BlockNode(nodeID)
		require.NoError(t, err)
	}

	// Step 3: Wait during partition - neither group should be able to commit
	timeout := config.GetTimeoutForView(0)
	partitionDuration := timeout * 2
	t.Logf("Partition active for %v - neither group should commit", partitionDuration)

	time.Sleep(partitionDuration)

	// Step 4: CRITICAL ASSERTION - No new commits during partition
	committedDuring := collectCommittedBlocksFromTracer(tracer)
	commitCountDuring := len(committedDuring)
	newCommitsDuringPartition := commitCountDuring - commitCountBefore

	t.Logf("Commits after partition period: %d events (new: %d)", commitCountDuring, newCommitsDuringPartition)

	// SAFETY ASSERTION: No commits should occur when neither partition has quorum
	assert.Equal(t, 0, newCommitsDuringPartition,
		"SAFETY VIOLATION: Commits occurred during split-brain when neither partition had quorum")

	// Step 5: Heal the partition
	t.Log("Healing partition...")
	for _, nodeID := range groupB {
		err = helpers.UnblockNode(nodeID)
		require.NoError(t, err)
		err = helpers.SyncNodeState(nodeID)
		require.NoError(t, err)
	}

	// Wait for network to reconverge
	time.Sleep(timeout + time.Second)

	// Step 6: CRITICAL ASSERTION - Single-chain convergence post-heal
	t.Log("Verifying single-chain convergence after partition heal...")

	// Collect committed blocks from each node's block tree
	nodeCommittedBlocks := make(map[types.NodeID]map[types.Height]types.BlockHash)
	for nodeID, node := range nodes {
		nodeCommittedBlocks[nodeID] = make(map[types.Height]types.BlockHash)
		blockTree := node.GetConsensus().GetBlockTree()
		committed := blockTree.GetCommitted()
		if committed != nil {
			nodeCommittedBlocks[nodeID][committed.Height] = committed.Hash
			t.Logf("Node%d committed block: height=%d hash=%x", nodeID, committed.Height, committed.Hash[:8])
		}
	}

	// Verify all nodes agree on committed block at each height
	heightToHashes := make(map[types.Height]map[types.BlockHash]bool)
	for _, blocks := range nodeCommittedBlocks {
		for height, hash := range blocks {
			if heightToHashes[height] == nil {
				heightToHashes[height] = make(map[types.BlockHash]bool)
			}
			heightToHashes[height][hash] = true
		}
	}

	// SAFETY ASSERTION: Single chain - only one block per height across all nodes
	for height, hashes := range heightToHashes {
		hashCount := len(hashes)
		if hashCount > 1 {
			t.Logf("SAFETY VIOLATION at height %d: %d different blocks committed:", height, hashCount)
			for hash := range hashes {
				t.Logf("  - Block %x", hash[:8])
			}
		}
		assert.Equal(t, 1, hashCount,
			"SAFETY VIOLATION: Nodes committed different blocks at height %d after partition heal", height)
	}

	// Verify the chain matches what was committed before partition
	for height, hashBefore := range heightToHashBefore {
		if hashesAfter, exists := heightToHashes[height]; exists {
			for hashAfter := range hashesAfter {
				assert.Equal(t, hashBefore, hashAfter,
					"SAFETY VIOLATION: Committed block at height %d changed after partition heal", height)
			}
		}
	}

	t.Log("=== Split-Brain Safety Test PASSED ===")
	t.Log("  ✓ No commits during partition (neither group had quorum)")
	t.Log("  ✓ Single-chain convergence after partition healed")
}

// getHeights extracts height keys from a map for logging.
func getHeights(m map[types.Height]types.BlockHash) []types.Height {
	heights := make([]types.Height, 0, len(m))
	for h := range m {
		heights = append(heights, h)
	}
	return heights
}
