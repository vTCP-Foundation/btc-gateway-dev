package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/mocks"
	testinglib "btc-gateway/pkg/consensus/testing"
	"btc-gateway/pkg/consensus/types"
)

// TestCoordinatedByzantine verifies that the system maintains safety and liveness
// when multiple Byzantine nodes coordinate their attacks. With n=5 and f=1,
// the system should tolerate 1 Byzantine node. This test verifies behavior
// when exactly f Byzantine nodes attempt coordinated attacks.
func TestCoordinatedByzantine(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Coordinated Attack Test: Multiple Byzantine Nodes ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// With n=5, f=1, we can tolerate 1 Byzantine node
	// For this test, we simulate 1 Byzantine node (the maximum allowed)
	byzantineNode := types.NodeID(4)

	// Get leader
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)
	t.Logf("Byzantine node: Node%d", byzantineNode)

	// Ensure Byzantine node is not the leader for cleaner test
	if byzantineNode == leader {
		t.Log("Byzantine node is leader - test will include Byzantine leader behavior")
	}

	// Start consensus
	err = nodes[leader].ProposeBlock([]byte("coordinated_attack_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Coordinated attack: Byzantine node attempts multiple attack vectors simultaneously
	t.Log("Byzantine node executing coordinated attack:")

	// 1. Partition the Byzantine node to withhold votes
	t.Log("  - Withholding votes (partition)")
	err = helpers.BlockNode(byzantineNode)
	require.NoError(t, err)

	// 2. While partitioned, inject conflicting votes to the leader
	coord := nodes[leader].GetCoordinator()

	// Create double vote (equivocation)
	t.Log("  - Injecting double vote")
	vote1 := &types.Vote{
		BlockHash: types.BlockHash{1, 1, 1},
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("coordinated_vote_1"),
	}
	vote2 := &types.Vote{
		BlockHash: types.BlockHash{2, 2, 2},
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("coordinated_vote_2"),
	}

	// Track if conflicting votes are rejected (expected behavior)
	vote1Err := coord.ProcessVote(vote1)
	vote2Err := coord.ProcessVote(vote2)
	if vote1Err != nil {
		t.Logf("Vote 1 result: %v", vote1Err)
	}
	// ASSERTION: Conflicting vote MUST be rejected (equivocation detection)
	require.Error(t, vote2Err, "Conflicting vote must be rejected as equivocation")
	t.Logf("Vote 2 (conflicting) correctly rejected: %v", vote2Err)

	// Wait for partial timeout
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout / 2)

	// 3. Unblock and try to confuse with late messages
	t.Log("  - Unblocking and sending stale messages")
	err = helpers.UnblockNode(byzantineNode)
	require.NoError(t, err)

	// Send messages for wrong view
	staleVote := &types.Vote{
		BlockHash: types.BlockHash{3, 3, 3},
		View:      99, // Future view
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("coordinated_stale"),
	}
	staleErr := coord.ProcessVote(staleVote)
	if staleErr != nil {
		t.Logf("Stale vote rejected: %v", staleErr)
	}

	// Wait for consensus to complete
	time.Sleep(viewTimeout + 500*time.Millisecond)

	// Verify system maintained safety and liveness

	// 1. Check for equivocation detection
	equivocationEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventEquivocationFound {
			equivocationEvents++
		}
	}
	t.Logf("Equivocation detection events: %d", equivocationEvents)

	// 2. Verify honest nodes reached consensus or properly handled the attack
	honestNodeViews := make(map[types.NodeID]types.ViewNumber)
	for nodeID, node := range nodes {
		if nodeID != byzantineNode {
			honestNodeViews[nodeID] = node.GetCoordinator().GetCurrentView()
		}
	}
	t.Logf("Honest node views: %v", honestNodeViews)

	// 3. All honest nodes should be at same or adjacent views
	minView := types.ViewNumber(^uint64(0))
	maxView := types.ViewNumber(0)
	for _, v := range honestNodeViews {
		if v < minView {
			minView = v
		}
		if v > maxView {
			maxView = v
		}
	}

	viewDiff := maxView - minView
	assert.LessOrEqual(t, viewDiff, types.ViewNumber(2),
		"Honest nodes should be within 2 views despite coordinated attack")

	// System should remain operational (liveness check)
	// All honest nodes should be responsive
	assert.Equal(t, len(honestNodeViews), totalNodes-1,
		"All honest nodes should remain responsive during coordinated attack")

	// ASSERTION: Coordinated attack vectors must be handled
	// Either conflicting votes rejected, stale votes rejected, or equivocation detected
	attackHandled := vote2Err != nil || staleErr != nil || equivocationEvents > 0
	assert.True(t, attackHandled,
		"Coordinated attack must be handled: conflicting/stale votes rejected or equivocation detected")

	// 4. System should remain operational
	// Try to sync the Byzantine node for cleanup
	err = helpers.SyncNodeState(byzantineNode)
	if err != nil {
		t.Logf("Byzantine node sync: %v", err)
	}

	// 5. LIVENESS & SAFETY: Verify honest nodes committed blocks and agree on chain
	commits := CollectCommittedBlocks(tracer)
	AssertHonestNodesCommitted(t, commits, byzantineNode, "coordinated Byzantine attack")

	t.Log("=== Coordinated Byzantine Test Complete ===")
}

// TestAdaptiveAttackRecovery verifies that the system can recover from
// Byzantine nodes that change their attack strategy over time, attempting
// to find vulnerabilities in the protocol.
func TestAdaptiveAttackRecovery(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Coordinated Attack Test: Adaptive Attack Recovery ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	byzantineNode := types.NodeID(3)

	// Get leader
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader: Node%d, Byzantine: Node%d", leader, byzantineNode)

	viewTimeout := consensusConfig.GetTimeoutForView(0)

	// Phase 1: Byzantine node acts normally (gaining trust)
	t.Log("Phase 1: Byzantine node acting normally")
	err = nodes[leader].ProposeBlock([]byte("adaptive_phase_1"))
	require.NoError(t, err)
	time.Sleep(viewTimeout)

	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after phase 1: %d", currentView)

	// Phase 2: Byzantine node starts withholding votes
	t.Log("Phase 2: Byzantine node withholding votes")
	err = helpers.BlockNode(byzantineNode)
	require.NoError(t, err)

	nextLeader, _ := consensusConfig.GetLeaderForView(currentView)
	if nodes[nextLeader] != nil && nextLeader != byzantineNode {
		err = nodes[nextLeader].ProposeBlock([]byte("adaptive_phase_2"))
		if err != nil {
			t.Logf("Phase 2 proposal: %v", err)
		}
	}
	time.Sleep(viewTimeout)

	err = helpers.UnblockNode(byzantineNode)
	require.NoError(t, err)

	currentView = nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after phase 2: %d", currentView)

	// Phase 3: Byzantine node sends invalid messages
	t.Log("Phase 3: Byzantine node sending invalid messages")
	coord := nodes[leader].GetCoordinator()

	// Invalid vote with wrong phase
	invalidVote := &types.Vote{
		BlockHash: types.BlockHash{9, 9, 9},
		View:      currentView,
		Phase:     types.ConsensusPhase(99), // Invalid phase
		Voter:     byzantineNode,
		Signature: []byte("adaptive_invalid"),
	}
	invalidVoteErr := coord.ProcessVote(invalidVote)
	if invalidVoteErr != nil {
		t.Logf("Invalid vote rejected: %v", invalidVoteErr)
	}

	nextLeader, _ = consensusConfig.GetLeaderForView(currentView)
	if nodes[nextLeader] != nil && nextLeader != byzantineNode {
		err = nodes[nextLeader].ProposeBlock([]byte("adaptive_phase_3"))
		if err != nil {
			t.Logf("Phase 3 proposal: %v", err)
		}
	}
	time.Sleep(viewTimeout)

	currentView = nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after phase 3: %d", currentView)

	// Phase 4: Byzantine node tries timing attack
	t.Log("Phase 4: Byzantine node timing attack")
	err = helpers.BlockNode(byzantineNode)
	require.NoError(t, err)
	time.Sleep(viewTimeout / 4)
	err = helpers.UnblockNode(byzantineNode)
	require.NoError(t, err)
	time.Sleep(viewTimeout / 4)
	err = helpers.BlockNode(byzantineNode)
	require.NoError(t, err)
	time.Sleep(viewTimeout / 4)
	err = helpers.UnblockNode(byzantineNode)
	require.NoError(t, err)

	time.Sleep(viewTimeout)

	// Final verification
	t.Log("Verifying system recovery after adaptive attack")

	// Sync Byzantine node
	err = helpers.SyncNodeState(byzantineNode)
	if err != nil {
		t.Logf("Byzantine node sync: %v", err)
	}

	// Check final state of all nodes
	finalViews := make(map[types.NodeID]types.ViewNumber)
	for nodeID, node := range nodes {
		finalViews[nodeID] = node.GetCoordinator().GetCurrentView()
	}
	t.Logf("Final views: %v", finalViews)

	// Analyze events
	timeoutEvents := 0
	viewChangeEvents := 0
	commitEvents := 0
	for _, ev := range tracer.GetEvents() {
		switch ev.EventType {
		case events.EventViewTimeout:
			timeoutEvents++
		case events.EventViewChange:
			viewChangeEvents++
		case events.EventBlockCommitted:
			commitEvents++
		}
	}

	t.Logf("Timeout events: %d", timeoutEvents)
	t.Logf("View change events: %d", viewChangeEvents)
	t.Logf("Commit events: %d", commitEvents)

	// Verify honest nodes are operational and at similar views
	honestNodeViews := make([]types.ViewNumber, 0)
	for nodeID, v := range finalViews {
		if nodeID != byzantineNode {
			honestNodeViews = append(honestNodeViews, v)
		}
	}

	// All honest nodes should be within 3 views of each other after all attacks
	minView := honestNodeViews[0]
	maxView := honestNodeViews[0]
	for _, v := range honestNodeViews {
		if v < minView {
			minView = v
		}
		if v > maxView {
			maxView = v
		}
	}

	viewSpread := maxView - minView
	assert.LessOrEqual(t, viewSpread, types.ViewNumber(3),
		"Honest nodes should maintain view consistency despite adaptive attack")

	// System should have made some progress (view-based check)
	assert.Greater(t, maxView, types.ViewNumber(0),
		"System should have made progress despite attacks")

	// ASSERTION: Invalid votes must be rejected
	assert.Error(t, invalidVoteErr,
		"Invalid vote with wrong phase must be rejected")

	// LIVENESS & SAFETY: Verify honest nodes committed blocks and agree on chain
	commits := CollectCommittedBlocks(tracer)
	AssertHonestNodesCommitted(t, commits, byzantineNode, "adaptive attack recovery")

	// Verify commits actually occurred (not just counted)
	require.Greater(t, commitEvents, 0,
		"LIVENESS VIOLATION: No commit events despite system making 'progress'")

	t.Log("=== Adaptive Attack Recovery Test Complete ===")
}
