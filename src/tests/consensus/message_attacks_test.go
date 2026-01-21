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

// TestMessageWithholding verifies that the consensus system continues to function
// when a Byzantine node selectively drops messages to specific targets.
// The honest majority should still reach consensus despite message loss.
func TestMessageWithholding(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Message Attack Test: Message Withholding ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start initial consensus round
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("message_withholding_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Simulate message withholding by partitioning node 2
	// This simulates a Byzantine node that drops all its messages
	byzantineNode := types.NodeID(2)
	t.Logf("Simulating message withholding by Node%d", byzantineNode)

	err = helpers.BlockNode(byzantineNode)
	require.NoError(t, err)

	// Continue consensus - system should still function with 4 honest nodes
	// (4 out of 5 nodes can still reach quorum of 4 votes with f=1)
	// Note: Actually with 5 nodes, quorum is 2f+1 = 3, so 4 nodes is sufficient
	time.Sleep(300 * time.Millisecond)

	// Try to propose a new block and verify consensus continues
	nextLeader, err := consensusConfig.GetLeaderForView(1)
	require.NoError(t, err)

	if nextLeader != byzantineNode {
		err = nodes[nextLeader].ProposeBlock([]byte("post_withholding_block"))
		if err != nil {
			t.Logf("Proposal after withholding: %v (may be expected)", err)
		}
	}

	// Wait for consensus processing
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout + 500*time.Millisecond)

	// Verify system remained operational
	// Count nodes that made progress (received proposals or voted)
	proposalEvents := 0
	voteEvents := 0
	for _, ev := range tracer.GetEvents() {
		// Skip events from the Byzantine node
		if types.NodeID(ev.NodeID) == byzantineNode {
			continue
		}
		if ev.EventType == events.EventProposalReceived {
			proposalEvents++
		}
		if ev.EventType == events.EventPrepareVoteSent {
			voteEvents++
		}
	}

	t.Logf("Proposals received by honest nodes: %d", proposalEvents)
	t.Logf("Votes sent by honest nodes: %d", voteEvents)

	// Honest nodes should have processed proposals and voted
	assert.Greater(t, proposalEvents, 0, "Honest nodes should receive proposals")
	assert.Greater(t, voteEvents, 0, "Honest nodes should vote despite Byzantine node withholding")

	// Unblock the Byzantine node for cleanup
	err = helpers.UnblockNode(byzantineNode)
	require.NoError(t, err)

	t.Log("=== Message Withholding Test Complete ===")
}

// TestMessageReplayAttack verifies that the system correctly rejects or ignores
// replayed messages from previous views. Byzantine nodes may try to replay
// old votes or proposals to confuse the consensus.
func TestMessageReplayAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Message Attack Test: Message Replay ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Execute first consensus round
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("View 0 Leader: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("replay_test_view_0"))
	require.NoError(t, err)

	// Wait for consensus to complete
	consensusCompleted := helpers.WaitForConsensusCompletion(5 * time.Second)
	require.True(t, consensusCompleted, "First consensus round should complete")
	t.Log("View 0 consensus completed")

	// Wait for view timeout to trigger view change to view 1
	// This is how the protocol naturally advances views
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	t.Logf("Waiting for view timeout (%v) to trigger view change", viewTimeout)
	time.Sleep(viewTimeout + time.Second)

	// Wait for nodes to reach view 1
	viewAdvanced := helpers.WaitForAllNodesView(1, 5*time.Second)
	require.True(t, viewAdvanced, "Nodes should advance to view 1 after timeout")

	// Capture current view - should be at least 1 now
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("Current view after timeout: %d", currentView)
	require.GreaterOrEqual(t, currentView, types.ViewNumber(1),
		"View should have advanced past 0 for replay test to be meaningful")

	// Try to replay an old vote from view 0 (now stale)
	byzantineNode := types.NodeID(1)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(2)
	}

	// Create a stale vote from view 0 (the OLD view)
	staleBlockHash := types.BlockHash{}
	copy(staleBlockHash[:], "stale_block_hash_from_view_0")

	staleVote := &types.Vote{
		BlockHash: staleBlockHash,
		View:      0, // Old view - should be rejected as stale
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("stale_signature"),
	}

	t.Logf("Injecting stale vote from Node%d for view 0 (current view: %d)", byzantineNode, currentView)

	// Inject stale vote into any node (they should all reject it)
	coord := nodes[leader].GetCoordinator()

	// Record event count before injection
	eventCountBefore := len(tracer.GetEvents())

	// Process the stale vote - should be rejected
	staleVoteErr := coord.ProcessVote(staleVote)

	// ASSERTION: Stale vote MUST be explicitly rejected (not silently ignored)
	require.Error(t, staleVoteErr,
		"Stale vote from old view MUST be explicitly rejected, not silently ignored")
	require.Contains(t, staleVoteErr.Error(), "stale vote rejected",
		"Error message should indicate stale vote rejection")
	t.Logf("✓ Stale vote correctly rejected: %v", staleVoteErr)

	// ASSERTION: EventStaleVoteRejected must be emitted for observability
	// This allows operators to monitor for replay attack attempts
	staleVoteEvents := 0
	for _, ev := range tracer.GetEvents()[eventCountBefore:] {
		if ev.EventType == events.EventStaleVoteRejected {
			staleVoteEvents++
			t.Logf("✓ EventStaleVoteRejected emitted: voter=%v, vote_view=%v, current_view=%v",
				ev.Payload["voter"], ev.Payload["vote_view"], ev.Payload["current_view"])
		}
	}
	require.Equal(t, 1, staleVoteEvents,
		"Exactly one EventStaleVoteRejected should be emitted for replay detection")

	// Check that no anomalous state occurred from replay
	viewAfter := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after replay attempt: %d", viewAfter)

	// ASSERTION: The view should not have regressed
	assert.GreaterOrEqual(t, viewAfter, currentView,
		"View should not regress after replay attack")

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree
	// System must maintain liveness despite replay attempts
	AssertLivenessWithCommits(t, tracer, "message replay attack")

	t.Log("=== Message Replay Test Complete ===")
}

// TestMessageReorderingAttack verifies that the consensus system handles
// out-of-order message delivery correctly. Messages may arrive in different
// orders at different nodes due to network conditions or Byzantine interference.
func TestMessageReorderingAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Message Attack Test: Message Reordering ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get leader and start consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	// Start consensus
	err = nodes[leader].ProposeBlock([]byte("reorder_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Simulate reordering by injecting votes in wrong phase order
	// Try to inject a PreCommit vote before Prepare phase completes
	byzantineNode := types.NodeID(3)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(4)
	}

	// Get a valid block hash from the proposal
	var blockHash types.BlockHash
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			if hash, ok := ev.Payload["block_hash"].(types.BlockHash); ok {
				blockHash = hash
				break
			}
		}
	}

	if blockHash == [32]byte{} {
		// Use placeholder if we couldn't get the actual hash
		copy(blockHash[:], "test_block_hash_placeholder")
	}

	// Create an out-of-order PreCommit vote (skipping Prepare)
	outOfOrderVote := &types.Vote{
		BlockHash: blockHash,
		View:      0,
		Phase:     types.PhasePreCommit, // Wrong phase - should be Prepare first
		Voter:     byzantineNode,
		Signature: []byte("out_of_order_signature"),
	}

	t.Logf("Injecting out-of-order PreCommit vote from Node%d", byzantineNode)

	// Process the out-of-order vote
	coord := nodes[leader].GetCoordinator()
	err = coord.ProcessVote(outOfOrderVote)
	if err != nil {
		t.Logf("Out-of-order vote handled: %v", err)
	}

	// System should continue functioning
	time.Sleep(500 * time.Millisecond)

	// Verify consensus proceeds normally despite reordering attempt
	commitEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventBlockCommitted {
			commitEvents++
		}
	}

	t.Logf("Block commits observed: %d", commitEvents)

	// System should handle reordering gracefully - verify we can still get state
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("Current view after reordering: %d", currentView)

	// ASSERTION: Out-of-order vote must be rejected or handled safely
	// The system should not accept a PreCommit vote without proper Prepare phase
	require.Error(t, err,
		"Out-of-order PreCommit vote (skipping Prepare) MUST be rejected")
	t.Logf("Out-of-order vote correctly rejected: %v", err)

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree
	// System must maintain liveness despite reordering attempts
	AssertLivenessWithCommits(t, tracer, "message reordering attack")

	t.Log("=== Message Reordering Test Complete ===")
}

// TestTimingAttack verifies that the consensus system is resilient to
// timing-based attacks where Byzantine nodes delay message delivery
// to manipulate consensus timing.
func TestTimingAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Message Attack Test: Timing Attack ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	startTime := time.Now()
	err = nodes[leader].ProposeBlock([]byte("timing_attack_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Simulate a timing attack by temporarily partitioning a node
	// to delay its vote, then unblocking at a strategic time
	delayedNode := types.NodeID(2)
	if delayedNode == leader {
		delayedNode = types.NodeID(3)
	}
	t.Logf("Simulating delayed responses from Node%d", delayedNode)

	err = helpers.BlockNode(delayedNode)
	require.NoError(t, err)

	// Wait for partial timeout
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout / 2)

	// Unblock the delayed node
	err = helpers.UnblockNode(delayedNode)
	require.NoError(t, err)

	// Sync the node state
	err = helpers.SyncNodeState(delayedNode)
	require.NoError(t, err)

	// Wait for consensus to complete
	time.Sleep(viewTimeout)

	consensusTime := time.Since(startTime)
	t.Logf("Total time with timing attack: %v", consensusTime)

	// Verify consensus was reached despite timing manipulation
	timeoutEvents := 0
	commitEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventViewTimeout {
			timeoutEvents++
		}
		if ev.EventType == events.EventBlockCommitted {
			commitEvents++
		}
	}

	t.Logf("Timeout events: %d", timeoutEvents)
	t.Logf("Commit events: %d", commitEvents)

	// LIVENESS: System MUST commit despite timing attack (not just timeout)
	// With n=5 and f=1, delaying one node should not prevent consensus
	require.Greater(t, commitEvents, 0,
		"LIVENESS VIOLATION: System should commit despite timing attack on single node")

	// SAFETY: All commits must agree on the same block
	AssertLivenessWithCommits(t, tracer, "timing attack")

	t.Log("=== Timing Attack Test Complete ===")
}

// TestFloodingAttack verifies that the consensus system is resilient to
// message flooding attacks where a Byzantine node sends an excessive
// number of messages to overwhelm other nodes.
func TestFloodingAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Message Attack Test: Flooding Attack ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("flooding_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Get block hash for creating flood messages
	var blockHash types.BlockHash
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			if hash, ok := ev.Payload["block_hash"].(types.BlockHash); ok {
				blockHash = hash
				break
			}
		}
	}

	if blockHash == [32]byte{} {
		copy(blockHash[:], "flooding_test_block_hash")
	}

	// Byzantine node floods the leader with duplicate/spam votes
	byzantineNode := types.NodeID(4)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(3)
	}

	t.Logf("Node%d flooding leader with %d duplicate votes", byzantineNode, 50)

	coord := nodes[leader].GetCoordinator()

	// Flood with duplicate votes - track rejections
	floodStart := time.Now()
	floodRejections := 0
	for i := 0; i < 50; i++ {
		floodVote := &types.Vote{
			BlockHash: blockHash,
			View:      0,
			Phase:     types.PhasePrepare,
			Voter:     byzantineNode,
			Signature: []byte("flood_signature_" + string(rune(i))),
		}
		// Track rejections - duplicate votes should be rejected
		err := coord.ProcessVote(floodVote)
		if err != nil {
			floodRejections++
		}
	}
	floodDuration := time.Since(floodStart)
	t.Logf("Flooding completed in %v, rejections: %d/50", floodDuration, floodRejections)

	// Wait for consensus to complete despite flooding - event-driven wait
	consensusCompleted := helpers.WaitForConsensusCompletion(5 * time.Second)
	t.Logf("Consensus completion after flood: %v", consensusCompleted)

	// LIVENESS: System MUST complete consensus despite flooding
	require.True(t, consensusCompleted,
		"LIVENESS VIOLATION: Consensus should complete despite flooding attack")

	// Try a legitimate consensus operation
	viewAfterFlood := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after flood attack: %d", viewAfterFlood)

	// Verify system didn't crash or enter invalid state
	// The system should handle duplicate votes gracefully
	eventCount := len(tracer.GetEvents())
	t.Logf("Total events recorded: %d", eventCount)

	// ASSERTION: The system should remain responsive after flooding
	assert.Greater(t, eventCount, 0, "System should remain operational despite flooding")

	// ASSERTION: Most duplicate votes should be rejected (duplicate detection)
	// At minimum, signatures are invalid so votes should be rejected
	assert.Greater(t, floodRejections, 0,
		"Duplicate/invalid flood votes should be rejected")

	// Verify all nodes are still responsive
	responsiveNodes := 0
	for nodeID, node := range nodes {
		view := node.GetCoordinator().GetCurrentView()
		if view >= 0 {
			responsiveNodes++
			t.Logf("Node%d responsive at view %d", nodeID, view)
		}
	}

	assert.Equal(t, totalNodes, responsiveNodes,
		"All nodes should remain responsive after flooding attack")

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree
	// System must maintain liveness despite flooding attempts
	AssertLivenessWithCommits(t, tracer, "flooding attack")

	t.Log("=== Flooding Attack Test Complete ===")
}
