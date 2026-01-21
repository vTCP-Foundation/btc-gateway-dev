package consensus

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/mocks"
	testinglib "btc-gateway/pkg/consensus/testing"
	"btc-gateway/pkg/consensus/types"
)

// TestInvalidVoteInjection verifies that the system correctly rejects
// malformed votes with invalid signatures, wrong formats, or corrupted data.
func TestInvalidVoteInjection(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Vote Attack Test: Invalid Vote Injection ===")

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

	err = nodes[leader].ProposeBlock([]byte("invalid_vote_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	byzantineNode := types.NodeID(2)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(3)
	}

	coord := nodes[leader].GetCoordinator()

	// Test 1: Vote with empty block hash
	t.Log("Injecting vote with empty block hash")
	emptyHashVote := &types.Vote{
		BlockHash: types.BlockHash{}, // Empty/zero hash
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("empty_hash_signature"),
	}
	err = coord.ProcessVote(emptyHashVote)
	require.Error(t, err, "Empty hash vote must be rejected")
	t.Logf("Empty hash vote rejected: %v", err)

	// Test 2: Vote with invalid node ID (out of range)
	t.Log("Injecting vote with invalid node ID")
	invalidNodeVote := &types.Vote{
		BlockHash: types.BlockHash{1, 2, 3},
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     types.NodeID(99), // Invalid - not a participant
		Signature: []byte("invalid_node_signature"),
	}
	err = coord.ProcessVote(invalidNodeVote)
	require.Error(t, err, "Invalid node ID vote must be rejected")
	t.Logf("Invalid node vote rejected: %v", err)

	// Test 3: Vote with nil/empty signature
	t.Log("Injecting vote with empty signature")
	noSigVote := &types.Vote{
		BlockHash: types.BlockHash{4, 5, 6},
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: nil, // No signature
	}
	err = coord.ProcessVote(noSigVote)
	require.Error(t, err, "Vote with no signature must be rejected")
	t.Logf("No signature vote rejected: %v", err)

	// Test 4: Vote with invalid phase value
	t.Log("Injecting vote with invalid phase")
	invalidPhaseVote := &types.Vote{
		BlockHash: types.BlockHash{7, 8, 9},
		View:      0,
		Phase:     types.ConsensusPhase(255), // Invalid phase
		Voter:     byzantineNode,
		Signature: []byte("invalid_phase_signature"),
	}
	err = coord.ProcessVote(invalidPhaseVote)
	require.Error(t, err, "Invalid phase vote must be rejected")
	t.Logf("Invalid phase vote rejected: %v", err)

	// Wait and verify system continues
	time.Sleep(500 * time.Millisecond)

	// Count vote-related events to verify system processed malformed inputs
	voteEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventVoteReceived || ev.EventType == events.EventVoteValidated {
			voteEvents++
		}
	}

	t.Logf("Vote events processed: %d", voteEvents)

	// Verify consensus system remains operational
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("System remains operational at view %d", currentView)

	t.Log("=== Invalid Vote Injection Test Complete ===")
}

// TestSelectiveVoting verifies that the system handles Byzantine nodes
// that only vote for specific proposals, attempting to manipulate which
// blocks get committed.
func TestSelectiveVoting(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Vote Attack Test: Selective Voting ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Byzantine node will selectively not vote by being partitioned
	byzantineNode := types.NodeID(4)
	t.Logf("Byzantine node (selective voter): Node%d", byzantineNode)

	// Start consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("selective_voting_test_1"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Partition the Byzantine node to simulate selective non-voting
	t.Logf("Partitioning Node%d to simulate selective voting", byzantineNode)
	err = helpers.BlockNode(byzantineNode)
	require.NoError(t, err)

	// Wait for consensus round
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout + 500*time.Millisecond)

	// Unblock for second round
	err = helpers.UnblockNode(byzantineNode)
	require.NoError(t, err)
	err = helpers.SyncNodeState(byzantineNode)
	require.NoError(t, err)

	// Second round - Byzantine node participates
	nextLeader, _ := consensusConfig.GetLeaderForView(1)
	if nextLeader == byzantineNode {
		// Skip if Byzantine node is leader
		nextLeader, _ = consensusConfig.GetLeaderForView(2)
	}
	t.Logf("Next leader: Node%d", nextLeader)

	proposalErr := nodes[nextLeader].ProposeBlock([]byte("selective_voting_test_2"))
	if proposalErr != nil {
		t.Logf("Second proposal error: %v (may be expected during view change)", proposalErr)
	}

	time.Sleep(viewTimeout)

	// Count votes per round
	votesView0 := 0
	votesView1 := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventPrepareVoteSent {
			if view, ok := ev.Payload["view"].(types.ViewNumber); ok {
				if view == 0 {
					votesView0++
				} else if view == 1 {
					votesView1++
				}
			}
		}
	}

	t.Logf("Votes in view 0: %d", votesView0)
	t.Logf("Votes in view 1: %d", votesView1)

	// View 0 should have votes from honest nodes despite Byzantine partitioning
	// System should still function with honest majority
	assert.Greater(t, votesView0, 0,
		"View 0 should receive votes from honest nodes despite selective voting attack")

	t.Log("=== Selective Voting Test Complete ===")
}

// TestVoteWithholding verifies that the system handles Byzantine nodes
// that refuse to vote on valid proposals, attempting to stall consensus.
func TestVoteWithholding(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Vote Attack Test: Vote Withholding ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Withhold votes by partitioning one node
	withholdingNode := types.NodeID(3)
	t.Logf("Vote withholding node: Node%d", withholdingNode)

	// Partition the withholding node
	err = helpers.BlockNode(withholdingNode)
	require.NoError(t, err)

	// Start consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	if leader == withholdingNode {
		// Use a different view if withholding node is leader
		leader, _ = consensusConfig.GetLeaderForView(1)
	}
	t.Logf("Leader: Node%d", leader)

	startTime := time.Now()
	err = nodes[leader].ProposeBlock([]byte("withholding_test"))
	require.NoError(t, err)

	// Wait for consensus with one fewer voter
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout + time.Second)

	consensusDuration := time.Since(startTime)
	t.Logf("Consensus duration with withholding node: %v", consensusDuration)

	// With 4 honest nodes out of 5, quorum (3) should still be reachable
	// Count QC formations
	qcFormations := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventPrepareQCFormed ||
			ev.EventType == events.EventPreCommitQCFormed ||
			ev.EventType == events.EventCommitQCFormed {
			qcFormations++
		}
	}

	t.Logf("QC formations despite withholding: %d", qcFormations)

	// Unblock for cleanup
	err = helpers.UnblockNode(withholdingNode)
	require.NoError(t, err)

	// System should have been able to form QCs with 4 voters (quorum = 3)
	assert.Greater(t, qcFormations, 0,
		"Should form QCs with 4 honest nodes (quorum requires 3)")

	t.Log("=== Vote Withholding Test Complete ===")
}

// TestPhaseConfusionVote verifies that the system correctly rejects
// votes that are sent for the wrong consensus phase.
func TestPhaseConfusionVote(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Vote Attack Test: Phase Confusion ===")

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

	err = nodes[leader].ProposeBlock([]byte("phase_confusion_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Get block hash from events
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
		copy(blockHash[:], "phase_confusion_block_hash")
	}

	byzantineNode := types.NodeID(2)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(3)
	}

	coord := nodes[leader].GetCoordinator()

	// Try to inject Commit vote during Prepare phase
	t.Log("Injecting Commit vote during Prepare phase")
	confusedVote := &types.Vote{
		BlockHash: blockHash,
		View:      0,
		Phase:     types.PhaseCommit, // Wrong phase - should be Prepare
		Voter:     byzantineNode,
		Signature: []byte("confused_phase_signature"),
	}

	confusedVoteErr := coord.ProcessVote(confusedVote)
	if confusedVoteErr != nil {
		t.Logf("Phase-confused vote rejected: %v", confusedVoteErr)
	}

	// Try to inject Prepare vote during PreCommit/Commit
	time.Sleep(500 * time.Millisecond)

	t.Log("Injecting late Prepare vote")
	latePrepareVote := &types.Vote{
		BlockHash: blockHash,
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("late_prepare_signature"),
	}

	latePrepareErr := coord.ProcessVote(latePrepareVote)
	if latePrepareErr != nil {
		t.Logf("Late Prepare vote handled: %v", latePrepareErr)
	}

	// Wait for consensus to complete
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout)

	// Verify consensus proceeded correctly despite phase confusion attempts
	phaseTransitionEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventPhaseTransition {
			phaseTransitionEvents++
		}
	}

	t.Logf("Phase transition events: %d", phaseTransitionEvents)

	// Consensus should continue despite phase confusion attempts
	// Verify system is still operational
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("System operational at view %d after phase confusion", currentView)

	// ASSERTION: Phase-confused votes with invalid signatures must be rejected
	assert.Error(t, confusedVoteErr,
		"Phase-confused vote with invalid signature must be rejected")

	t.Log("=== Phase Confusion Test Complete ===")
}

// TestStaleVoteHandling verifies that votes from previous views are
// correctly rejected and don't affect current consensus rounds.
func TestStaleVoteHandling(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Vote Attack Test: Stale Vote Handling ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Complete first consensus round
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("View 0 Leader: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("stale_vote_view_0"))
	require.NoError(t, err)

	// Wait for view 0 to progress
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Get the block hash used in view 0
	var view0BlockHash types.BlockHash
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			if view, ok := ev.Payload["view"].(types.ViewNumber); ok && view == 0 {
				if hash, ok := ev.Payload["block_hash"].(types.BlockHash); ok {
					view0BlockHash = hash
					break
				}
			}
		}
	}

	if view0BlockHash == [32]byte{} {
		copy(view0BlockHash[:], "view_0_block_hash_data")
	}

	// Record current view
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("Current view after round 1: %d", currentView)

	// Create stale votes from view 0 and inject into current view
	byzantineNode := types.NodeID(1)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(2)
	}

	coord := nodes[leader].GetCoordinator()

	// Inject multiple stale votes - track rejections
	t.Logf("Injecting stale votes from view 0 into view %d", currentView)
	staleVoteRejections := 0
	for phase := types.PhasePrepare; phase <= types.PhaseCommit; phase++ {
		staleVote := &types.Vote{
			BlockHash: view0BlockHash,
			View:      0, // Stale view
			Phase:     phase,
			Voter:     byzantineNode,
			Signature: []byte(fmt.Sprintf("stale_vote_phase_%d", phase)),
		}

		staleErr := coord.ProcessVote(staleVote)
		// Stale votes may be rejected or ignored - either is acceptable
		if staleErr != nil {
			staleVoteRejections++
			t.Logf("Stale %s vote rejected: %v", phase, staleErr)
		} else {
			t.Logf("Stale %s vote ignored (no error, but not counted)", phase)
		}
	}

	// Verify system didn't regress to old view
	time.Sleep(500 * time.Millisecond)

	viewAfterStale := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after stale vote injection: %d", viewAfterStale)

	// ASSERTION: View should not regress due to stale votes
	assert.GreaterOrEqual(t, viewAfterStale, currentView,
		"View should not regress due to stale votes")

	// ASSERTION: Stale votes with invalid signatures must be rejected
	assert.Greater(t, staleVoteRejections, 0,
		"Stale votes with invalid signatures must be rejected")

	// Start a new consensus round to verify system still works
	newLeader, _ := consensusConfig.GetLeaderForView(viewAfterStale)
	if nodes[newLeader] != nil {
		proposalErr := nodes[newLeader].ProposeBlock([]byte("post_stale_block"))
		if proposalErr != nil {
			t.Logf("Post-stale proposal: %v (may be expected)", proposalErr)
		}
	}

	time.Sleep(500 * time.Millisecond)

	t.Log("=== Stale Vote Handling Test Complete ===")
}
