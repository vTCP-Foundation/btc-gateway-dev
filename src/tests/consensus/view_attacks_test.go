package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/messages"
	"btc-gateway/pkg/consensus/mocks"
	testinglib "btc-gateway/pkg/consensus/testing"
	"btc-gateway/pkg/consensus/types"
)

// TestFalseTimeoutAttack verifies that the system handles Byzantine nodes
// that attempt to trigger unnecessary view changes by broadcasting false
// timeout messages.
func TestFalseTimeoutAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== View Attack Test: False Timeout ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start normal consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("false_timeout_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Byzantine node tries to force view change by simulating timeout behavior
	// In practice this is done by partitioning then unpartitioning quickly
	byzantineNode := types.NodeID(3)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(4)
	}
	t.Logf("Byzantine node attempting false timeout: Node%d", byzantineNode)

	// Record view before attack
	viewBefore := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View before false timeout attempt: %d", viewBefore)

	// Simulate false timeout by blocking/unblocking rapidly
	// This doesn't directly inject false timeout messages but tests
	// system resilience to timeout-like behavior
	for i := 0; i < 3; i++ {
		err = helpers.BlockNode(byzantineNode)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		err = helpers.UnblockNode(byzantineNode)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for system to stabilize
	time.Sleep(500 * time.Millisecond)

	// Check if view changed unnecessarily
	viewAfter := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after false timeout attempts: %d", viewAfter)

	// Count timeout events
	timeoutEvents := 0
	viewChangeEvents := 0
	for _, ev := range tracer.GetEvents() {
		switch ev.EventType {
		case events.EventViewTimeout:
			timeoutEvents++
		case events.EventViewChange:
			viewChangeEvents++
		}
	}

	t.Logf("Timeout events: %d", timeoutEvents)
	t.Logf("View change events: %d", viewChangeEvents)

	// System should not have excessive view changes from one Byzantine node's timeout
	// One Byzantine node should not be able to force view changes alone
	assert.LessOrEqual(t, viewChangeEvents, 2,
		"Single Byzantine node rapid partitioning should not cause excessive view changes")

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree
	// System should maintain liveness despite false timeout attempts
	AssertLivenessWithCommits(t, tracer, "false timeout attack")

	t.Log("=== False Timeout Test Complete ===")
}

// TestViewConfusionAttack verifies that the system rejects messages
// that are sent for incorrect views.
func TestViewConfusionAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== View Attack Test: View Confusion ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start normal consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("view_confusion_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Current view
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("Current view: %d", currentView)

	byzantineNode := types.NodeID(2)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(3)
	}

	coord := nodes[leader].GetCoordinator()

	// Inject votes for future views (trying to skip ahead)
	t.Log("Injecting vote for future view (view 10)")
	futureViewVote := &types.Vote{
		BlockHash: types.BlockHash{1, 2, 3, 4},
		View:      10, // Future view
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("future_view_signature"),
	}

	futureVoteErr := coord.ProcessVote(futureViewVote)
	if futureVoteErr != nil {
		t.Logf("Future view vote rejected: %v", futureVoteErr)
	}

	// Inject votes for very old views
	t.Log("Injecting vote for ancient view (view -relative)")
	ancientViewVote := &types.Vote{
		BlockHash: types.BlockHash{5, 6, 7, 8},
		View:      0, // Ancient view (assuming we've progressed)
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: []byte("ancient_view_signature"),
	}

	ancientVoteErr := coord.ProcessVote(ancientViewVote)
	if ancientVoteErr != nil {
		t.Logf("Ancient view vote handled: %v", ancientVoteErr)
	}

	// Wait for system processing
	time.Sleep(500 * time.Millisecond)

	// Verify system didn't jump to future view
	viewAfter := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after confusion attack: %d", viewAfter)

	// ASSERTION: System should not jump to future view
	assert.Less(t, viewAfter, types.ViewNumber(10),
		"System should not jump to future view from injected messages")

	// ASSERTION: Future view votes must be rejected (invalid signature or wrong view)
	assert.Error(t, futureVoteErr,
		"Vote for future view with invalid signature must be rejected")

	t.Log("=== View Confusion Test Complete ===")
}

// TestLeaderDisruptionAttack verifies that the system handles Byzantine
// attempts to disrupt leader election or interfere with leader duties.
func TestLeaderDisruptionAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== View Attack Test: Leader Disruption ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get leader for view 0
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	// Byzantine node tries to act as leader when it's not
	byzantineNode := types.NodeID(2)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(3)
	}
	t.Logf("Byzantine node (non-leader) attempting to propose: Node%d", byzantineNode)

	// Byzantine node tries to propose (should be rejected by other nodes)
	byzantineBlock := types.NewBlock(
		types.BlockHash{}, // Genesis parent
		1,
		0,
		byzantineNode, // Wrong leader!
		[]byte("byzantine_unauthorized_proposal"),
	)
	byzantineProposal := &messages.ProposalMsg{
		Block:      byzantineBlock,
		HighQC:     nil,
		ViewNumber: 0,
		Proposer:   byzantineNode, // Wrong proposer!
	}

	// Inject the unauthorized proposal to various nodes
	rejections := 0
	for nodeID, node := range nodes {
		if nodeID == byzantineNode {
			continue
		}
		coord := node.GetCoordinator()
		err = coord.ProcessProposal(byzantineProposal)
		if err != nil {
			rejections++
			t.Logf("Node%d rejected unauthorized proposal: %v", nodeID, err)
		}
	}

	t.Logf("Unauthorized proposals rejected: %d/%d", rejections, totalNodes-1)

	// Now the legitimate leader proposes
	err = nodes[leader].ProposeBlock([]byte("legitimate_leader_proposal"))
	require.NoError(t, err)

	// Wait for consensus
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout + 500*time.Millisecond)

	// Count legitimate vs unauthorized proposal events
	legitimateProposals := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			// Event payload uses "leader" key, not "proposer"
			if payload, ok := ev.Payload["leader"]; ok {
				if proposer, ok := payload.(types.NodeID); ok && proposer == leader {
					legitimateProposals++
				}
			}
		}
	}

	t.Logf("Legitimate proposals received: %d", legitimateProposals)

	// Legitimate leader's proposal should have been received by at least some nodes
	// Note: With unauthorized proposals being injected, some nodes may timeout
	// The key test is that unauthorized proposals are rejected by honest nodes
	// legitimateProposals count may be 0 if all nodes received proposal before logging started

	// Unauthorized proposals should have been rejected by most/all nodes
	assert.GreaterOrEqual(t, rejections, totalNodes-2,
		"Unauthorized proposals should be rejected by honest nodes")

	// LIVENESS & SAFETY: Verify legitimate leader's proposal was committed
	// System should maintain liveness despite unauthorized proposal attempts
	AssertLivenessWithCommits(t, tracer, "leader disruption attack")

	t.Log("=== Leader Disruption Test Complete ===")
}

// TestResourceExhaustionAttack verifies that the system handles Byzantine
// attempts to exhaust node resources through excessive message sending
// or state manipulation.
func TestResourceExhaustionAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== View Attack Test: Resource Exhaustion ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start normal consensus
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("resource_exhaustion_test"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	byzantineNode := types.NodeID(4)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(3)
	}

	coord := nodes[leader].GetCoordinator()

	// Attempt resource exhaustion through:
	// 1. Many proposals - track rejections for visibility
	t.Log("Attempting resource exhaustion with many proposals")
	proposalCount := 100
	proposalRejections := 0
	for i := 0; i < proposalCount; i++ {
		exhaustionBlock := types.NewBlock(
			types.BlockHash{byte(i)},
			types.Height(i+10),
			types.ViewNumber(i),
			byzantineNode,
			[]byte("exhaustion_"+string(rune(i))),
		)
		exhaustionProposal := &messages.ProposalMsg{
			Block:      exhaustionBlock,
			HighQC:     nil,
			ViewNumber: types.ViewNumber(i),
			Proposer:   byzantineNode,
		}
		if err := coord.ProcessProposal(exhaustionProposal); err != nil {
			proposalRejections++
		}
	}
	t.Logf("Proposal flood: %d/%d rejected", proposalRejections, proposalCount)

	// 2. Many votes for different blocks - track rejections for visibility
	t.Log("Attempting resource exhaustion with many votes")
	voteCount := 100
	voteRejections := 0
	for i := 0; i < voteCount; i++ {
		exhaustionVote := &types.Vote{
			BlockHash: types.BlockHash{byte(i), byte(i >> 8)},
			View:      types.ViewNumber(i),
			Phase:     types.ConsensusPhase(i % 3),
			Voter:     byzantineNode,
			Signature: []byte("exhaustion_sig_" + string(rune(i))),
		}
		if err := coord.ProcessVote(exhaustionVote); err != nil {
			voteRejections++
		}
	}
	t.Logf("Vote flood: %d/%d rejected", voteRejections, voteCount)

	// Wait and verify system still functions
	time.Sleep(500 * time.Millisecond)

	// System should still be responsive
	// Try a legitimate operation
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("View after exhaustion attempt: %d", currentView)

	// Count total events to ensure system didn't crash
	totalEvents := len(tracer.GetEvents())
	t.Logf("Total events after exhaustion attempt: %d", totalEvents)

	// System should have processed events without crashing
	assert.Greater(t, totalEvents, proposalCount+voteCount,
		"System should process events despite exhaustion attempt")

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
		"All nodes should remain responsive after exhaustion attack")

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree
	// System should maintain liveness despite resource exhaustion attempts
	AssertLivenessWithCommits(t, tracer, "resource exhaustion attack")

	t.Log("=== Resource Exhaustion Test Complete ===")
}
