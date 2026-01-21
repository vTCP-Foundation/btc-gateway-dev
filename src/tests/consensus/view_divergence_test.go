//go:build consensus_testing

package consensus

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/messages"
	"btc-gateway/pkg/consensus/mocks"
	testinglib "btc-gateway/pkg/consensus/testing"
	"btc-gateway/pkg/consensus/types"
)

// =============================================================================
// View Divergence Tests
// =============================================================================
// These tests verify the fix for the view divergence liveness bug where nodes
// starting at different view numbers (after cluster restart with divergent TiKV
// state) could not reach timeout quorum and thus could not advance.
//
// The fix adds view advancement when receiving a timeout message for a higher
// view, mirroring the existing behavior for NewView messages.
// =============================================================================

// TestViewDivergenceAfterRestart verifies that nodes starting at different views
// can converge and resume consensus (regression test for view divergence bug).
func TestViewDivergenceAfterRestart(t *testing.T) {
	t.Parallel()

	// Setup: 5-node network with divergent initial views
	const nodeCount = 5
	divergentViews := []types.ViewNumber{5, 6, 7, 8, 4} // Bug scenario from report

	t.Log("=== View Divergence Test: Recovery After Restart ===")
	t.Logf("Setting up %d nodes with divergent views: %v", nodeCount, divergentViews)

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	// Create view change helpers
	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Set divergent views while network is running (simulating restart with divergent state)
	// ForceSetView is thread-safe and resets the view timer
	err = helpers.SetupDivergentViews(divergentViews)
	require.NoError(t, err, "Should be able to set divergent views")

	// Verify divergent views are set
	for nodeID, expectedView := range divergentViews {
		actualView := nodes[types.NodeID(nodeID)].GetCoordinator().GetCurrentView()
		t.Logf("Node %d: expected view %d, actual view %d", nodeID, expectedView, actualView)
		assert.Equal(t, expectedView, actualView, "Node %d should be at view %d", nodeID, expectedView)
	}

	// Wait for view convergence - when view timers expire, nodes broadcast timeouts
	// and should advance to the highest view due to our fix
	maxView := types.ViewNumber(8)
	convergenceTimeout := consensusConfig.GetTimeoutForView(maxView) * 3

	t.Logf("Waiting up to %v for view convergence...", convergenceTimeout)

	converged := helpers.WaitForViewConvergence(convergenceTimeout)

	// Log final views regardless of result
	finalViews := helpers.GetNodeViews()
	t.Logf("Final node views: %v", finalViews)

	require.True(t, converged, "Nodes failed to converge to same view within timeout")
	t.Log("All nodes converged to the same view - view divergence bug is fixed!")

	// The key assertion is that convergence happened - nodes with divergent views
	// were able to sync to the same view. The consensus proposal test is done
	// separately in TestTimeoutMessageAdvancesViewOnHigherView.
}

// TestTimeoutMessageAdvancesViewOnHigherView verifies that receiving a valid
// timeout message for a higher view causes the local node to advance its view.
// This test uses direct message injection to test the core logic without
// depending on network routing.
func TestTimeoutMessageAdvancesViewOnHigherView(t *testing.T) {
	t.Parallel()

	const nodeCount = 5
	t.Log("=== View Divergence Test: Timeout Advances View ===")

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Set node 0 to view 5
	initialView := types.ViewNumber(5)
	err = helpers.ForceNodeToView(0, initialView)
	require.NoError(t, err)

	t.Logf("Node 0 set to view %d", initialView)

	// Verify initial state
	require.Equal(t, initialView, nodes[0].GetCoordinator().GetCurrentView())

	// Create a timeout message from a hypothetical higher-view node
	higherView := types.ViewNumber(10)

	// Get node 1's crypto for signing
	node1Crypto := nodes[1].GetCrypto()
	timeoutData := fmt.Sprintf("timeout:%d:%d", higherView, 1)
	signature, err := node1Crypto.Sign([]byte(timeoutData))
	require.NoError(t, err)

	timeoutMsg := messages.NewTimeoutMsg(higherView, nil, types.NodeID(1), signature)
	t.Logf("Created timeout message for view %d from Node 1", higherView)

	// Inject the timeout message directly to node 0's coordinator
	coordinator0 := nodes[0].GetCoordinator()
	err = coordinator0.ProcessTimeoutMessage(timeoutMsg)
	require.NoError(t, err)

	// Verify node 0 advanced to view 10
	finalView := nodes[0].GetCoordinator().GetCurrentView()
	t.Logf("Node 0 view after receiving higher timeout: %d", finalView)

	require.Equal(t, higherView, finalView,
		"Node should advance view when receiving higher-view timeout")

	t.Log("Node correctly advanced view upon receiving higher-view timeout message")
}

// TestLowerViewTimeoutDoesNotAdvanceView verifies that receiving a timeout
// message for a lower or equal view does NOT cause view advancement.
// This is a negative test to protect against regressions.
func TestLowerViewTimeoutDoesNotAdvanceView(t *testing.T) {
	t.Parallel()

	const nodeCount = 5
	t.Log("=== View Divergence Test: Lower View Timeout (Negative) ===")

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Set node 0 to view 10
	initialView := types.ViewNumber(10)
	err = helpers.ForceNodeToView(0, initialView)
	require.NoError(t, err)

	t.Logf("Node 0 set to view %d", initialView)

	// Create timeout messages for lower and equal views
	testCases := []struct {
		name        string
		timeoutView types.ViewNumber
	}{
		{"lower view", 5},
		{"equal view", 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a valid timeout message for the test view
			node1Crypto := nodes[1].GetCrypto()
			timeoutData := fmt.Sprintf("timeout:%d:%d", tc.timeoutView, 1)
			signature, err := node1Crypto.Sign([]byte(timeoutData))
			require.NoError(t, err)

			timeoutMsg := messages.NewTimeoutMsg(tc.timeoutView, nil, types.NodeID(1), signature)

			// Inject the timeout message
			coordinator0 := nodes[0].GetCoordinator()
			err = coordinator0.ProcessTimeoutMessage(timeoutMsg)
			require.NoError(t, err)

			// Verify node 0 did NOT change view
			finalView := nodes[0].GetCoordinator().GetCurrentView()
			assert.Equal(t, initialView, finalView,
				"Node should NOT change view when receiving %s timeout (view %d)",
				tc.name, tc.timeoutView)
		})
	}

	t.Log("Correctly ignored lower and equal view timeouts")
}

// TestTimeoutBroadcastAfterViewAdvancement verifies that when a node advances
// its view due to receiving a higher-view timeout, it immediately broadcasts
// its own timeout to help other nodes converge faster.
func TestTimeoutBroadcastAfterViewAdvancement(t *testing.T) {
	t.Parallel()

	const nodeCount = 5
	t.Log("=== View Divergence Test: Timeout Broadcast After Advancement ===")

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Set node 0 to view 5
	initialView := types.ViewNumber(5)
	err = helpers.ForceNodeToView(0, initialView)
	require.NoError(t, err)

	// Clear events to isolate our test
	tracer.Reset()

	// Create a timeout message for a higher view
	higherView := types.ViewNumber(10)
	node1Crypto := nodes[1].GetCrypto()
	timeoutData := fmt.Sprintf("timeout:%d:%d", higherView, 1)
	signature, err := node1Crypto.Sign([]byte(timeoutData))
	require.NoError(t, err)

	timeoutMsg := messages.NewTimeoutMsg(higherView, nil, types.NodeID(1), signature)

	// Inject the timeout message - should trigger view advancement AND broadcast
	coordinator0 := nodes[0].GetCoordinator()
	err = coordinator0.ProcessTimeoutMessage(timeoutMsg)
	require.NoError(t, err)

	// Verify node 0 advanced to view 10
	require.Equal(t, higherView, nodes[0].GetCoordinator().GetCurrentView())

	// Verify a timeout message was broadcast for the new view
	// Check for TimeoutMessageSent event from node 0 for view 10
	timeoutSentEvents := tracer.GetEventsByNodeAndType(0, events.EventTimeoutMessageSent)

	found := false
	for _, event := range timeoutSentEvents {
		if viewVal, ok := event.Payload["view"]; ok {
			var eventView types.ViewNumber
			switch v := viewVal.(type) {
			case types.ViewNumber:
				eventView = v
			case uint64:
				eventView = types.ViewNumber(v)
			}
			if eventView == higherView {
				found = true
				t.Logf("Found timeout broadcast for view %d from node 0", eventView)
				break
			}
		}
	}

	require.True(t, found,
		"Node should broadcast timeout for new view after advancing (helps convergence)")

	t.Log("Node correctly broadcast timeout after view advancement")
}

// TestTimeoutQuorumAchievedAfterViewSync verifies that after nodes sync to the
// highest view, they can successfully collect quorum timeouts and advance.
func TestTimeoutQuorumAchievedAfterViewSync(t *testing.T) {
	t.Parallel()

	const nodeCount = 5
	quorum := (nodeCount*2)/3 + 1 // 4 for 5 nodes
	divergentViews := []types.ViewNumber{10, 8, 9, 7, 10}

	t.Log("=== View Divergence Test: Timeout Quorum After Sync ===")
	t.Logf("Divergent views: %v, quorum required: %d", divergentViews, quorum)

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Set divergent views while network is running
	err = helpers.SetupDivergentViews(divergentViews)
	require.NoError(t, err)

	// Wait for all nodes to reach view 10 (the highest)
	targetView := types.ViewNumber(10)
	syncTimeout := consensusConfig.GetTimeoutForView(targetView) * 2

	t.Logf("Waiting for all nodes to sync to view %d...", targetView)

	synced := helpers.WaitForAllNodesView(targetView, syncTimeout)

	finalViews := helpers.GetNodeViews()
	t.Logf("Node views after sync period: %v", finalViews)

	require.True(t, synced, "All nodes should sync to view 10")

	// Now trigger timeouts on all nodes - all at view 10+, should reach quorum
	helpers.TriggerTimeoutsForAllNodes()

	// Verify timeout quorum was reached by checking for view change event
	viewChangeTimeout := consensusConfig.GetTimeoutForView(targetView) * 2
	viewChanged := helpers.WaitForEvent(events.EventViewChangeStarted, viewChangeTimeout)

	if !viewChanged {
		// Log diagnostic info
		timeoutEvents := helpers.GetEventsCount(events.EventTimeoutMessageSent)
		t.Logf("Timeout messages sent: %d", timeoutEvents)
	}

	require.True(t, viewChanged, "View change should trigger after timeout quorum")
	t.Log("View change triggered successfully after timeout quorum")
}

// TestByzantineHighViewTimeoutSpam verifies that Byzantine nodes cannot disrupt
// consensus by sending timeout messages with artificially high view numbers.
func TestByzantineHighViewTimeoutSpam(t *testing.T) {
	t.Parallel()

	const nodeCount = 5
	const byzantineCount = 1 // f=1

	t.Log("=== View Divergence Test: Byzantine High-View Timeout Spam ===")

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Run initial consensus round to establish baseline
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)

	t.Logf("Running initial consensus with leader Node %d", leader)
	err = nodes[leader].ProposeBlock([]byte("initial-block"))
	require.NoError(t, err)

	commitTimeout := consensusConfig.GetTimeoutForView(0) * 3
	committed := helpers.WaitForConsensusCompletion(commitTimeout)
	require.True(t, committed, "Initial block should be committed")

	t.Log("Initial consensus completed, now testing Byzantine behavior")

	// Record current view before Byzantine activity
	currentView := nodes[0].GetCoordinator().GetCurrentView()
	t.Logf("Current view before Byzantine spam: %d", currentView)

	// Byzantine node: force to artificially high view and trigger timeouts
	byzantineNode := types.NodeID(nodeCount - 1)
	spamView := types.ViewNumber(1000)

	// Set Byzantine node to high view while network is running
	err = helpers.ForceNodeToView(byzantineNode, spamView)
	require.NoError(t, err)

	// Byzantine node sends high-view timeouts
	// Since it's just 1 Byzantine node, honest nodes may advance view but
	// the Byzantine node alone cannot achieve quorum
	byzantineCoordinator := nodes[byzantineNode].GetCoordinator()
	if testable, ok := interface{}(byzantineCoordinator).(integration.CoordinatorTestable); ok {
		for i := 0; i < 10; i++ {
			testable.TriggerViewTimeout()
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Give some time for messages to propagate
	time.Sleep(500 * time.Millisecond)

	// Verify honest nodes may have advanced but can still do consensus
	t.Log("Testing that consensus continues despite Byzantine spam...")

	// Get current view (may have advanced due to Byzantine timeout)
	newCurrentView := nodes[0].GetCoordinator().GetCurrentView()
	t.Logf("View after Byzantine spam: %d", newCurrentView)

	// Try to do another consensus round
	newLeader, err := consensusConfig.GetLeaderForView(newCurrentView)
	require.NoError(t, err)

	// Clear previous events
	tracer.Reset()

	// Propose new block
	err = nodes[newLeader].ProposeBlock([]byte("legitimate-block-after-spam"))
	require.NoError(t, err)

	// Wait for commit
	committed = helpers.WaitForConsensusCompletion(commitTimeout * 2)
	require.True(t, committed, "Consensus should continue despite Byzantine timeout spam")

	// Verify safety: all commits agree
	commits := CollectCommittedBlocks(tracer)
	blockHashes := make(map[types.BlockHash]bool)
	for _, commit := range commits {
		blockHashes[commit.Hash] = true
	}
	assert.Equal(t, 1, len(blockHashes), "All commits should be for the same block")

	t.Log("Consensus continued successfully despite Byzantine timeout spam")
}

// TestSafetyPreservedDuringViewAdvancement verifies that the lockedQC safety
// invariant is maintained when nodes advance views due to higher timeout messages.
func TestSafetyPreservedDuringViewAdvancement(t *testing.T) {
	t.Parallel()

	const nodeCount = 5
	t.Log("=== View Divergence Test: Safety Preserved During View Advancement ===")

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Run several consensus rounds to establish committed blocks and locked QCs
	t.Log("Running initial consensus rounds to establish locked QCs...")

	for round := 0; round < 3; round++ {
		currentView := nodes[0].GetCoordinator().GetCurrentView()
		leader, err := consensusConfig.GetLeaderForView(currentView)
		require.NoError(t, err)

		payload := []byte("safety-test-block-" + string(rune('0'+round)))
		err = nodes[leader].ProposeBlock(payload)
		require.NoError(t, err)

		commitTimeout := consensusConfig.GetTimeoutForView(currentView) * 3
		committed := helpers.WaitForConsensusCompletion(commitTimeout)
		require.True(t, committed, "Round %d should complete", round)

		tracer.Reset()
		t.Logf("Completed consensus round %d", round)
	}

	// Record locked QCs before view advancement
	lockedQCs := make(map[types.NodeID]*types.QuorumCertificate)
	for nodeID := range nodes {
		lockedQCs[nodeID] = helpers.GetLockedQC(nodeID)
		if lockedQCs[nodeID] != nil {
			t.Logf("Node %d lockedQC view: %d", nodeID, lockedQCs[nodeID].View)
		}
	}

	// Force view advancement via high-view timeout
	currentView := nodes[0].GetCoordinator().GetCurrentView()
	higherView := currentView + 100

	t.Logf("Forcing view advancement from %d to %d", currentView, higherView)

	// Set one node to high view while network is running
	err = helpers.ForceNodeToView(0, higherView)
	require.NoError(t, err)

	// Trigger timeout from node 0 (at high view)
	coordinator0 := nodes[0].GetCoordinator()
	if testable, ok := interface{}(coordinator0).(integration.CoordinatorTestable); ok {
		testable.TriggerViewTimeout()
	}

	// Wait for other nodes to advance
	time.Sleep(500 * time.Millisecond)

	// Verify lockedQCs are preserved (safety invariant)
	t.Log("Verifying lockedQC safety invariant...")

	for nodeID := range nodes {
		currentLockedQC := helpers.GetLockedQC(nodeID)
		originalLockedQC := lockedQCs[nodeID]

		if originalLockedQC != nil && currentLockedQC != nil {
			// Locked QC should either be the same or higher (never lower)
			assert.GreaterOrEqual(t, currentLockedQC.View, originalLockedQC.View,
				"Node %d: lockedQC should not decrease after view advancement (was %d, now %d)",
				nodeID, originalLockedQC.View, currentLockedQC.View)
		}
	}

	t.Log("Safety invariant preserved: lockedQC never decreased")

	// Verify consensus can still proceed
	tracer.Reset()
	finalView := nodes[0].GetCoordinator().GetCurrentView()
	finalLeader, err := consensusConfig.GetLeaderForView(finalView)
	require.NoError(t, err)

	err = nodes[finalLeader].ProposeBlock([]byte("post-advancement-block"))
	require.NoError(t, err)

	commitTimeout := consensusConfig.GetTimeoutForView(finalView) * 3
	committed := helpers.WaitForConsensusCompletion(commitTimeout)
	require.True(t, committed, "Consensus should work after view advancement")

	t.Log("Consensus continues after view advancement with safety preserved")
}

// TestViewConvergenceMultipleRestartCycles verifies that the system can handle
// multiple restart cycles with varying degrees of view divergence.
func TestViewConvergenceMultipleRestartCycles(t *testing.T) {
	t.Parallel()

	const nodeCount = 5
	const restartCycles = 3

	t.Log("=== View Divergence Test: Multiple Restart Cycles ===")

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, consensusConfig, err := CreateTestNetwork(nodeCount, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, consensusConfig)

	// Use fixed seed for deterministic test behavior (aids debugging failures)
	rng := rand.New(rand.NewSource(42))

	for cycle := 0; cycle < restartCycles; cycle++ {
		t.Logf("=== Restart Cycle %d ===", cycle)

		// Generate random divergent views
		divergentViews := make([]types.ViewNumber, nodeCount)
		baseView := types.ViewNumber(cycle * 10)
		for i := 0; i < nodeCount; i++ {
			divergentViews[i] = baseView + types.ViewNumber(rng.Intn(15))
		}
		t.Logf("Divergent views: %v", divergentViews)

		// Set divergent views while network is running
		err = helpers.SetupDivergentViews(divergentViews)
		require.NoError(t, err)

		// Wait for convergence
		maxView := helpers.GetMaxCurrentView()
		convergenceTimeout := consensusConfig.GetTimeoutForView(maxView) * 3

		converged := helpers.WaitForCondition(func() bool {
			views := make(map[types.ViewNumber]int)
			for _, node := range nodes {
				views[node.GetCoordinator().GetCurrentView()]++
			}
			// Check if quorum nodes are at the same view
			quorum := consensusConfig.QuorumThreshold()
			for _, count := range views {
				if count >= quorum {
					return true
				}
			}
			return false
		}, convergenceTimeout)

		finalViews := helpers.GetNodeViews()
		t.Logf("Cycle %d final views: %v", cycle, finalViews)

		require.True(t, converged, "Cycle %d: nodes should converge to quorum at same view", cycle)
		t.Logf("Cycle %d: convergence verified", cycle)
	}

	t.Logf("All %d restart cycles completed successfully", restartCycles)
}
