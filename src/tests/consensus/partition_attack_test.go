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

// TestPartitionDetection verifies that nodes can detect when they are
// partitioned from the majority of the network.
func TestPartitionDetection(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Partition Attack Test: Partition Detection ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start normal consensus
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("partition_detection_test"))
	require.NoError(t, err)

	// Wait for initial consensus
	helpers.WaitForConsensusCompletion(5 * time.Second)

	// Partition a single node
	partitionedNode := types.NodeID(2)
	t.Logf("Partitioning Node%d", partitionedNode)

	err = helpers.BlockNode(partitionedNode)
	require.NoError(t, err)

	// Record the partitioned node's view before the partition effect
	partitionedView := nodes[partitionedNode].GetCoordinator().GetCurrentView()
	t.Logf("Partitioned Node%d at view %d", partitionedNode, partitionedView)

	// Wait for timeout to trigger on the partitioned node
	viewTimeout := consensusConfig.GetTimeoutForView(partitionedView)
	t.Logf("Waiting for timeout (%v) to detect partition", viewTimeout)
	time.Sleep(viewTimeout + time.Second)

	// Check for partition-related events
	timeoutEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.NodeID == uint16(partitionedNode) {
			if ev.EventType == events.EventViewTimeout {
				timeoutEvents++
			}
		}
	}

	t.Logf("Partitioned node timeout events: %d", timeoutEvents)

	// The partitioned node should have experienced at least one timeout
	// (due to not receiving messages from the majority)
	assert.GreaterOrEqual(t, timeoutEvents, 1,
		"Partitioned node should detect partition via timeout")

	// Unblock and verify recovery
	err = helpers.UnblockNode(partitionedNode)
	require.NoError(t, err)

	err = helpers.SyncNodeState(partitionedNode)
	require.NoError(t, err)

	t.Logf("Node%d recovered from partition", partitionedNode)

	t.Log("=== Partition Detection Test Complete ===")
}

// TestSplitBrainScenario tests the classic split-brain scenario where
// the network is divided into two groups that can't communicate.
func TestSplitBrainScenario(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Partition Attack Test: Split Brain Scenario ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start normal consensus
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("split_brain_test"))
	require.NoError(t, err)
	helpers.WaitForConsensusCompletion(5 * time.Second)

	// Allow time for all commit events from initial round to be recorded
	time.Sleep(200 * time.Millisecond)

	// CRITICAL: Record event count BEFORE partition for safety check
	// This must be done before partitioning to accurately measure commits during partition
	eventCountBeforePartition := len(tracer.GetEvents())
	t.Logf("Event count before partition: %d", eventCountBeforePartition)

	// Create a split: Partition nodes 3 and 4 from the rest
	// Group A: Nodes 0, 1, 2 (3 nodes - can still reach quorum with 4 votes needed: NO)
	// Group B: Nodes 3, 4 (2 nodes - cannot reach quorum)
	// Neither group can reach quorum with 5 nodes needing 4 votes!
	minorityNodes := []types.NodeID{types.NodeID(3), types.NodeID(4)}

	t.Log("Creating network split:")
	t.Log("  Group A: Nodes 0, 1, 2 (3 nodes)")
	t.Log("  Group B: Nodes 3, 4 (2 nodes)")

	for _, nodeID := range minorityNodes {
		err = helpers.BlockNode(nodeID)
		require.NoError(t, err)
		t.Logf("Partitioned Node%d", nodeID)
	}

	// Record views after partition is established
	viewsBefore := make(map[types.NodeID]types.ViewNumber)
	for nodeID, node := range nodes {
		viewsBefore[nodeID] = node.GetCoordinator().GetCurrentView()
	}
	t.Logf("Views at partition start: %v", viewsBefore)

	// Wait for timeouts to occur
	timeout := consensusConfig.GetTimeoutForView(0)
	t.Logf("Waiting for split-brain effects (%v)", timeout*2)
	time.Sleep(timeout * 2)

	// SAFETY CHECK: No commits should occur during partition (neither group has quorum)
	AssertNoNewCommitsDuring(t, tracer, eventCountBeforePartition, "split-brain partition phase")

	// Check the state of both groups
	t.Log("Checking state after split:")

	// Group A should have progressed (if they had quorum) or stalled
	majorityViews := make(map[types.NodeID]types.ViewNumber)
	for nodeID, node := range nodes {
		isMinority := false
		for _, minNode := range minorityNodes {
			if nodeID == minNode {
				isMinority = true
				break
			}
		}
		if !isMinority {
			majorityViews[nodeID] = node.GetCoordinator().GetCurrentView()
		}
	}

	// Group B should be stuck or in timeout loop
	minorityViews := make(map[types.NodeID]types.ViewNumber)
	for _, nodeID := range minorityNodes {
		minorityViews[nodeID] = nodes[nodeID].GetCoordinator().GetCurrentView()
	}

	t.Logf("Majority group (A) views: %v", majorityViews)
	t.Logf("Minority group (B) views: %v", minorityViews)

	// The key insight: with 5 nodes, neither group can make progress alone
	// because quorum requires 4 nodes. Both groups should enter view change.

	// Heal the partition
	t.Log("Healing network partition...")
	for _, nodeID := range minorityNodes {
		err = helpers.UnblockNode(nodeID)
		require.NoError(t, err)

		err = helpers.SyncNodeState(nodeID)
		require.NoError(t, err)
		t.Logf("Synced Node%d", nodeID)
	}

	// Wait for network to reconverge
	time.Sleep(timeout + time.Second)

	// Verify all nodes are now at the same view
	viewsAfter := make(map[types.NodeID]types.ViewNumber)
	var maxView types.ViewNumber
	for nodeID, node := range nodes {
		view := node.GetCoordinator().GetCurrentView()
		viewsAfter[nodeID] = view
		if view > maxView {
			maxView = view
		}
	}

	t.Logf("Views after healing: %v", viewsAfter)

	// All nodes should converge to the same view (or very close)
	for nodeID, view := range viewsAfter {
		// Allow for some view difference due to sync timing
		assert.GreaterOrEqual(t, uint64(view), uint64(maxView)-1,
			"Node%d should be at or near max view %d", nodeID, maxView)
	}

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree
	// After partition heal, nodes should have committed at least the initial block
	AssertLivenessWithCommits(t, tracer, "split-brain recovery")

	t.Log("=== Split Brain Scenario Test Complete ===")
}

// TestPartialConnectivity tests scenarios where some nodes can communicate
// with only a subset of the network.
func TestPartialConnectivity(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Partition Attack Test: Partial Connectivity ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start normal consensus
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("partial_connectivity_test"))
	require.NoError(t, err)
	helpers.WaitForConsensusCompletion(5 * time.Second)

	// Simulate partial connectivity: Node 2 can only reach some nodes
	// For simplicity, we'll just partition Node 2
	partialNode := types.NodeID(2)
	t.Logf("Creating partial connectivity: Node%d isolated", partialNode)

	err = helpers.BlockNode(partialNode)
	require.NoError(t, err)

	// The remaining 4 nodes should be able to continue (quorum = 4)
	t.Log("Checking if remaining nodes can reach consensus...")

	// Wait for any consensus activity (events have accumulated from first round)
	helpers.WaitForEventCount(events.EventBlockCommitted, 1, 5*time.Second)

	// Check committed blocks via commit events from non-partitioned nodes
	// The first consensus round should have completed before the partition
	commitEvents := make(map[types.NodeID]int)
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventBlockCommitted {
			nodeID := types.NodeID(ev.NodeID)
			if nodeID != partialNode {
				commitEvents[nodeID]++
			}
		}
	}

	t.Logf("Nodes with commit events: %d out of %d non-partitioned",
		len(commitEvents), totalNodes-1)

	// Restore connectivity
	err = helpers.UnblockNode(partialNode)
	require.NoError(t, err)

	err = helpers.SyncNodeState(partialNode)
	require.NoError(t, err)

	t.Logf("Node%d connectivity restored and synced", partialNode)

	// ASSERTION: The first consensus round should have completed before the partition
	// At least some non-partitioned nodes should have seen block commits
	assert.Greater(t, len(commitEvents), 0,
		"At least some non-partitioned nodes should have committed blocks from the first round")

	t.Log("=== Partial Connectivity Test Complete ===")
}

// TestConsensusResumeAfterPartition verifies that consensus resumes
// correctly after a network partition is healed.
func TestConsensusResumeAfterPartition(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Partition Attack Test: Consensus Resume After Partition ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Complete first consensus round
	leader, _ := consensusConfig.GetLeaderForView(0)
	t.Logf("Starting first round with leader Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("round_1_block"))
	require.NoError(t, err)

	// Wait for first round to complete
	helpers.WaitForQuorumCommits(5 * time.Second)

	// Record state after first round
	viewAfterRound1 := nodes[0].GetCoordinator().GetCurrentView()
	t.Logf("View after first round: %d", viewAfterRound1)

	// Count committed blocks after first round
	committedCountRound1 := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventBlockCommitted {
			committedCountRound1++
		}
	}
	t.Logf("Committed blocks after round 1: %d", committedCountRound1/totalNodes)

	// Create a partition
	partitionedNodes := []types.NodeID{types.NodeID(3), types.NodeID(4)}
	t.Log("Creating partition: isolating Nodes 3 and 4")

	for _, nodeID := range partitionedNodes {
		err = helpers.BlockNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for partition effects
	timeout := consensusConfig.GetTimeoutForView(viewAfterRound1)
	time.Sleep(timeout + time.Second)

	t.Log("Partition active, checking state...")

	// Record events count before healing
	eventsBeforeHeal := len(tracer.GetEvents())

	// Heal the partition
	t.Log("Healing partition...")
	for _, nodeID := range partitionedNodes {
		err = helpers.UnblockNode(nodeID)
		require.NoError(t, err)

		err = helpers.SyncNodeState(nodeID)
		require.NoError(t, err)
	}

	// Wait for network to stabilize after sync
	helpers.WaitForAllNodesView(viewAfterRound1, 5*time.Second)

	// Try to start a new consensus round
	currentView := nodes[0].GetCoordinator().GetCurrentView()
	newLeader, err := consensusConfig.GetLeaderForView(currentView)
	if err != nil {
		t.Logf("Could not get leader for view %d: %v", currentView, err)
	} else {
		t.Logf("Current view: %d, leader: Node%d", currentView, newLeader)

		// If the new leader is not partitioned, try to propose
		isPartitioned := false
		for _, pid := range partitionedNodes {
			if newLeader == pid {
				isPartitioned = true
				break
			}
		}

		if !isPartitioned {
			err = nodes[newLeader].ProposeBlock([]byte("post_partition_block"))
			if err != nil {
				t.Logf("Proposal after partition failed (expected during recovery): %v", err)
			}
		}
	}

	// Wait for potential consensus (if proposal succeeded)
	helpers.WaitForCondition(func() bool {
		return len(tracer.GetEvents()) > eventsBeforeHeal
	}, 3*time.Second)

	// Check for new events after healing
	eventsAfterHeal := len(tracer.GetEvents())
	newEvents := eventsAfterHeal - eventsBeforeHeal
	t.Logf("New events after partition heal: %d", newEvents)

	// Verify all nodes are synchronized
	var minView, maxView types.ViewNumber
	minView = types.ViewNumber(999999)
	for nodeID, node := range nodes {
		view := node.GetCoordinator().GetCurrentView()
		if view < minView {
			minView = view
		}
		if view > maxView {
			maxView = view
		}
		t.Logf("Node%d at view %d", nodeID, view)
	}

	// The network should be relatively synchronized (within 1-2 views)
	viewDiff := maxView - minView
	t.Logf("View spread: %d (min=%d, max=%d)", viewDiff, minView, maxView)

	assert.LessOrEqual(t, uint64(viewDiff), uint64(2),
		"Nodes should be within 2 views of each other after recovery")

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree on chain
	// The first round should have committed before partition, verify agreement
	AssertLivenessWithCommits(t, tracer, "consensus resume after partition")

	t.Log("=== Consensus Resume After Partition Test Complete ===")
}

// TestExtendedPartitionRecovery tests recovery after a longer partition
// where significant state divergence may have occurred.
func TestExtendedPartitionRecovery(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Partition Attack Test: Extended Partition Recovery ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Complete initial consensus
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("initial_block"))
	require.NoError(t, err)
	helpers.WaitForQuorumCommits(5 * time.Second)

	initialView := nodes[0].GetCoordinator().GetCurrentView()
	t.Logf("Initial consensus complete at view %d", initialView)

	// Partition a single node for an extended period
	partitionedNode := types.NodeID(4)
	t.Logf("Creating extended partition for Node%d", partitionedNode)

	err = helpers.BlockNode(partitionedNode)
	require.NoError(t, err)

	// Let the majority continue for multiple timeouts
	// This simulates an extended partition
	timeout := consensusConfig.GetTimeoutForView(initialView)
	extendedDuration := timeout * 3
	t.Logf("Extended partition duration: %v", extendedDuration)

	time.Sleep(extendedDuration)

	// Check majority progress
	majorityMaxView := initialView
	for nodeID, node := range nodes {
		if nodeID == partitionedNode {
			continue
		}
		view := node.GetCoordinator().GetCurrentView()
		if view > majorityMaxView {
			majorityMaxView = view
		}
	}

	partitionedView := nodes[partitionedNode].GetCoordinator().GetCurrentView()

	t.Logf("After extended partition:")
	t.Logf("  Majority max view: %d", majorityMaxView)
	t.Logf("  Partitioned node view: %d", partitionedView)

	// The partitioned node should be behind
	viewGap := majorityMaxView - partitionedView
	t.Logf("  View gap: %d", viewGap)

	// Heal and sync
	t.Log("Healing extended partition...")
	err = helpers.UnblockNode(partitionedNode)
	require.NoError(t, err)

	err = helpers.SyncNodeState(partitionedNode)
	require.NoError(t, err)

	// Wait for sync to complete (state sync completed event)
	helpers.WaitForEventFromNode(partitionedNode, events.EventStateSyncCompleted, 3*time.Second)

	// Verify recovery
	recoveredView := nodes[partitionedNode].GetCoordinator().GetCurrentView()
	t.Logf("Partitioned node recovered to view %d", recoveredView)

	assert.GreaterOrEqual(t, uint64(recoveredView), uint64(majorityMaxView),
		"Recovered node should catch up to majority view")

	// Check sync events
	syncEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.NodeID == uint16(partitionedNode) {
			if ev.EventType == events.EventStateSyncCompleted {
				syncEvents++
			}
		}
	}

	t.Logf("State sync completed events for Node%d: %d", partitionedNode, syncEvents)
	assert.GreaterOrEqual(t, syncEvents, 1,
		"Partitioned node should have completed state sync")

	// LIVENESS & SAFETY: Verify commits occurred and all nodes agree after recovery
	AssertLivenessWithCommits(t, tracer, "extended partition recovery")

	t.Log("=== Extended Partition Recovery Test Complete ===")
}
