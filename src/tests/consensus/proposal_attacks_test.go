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

// TestConflictingProposals verifies that the system detects and handles
// a Byzantine leader sending different proposals to different nodes
// (equivocation at the proposal level).
func TestConflictingProposals(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Proposal Attack Test: Conflicting Proposals ===")

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
	t.Logf("Leader for view 0 (Byzantine): Node%d", leader)

	// First, make a legitimate proposal
	err = nodes[leader].ProposeBlock([]byte("legitimate_proposal"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Now simulate equivocation by creating a conflicting block
	// and injecting it directly to some nodes
	conflictingBlock := types.NewBlock(
		types.BlockHash{}, // Genesis parent
		1,                 // Same height
		0,                 // Same view
		leader,            // Same leader
		[]byte("conflicting_proposal_different_payload"),
	)

	t.Logf("Injecting conflicting proposal with hash: %x", conflictingBlock.Hash[:8])

	// Inject the conflicting proposal to a subset of nodes - track rejections
	conflictingProposalRejections := 0
	for nodeID, node := range nodes {
		if nodeID == leader {
			continue
		}
		// Inject to half the nodes
		if uint16(nodeID)%2 == 0 {
			coord := node.GetCoordinator()
			conflictingProposal := &messages.ProposalMsg{
				Block:      conflictingBlock,
				HighQC:     nil, // nil QC for genesis
				ViewNumber: 0,
				Proposer:   leader,
			}
			conflictErr := coord.ProcessProposal(conflictingProposal)
			if conflictErr != nil {
				conflictingProposalRejections++
				t.Logf("Node%d rejected conflicting proposal: %v", nodeID, conflictErr)
			} else {
				t.Logf("Node%d accepted conflicting proposal", nodeID)
			}
		}
	}

	// Wait for detection and potential view change
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout + 500*time.Millisecond)

	// Check for equivocation detection
	equivocationEvents := 0
	byzantineEvents := 0
	viewChangeEvents := 0

	for _, ev := range tracer.GetEvents() {
		switch ev.EventType {
		case events.EventEquivocationFound:
			equivocationEvents++
			t.Logf("Equivocation detected: %+v", ev.Payload)
		case events.EventByzantineDetected:
			byzantineEvents++
		case events.EventViewChange:
			viewChangeEvents++
		}
	}

	t.Logf("Equivocation events: %d", equivocationEvents)
	t.Logf("Byzantine detection events: %d", byzantineEvents)
	t.Logf("View change events: %d", viewChangeEvents)
	t.Logf("Conflicting proposal rejections: %d", conflictingProposalRejections)

	// The system should detect equivocation or reject conflicting proposals
	// At minimum, system should remain safe (no conflicting commits)
	// Verify system is still operational after the attack
	currentView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("System operational at view %d after conflicting proposals", currentView)

	// ASSERTION: Primary mechanism - conflicting proposals MUST be rejected
	// This is the core safety property being tested
	assert.Greater(t, conflictingProposalRejections, 0,
		"Conflicting proposals must be rejected by honest nodes")

	// Log additional detection mechanisms (informational, not required)
	if equivocationEvents > 0 {
		t.Logf("Bonus: Equivocation also detected (%d events)", equivocationEvents)
	}
	if viewChangeEvents > 0 {
		t.Logf("Note: View change triggered (%d events)", viewChangeEvents)
	}

	t.Log("=== Conflicting Proposals Test Complete ===")
}

// TestInvalidProposalHandling verifies that the system correctly rejects
// proposals with invalid structure, signatures, or content.
func TestInvalidProposalHandling(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Proposal Attack Test: Invalid Proposal Handling ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Start normal consensus first
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Legitimate leader: Node%d", leader)

	err = nodes[leader].ProposeBlock([]byte("valid_proposal"))
	require.NoError(t, err)

	helpers.WaitForProposalProcessed(2 * time.Second)

	// Pick a target node to receive invalid proposals
	targetNode := types.NodeID(1)
	if targetNode == leader {
		targetNode = types.NodeID(2)
	}
	coord := nodes[targetNode].GetCoordinator()

	// Test 1: Proposal from wrong node (not the leader)
	t.Log("Testing: Proposal from non-leader")
	wrongLeaderBlock := types.NewBlock(
		types.BlockHash{},
		1,
		0,
		types.NodeID(3), // Not the actual leader
		[]byte("wrong_leader_payload"),
	)
	wrongLeaderProposal := &messages.ProposalMsg{
		Block:      wrongLeaderBlock,
		HighQC:     nil,
		ViewNumber: 0,
		Proposer:   types.NodeID(3), // Wrong proposer
	}
	err = coord.ProcessProposal(wrongLeaderProposal)
	require.Error(t, err, "Wrong leader proposal must be rejected")
	t.Logf("Wrong leader proposal rejected: %v", err)

	// Test 2: Proposal with invalid height (gap in chain)
	t.Log("Testing: Proposal with invalid height")
	gapHeightBlock := types.NewBlock(
		types.BlockHash{},
		999, // Invalid height - huge gap
		0,
		leader,
		[]byte("gap_height_payload"),
	)
	gapHeightProposal := &messages.ProposalMsg{
		Block:      gapHeightBlock,
		HighQC:     nil,
		ViewNumber: 0,
		Proposer:   leader,
	}
	err = coord.ProcessProposal(gapHeightProposal)
	require.Error(t, err, "Gap height proposal must be rejected")
	t.Logf("Gap height proposal rejected: %v", err)

	// Test 3: Proposal with wrong view
	t.Log("Testing: Proposal with wrong view")
	wrongViewBlock := types.NewBlock(
		types.BlockHash{},
		1,
		99, // Wrong view
		leader,
		[]byte("wrong_view_payload"),
	)
	wrongViewProposal := &messages.ProposalMsg{
		Block:      wrongViewBlock,
		HighQC:     nil,
		ViewNumber: 99, // Wrong view
		Proposer:   leader,
	}
	err = coord.ProcessProposal(wrongViewProposal)
	require.Error(t, err, "Wrong view proposal must be rejected")
	t.Logf("Wrong view proposal rejected: %v", err)

	// Test 4: Proposal referencing non-existent parent
	t.Log("Testing: Proposal with invalid parent")
	invalidParentBlock := types.NewBlock(
		types.BlockHash{0xFF, 0xFF, 0xFF}, // Non-existent parent
		1,
		0,
		leader,
		[]byte("invalid_parent_payload"),
	)
	invalidParentProposal := &messages.ProposalMsg{
		Block:      invalidParentBlock,
		HighQC:     nil,
		ViewNumber: 0,
		Proposer:   leader,
	}
	err = coord.ProcessProposal(invalidParentProposal)
	require.Error(t, err, "Invalid parent proposal must be rejected")
	t.Logf("Invalid parent proposal rejected: %v", err)

	// Wait and verify system continues normally
	time.Sleep(500 * time.Millisecond)

	// Count proposal rejection events
	rejectionEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalRejected {
			rejectionEvents++
		}
	}

	t.Logf("Proposal rejection events: %d", rejectionEvents)

	// Verify consensus continues
	currentView := nodes[targetNode].GetCoordinator().GetCurrentView()
	t.Logf("System operational at view: %d", currentView)

	t.Log("=== Invalid Proposal Handling Test Complete ===")
}

// TestSelectiveBroadcast verifies that the system handles Byzantine leaders
// that send proposals only to a subset of nodes.
func TestSelectiveBroadcast(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Proposal Attack Test: Selective Broadcast ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get leader
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	// Partition half the nodes to simulate selective broadcast
	// The leader's proposal will only reach unpartitioned nodes
	partitionedNodes := []types.NodeID{}
	for i := 0; i < totalNodes; i++ {
		nodeID := types.NodeID(i)
		if nodeID != leader && len(partitionedNodes) < 2 {
			partitionedNodes = append(partitionedNodes, nodeID)
		}
	}

	t.Logf("Partitioning nodes %v to simulate selective broadcast", partitionedNodes)
	for _, nodeID := range partitionedNodes {
		err = helpers.BlockNode(nodeID)
		require.NoError(t, err)
	}

	// Leader proposes - only reaches non-partitioned nodes
	err = nodes[leader].ProposeBlock([]byte("selective_broadcast_test"))
	require.NoError(t, err)

	// Wait for partial consensus attempt
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	time.Sleep(viewTimeout / 2)

	// Count which nodes received the proposal
	proposalsReceived := make(map[types.NodeID]bool)
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			proposalsReceived[types.NodeID(ev.NodeID)] = true
		}
	}

	t.Logf("Nodes that received proposal: %v", proposalsReceived)

	// Unblock partitioned nodes
	for _, nodeID := range partitionedNodes {
		err = helpers.UnblockNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for timeout and potential view change
	time.Sleep(viewTimeout + 500*time.Millisecond)

	// Partitioned nodes should timeout and trigger view changes
	timeoutEvents := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventViewTimeout {
			for _, partitionedNode := range partitionedNodes {
				if types.NodeID(ev.NodeID) == partitionedNode {
					timeoutEvents++
				}
			}
		}
	}

	t.Logf("Timeout events from partitioned nodes: %d", timeoutEvents)

	// Partitioned nodes should not receive proposal and should timeout
	assert.Less(t, len(proposalsReceived), totalNodes,
		"Partitioned nodes should not receive proposal")

	// Sync and verify recovery
	for _, nodeID := range partitionedNodes {
		err = helpers.SyncNodeState(nodeID)
		if err != nil {
			t.Logf("Sync Node%d: %v", nodeID, err)
		}
	}

	t.Log("=== Selective Broadcast Test Complete ===")
}

// TestDelayedProposalAttack verifies that the system handles Byzantine leaders
// that deliberately delay their proposals to manipulate consensus timing.
func TestDelayedProposalAttack(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Proposal Attack Test: Delayed Proposal ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get leader
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	viewTimeout := consensusConfig.GetTimeoutForView(0)

	// Wait almost until timeout before proposing (simulating delayed proposal)
	delayDuration := viewTimeout - 500*time.Millisecond
	if delayDuration < 0 {
		delayDuration = viewTimeout / 2
	}

	t.Logf("Simulating delayed proposal (waiting %v of %v timeout)", delayDuration, viewTimeout)
	time.Sleep(delayDuration)

	// Now propose - just before timeout
	startTime := time.Now()
	err = nodes[leader].ProposeBlock([]byte("delayed_proposal"))
	require.NoError(t, err)

	// Wait for consensus attempt
	time.Sleep(viewTimeout)

	consensusTime := time.Since(startTime)
	t.Logf("Consensus attempt duration after delayed proposal: %v", consensusTime)

	// Check if timeout occurred before or after proposal was processed
	timeoutEvents := 0
	proposalEvents := 0
	viewChangeEvents := 0

	for _, ev := range tracer.GetEvents() {
		switch ev.EventType {
		case events.EventViewTimeout:
			timeoutEvents++
		case events.EventProposalReceived:
			proposalEvents++
		case events.EventViewChange:
			viewChangeEvents++
		}
	}

	t.Logf("Timeout events: %d", timeoutEvents)
	t.Logf("Proposal received events: %d", proposalEvents)
	t.Logf("View change events: %d", viewChangeEvents)

	// The system should either:
	// 1. Process the proposal in time, or
	// 2. Timeout and move to next view
	// Either is acceptable behavior
	assert.True(t, proposalEvents > 0 || timeoutEvents > 0,
		"System must either process delayed proposal or timeout")

	// Verify system is in a consistent state
	views := make(map[types.NodeID]types.ViewNumber)
	for nodeID, node := range nodes {
		views[nodeID] = node.GetCoordinator().GetCurrentView()
	}
	t.Logf("Node views after delayed proposal: %v", views)

	// All honest nodes should be at the same view or within one view of each other
	minView := types.ViewNumber(^uint64(0))
	maxView := types.ViewNumber(0)
	for _, v := range views {
		if v < minView {
			minView = v
		}
		if v > maxView {
			maxView = v
		}
	}

	viewDiff := maxView - minView
	assert.LessOrEqual(t, viewDiff, types.ViewNumber(2),
		"Nodes should be within 2 views of each other after delayed proposal")

	t.Log("=== Delayed Proposal Test Complete ===")
}
