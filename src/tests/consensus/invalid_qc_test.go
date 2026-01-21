package consensus

import (
	"fmt"
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

// TestForgedSignatureQCRejection verifies that a QC with forged signatures
// is rejected by honest nodes through the real ProcessProposal path.
// This is critical for preventing Byzantine leaders from creating fake consensus.
func TestForgedSignatureQCRejection(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Invalid QC Test: Forged Signature Rejection ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get leader and start normal consensus to establish a valid block in the tree
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	// Start consensus to get a valid block
	err = nodes[leader].ProposeBlock([]byte("test_block_for_qc"))
	require.NoError(t, err)

	// Wait for consensus to complete and establish a valid block tree
	helpers.WaitForConsensusCompletion(5 * time.Second)

	// Get a valid block hash from the events (a block that actually exists in tree)
	var validBlockHash types.BlockHash
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			if hash, ok := ev.Payload["block_hash"].(types.BlockHash); ok {
				validBlockHash = hash
				break
			}
		}
	}
	require.NotEqual(t, types.BlockHash{}, validBlockHash, "Should find a valid block hash from consensus")

	// Get quorum threshold
	quorum := consensusConfig.QuorumThreshold()
	t.Logf("Quorum threshold: %d", quorum)

	// Create forged votes with invalid signatures
	forgedVotes := make([]types.Vote, quorum)
	for i := 0; i < quorum; i++ {
		forgedVotes[i] = types.Vote{
			BlockHash: validBlockHash,
			View:      0,
			Phase:     types.PhasePrepare,
			Voter:     types.NodeID(i),
			Signature: []byte(fmt.Sprintf("FORGED_SIGNATURE_%d", i)), // Invalid!
		}
	}

	forgedQC := &types.QuorumCertificate{
		BlockHash: validBlockHash,
		View:      0,
		Phase:     types.PhasePrepare,
		Votes:     forgedVotes,
	}

	t.Logf("Created forged QC with %d votes (all forged signatures)", len(forgedVotes))

	// Create a proposal with the forged QC as HighQC
	// Use a non-leader node to receive the proposal
	targetNode := types.NodeID(1)
	if targetNode == leader {
		targetNode = types.NodeID(2)
	}

	// For ProcessProposal to reach verifyQC, we need:
	// - Correct view (proposal.View == coordinator.currentView)
	// - Parent exists in tree
	// - Sender is expected leader
	currentView := nodes[targetNode].GetCoordinator().GetCurrentView()
	expectedLeader, _ := consensusConfig.GetLeaderForView(currentView)

	// Create a block that would be valid except for the forged QC
	block := types.NewBlock(
		validBlockHash, // Parent exists
		2,              // Height 2 (parent is height 1)
		currentView,    // Current view
		expectedLeader, // Correct leader
		[]byte("test_payload"),
	)

	proposal := &messages.ProposalMsg{
		Block:      block,
		HighQC:     forgedQC,
		ViewNumber: currentView,
		Proposer:   expectedLeader,
	}

	// Process the proposal - should be rejected due to forged signatures
	coord := nodes[targetNode].GetCoordinator()
	err = coord.ProcessProposal(proposal)

	// Assert rejection
	require.Error(t, err, "Proposal with forged QC must be rejected")
	assert.Contains(t, err.Error(), "invalid",
		"Error should indicate invalid QC: %v", err)
	t.Logf("Forged QC correctly rejected: %v", err)

	t.Log("=== Forged Signature Rejection Test Complete ===")
}

// TestInsufficientStakeQCRejection verifies that a QC with votes from
// fewer nodes than the quorum threshold is rejected through ProcessProposal.
func TestInsufficientStakeQCRejection(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Invalid QC Test: Insufficient Stake Rejection ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)

	// Calculate quorum requirements
	quorum := consensusConfig.QuorumThreshold()
	t.Logf("Quorum threshold: %d out of %d nodes", quorum, totalNodes)

	// Start normal consensus to establish block tree
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("stake_test_block"))
	require.NoError(t, err)
	helpers.WaitForConsensusCompletion(5 * time.Second)

	// Get a valid block hash from the events
	var validBlockHash types.BlockHash
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			if hash, ok := ev.Payload["block_hash"].(types.BlockHash); ok {
				validBlockHash = hash
				break
			}
		}
	}
	require.NotEqual(t, types.BlockHash{}, validBlockHash, "Should find a valid block hash")

	// Create a QC with only 2 valid votes (below quorum of 4)
	insufficientVotes := make([]types.Vote, 2)
	for i := 0; i < 2; i++ {
		crypto := nodes[types.NodeID(i)].GetCrypto()
		voteData := fmt.Sprintf("%x:%d:%s:%d", validBlockHash, 0, types.PhasePrepare, types.NodeID(i))
		sig, err := crypto.Sign([]byte(voteData))
		require.NoError(t, err)

		insufficientVotes[i] = types.Vote{
			BlockHash: validBlockHash,
			View:      0,
			Phase:     types.PhasePrepare,
			Voter:     types.NodeID(i),
			Signature: sig,
		}
	}

	insufficientQC := &types.QuorumCertificate{
		BlockHash: validBlockHash,
		View:      0,
		Phase:     types.PhasePrepare,
		Votes:     insufficientVotes,
	}

	t.Logf("Created QC with %d votes (quorum needs %d)", len(insufficientVotes), quorum)

	// Create proposal with insufficient QC
	targetNode := types.NodeID(1)
	if targetNode == leader {
		targetNode = types.NodeID(2)
	}

	currentView := nodes[targetNode].GetCoordinator().GetCurrentView()
	expectedLeader, _ := consensusConfig.GetLeaderForView(currentView)

	block := types.NewBlock(
		validBlockHash,
		2,
		currentView,
		expectedLeader,
		[]byte("test_payload"),
	)

	proposal := &messages.ProposalMsg{
		Block:      block,
		HighQC:     insufficientQC,
		ViewNumber: currentView,
		Proposer:   expectedLeader,
	}

	// Process and assert rejection
	coord := nodes[targetNode].GetCoordinator()
	err = coord.ProcessProposal(proposal)

	require.Error(t, err, "Proposal with insufficient votes must be rejected")
	assert.Contains(t, err.Error(), "insufficient votes",
		"Error should indicate insufficient votes: %v", err)
	t.Logf("Insufficient QC correctly rejected: %v", err)

	t.Log("=== Insufficient Stake Rejection Test Complete ===")
}

// TestDuplicateValidatorQCRejection verifies that a QC that includes
// duplicate votes from the same validator is rejected through ProcessProposal.
func TestDuplicateValidatorQCRejection(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Invalid QC Test: Duplicate Validator Rejection ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)

	// Start normal consensus
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("duplicate_validator_test"))
	require.NoError(t, err)
	helpers.WaitForConsensusCompletion(5 * time.Second)

	// Get a valid block hash from the events
	var validBlockHash types.BlockHash
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			if hash, ok := ev.Payload["block_hash"].(types.BlockHash); ok {
				validBlockHash = hash
				break
			}
		}
	}
	require.NotEqual(t, types.BlockHash{}, validBlockHash, "Should find a valid block hash")

	// Create a QC where Node 0 votes multiple times (Byzantine attack)
	byzantineNode := types.NodeID(0)
	crypto := nodes[byzantineNode].GetCrypto()

	// Byzantine node includes 4 votes (trying to reach quorum alone)
	duplicateVotes := make([]types.Vote, 4)
	for i := 0; i < 4; i++ {
		// Sign with the same voter ID
		voteData := fmt.Sprintf("%x:%d:%s:%d", validBlockHash, 0, types.PhasePrepare, byzantineNode)
		sig, err := crypto.Sign([]byte(voteData))
		require.NoError(t, err)

		duplicateVotes[i] = types.Vote{
			BlockHash: validBlockHash,
			View:      0,
			Phase:     types.PhasePrepare,
			Voter:     byzantineNode, // Same voter for all!
			Signature: sig,
		}
	}

	duplicateQC := &types.QuorumCertificate{
		BlockHash: validBlockHash,
		View:      0,
		Phase:     types.PhasePrepare,
		Votes:     duplicateVotes,
	}

	t.Logf("Created QC with %d votes, all from Node%d (Byzantine duplicate attack)", len(duplicateVotes), byzantineNode)

	// Create proposal with duplicate voter QC
	targetNode := types.NodeID(1)
	if targetNode == leader {
		targetNode = types.NodeID(2)
	}

	currentView := nodes[targetNode].GetCoordinator().GetCurrentView()
	expectedLeader, _ := consensusConfig.GetLeaderForView(currentView)

	block := types.NewBlock(
		validBlockHash,
		2,
		currentView,
		expectedLeader,
		[]byte("test_payload"),
	)

	proposal := &messages.ProposalMsg{
		Block:      block,
		HighQC:     duplicateQC,
		ViewNumber: currentView,
		Proposer:   expectedLeader,
	}

	// Process and assert rejection
	coord := nodes[targetNode].GetCoordinator()
	err = coord.ProcessProposal(proposal)

	require.Error(t, err, "Proposal with duplicate voters must be rejected")
	assert.Contains(t, err.Error(), "duplicate voter",
		"Error should indicate duplicate voter: %v", err)
	t.Logf("Duplicate voter QC correctly rejected: %v", err)

	t.Log("=== Duplicate Validator Rejection Test Complete ===")
}

// TestInvalidQCTriggersLeaderExclusion verifies that when a leader
// fails (simulated by blocking), the network triggers a view change.
// This tests view change behavior, not QC verification.
func TestInvalidQCTriggersLeaderExclusion(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Invalid QC Test: Leader Exclusion ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get initial leader
	leader, _ := consensusConfig.GetLeaderForView(0)
	t.Logf("Initial leader (view 0): Node%d", leader)

	// Record initial state
	initialView := nodes[0].GetCoordinator().GetCurrentView()
	t.Logf("Initial view: %d", initialView)

	// Simulate Byzantine leader by blocking it (forcing timeout)
	err = helpers.BlockNode(leader)
	require.NoError(t, err)
	t.Logf("Blocked Byzantine leader Node%d", leader)

	// Wait for timeout and view change
	viewTimeout := consensusConfig.GetTimeoutForView(0)
	t.Logf("Waiting for view timeout (%v) + view change", viewTimeout)

	time.Sleep(viewTimeout + 3*time.Second)

	// Verify view changed
	var maxView types.ViewNumber
	for nodeID, node := range nodes {
		if nodeID == leader {
			continue
		}
		view := node.GetCoordinator().GetCurrentView()
		if view > maxView {
			maxView = view
		}
	}

	t.Logf("Max view after leader exclusion: %d", maxView)

	// Get new leader
	newLeader, err := consensusConfig.GetLeaderForView(maxView)
	if err != nil {
		t.Logf("Could not determine new leader: %v", err)
	} else {
		t.Logf("New leader for view %d: Node%d", maxView, newLeader)
		assert.NotEqual(t, leader, newLeader,
			"New leader should be different from excluded Byzantine leader")
	}

	// Unblock and sync
	err = helpers.UnblockNode(leader)
	require.NoError(t, err)
	err = helpers.SyncNodeState(leader)
	require.NoError(t, err)

	// Verify Byzantine node caught up
	byzantineView := nodes[leader].GetCoordinator().GetCurrentView()
	t.Logf("Byzantine node view after sync: %d", byzantineView)

	assert.GreaterOrEqual(t, uint64(byzantineView), uint64(maxView),
		"Previously Byzantine node should catch up after sync")

	// Check for view change events
	viewChangeCount := 0
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventViewChangeStarted {
			viewChangeCount++
		}
	}

	t.Logf("View change events: %d", viewChangeCount)
	assert.Greater(t, viewChangeCount, 0,
		"At least one view change should have been triggered")

	t.Log("=== Leader Exclusion Test Complete ===")
}
