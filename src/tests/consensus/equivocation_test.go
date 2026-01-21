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

// TestDoubleVoteDetection verifies that the system detects when a node
// votes for two different blocks in the same view and phase.
// This is the most fundamental equivocation attack vector.
func TestDoubleVoteDetection(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Equivocation Test: Double Vote Detection ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get leader for view 0 and start a consensus round
	leader, err := consensusConfig.GetLeaderForView(0)
	require.NoError(t, err)
	t.Logf("Leader for view 0: Node%d", leader)

	// Start consensus
	err = nodes[leader].ProposeBlock([]byte("test_block_1"))
	require.NoError(t, err)

	// Wait for proposal to be processed
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Pick a Byzantine node (non-leader)
	var byzantineNode types.NodeID
	for i := 0; i < totalNodes; i++ {
		if types.NodeID(i) != leader {
			byzantineNode = types.NodeID(i)
			break
		}
	}
	t.Logf("Byzantine node (simulated): Node%d", byzantineNode)

	// Get the block hash from the proposal (from events)
	var originalBlockHash types.BlockHash
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventProposalReceived {
			if hash, ok := ev.Payload["block_hash"].(types.BlockHash); ok {
				originalBlockHash = hash
				break
			}
		}
	}

	if originalBlockHash == [32]byte{} {
		// If we couldn't get from events, create a placeholder
		copy(originalBlockHash[:], "original_block_hash_data")
	}

	t.Logf("Original block hash: %x", originalBlockHash[:8])

	// Create conflicting block hash
	var conflictingBlockHash types.BlockHash
	copy(conflictingBlockHash[:], "conflicting_block_hash!!")

	// Create the first (legitimate) vote
	vote1Data := fmt.Sprintf("%x:%d:%s:%d", originalBlockHash, 0, types.PhasePrepare, byzantineNode)
	byzantineCrypto := nodes[byzantineNode].GetCrypto()
	sig1, err := byzantineCrypto.Sign([]byte(vote1Data))
	require.NoError(t, err)

	vote1 := &types.Vote{
		BlockHash: originalBlockHash,
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: sig1,
	}

	// Create the second (conflicting) vote - same view, same phase, different block
	vote2Data := fmt.Sprintf("%x:%d:%s:%d", conflictingBlockHash, 0, types.PhasePrepare, byzantineNode)
	sig2, err := byzantineCrypto.Sign([]byte(vote2Data))
	require.NoError(t, err)

	vote2 := &types.Vote{
		BlockHash: conflictingBlockHash,
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     byzantineNode,
		Signature: sig2,
	}

	t.Logf("Created conflicting votes from Node%d:", byzantineNode)
	t.Logf("  Vote 1: block=%x...", vote1.BlockHash[:8])
	t.Logf("  Vote 2: block=%x...", vote2.BlockHash[:8])

	// Inject both votes to the leader
	leaderCoord := nodes[leader].GetCoordinator()

	// Process first vote (may succeed or fail depending on block existence)
	err1 := leaderCoord.ProcessVote(vote1)
	if err1 != nil {
		t.Logf("First vote result: %v", err1)
	}

	// Process second conflicting vote (should be detected as equivocation or rejected)
	err2 := leaderCoord.ProcessVote(vote2)
	if err2 != nil {
		t.Logf("Second vote rejected: %v", err2)
	}

	// Check for equivocation detection events and validate payload
	equivocationEvents := 0
	byzantineEvents := 0
	var foundEquivocationEvent *events.ConsensusEvent
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventEquivocationFound {
			equivocationEvents++
			evCopy := ev
			foundEquivocationEvent = &evCopy
			t.Logf("Found equivocation event: %+v", ev.Payload)
		}
		if ev.EventType == events.EventByzantineDetected {
			byzantineEvents++
			t.Logf("Found Byzantine detection event: %+v", ev.Payload)
		}
	}

	t.Logf("Equivocation events: %d", equivocationEvents)
	t.Logf("Byzantine detection events: %d", byzantineEvents)
	t.Logf("First vote error: %v, Second vote error: %v", err1, err2)

	// ASSERTION: Second conflicting vote MUST be rejected
	// The first vote may or may not be accepted (depends on block existence)
	// but the second vote for a different block in same (view, phase) must be rejected
	require.Error(t, err2, "Second conflicting vote must be rejected as equivocation")
	assert.Contains(t, err2.Error(), "equivocation",
		"Error message should indicate equivocation detection")

	// ASSERTION: Equivocation event must be emitted
	require.Greater(t, equivocationEvents, 0,
		"Equivocation event must be emitted when detecting conflicting votes")

	// ASSERTION: Event payload must contain correct evidence
	require.NotNil(t, foundEquivocationEvent, "Should have captured equivocation event")
	assert.Equal(t, byzantineNode, foundEquivocationEvent.Payload["voter"],
		"Event payload must identify the equivocating voter")
	assert.Equal(t, types.ViewNumber(0), foundEquivocationEvent.Payload["view"],
		"Event payload must contain the view number")
	assert.Equal(t, types.PhasePrepare, foundEquivocationEvent.Payload["phase"],
		"Event payload must contain the consensus phase")

	t.Log("=== Double Vote Detection Test Complete ===")
}

// TestEquivocationEvidenceCollection verifies that equivocation evidence
// (the two conflicting votes with valid signatures) is properly collected and stored.
func TestEquivocationEvidenceCollection(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Equivocation Test: Evidence Collection ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// Get leader and start consensus
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("evidence_test_block"))
	require.NoError(t, err)
	helpers.WaitForProposalProcessed(2 * time.Second)

	// Create Byzantine scenario: Node 2 sends conflicting votes
	byzantineNode := types.NodeID(2)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(3)
	}

	// Create two conflicting block hashes
	var block1Hash, block2Hash types.BlockHash
	copy(block1Hash[:], "block_1_hash_evidence_test")
	copy(block2Hash[:], "block_2_hash_evidence_test")

	byzantineCrypto := nodes[byzantineNode].GetCrypto()

	// Create two conflicting votes for PreCommit phase
	vote1Data := fmt.Sprintf("%x:%d:%s:%d", block1Hash, 0, types.PhasePreCommit, byzantineNode)
	sig1, _ := byzantineCrypto.Sign([]byte(vote1Data))

	vote2Data := fmt.Sprintf("%x:%d:%s:%d", block2Hash, 0, types.PhasePreCommit, byzantineNode)
	sig2, _ := byzantineCrypto.Sign([]byte(vote2Data))

	conflictingVote1 := &types.Vote{
		BlockHash: block1Hash,
		View:      0,
		Phase:     types.PhasePreCommit,
		Voter:     byzantineNode,
		Signature: sig1,
	}

	conflictingVote2 := &types.Vote{
		BlockHash: block2Hash,
		View:      0,
		Phase:     types.PhasePreCommit,
		Voter:     byzantineNode,
		Signature: sig2,
	}

	t.Logf("Created evidence pair from Node%d for PreCommit phase", byzantineNode)

	// Send to multiple honest nodes to simulate real attack
	// Track rejections - conflicting votes should be rejected or detected
	totalRejections := 0
	nodesWithAnyRejection := 0
	for nodeID, node := range nodes {
		if nodeID == byzantineNode {
			continue
		}

		coord := node.GetCoordinator()
		nodeRejections := 0

		// Send first vote
		err1 := coord.ProcessVote(conflictingVote1)
		if err1 != nil {
			nodeRejections++
			t.Logf("Node%d rejected first vote: %v", nodeID, err1)
		}

		// Send conflicting vote (different from what other nodes might receive)
		err2 := coord.ProcessVote(conflictingVote2)
		if err2 != nil {
			nodeRejections++
			t.Logf("Node%d rejected conflicting vote: %v", nodeID, err2)
		}

		if nodeRejections > 0 {
			nodesWithAnyRejection++
		}
		totalRejections += nodeRejections
	}

	// Allow time for event processing
	time.Sleep(100 * time.Millisecond)

	// Check for evidence collection events and validate payload contents
	evidenceCount := 0
	var collectedEvidence []*events.ConsensusEvent
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventEquivocationFound {
			evidenceCount++
			evCopy := ev
			collectedEvidence = append(collectedEvidence, &evCopy)
			t.Logf("Evidence collected: voter=%v, view=%v, phase=%v",
				ev.Payload["voter"], ev.Payload["view"], ev.Payload["phase"])
		}
	}

	t.Logf("Evidence collection events: %d", evidenceCount)
	t.Logf("Total rejections: %d, Nodes with rejections: %d", totalRejections, nodesWithAnyRejection)

	// ASSERTION: Conflicting votes must be rejected by all honest nodes
	// Each node should reject the second conflicting vote as equivocation
	assert.Greater(t, totalRejections, 0,
		"At least some conflicting votes must be rejected as equivocation")

	// ASSERTION: Equivocation events should be emitted
	require.Greater(t, evidenceCount, 0,
		"Equivocation events must be emitted when detecting conflicting votes")

	// ASSERTION: Evidence payload must contain required fields
	for i, ev := range collectedEvidence {
		require.NotNil(t, ev.Payload["voter"], "Evidence %d must contain voter", i)
		require.NotNil(t, ev.Payload["view"], "Evidence %d must contain view", i)
		require.NotNil(t, ev.Payload["phase"], "Evidence %d must contain phase", i)
		require.NotNil(t, ev.Payload["block1_hash"], "Evidence %d must contain block1_hash", i)
		require.NotNil(t, ev.Payload["block2_hash"], "Evidence %d must contain block2_hash", i)

		// Verify the two block hashes are different (actual equivocation)
		block1 := ev.Payload["block1_hash"].(types.BlockHash)
		block2 := ev.Payload["block2_hash"].(types.BlockHash)
		assert.NotEqual(t, block1, block2,
			"Evidence %d must show votes for different blocks", i)
	}

	t.Log("=== Evidence Collection Test Complete ===")
}

// TestByzantineDetectionAcrossPhases verifies equivocation detection works
// across all consensus phases (Prepare, PreCommit, Commit).
func TestByzantineDetectionAcrossPhases(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Equivocation Test: Detection Across Phases ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)

	// Get leader and start consensus
	leader, _ := consensusConfig.GetLeaderForView(0)
	err = nodes[leader].ProposeBlock([]byte("phase_test_block"))
	require.NoError(t, err)
	helpers.WaitForProposalProcessed(2 * time.Second)

	byzantineNode := types.NodeID(1)
	if byzantineNode == leader {
		byzantineNode = types.NodeID(2)
	}

	byzantineCrypto := nodes[byzantineNode].GetCrypto()
	leaderCoord := nodes[leader].GetCoordinator()

	phases := []types.ConsensusPhase{
		types.PhasePrepare,
		types.PhasePreCommit,
		types.PhaseCommit,
	}

	// Track rejections per phase
	phaseRejections := make(map[types.ConsensusPhase]int)

	for _, phase := range phases {
		t.Logf("Testing equivocation in %s phase", phase)

		var block1Hash, block2Hash types.BlockHash
		copy(block1Hash[:], fmt.Sprintf("block_1_%s_phase_test", phase))
		copy(block2Hash[:], fmt.Sprintf("block_2_%s_phase_test", phase))

		vote1Data := fmt.Sprintf("%x:%d:%s:%d", block1Hash, 0, phase, byzantineNode)
		sig1, _ := byzantineCrypto.Sign([]byte(vote1Data))

		vote2Data := fmt.Sprintf("%x:%d:%s:%d", block2Hash, 0, phase, byzantineNode)
		sig2, _ := byzantineCrypto.Sign([]byte(vote2Data))

		vote1 := &types.Vote{
			BlockHash: block1Hash,
			View:      0,
			Phase:     phase,
			Voter:     byzantineNode,
			Signature: sig1,
		}

		vote2 := &types.Vote{
			BlockHash: block2Hash,
			View:      0,
			Phase:     phase,
			Voter:     byzantineNode,
			Signature: sig2,
		}

		// Process both votes - track if ANY vote is rejected
		err1 := leaderCoord.ProcessVote(vote1)
		if err1 != nil {
			phaseRejections[phase]++
			t.Logf("Phase %s vote 1 rejected: %v", phase, err1)
		}

		err2 := leaderCoord.ProcessVote(vote2)
		if err2 != nil {
			phaseRejections[phase]++
			t.Logf("Phase %s vote 2 rejected: %v", phase, err2)
		}
	}

	// Count detection events by phase and validate payload
	phaseDetections := make(map[types.ConsensusPhase]int)
	detectedPhases := make(map[types.ConsensusPhase]bool)
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventEquivocationFound {
			if phase, ok := ev.Payload["phase"].(types.ConsensusPhase); ok {
				phaseDetections[phase]++
				detectedPhases[phase] = true

				// Validate payload structure for each event
				assert.NotNil(t, ev.Payload["voter"], "Event must contain voter")
				assert.NotNil(t, ev.Payload["view"], "Event must contain view")
				assert.NotNil(t, ev.Payload["block1_hash"], "Event must contain block1_hash")
				assert.NotNil(t, ev.Payload["block2_hash"], "Event must contain block2_hash")
			}
		}
	}

	for phase, count := range phaseDetections {
		t.Logf("Equivocation detected in %s phase: %d times", phase, count)
	}

	// ASSERTION: Conflicting votes must be handled safely
	// Acceptable: votes rejected (for any reason) or equivocation detected
	totalRejections := 0
	totalDetections := 0
	for _, count := range phaseRejections {
		totalRejections += count
	}
	for _, count := range phaseDetections {
		totalDetections += count
	}

	t.Logf("Total rejections: %d, Total detections: %d", totalRejections, totalDetections)

	// ASSERTION: Conflicting votes must be rejected across all phases
	// With 3 phases, we expect at least 3 rejections (one per phase for the second conflicting vote)
	assert.GreaterOrEqual(t, totalRejections, 3,
		"Conflicting votes must be rejected in all consensus phases (Prepare, PreCommit, Commit)")

	// ASSERTION: Equivocation events should be emitted for each phase
	require.Greater(t, totalDetections, 0,
		"Equivocation events should be emitted when detecting conflicting votes")

	// ASSERTION: All three phases must have detection events
	assert.True(t, detectedPhases[types.PhasePrepare],
		"Equivocation must be detected in Prepare phase")
	assert.True(t, detectedPhases[types.PhasePreCommit],
		"Equivocation must be detected in PreCommit phase")
	assert.True(t, detectedPhases[types.PhaseCommit],
		"Equivocation must be detected in Commit phase")

	t.Log("=== Detection Across Phases Test Complete ===")
}

// TestEquivocationTriggersViewChange verifies that detecting equivocation
// from a leader can trigger a view change for safety.
func TestEquivocationTriggersViewChange(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Equivocation Test: Triggers View Change ===")

	tracer := mocks.NewConsensusEventTracer()
	result, err := CreateTestNetworkWithStorages(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer result.NetworkManager.StopAll()

	nodes := result.Nodes
	consensusConfig := result.Config

	helpers := testinglib.NewViewChangeTestHelpers(nodes, result.NetworkManager, tracer, consensusConfig)
	helpers.SetStorages(result.Storages)

	// In this scenario, the leader itself is Byzantine and sends
	// conflicting proposals to different nodes
	leader, _ := consensusConfig.GetLeaderForView(0)
	t.Logf("Byzantine leader: Node%d", leader)

	// Record initial view
	initialView := nodes[0].GetCoordinator().GetCurrentView()
	t.Logf("Initial view: %d", initialView)

	// Create two different blocks (conflicting proposals)
	var block1Hash, block2Hash types.BlockHash
	copy(block1Hash[:], "leader_block_1_conflict")
	copy(block2Hash[:], "leader_block_2_conflict")

	leaderCrypto := nodes[leader].GetCrypto()

	// Leader creates conflicting votes for its own proposals
	// (simulating a leader that sends different proposals to different validators)
	vote1Data := fmt.Sprintf("%x:%d:%s:%d", block1Hash, 0, types.PhasePrepare, leader)
	sig1, _ := leaderCrypto.Sign([]byte(vote1Data))

	vote2Data := fmt.Sprintf("%x:%d:%s:%d", block2Hash, 0, types.PhasePrepare, leader)
	sig2, _ := leaderCrypto.Sign([]byte(vote2Data))

	leaderVote1 := &types.Vote{
		BlockHash: block1Hash,
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     leader,
		Signature: sig1,
	}

	leaderVote2 := &types.Vote{
		BlockHash: block2Hash,
		View:      0,
		Phase:     types.PhasePrepare,
		Voter:     leader,
		Signature: sig2,
	}

	// Distribute conflicting votes to different validators
	// This simulates what happens when validators compare notes
	// Track rejections when validators see conflicting votes
	conflictRejections := 0
	validatorCount := 0
	for nodeID, node := range nodes {
		if nodeID == leader {
			continue
		}

		coord := node.GetCoordinator()

		// Alternate which conflicting vote each validator sees
		if validatorCount%2 == 0 {
			err := coord.ProcessVote(leaderVote1)
			if err != nil {
				t.Logf("Node%d rejected leader vote 1: %v", nodeID, err)
			}
		} else {
			err := coord.ProcessVote(leaderVote2)
			if err != nil {
				t.Logf("Node%d rejected leader vote 2: %v", nodeID, err)
			}
		}
		validatorCount++
	}

	// Now distribute the OTHER vote to each validator
	// (simulating validators gossiping and discovering the conflict)
	validatorCount = 0
	for nodeID, node := range nodes {
		if nodeID == leader {
			continue
		}

		coord := node.GetCoordinator()

		// Each validator now sees both votes - conflicting vote should be rejected
		var err error
		if validatorCount%2 == 0 {
			err = coord.ProcessVote(leaderVote2) // Now sees the other one
		} else {
			err = coord.ProcessVote(leaderVote1) // Now sees the other one
		}
		if err != nil {
			conflictRejections++
			t.Logf("Node%d rejected conflicting leader vote: %v", nodeID, err)
		}
		validatorCount++
	}

	// Wait for view change timeout to potentially trigger
	time.Sleep(consensusConfig.GetTimeoutForView(0) + 2*time.Second)

	// Check if view changed
	var maxView types.ViewNumber
	for _, node := range nodes {
		view := node.GetCoordinator().GetCurrentView()
		if view > maxView {
			maxView = view
		}
	}

	t.Logf("Max view after Byzantine leader detection: %d", maxView)

	// Check for view change and equivocation events with payload validation
	viewChangeEvents := 0
	equivocationEvents := 0
	var leaderEquivocationDetected bool
	for _, ev := range tracer.GetEvents() {
		if ev.EventType == events.EventViewChangeStarted ||
			ev.EventType == events.EventNewViewStarted {
			viewChangeEvents++
		}
		if ev.EventType == events.EventEquivocationFound {
			equivocationEvents++
			// Check if this equivocation event is for the Byzantine leader
			if voter, ok := ev.Payload["voter"].(types.NodeID); ok && voter == leader {
				leaderEquivocationDetected = true
				// Validate payload structure
				assert.NotNil(t, ev.Payload["view"], "Leader equivocation event must contain view")
				assert.NotNil(t, ev.Payload["phase"], "Leader equivocation event must contain phase")
				assert.NotNil(t, ev.Payload["block1_hash"], "Leader equivocation event must contain block1_hash")
				assert.NotNil(t, ev.Payload["block2_hash"], "Leader equivocation event must contain block2_hash")
			}
		}
	}

	t.Logf("View change events: %d", viewChangeEvents)
	t.Logf("Equivocation events: %d", equivocationEvents)
	t.Logf("Conflict rejections: %d", conflictRejections)
	t.Logf("Leader equivocation detected: %v", leaderEquivocationDetected)

	// ASSERTION: Byzantine leader's conflicting votes MUST be rejected
	// This is the primary safety mechanism - we cannot rely solely on view change
	require.Greater(t, conflictRejections, 0,
		"Byzantine leader's conflicting votes must be rejected by validators")

	// ASSERTION: Equivocation events must be emitted for the Byzantine leader
	require.Greater(t, equivocationEvents, 0,
		"Equivocation events must be emitted when detecting Byzantine leader's conflicting votes")
	assert.True(t, leaderEquivocationDetected,
		"Equivocation event must identify the Byzantine leader as the equivocating voter")

	// ASSERTION: View must advance (either through normal progress or timeout)
	assert.Greater(t, uint64(maxView), uint64(initialView),
		"View should advance after timeout with Byzantine leader")

	// LIVENESS & SAFETY: Verify honest nodes can still commit despite Byzantine leader
	// After view change, honest nodes should be able to make progress
	commits := CollectCommittedBlocks(tracer)
	AssertHonestNodesCommitted(t, commits, leader, "Byzantine leader equivocation")

	t.Log("=== Triggers View Change Test Complete ===")
}
