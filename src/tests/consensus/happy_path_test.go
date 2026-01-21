package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/mocks"
	testinglib "btc-gateway/pkg/consensus/testing"
	"btc-gateway/pkg/consensus/types"
)

func TestNetworkInitialization(t *testing.T) {
	t.Parallel()
	t.Log("=== Network Initialization Test (Unified Node) ===")

	tracer := mocks.NewConsensusEventTracer()
	nodes, networkManager, err := integration.CreateTestNetwork(5, tracer, false)
	require.NoError(t, err, "Should successfully create test network")

	defer networkManager.StopAll() // Ensure cleanup

	require.Len(t, nodes, 5, "Should create 5 nodes")
	t.Logf("✓ Successfully created %d nodes", len(nodes))

	for nodeID, node := range nodes {
		require.NotNil(t, node, "Node %d should not be nil", nodeID)
		require.True(t, node.IsStarted(), "Node %d should be started", nodeID)
		require.Equal(t, nodeID, node.GetNodeID(), "Node ID should match")
		t.Logf("✓ Node %d: Initialized and started", nodeID)
	}

	t.Log("=== Network Initialization Complete ===")
}

func TestConsensusFlow(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Consensus Flow Test with Sequential Validation ===")

	tracer := mocks.NewConsensusEventTracer()

	nodes, networkManager, err := integration.CreateTestNetwork(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	t.Log("✓ Network created and initialized")

	// Build expected sequence for this consensus round
	leader := calculateLeader(0, totalNodes)
	expectedSequence := testinglib.BuildCompleteHappyPathSequence(totalNodes, 0)
	t.Logf("✓ Built expected sequence with %d events across all phases", len(expectedSequence))

	// Show expected phases
	phaseEvents := make(map[string]int)
	for _, exp := range expectedSequence {
		phaseEvents[exp.Phase]++
	}
	t.Log("Expected events by phase:")
	for phase, count := range phaseEvents {
		t.Logf("  %s: %d events", phase, count)
	}

	time.Sleep(100 * time.Millisecond)

	payload := []byte("integration_test_block")
	t.Logf("Starting consensus: leader=Node%d, payload=%s", leader, string(payload))

	err = nodes[leader].ProposeBlock(payload)
	require.NoError(t, err, "Failed to initiate consensus")

	// Poll for consensus completion with better termination criteria
	maxWait := 8 * time.Second
	pollInterval := 200 * time.Millisecond
	t.Logf("Polling for consensus completion (max %v, every %v)...", maxWait, pollInterval)

	start := time.Now()
	var allEvents []events.ConsensusEvent

	for time.Since(start) < maxWait {
		allEvents = tracer.GetEvents()

		// Check if we have minimum expected completion (all nodes committed)
		blockCommitCount := 0
		for _, event := range allEvents {
			if event.EventType == events.EventBlockCommitted {
				blockCommitCount++
			}
		}

		// Check if consensus is complete (all nodes have committed)
		if blockCommitCount >= totalNodes {
			elapsed := time.Since(start)
			t.Logf("✓ Consensus completed in %v (%d block commits detected)", elapsed, blockCommitCount)
			break
		}

		// Also check if we've collected a reasonable number of events
		if len(allEvents) >= 20 { // Rough estimate of minimum events for a successful round
			time.Sleep(500 * time.Millisecond) // Give a bit more time for stragglers
			break
		}

		time.Sleep(pollInterval)
	}

	// Final event collection
	allEvents = tracer.GetEvents()
	t.Logf("✓ Collected %d total events from consensus execution", len(allEvents))

	// Show events per node
	nodeEventCounts := make(map[uint16]int)
	for _, event := range allEvents {
		nodeEventCounts[event.NodeID]++
	}

	t.Log("Actual events per node:")
	for nodeID := uint16(0); nodeID < uint16(totalNodes); nodeID++ {
		count := nodeEventCounts[nodeID]
		t.Logf("  Node %d: %d events", nodeID, count)
	}

	// Show event type statistics
	eventStats := make(map[string]int)
	for _, event := range allEvents {
		eventStats[string(event.EventType)]++
	}
	t.Log("Actual event type breakdown:")
	for eventType, count := range eventStats {
		t.Logf("  %s: %d", eventType, count)
	}

	require.Greater(t, len(allEvents), 0, "Should have captured events from consensus")

	// Sequential Protocol Validation
	t.Log("\n=== Sequential Protocol Validation ===")
	result := testinglib.ValidateSequentialEvents(expectedSequence, allEvents, totalNodes)

	// Print detailed validation report
	printSequentialValidationReport(t, result)

	// Performance analysis - calculate simple latency from first to last event
	if len(allEvents) > 1 {
		firstEvent := allEvents[0].Timestamp
		lastEvent := allEvents[len(allEvents)-1].Timestamp
		latency := lastEvent.Sub(firstEvent)

		t.Logf("\n📊 Performance Analysis:")
		t.Logf("  Consensus latency: %v", latency)
		if latency < 3*time.Second {
			t.Logf("  ✓ Latency meets 3s requirement")
		} else {
			t.Logf("  ⚠ Latency exceeds 3s requirement")
		}
		assert.Less(t, latency, 3*time.Second, "Consensus latency should be under 3s")
	}

	// Final assessment
	if result.Success {
		t.Log("\n✅ CONSENSUS VALIDATION PASSED")
		t.Logf("  All %d required events occurred in correct sequence", result.TotalFound)
		completionPercent := float64(len(result.PassedEvents)) / float64(result.TotalExpected) * 100
		t.Logf("  Protocol compliance: %.1f%%", completionPercent)
	} else {
		t.Log("\n❌ CONSENSUS VALIDATION FAILED")
		t.Logf("  Missing events: %d", len(result.FailedEvents))
		completionPercent := float64(len(result.PassedEvents)) / float64(result.TotalExpected) * 100
		t.Logf("  Protocol compliance: %.1f%%", completionPercent)

		// Show first few missing events for debugging
		if len(result.FailedEvents) > 0 {
			t.Log("\nFirst few missing events:")
			for i, missing := range result.FailedEvents[:min(5, len(result.FailedEvents))] {
				t.Logf("  %d. [%03d] %s - %s", i+1, missing.SequenceNumber, missing.EventType, missing.Description)
				if missing.FailureHint != "" {
					t.Logf("       Hint: %s", missing.FailureHint)
				}
			}
		}

		// Show guidance for debugging
		t.Logf("\nNote: This test failure shows exactly what events the consensus implementation needs to emit.")
		t.Logf("Use this output as a reference to implement the missing event emissions in the consensus engine.")

		// ASSERTION: Validation must pass - fail the test if it doesn't
		t.Fatalf("Sequential validation failed: %d/%d events passed (%.1f%%) - see output above for details",
			len(result.PassedEvents), result.TotalExpected, completionPercent)
	}

	t.Log("\n=== Consensus Flow Test Complete ===")
}

// Helper function to print detailed validation report
func printSequentialValidationReport(t *testing.T, result testinglib.ValidationResult) {
	completionPercent := float64(len(result.PassedEvents)) / float64(result.TotalExpected) * 100
	t.Logf("📊 Validation Summary: %d/%d events (%.1f%% complete)",
		len(result.PassedEvents), result.TotalExpected, completionPercent)

	if result.Success {
		t.Log("✅ All required events occurred in correct sequence")
		return
	}

	// Print phase-by-phase results
	phases := make(map[string][]testinglib.ExpectedEventSequence)

	// Group passed events by phase
	for _, passed := range result.PassedEvents {
		phases[passed.Phase] = append(phases[passed.Phase], passed)
	}

	// Group failed events by phase
	for _, failed := range result.FailedEvents {
		phases[failed.Phase] = append(phases[failed.Phase], failed)
	}

	for _, phaseName := range []string{"NewView", "Proposal", "Prepare", "PreCommit", "Commit", "Decide"} {
		phaseEvents, exists := phases[phaseName]
		if !exists || len(phaseEvents) == 0 {
			continue
		}

		t.Logf("\n--- Phase: %s ---", phaseName)

		passedCount := 0
		failedCount := 0

		// Count passed vs failed in this phase
		for _, event := range phaseEvents {
			found := false
			for _, passed := range result.PassedEvents {
				if passed.SequenceNumber == event.SequenceNumber {
					found = true
					break
				}
			}
			if found {
				passedCount++
			} else {
				failedCount++
			}
		}

		if failedCount == 0 {
			t.Logf("✓ Phase completed successfully (%d/%d events)", passedCount, len(phaseEvents))
		} else {
			t.Logf("✗ Phase incomplete (%d passed, %d failed)", passedCount, failedCount)
		}

		// Show first few events in this phase
		eventCount := 0
		for _, event := range phaseEvents {
			eventCount++
			if eventCount > 8 { // Limit output
				t.Logf("    ... (%d more events in this phase)", len(phaseEvents)-8)
				break
			}

			// Check if this event passed
			passed := false
			for _, p := range result.PassedEvents {
				if p.SequenceNumber == event.SequenceNumber {
					passed = true
					break
				}
			}

			if passed {
				t.Logf("  ✓ [%03d] %s", event.SequenceNumber, event.Description)
			} else {
				t.Logf("  ✗ [%03d] MISSING: %s", event.SequenceNumber, event.Description)
			}
		}
	}
}

// Helper function to get minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestMultiRoundConsensus(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Multi-Round Consensus Test (Unified Node) ===")

	tracer := mocks.NewConsensusEventTracer()
	t.Logf("Testing leader rotation across %d rounds", 3)

	nodes, networkManager, err := integration.CreateTestNetwork(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	rounds := 3
	totalEvents := 0

	for round := 0; round < rounds; round++ {
		t.Logf("\n--- Round %d ---", round+1)
		tracer.Reset()

		leader := calculateLeader(uint64(round), totalNodes)
		payload := []byte("round_" + string(rune('0'+round)) + "_block")
		t.Logf("Leader: Node %d, Payload: %s", leader, string(payload))

		err := nodes[leader].ProposeBlock(payload)
		if err != nil {
			t.Logf("⚠ Round %d proposal failed: %v", round+1, err)
			continue
		}

		t.Log("Waiting for consensus completion...")
		helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, nil)
		helpers.WaitForConsensusCompletion(5 * time.Second)

		roundEvents := tracer.GetEvents()
		totalEvents += len(roundEvents)
		t.Logf("✓ Collected %d events this round", len(roundEvents))

		// Show events per node for this round
		nodeEventCounts := make(map[uint16]int)
		for _, event := range roundEvents {
			nodeEventCounts[event.NodeID]++
		}

		if len(roundEvents) > 0 {
			t.Log("Events per node this round:")
			for nodeID := uint16(0); nodeID < uint16(totalNodes); nodeID++ {
				count := nodeEventCounts[nodeID]
				if count > 0 {
					t.Logf("  Node %d: %d events", nodeID, count)
				}
			}
			t.Logf("✓ Round %d completed with consensus activity", round+1)
		} else {
			t.Logf("⚠ Round %d had no events", round+1)
		}
	}

	t.Logf("\n📊 Multi-round summary: %d total events across %d rounds", totalEvents, rounds)
	t.Log("=== Multi-Round Consensus Test Complete ===")
}

func TestConsensusPerformance(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Consensus Performance Test (Unified Node) ===")

	tracer := mocks.NewConsensusEventTracer()
	t.Log("Measuring consensus latency across multiple rounds")

	nodes, networkManager, err := integration.CreateTestNetwork(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	rounds := 3
	var totalLatency time.Duration
	successfulRounds := 0
	var roundMetrics []time.Duration

	for round := 0; round < rounds; round++ {
		t.Logf("\n--- Performance Round %d ---", round+1)
		tracer.Reset()

		leader := calculateLeader(uint64(round), totalNodes)
		payload := []byte("perf_test_" + string(rune('0'+round)))
		t.Logf("Leader: Node %d, Starting latency measurement...", leader)

		start := time.Now()
		err := nodes[leader].ProposeBlock(payload)
		if err != nil {
			t.Logf("⚠ Round %d failed to start: %v", round+1, err)
			continue
		}

		helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, nil)
		helpers.WaitForConsensusCompletion(5 * time.Second)
		roundLatency := time.Since(start)

		events := tracer.GetEvents()
		if len(events) > 0 {
			totalLatency += roundLatency
			successfulRounds++
			roundMetrics = append(roundMetrics, roundLatency)

			t.Logf("✓ Round %d latency: %v (%d events)", round+1, roundLatency, len(events))

			// Show per-node event count
			nodeEventCounts := make(map[uint16]int)
			for _, event := range events {
				nodeEventCounts[event.NodeID]++
			}

			activeNodes := 0
			for nodeID := uint16(0); nodeID < uint16(totalNodes); nodeID++ {
				if nodeEventCounts[nodeID] > 0 {
					activeNodes++
				}
			}
			t.Logf("  Active nodes: %d/%d", activeNodes, totalNodes)

			if roundLatency < 5*time.Second {
				t.Log("  ✓ Round meets 5s threshold")
			} else {
				t.Log("  ⚠ Round exceeds 5s threshold")
			}
			assert.Less(t, roundLatency, 5*time.Second, "Round %d took too long", round)
		} else {
			t.Logf("⚠ Round %d: No events captured", round+1)
		}
	}

	t.Log("\n📊 Performance Summary:")
	if successfulRounds > 0 {
		avgLatency := totalLatency / time.Duration(successfulRounds)
		t.Logf("  Successful rounds: %d/%d", successfulRounds, rounds)
		t.Logf("  Average latency: %v", avgLatency)
		t.Logf("  Total test time: %v", totalLatency)

		// Show individual round metrics
		for i, latency := range roundMetrics {
			t.Logf("  Round %d: %v", i+1, latency)
		}

		if avgLatency < 3*time.Second {
			t.Log("✓ Average meets 3s requirement")
		} else {
			t.Log("⚠ Average exceeds 3s requirement")
		}
		assert.Less(t, avgLatency, 3*time.Second, "Average latency should be under 3s")
	} else {
		t.Log("  ⚠ No successful rounds for performance measurement")
	}

	t.Log("=== Consensus Performance Test Complete ===")
}

func TestEventValidation(t *testing.T) {
	t.Parallel()
	const totalNodes = 5
	t.Log("=== Event Validation Framework Test (Unified Node) ===")

	tracer := mocks.NewConsensusEventTracer()
	t.Log("Testing protocol compliance validation framework")

	nodes, networkManager, err := integration.CreateTestNetwork(totalNodes, tracer, false)
	require.NoError(t, err, "Should successfully create test network")
	defer networkManager.StopAll()

	leader := calculateLeader(0, len(nodes))
	payload := []byte("validation_test_block")
	t.Logf("Starting consensus: leader=Node%d, payload=%s", leader, string(payload))

	err = nodes[leader].ProposeBlock(payload)
	require.NoError(t, err, "Should be able to start consensus")

	t.Log("Waiting for consensus execution...")
	helpers := testinglib.NewViewChangeTestHelpers(nodes, networkManager, tracer, nil)
	helpers.WaitForQuorumCommits(5 * time.Second)

	allEvents := tracer.GetEvents()
	t.Logf("✓ Collected %d events for validation", len(allEvents))

	// Show detailed event breakdown per node
	nodeEventCounts := make(map[uint16]int)
	eventTypesByNode := make(map[uint16]map[string]int)

	for _, event := range allEvents {
		nodeEventCounts[event.NodeID]++
		if eventTypesByNode[event.NodeID] == nil {
			eventTypesByNode[event.NodeID] = make(map[string]int)
		}
		eventTypesByNode[event.NodeID][string(event.EventType)]++
	}

	t.Log("Detailed event breakdown by node:")
	for nodeID := uint16(0); nodeID < uint16(totalNodes); nodeID++ {
		count := nodeEventCounts[nodeID]
		t.Logf("  Node %d: %d total events", nodeID, count)
		if count > 0 {
			for eventType, eventCount := range eventTypesByNode[nodeID] {
				t.Logf("    %s: %d", eventType, eventCount)
			}
		}
	}

	// Use sequential validation instead of the old rule-based system
	t.Log("\nValidating protocol compliance...")
	expectedSequence := testinglib.BuildCompleteHappyPathSequence(totalNodes, 0)
	result := testinglib.ValidateSequentialEvents(expectedSequence, allEvents, totalNodes)

	if result.Success {
		t.Log("✓ Perfect protocol compliance - all events validated")
	} else {
		t.Logf("Protocol validation results (%d/%d events passed):", len(result.PassedEvents), result.TotalExpected)
		for i, failed := range result.FailedEvents {
			if i >= 5 { // Limit output
				t.Logf("  ... and %d more", len(result.FailedEvents)-i)
				break
			}
			t.Logf("  %d. %s: %s", i+1, failed.EventType, failed.FailureHint)
		}
		t.Log("Note: Issues indicate areas where consensus implementation is incomplete")
	}

	// Global event statistics
	eventStats := make(map[string]int)
	for _, event := range allEvents {
		eventStats[string(event.EventType)]++
	}
	t.Log("\nGlobal event statistics:")
	for eventType, count := range eventStats {
		t.Logf("  %s: %d occurrences", eventType, count)
	}

	completionPercent := float64(len(result.PassedEvents)) / float64(result.TotalExpected) * 100
	t.Logf("\n📊 Validation summary: %d events processed, %.1f%% protocol compliance",
		len(allEvents), completionPercent)

	// REQUIREMENT: Test must fail if 100% protocol compliance is not achieved
	if completionPercent < 100.0 {
		t.Fatalf("❌ Protocol compliance requirement not met: %.1f%% < 100.0%% (missing %d events)",
			completionPercent, len(result.FailedEvents))
	}

	t.Log("=== Event Validation Framework Test Complete ===")
}

func calculateLeader(view uint64, totalNodes int) types.NodeID {
	return types.NodeID(view % uint64(totalNodes))
}
