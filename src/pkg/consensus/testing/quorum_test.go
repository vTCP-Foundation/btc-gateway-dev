//go:build testing
// +build testing

package testing

import (
	"testing"

	"btc-gateway/pkg/consensus/types"

	"github.com/stretchr/testify/assert"
)

// TestQuorumCalculationMatchesConfig ensures that CalculateQuorum in the testing
// package produces the same result as ConsensusConfig.QuorumThreshold().
// This prevents test expectations from drifting from actual protocol requirements.
func TestQuorumCalculationMatchesConfig(t *testing.T) {
	testCases := []struct {
		nodeCount     int
		expectedQuorum int
	}{
		{4, 3},  // (8)/3 + 1 = 3
		{5, 4},  // (10)/3 + 1 = 4
		{7, 5},  // (14)/3 + 1 = 5
		{10, 7}, // (20)/3 + 1 = 7
		{13, 9}, // (26)/3 + 1 = 9
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			// Test our CalculateQuorum function
			testingQuorum := CalculateQuorum(tc.nodeCount)
			assert.Equal(t, tc.expectedQuorum, testingQuorum,
				"CalculateQuorum(%d) should return %d", tc.nodeCount, tc.expectedQuorum)

			// Create a config with the same node count and verify it matches
			publicKeys := make([]types.PublicKey, tc.nodeCount)
			for i := 0; i < tc.nodeCount; i++ {
				publicKeys[i] = []byte{byte(i)} // dummy key
			}
			config, err := types.NewConsensusConfig(publicKeys)
			if err != nil {
				t.Fatalf("Failed to create config: %v", err)
			}

			configQuorum := config.QuorumThreshold()
			assert.Equal(t, configQuorum, testingQuorum,
				"CalculateQuorum(%d)=%d MUST match ConsensusConfig.QuorumThreshold()=%d",
				tc.nodeCount, testingQuorum, configQuorum)
		})
	}
}

// TestQuorumSafetyProperty verifies the BFT safety property: quorum > 2n/3
func TestQuorumSafetyProperty(t *testing.T) {
	for n := 4; n <= 20; n++ {
		quorum := CalculateQuorum(n)
		twoThirds := float64(2*n) / 3.0

		assert.Greater(t, float64(quorum), twoThirds,
			"Quorum for n=%d must be > 2n/3 (%.2f) for BFT safety, got %d",
			n, twoThirds, quorum)
	}
}
