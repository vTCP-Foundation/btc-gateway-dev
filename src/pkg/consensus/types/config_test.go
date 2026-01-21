package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQuorumThreshold verifies the BFT quorum formula: ⌊2n/3⌋+1
// This is the ONLY place where the formula may be hardcoded (for testing the formula itself).
func TestQuorumThreshold(t *testing.T) {
	testCases := []struct {
		nodes    int
		expected int
		desc     string
	}{
		{4, 3, "n=4: ⌊8/3⌋+1 = 3"},
		{5, 4, "n=5: ⌊10/3⌋+1 = 4"},
		{6, 5, "n=6: ⌊12/3⌋+1 = 5"},
		{7, 5, "n=7: ⌊14/3⌋+1 = 5"},
		{10, 7, "n=10: ⌊20/3⌋+1 = 7"},
		{100, 67, "n=100: ⌊200/3⌋+1 = 67"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			keys := make([]PublicKey, tc.nodes)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key%d", i))
			}
			config, err := NewConsensusConfig(keys)
			require.NoError(t, err)

			actual := config.QuorumThreshold()
			assert.Equal(t, tc.expected, actual,
				"QuorumThreshold for %d nodes should be %d, got %d",
				tc.nodes, tc.expected, actual)

			// Verify >2/3 property: quorum > 2n/3
			assert.Greater(t, float64(actual), float64(tc.nodes)*2/3,
				"Quorum must be >2/3 of total nodes for BFT safety")
		})
	}
}

// TestFaultyNodes verifies the Byzantine fault tolerance calculation: f = (n-1)/3
func TestFaultyNodes(t *testing.T) {
	testCases := []struct {
		nodes    int
		expected int
		desc     string
	}{
		{4, 1, "n=4: f=1"},
		{5, 1, "n=5: f=1"},
		{6, 1, "n=6: f=1"},
		{7, 2, "n=7: f=2"},
		{10, 3, "n=10: f=3"},
		{100, 33, "n=100: f=33"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			keys := make([]PublicKey, tc.nodes)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key%d", i))
			}
			config, err := NewConsensusConfig(keys)
			require.NoError(t, err)

			actual := config.FaultyNodes()
			assert.Equal(t, tc.expected, actual,
				"FaultyNodes for %d nodes should be %d, got %d",
				tc.nodes, tc.expected, actual)
		})
	}
}

// TestHasQuorum verifies the quorum check method
func TestHasQuorum(t *testing.T) {
	keys := make([]PublicKey, 5)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
	}
	config, err := NewConsensusConfig(keys)
	require.NoError(t, err)

	// For n=5, quorum = 4
	assert.False(t, config.HasQuorum(0), "0 votes should not meet quorum")
	assert.False(t, config.HasQuorum(1), "1 vote should not meet quorum")
	assert.False(t, config.HasQuorum(2), "2 votes should not meet quorum")
	assert.False(t, config.HasQuorum(3), "3 votes should not meet quorum")
	assert.True(t, config.HasQuorum(4), "4 votes should meet quorum")
	assert.True(t, config.HasQuorum(5), "5 votes should meet quorum")
}
