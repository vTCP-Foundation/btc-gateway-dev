package mocks

import (
	"time"

	"btc-gateway/pkg/consensus/types"
)

// MockConfig contains configuration for all mock components.
type MockConfig struct {
	Network NetworkConfig
	Storage StorageConfig
	Crypto  CryptoConfig
}

// DefaultMockConfig returns a configuration suitable for most tests.
func DefaultMockConfig() MockConfig {
	return MockConfig{
		Network: DefaultNetworkConfig(),
		Storage: DefaultStorageConfig(),
		Crypto:  DefaultCryptoConfig(),
	}
}

// FailureConfig contains failure injection configuration for all mock components.
type FailureConfig struct {
	Network NetworkFailureConfig
	Storage StorageFailureConfig
	Crypto  CryptoFailureConfig
}

// DefaultFailureConfig returns a failure configuration with no failures.
func DefaultFailureConfig() FailureConfig {
	return FailureConfig{
		Network: DefaultNetworkFailureConfig(),
		Storage: DefaultStorageFailureConfig(),
		Crypto:  DefaultCryptoFailureConfig(),
	}
}

// TestScenarioConfig contains configuration for specific test scenarios.
type TestScenarioConfig struct {
	// Name is a descriptive name for the test scenario
	Name string
	// NodeCount is the number of nodes in the test network
	NodeCount int
	// MockConfig contains the base mock configuration
	MockConfig MockConfig
	// FailureConfig contains failure injection configuration
	FailureConfig FailureConfig
	// Duration is how long the test should run
	Duration time.Duration
	// NetworkPartitions defines network partition scenarios
	NetworkPartitions []PartitionScenario
	// ByzantineNodes defines which nodes should exhibit Byzantine behavior
	ByzantineNodes []types.NodeID
}

// PartitionScenario defines a network partition configuration.
type PartitionScenario struct {
	// StartTime is when the partition should start (relative to test start)
	StartTime time.Duration
	// Duration is how long the partition should last
	Duration time.Duration
	// PartitionedNodes are the nodes that should be isolated
	PartitionedNodes []types.NodeID
}

// Predefined test scenario configurations

// HappyPathScenario returns a configuration for testing normal consensus operation.
func HappyPathScenario() TestScenarioConfig {
	return TestScenarioConfig{
		Name:              "Happy Path Consensus",
		NodeCount:         5,
		MockConfig:        DefaultMockConfig(),
		FailureConfig:     DefaultFailureConfig(),
		Duration:          30 * time.Second,
		NetworkPartitions: nil,
		ByzantineNodes:    nil,
	}
}

// NetworkPartitionScenario returns a configuration for testing network partition recovery.
func NetworkPartitionScenario() TestScenarioConfig {
	config := DefaultMockConfig()
	failures := DefaultFailureConfig()
	
	return TestScenarioConfig{
		Name:              "Network Partition Recovery",
		NodeCount:         5,
		MockConfig:        config,
		FailureConfig:     failures,
		Duration:          60 * time.Second,
		NetworkPartitions: []PartitionScenario{
			{
				StartTime:        10 * time.Second,
				Duration:         20 * time.Second,
				PartitionedNodes: []types.NodeID{0, 1}, // Partition minority
			},
		},
		ByzantineNodes: nil,
	}
}

// ByzantineLeaderScenario returns a configuration for testing Byzantine leader behavior.
func ByzantineLeaderScenario() TestScenarioConfig {
	config := DefaultMockConfig()
	failures := DefaultFailureConfig()
	
	// Configure crypto to occasionally generate invalid signatures from Byzantine node
	failures.Crypto.InvalidSignatureRate = 0.3
	
	return TestScenarioConfig{
		Name:              "Byzantine Leader Behavior",
		NodeCount:         5,
		MockConfig:        config,
		FailureConfig:     failures,
		Duration:          45 * time.Second,
		NetworkPartitions: nil,
		ByzantineNodes:    []types.NodeID{0}, // Node 0 is Byzantine leader
	}
}

// HighLatencyScenario returns a configuration for testing high network latency.
func HighLatencyScenario() TestScenarioConfig {
	config := DefaultMockConfig()
	failures := DefaultFailureConfig()
	
	// Increase network delays
	config.Network.BaseDelay = 500 * time.Millisecond
	config.Network.DelayVariation = 200 * time.Millisecond
	config.Network.PacketLossRate = 0.05
	
	return TestScenarioConfig{
		Name:              "High Network Latency",
		NodeCount:         5,
		MockConfig:        config,
		FailureConfig:     failures,
		Duration:          60 * time.Second,
		NetworkPartitions: nil,
		ByzantineNodes:    nil,
	}
}

// StorageFailureScenario returns a configuration for testing storage failures.
func StorageFailureScenario() TestScenarioConfig {
	config := DefaultMockConfig()
	failures := DefaultFailureConfig()
	
	// Configure storage failures
	failures.Storage.WriteFailureRate = 0.1
	failures.Storage.ReadFailureRate = 0.05
	failures.Storage.CorruptionRate = 0.02
	
	return TestScenarioConfig{
		Name:              "Storage Reliability Test",
		NodeCount:         5,
		MockConfig:        config,
		FailureConfig:     failures,
		Duration:          45 * time.Second,
		NetworkPartitions: nil,
		ByzantineNodes:    nil,
	}
}

// ChaosScenario returns a configuration combining multiple failure modes.
func ChaosScenario() TestScenarioConfig {
	config := DefaultMockConfig()
	failures := DefaultFailureConfig()
	
	// Configure multiple failure modes
	config.Network.BaseDelay = 100 * time.Millisecond
	config.Network.DelayVariation = 50 * time.Millisecond
	config.Network.PacketLossRate = 0.03
	config.Network.DuplicationRate = 0.02
	
	failures.Network.FailingSendRate = 0.02
	failures.Storage.WriteFailureRate = 0.05
	failures.Storage.ReadFailureRate = 0.02
	failures.Crypto.VerificationRejectRate = 0.01
	
	return TestScenarioConfig{
		Name:              "Chaos Engineering Test",
		NodeCount:         7, // Larger network for more complexity
		MockConfig:        config,
		FailureConfig:     failures,
		Duration:          90 * time.Second,
		NetworkPartitions: []PartitionScenario{
			{
				StartTime:        20 * time.Second,
				Duration:         15 * time.Second,
				PartitionedNodes: []types.NodeID{0},
			},
			{
				StartTime:        50 * time.Second,
				Duration:         10 * time.Second,
				PartitionedNodes: []types.NodeID{1, 2},
			},
		},
		ByzantineNodes: []types.NodeID{6}, // One Byzantine node
	}
}

// ValidationConfig contains validation parameters for test scenarios.
type ValidationConfig struct {
	// ExpectedConsensus indicates whether consensus should be reached
	ExpectedConsensus bool
	// MinBlocks is the minimum number of blocks that should be committed
	MinBlocks int
	// MaxConsensusTime is the maximum time allowed to reach consensus
	MaxConsensusTime time.Duration
	// ToleratedFailures is the number of failures the system should handle
	ToleratedFailures int
}

// GetValidationConfig returns appropriate validation criteria for a scenario.
func (tsc TestScenarioConfig) GetValidationConfig() ValidationConfig {
	byzantineCount := len(tsc.ByzantineNodes)
	faultTolerance := (tsc.NodeCount - 1) / 3
	
	switch tsc.Name {
	case "Happy Path Consensus":
		return ValidationConfig{
			ExpectedConsensus: true,
			MinBlocks:         3,
			MaxConsensusTime:  3 * time.Second,
			ToleratedFailures: 0,
		}
	case "Network Partition Recovery":
		return ValidationConfig{
			ExpectedConsensus: true, // Should recover after partition
			MinBlocks:         2,
			MaxConsensusTime:  10 * time.Second, // Longer due to recovery
			ToleratedFailures: 2, // Minority partition
		}
	case "Byzantine Leader Behavior":
		return ValidationConfig{
			ExpectedConsensus: byzantineCount <= faultTolerance,
			MinBlocks:         1,
			MaxConsensusTime:  15 * time.Second, // Longer due to view changes
			ToleratedFailures: byzantineCount,
		}
	default:
		// Generic validation for other scenarios
		return ValidationConfig{
			ExpectedConsensus: byzantineCount <= faultTolerance,
			MinBlocks:         1,
			MaxConsensusTime:  20 * time.Second,
			ToleratedFailures: byzantineCount,
		}
	}
}