package consensus

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/mocks"
	"btc-gateway/pkg/consensus/types"
)

// TestNetworkResult contains all the components created by CreateTestNetwork
type TestNetworkResult struct {
	Nodes          map[types.NodeID]*integration.Node
	NetworkManager *integration.NetworkManager
	Config         *types.ConsensusConfig
	Storages       map[types.NodeID]*mocks.MockStorage
}

// CreateTestNetwork creates a test consensus network with the specified number of nodes.
// Returns the nodes, network manager, consensus config, and error.
// For backwards compatibility, use CreateTestNetworkWithStorages if you need access to storages.
func CreateTestNetwork(nodeCount int, tracer events.EventTracer, enableMessageLogging bool) (map[types.NodeID]*integration.Node, *integration.NetworkManager, *types.ConsensusConfig, error) {
	result, err := CreateTestNetworkWithStorages(nodeCount, tracer, enableMessageLogging)
	if err != nil {
		return nil, nil, nil, err
	}
	return result.Nodes, result.NetworkManager, result.Config, nil
}

// CreateTestNetworkWithStorages creates a test consensus network and returns all components
// including mock storages for state sync testing.
func CreateTestNetworkWithStorages(nodeCount int, tracer events.EventTracer, enableMessageLogging bool) (*TestNetworkResult, error) {
	// Create consensus config
	publicKeys := make([]types.PublicKey, nodeCount)
	for i := 0; i < nodeCount; i++ {
		publicKeys[i] = []byte(fmt.Sprintf("public-key-node-%d", i))
	}

	consensusConfig, err := types.NewConsensusConfig(publicKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to create consensus config: %w", err)
	}

	// Create validators list
	validators := make([]types.NodeID, nodeCount)
	for i := 0; i < nodeCount; i++ {
		validators[i] = types.NodeID(i)
	}

	// Create shared genesis block
	genesisBlock := types.NewBlock(
		types.BlockHash{},              // No parent
		0,                              // Height 0
		0,                              // View 0
		0,                              // Genesis always proposed by Node 0
		[]byte("shared_genesis_block"), // Deterministic genesis payload
	)

	// Create mock infrastructure for all nodes
	mockNetworks := make(map[types.NodeID]*mocks.MockNetwork)
	mockStorages := make(map[types.NodeID]*mocks.MockStorage)
	mockCryptos := make(map[types.NodeID]*mocks.MockCrypto)

	// Create infrastructure components
	for _, nodeID := range validators {
		// Create mock network
		networkConfig := mocks.DefaultNetworkConfig()
		networkFailures := mocks.DefaultNetworkFailureConfig()
		mockNetwork := mocks.NewMockNetwork(nodeID, networkConfig, networkFailures)
		mockNetwork.SetEventTracer(tracer)
		mockNetworks[nodeID] = mockNetwork

		// Create mock storage
		storageConfig := mocks.DefaultStorageConfig()
		storageFailures := mocks.DefaultStorageFailureConfig()
		mockStorages[nodeID] = mocks.NewMockStorage(storageConfig, storageFailures)

		// Create mock crypto
		cryptoConfig := mocks.DefaultCryptoConfig()
		cryptoFailures := mocks.DefaultCryptoFailureConfig()
		mockCryptos[nodeID] = mocks.NewMockCrypto(nodeID, cryptoConfig, cryptoFailures)
	}

	// Set up full mesh connectivity for networks
	for _, nodeID := range validators {
		mockNetworks[nodeID].SetPeers(mockNetworks)
	}

	// Set up cryptographic trust relationships
	for _, nodeID := range validators {
		for _, otherNodeID := range validators {
			mockCryptos[nodeID].AddNode(otherNodeID)
		}
	}

	// Create nodes with injected dependencies
	nodes := make(map[types.NodeID]*integration.Node)
	for _, nodeID := range validators {
		config := &integration.NodeConfig{
			NodeID:       nodeID,
			Validators:   validators,
			Config:       consensusConfig,
			EventTracer:  tracer,
			GenesisBlock: genesisBlock,
		}

		node, err := integration.NewNode(config, mockNetworks[nodeID], mockStorages[nodeID], mockCryptos[nodeID])
		if err != nil {
			return nil, fmt.Errorf("failed to create node %d: %w", nodeID, err)
		}

		nodes[nodeID] = node
	}

	// Start all nodes
	for nodeID, node := range nodes {
		if err := node.Start(); err != nil {
			return nil, fmt.Errorf("failed to start node %d: %w", nodeID, err)
		}
	}

	// Create network manager for message processing
	networkManager := integration.NewNetworkManager()
	networkManager.EnableLogging(enableMessageLogging)

	for _, node := range nodes {
		networkManager.AddNode(node)
	}

	// Start message processing
	networkManager.StartAll()

	return &TestNetworkResult{
		Nodes:          nodes,
		NetworkManager: networkManager,
		Config:         consensusConfig,
		Storages:       mockStorages,
	}, nil
}

// =============================================================================
// Commit Verification Helpers for Liveness Testing
// =============================================================================

// CommittedBlockInfo holds info about a committed block from events.
type CommittedBlockInfo struct {
	Hash   types.BlockHash
	Height types.Height
	NodeID types.NodeID
}

// CollectCommittedBlocks extracts committed block info from event tracer.
func CollectCommittedBlocks(tracer *mocks.ConsensusEventTracer) []CommittedBlockInfo {
	var committed []CommittedBlockInfo
	allEvents := tracer.GetEventsByType(events.EventBlockCommitted)

	for _, event := range allEvents {
		payload := event.Payload
		if hash, ok := payload["block_hash"].(types.BlockHash); ok {
			height, _ := payload["height"].(types.Height)
			committed = append(committed, CommittedBlockInfo{
				Hash:   hash,
				Height: height,
				NodeID: types.NodeID(event.NodeID),
			})
		}
	}
	return committed
}

// CollectCommitsAfterCount collects commits that occurred after a certain event count.
// Useful for measuring commits during a specific test phase.
func CollectCommitsAfterCount(tracer *mocks.ConsensusEventTracer, afterCount int) []CommittedBlockInfo {
	var committed []CommittedBlockInfo
	allEvents := tracer.GetEvents()

	for i, event := range allEvents {
		if i < afterCount {
			continue
		}
		if event.EventType == events.EventBlockCommitted {
			payload := event.Payload
			if hash, ok := payload["block_hash"].(types.BlockHash); ok {
				height, _ := payload["height"].(types.Height)
				committed = append(committed, CommittedBlockInfo{
					Hash:   hash,
					Height: height,
					NodeID: types.NodeID(event.NodeID),
				})
			}
		}
	}
	return committed
}

// GroupCommitsByHeight groups committed blocks by height.
// Returns a map of height -> set of unique block hashes committed at that height.
func GroupCommitsByHeight(commits []CommittedBlockInfo) map[types.Height]map[types.BlockHash]bool {
	heightToHashes := make(map[types.Height]map[types.BlockHash]bool)
	for _, block := range commits {
		if heightToHashes[block.Height] == nil {
			heightToHashes[block.Height] = make(map[types.BlockHash]bool)
		}
		heightToHashes[block.Height][block.Hash] = true
	}
	return heightToHashes
}

// AssertCommitAgreement verifies that all commits at each height agree on the same block.
// This is a core safety property of BFT consensus.
func AssertCommitAgreement(t *testing.T, commits []CommittedBlockInfo) {
	t.Helper()
	heightToHashes := GroupCommitsByHeight(commits)

	for height, hashes := range heightToHashes {
		hashCount := len(hashes)
		if hashCount > 1 {
			t.Logf("SAFETY VIOLATION: Height %d has %d different committed blocks:", height, hashCount)
			for hash := range hashes {
				t.Logf("  - Block %x", hash[:8])
			}
		}
		assert.Equal(t, 1, hashCount,
			"SAFETY VIOLATION: Multiple different blocks committed at height %d", height)
	}
}

// AssertLivenessWithCommits verifies both liveness (commits occurred) and safety (commits agree).
// This is the primary assertion for Byzantine fault tolerance tests.
func AssertLivenessWithCommits(t *testing.T, tracer *mocks.ConsensusEventTracer, description string) {
	t.Helper()
	commits := CollectCommittedBlocks(tracer)

	// LIVENESS: At least one commit must have occurred
	require.Greater(t, len(commits), 0,
		"LIVENESS VIOLATION [%s]: No commits occurred - system failed to make progress", description)

	// SAFETY: All commits must agree
	AssertCommitAgreement(t, commits)

	t.Logf("Liveness verified [%s]: %d commit events, all in agreement", description, len(commits))
}

// AssertLivenessAfterByzantine verifies liveness and safety after Byzantine behavior.
// Checks that honest nodes committed blocks and all agree on the same chain.
// minExpectedHeight is the minimum height that should have been committed.
func AssertLivenessAfterByzantine(t *testing.T, tracer *mocks.ConsensusEventTracer, minExpectedHeight types.Height, description string) {
	t.Helper()
	commits := CollectCommittedBlocks(tracer)
	heightToHashes := GroupCommitsByHeight(commits)

	// LIVENESS: Commits must have occurred
	require.Greater(t, len(commits), 0,
		"LIVENESS VIOLATION [%s]: No commits occurred despite Byzantine tolerance", description)

	// LIVENESS: Expected height must have been reached
	maxCommittedHeight := types.Height(0)
	for height := range heightToHashes {
		if height > maxCommittedHeight {
			maxCommittedHeight = height
		}
	}
	require.GreaterOrEqual(t, maxCommittedHeight, minExpectedHeight,
		"LIVENESS VIOLATION [%s]: Expected commits at height >= %d, got max height %d",
		description, minExpectedHeight, maxCommittedHeight)

	// SAFETY: All commits must agree at each height
	AssertCommitAgreement(t, commits)

	t.Logf("Byzantine liveness verified [%s]: max committed height %d, all heights in agreement",
		description, maxCommittedHeight)
}

// AssertNoNewCommitsDuring verifies that no new commits occurred during a partition/attack.
// Takes the event count before the phase and verifies no commits after that point.
func AssertNoNewCommitsDuring(t *testing.T, tracer *mocks.ConsensusEventTracer, eventCountBefore int, description string) {
	t.Helper()
	newCommits := CollectCommitsAfterCount(tracer, eventCountBefore)

	assert.Equal(t, 0, len(newCommits),
		"SAFETY VIOLATION [%s]: %d commits occurred when none should have (e.g., during partition without quorum)",
		description, len(newCommits))
}

// AssertHonestNodesCommitted verifies that commits from honest nodes (excluding Byzantine node) all agree.
func AssertHonestNodesCommitted(t *testing.T, commits []CommittedBlockInfo, byzantineNode types.NodeID, description string) {
	t.Helper()

	// Filter to only honest node commits
	var honestCommits []CommittedBlockInfo
	for _, c := range commits {
		if c.NodeID != byzantineNode {
			honestCommits = append(honestCommits, c)
		}
	}

	require.Greater(t, len(honestCommits), 0,
		"LIVENESS VIOLATION [%s]: No commits from honest nodes", description)

	AssertCommitAgreement(t, honestCommits)

	t.Logf("Honest node commits verified [%s]: %d commits from honest nodes, all in agreement",
		description, len(honestCommits))
}
