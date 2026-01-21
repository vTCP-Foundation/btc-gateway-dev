package mocks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"btc-gateway/pkg/consensus/types"
)

// MockConsensusNode represents a single consensus node in the test environment.
type MockConsensusNode struct {
	NodeID   types.NodeID
	Network  *MockNetwork
	Storage  *MockStorage
	Crypto   *MockCrypto
	Config   *types.ConsensusConfig
	
	// Runtime state
	IsRunning    bool
	IsByzantine  bool
	StartTime    time.Time
	mu           sync.RWMutex
}

// NewMockConsensusNode creates a new mock consensus node.
func NewMockConsensusNode(nodeID types.NodeID, config *types.ConsensusConfig, mockConfig MockConfig, failureConfig FailureConfig) *MockConsensusNode {
	node := &MockConsensusNode{
		NodeID:      nodeID,
		Config:      config,
		IsRunning:   false,
		IsByzantine: false,
		StartTime:   time.Now(),
	}
	
	// Create mock components
	node.Network = NewMockNetwork(nodeID, mockConfig.Network, failureConfig.Network)
	node.Storage = NewMockStorage(mockConfig.Storage, failureConfig.Storage)
	node.Crypto = NewMockCrypto(nodeID, mockConfig.Crypto, failureConfig.Crypto)
	
	// Add all nodes to crypto component
	for nodeID := range config.PublicKeys {
		node.Crypto.AddNode(nodeID)
	}
	
	return node
}

// Start begins operation of the consensus node.
func (mcn *MockConsensusNode) Start() {
	mcn.mu.Lock()
	defer mcn.mu.Unlock()
	
	mcn.IsRunning = true
	mcn.StartTime = time.Now()
}

// Stop stops operation of the consensus node.
func (mcn *MockConsensusNode) Stop() {
	mcn.mu.Lock()
	defer mcn.mu.Unlock()
	
	mcn.IsRunning = false
	mcn.Network.Stop()
}

// SetByzantine configures whether this node should exhibit Byzantine behavior.
func (mcn *MockConsensusNode) SetByzantine(byzantine bool) {
	mcn.mu.Lock()
	defer mcn.mu.Unlock()
	
	mcn.IsByzantine = byzantine
	
	// Update failure configurations for Byzantine behavior
	if byzantine {
		failures := DefaultCryptoFailureConfig()
		failures.InvalidSignatureRate = 0.3
		failures.VerificationRejectRate = 0.2
		mcn.Crypto.UpdateFailures(failures)
		
		networkFailures := DefaultNetworkFailureConfig()
		networkFailures.FailingSendRate = 0.1
		mcn.Network.UpdateFailures(networkFailures)
	} else {
		mcn.Crypto.UpdateFailures(DefaultCryptoFailureConfig())
		mcn.Network.UpdateFailures(DefaultNetworkFailureConfig())
	}
}

// GetStats returns runtime statistics for this node.
func (mcn *MockConsensusNode) GetStats() NodeStats {
	mcn.mu.RLock()
	defer mcn.mu.RUnlock()
	
	return NodeStats{
		NodeID:      mcn.NodeID,
		IsRunning:   mcn.IsRunning,
		IsByzantine: mcn.IsByzantine,
		UpTime:      time.Since(mcn.StartTime),
		Network:     mcn.Network.GetStats(),
		Storage:     mcn.Storage.GetStats(),
		Crypto:      mcn.Crypto.GetStats(),
	}
}

// NodeStats contains comprehensive statistics for a mock consensus node.
type NodeStats struct {
	NodeID      types.NodeID
	IsRunning   bool
	IsByzantine bool
	UpTime      time.Duration
	Network     NetworkStats
	Storage     StorageStats
	Crypto      CryptoStats
}

// TestEnvironment orchestrates multiple consensus nodes for testing.
type TestEnvironment struct {
	nodes           map[types.NodeID]*MockConsensusNode
	config          *types.ConsensusConfig
	scenario        TestScenarioConfig
	simulationTime  time.Time
	realStartTime   time.Time
	partitionActive bool
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewTestEnvironment creates a new test environment with the specified configuration.
func NewTestEnvironment(scenario TestScenarioConfig) *TestEnvironment {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create consensus configuration with mock public keys
	publicKeys := make([]types.PublicKey, scenario.NodeCount)
	for i := 0; i < scenario.NodeCount; i++ {
		// Generate deterministic mock public key
		key := make([]byte, 32)
		for j := range key {
			key[j] = byte(i*13 + j*7) // Simple deterministic pattern
		}
		publicKeys[i] = key
	}
	
	consensusConfig, err := types.NewConsensusConfig(publicKeys)
	if err != nil {
		panic(fmt.Sprintf("failed to create consensus config: %v", err))
	}
	
	env := &TestEnvironment{
		nodes:           make(map[types.NodeID]*MockConsensusNode),
		config:          consensusConfig,
		scenario:        scenario,
		simulationTime:  time.Now(),
		realStartTime:   time.Now(),
		partitionActive: false,
		ctx:             ctx,
		cancel:          cancel,
	}
	
	return env
}

// CreateNodes creates the specified number of consensus nodes.
func (te *TestEnvironment) CreateNodes() error {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	// Create nodes
	for i := 0; i < te.scenario.NodeCount; i++ {
		nodeID := types.NodeID(i)
		node := NewMockConsensusNode(nodeID, te.config, te.scenario.MockConfig, te.scenario.FailureConfig)
		te.nodes[nodeID] = node
	}
	
	return nil
}

// ConnectNodes establishes network connections between all nodes.
func (te *TestEnvironment) ConnectNodes() error {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	// Build peer map for each node
	for _, node := range te.nodes {
		peers := make(map[types.NodeID]*MockNetwork)
		for _, peer := range te.nodes {
			if peer.NodeID != node.NodeID {
				peers[peer.NodeID] = peer.Network
			}
		}
		node.Network.SetPeers(peers)
	}
	
	return nil
}

// StartAllNodes starts all consensus nodes in the environment.
func (te *TestEnvironment) StartAllNodes() {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	te.realStartTime = time.Now()
	te.simulationTime = time.Now()
	
	for _, node := range te.nodes {
		node.Start()
	}
}

// StopAllNodes stops all consensus nodes in the environment.
func (te *TestEnvironment) StopAllNodes() {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	for _, node := range te.nodes {
		node.Stop()
	}
	
	te.cancel()
}

// SimulatePartition applies a network partition scenario.
func (te *TestEnvironment) SimulatePartition(partitionedNodes []types.NodeID) error {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	te.partitionActive = true
	
	for _, nodeID := range partitionedNodes {
		if node, exists := te.nodes[nodeID]; exists {
			node.Network.SetPartitioned(true)
		}
	}
	
	return nil
}

// EndPartition ends the current network partition.
func (te *TestEnvironment) EndPartition() {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	te.partitionActive = false
	
	for _, node := range te.nodes {
		node.Network.SetPartitioned(false)
	}
}

// SetByzantineNodes configures which nodes should exhibit Byzantine behavior.
func (te *TestEnvironment) SetByzantineNodes(byzantineNodes []types.NodeID) {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	// Reset all nodes to honest behavior first
	for _, node := range te.nodes {
		node.SetByzantine(false)
	}
	
	// Set specified nodes to Byzantine
	for _, nodeID := range byzantineNodes {
		if node, exists := te.nodes[nodeID]; exists {
			node.SetByzantine(true)
		}
	}
}

// InjectFailures applies failure configurations to the environment.
func (te *TestEnvironment) InjectFailures(failures FailureConfig) {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	for _, node := range te.nodes {
		node.Network.UpdateFailures(failures.Network)
		node.Storage.UpdateFailures(failures.Storage)
		node.Crypto.UpdateFailures(failures.Crypto)
	}
}

// StepTime advances the simulation time (mainly for logging/metrics).
func (te *TestEnvironment) StepTime(duration time.Duration) {
	te.mu.Lock()
	defer te.mu.Unlock()
	
	te.simulationTime = te.simulationTime.Add(duration)
}

// RunScenario executes the configured test scenario.
func (te *TestEnvironment) RunScenario() error {
	// Create and connect nodes
	if err := te.CreateNodes(); err != nil {
		return fmt.Errorf("failed to create nodes: %w", err)
	}
	
	if err := te.ConnectNodes(); err != nil {
		return fmt.Errorf("failed to connect nodes: %w", err)
	}
	
	// Set Byzantine nodes
	te.SetByzantineNodes(te.scenario.ByzantineNodes)
	
	// Start all nodes
	te.StartAllNodes()
	
	// Apply network partitions according to schedule
	go te.executePartitionSchedule()
	
	// Run for configured duration
	select {
	case <-time.After(te.scenario.Duration):
		// Scenario completed
	case <-te.ctx.Done():
		// Scenario cancelled
	}
	
	// Stop all nodes
	te.StopAllNodes()
	
	return nil
}

// GetEnvironmentStats returns comprehensive statistics for all nodes.
func (te *TestEnvironment) GetEnvironmentStats() EnvironmentStats {
	te.mu.RLock()
	defer te.mu.RUnlock()
	
	stats := EnvironmentStats{
		NodeCount:       len(te.nodes),
		RunningNodes:    0,
		ByzantineNodes:  0,
		PartitionActive: te.partitionActive,
		RunTime:         time.Since(te.realStartTime),
		Nodes:           make(map[types.NodeID]NodeStats),
	}
	
	for nodeID, node := range te.nodes {
		nodeStats := node.GetStats()
		stats.Nodes[nodeID] = nodeStats
		
		if nodeStats.IsRunning {
			stats.RunningNodes++
		}
		if nodeStats.IsByzantine {
			stats.ByzantineNodes++
		}
	}
	
	return stats
}

// EnvironmentStats contains comprehensive statistics for the test environment.
type EnvironmentStats struct {
	NodeCount       int
	RunningNodes    int
	ByzantineNodes  int
	PartitionActive bool
	RunTime         time.Duration
	Nodes           map[types.NodeID]NodeStats
}

// executePartitionSchedule handles scheduled network partitions.
func (te *TestEnvironment) executePartitionSchedule() {
	for _, partition := range te.scenario.NetworkPartitions {
		// Wait for partition start time
		select {
		case <-time.After(partition.StartTime):
			// Apply partition
			te.SimulatePartition(partition.PartitionedNodes)
			
			// Wait for partition duration
			select {
			case <-time.After(partition.Duration):
				// End partition
				te.EndPartition()
			case <-te.ctx.Done():
				return
			}
		case <-te.ctx.Done():
			return
		}
	}
}

// Cleanup performs cleanup operations for the test environment.
func (te *TestEnvironment) Cleanup() {
	te.StopAllNodes()
	
	te.mu.Lock()
	defer te.mu.Unlock()
	
	// Clear all mock components
	for _, node := range te.nodes {
		node.Storage.Clear()
		node.Crypto.Clear()
	}
}