package integration

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"btc-gateway/pkg/consensus/crypto"
	"btc-gateway/pkg/consensus/engine"
	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/mocks"
	"btc-gateway/pkg/consensus/network"
	"btc-gateway/pkg/consensus/storage"
	"btc-gateway/pkg/consensus/types"
)

// NodeConfig contains the essential configuration for creating a consensus node
type NodeConfig struct {
	NodeID       types.NodeID
	Validators   []types.NodeID
	Config       *types.ConsensusConfig
	EventTracer  events.EventTracer
	GenesisBlock *types.Block // optional - will create default if nil
}

// DefaultNodeConfig creates a node configuration with sensible defaults
func DefaultNodeConfig(nodeID types.NodeID, validators []types.NodeID, consensusConfig *types.ConsensusConfig) *NodeConfig {
	return &NodeConfig{
		NodeID:       nodeID,
		Validators:   validators,
		Config:       consensusConfig,
		EventTracer:  &events.NoOpEventTracer{},
		GenesisBlock: nil, // Will create default genesis block
	}
}

// Node represents a complete consensus node with all infrastructure components
type Node struct {
	// Configuration
	nodeID      types.NodeID
	validators  []types.NodeID
	config      *types.ConsensusConfig
	eventTracer events.EventTracer
	
	// Core consensus
	coordinator *HotStuffCoordinator
	consensus   *engine.HotStuffConsensus
	
	// Infrastructure components (injected via constructor)
	network network.NetworkInterface
	storage storage.StorageInterface
	crypto  crypto.CryptoInterface
	
	// State management
	mu      sync.RWMutex
	started bool
	
	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// NewNode creates a new consensus node with injected infrastructure interfaces
func NewNode(
	config *NodeConfig,
	network network.NetworkInterface,
	storage storage.StorageInterface,
	crypto crypto.CryptoInterface,
) (*Node, error) {
	if config == nil {
		return nil, fmt.Errorf("node configuration cannot be nil")
	}
	
	if err := validateNodeConfig(config); err != nil {
		return nil, fmt.Errorf("invalid node configuration: %w", err)
	}
	
	if network == nil {
		return nil, fmt.Errorf("network interface cannot be nil")
	}
	
	if storage == nil {
		return nil, fmt.Errorf("storage interface cannot be nil")
	}
	
	if crypto == nil {
		return nil, fmt.Errorf("crypto interface cannot be nil")
	}
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create genesis block if not provided
	genesisBlock := config.GenesisBlock
	if genesisBlock == nil {
		genesisBlock = types.NewGenesisBlock()
	}
	
	// Create consensus engine
	consensus, err := engine.NewHotStuffConsensusWithGenesis(
		config.NodeID,
		config.Config,
		genesisBlock,
		config.EventTracer,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create consensus engine: %w", err)
	}
	
	// Create logger for this node
	logger := zerolog.Nop().With().
		Uint16("node_id", uint16(config.NodeID)).
		Str("component", "coordinator").
		Logger()

	// Create coordinator
	coordinator := NewHotStuffCoordinator(
		config.NodeID,
		config.Validators,
		config.Config,
		consensus,
		network,
		storage,
		crypto,
		config.EventTracer,
		logger,
	)
	
	return &Node{
		nodeID:      config.NodeID,
		validators:  config.Validators,
		config:      config.Config,
		eventTracer: config.EventTracer,
		consensus:   consensus,
		coordinator: coordinator,
		network:     network,
		storage:     storage,
		crypto:      crypto,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}


// Start initializes and starts the consensus node
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.started {
		return fmt.Errorf("node already started")
	}
	
	// Start coordinator
	if err := n.coordinator.Start(); err != nil {
		return fmt.Errorf("failed to start coordinator: %w", err)
	}
	
	n.started = true
	return nil
}

// Stop gracefully shuts down the consensus node
func (n *Node) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if !n.started {
		return nil // Already stopped
	}
	
	// Cancel context to signal shutdown
	n.cancel()
	
	// Stop coordinator
	if err := n.coordinator.Stop(); err != nil {
		return fmt.Errorf("failed to stop coordinator: %w", err)
	}
	
	// Stop infrastructure components
	if mockNetwork, ok := n.network.(*mocks.MockNetwork); ok {
		mockNetwork.Stop()
	}
	
	n.started = false
	return nil
}

// GetCoordinator returns the HotStuff coordinator for this node
func (n *Node) GetCoordinator() *HotStuffCoordinator {
	return n.coordinator
}

// GetConsensus returns the consensus engine for this node
func (n *Node) GetConsensus() *engine.HotStuffConsensus {
	return n.consensus
}

// GetNetwork returns the network interface for this node
func (n *Node) GetNetwork() network.NetworkInterface {
	return n.network
}

// GetStorage returns the storage interface for this node
func (n *Node) GetStorage() storage.StorageInterface {
	return n.storage
}

// GetCrypto returns the crypto interface for this node
func (n *Node) GetCrypto() crypto.CryptoInterface {
	return n.crypto
}

// GetNodeID returns this node's identifier
func (n *Node) GetNodeID() types.NodeID {
	return n.nodeID
}

// IsStarted returns true if the node has been started
func (n *Node) IsStarted() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.started
}

// ProposeBlock proposes a new block to the consensus network
func (n *Node) ProposeBlock(payload []byte) error {
	if !n.IsStarted() {
		return fmt.Errorf("node not started")
	}
	
	return n.coordinator.ProposeBlock(payload)
}

// validateNodeConfig checks if the node configuration is valid
func validateNodeConfig(c *NodeConfig) error {
	if c.Config == nil {
		return fmt.Errorf("consensus configuration cannot be nil")
	}
	
	if !c.Config.IsValidNodeID(c.NodeID) {
		return fmt.Errorf("invalid node ID: %d", c.NodeID)
	}
	
	if len(c.Validators) == 0 {
		return fmt.Errorf("validator list cannot be empty")
	}
	
	if c.EventTracer == nil {
		return fmt.Errorf("event tracer cannot be nil")
	}
	
	return nil
}

// CreateTestNetwork is a convenience function that creates a complete test network with mock infrastructure
func CreateTestNetwork(nodeCount int, tracer events.EventTracer, enableMessageLogging bool) (map[types.NodeID]*Node, *NetworkManager, error) {
	// Create consensus config
	publicKeys := make([]types.PublicKey, nodeCount)
	for i := 0; i < nodeCount; i++ {
		publicKeys[i] = []byte(fmt.Sprintf("public-key-node-%d", i))
	}
	
	consensusConfig, err := types.NewConsensusConfig(publicKeys)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create consensus config: %w", err)
	}
	
	// Create validators list
	validators := make([]types.NodeID, nodeCount)
	for i := 0; i < nodeCount; i++ {
		validators[i] = types.NodeID(i)
	}
	
	// Create shared deterministic genesis block
	genesisBlock := types.NewGenesisBlock()
	
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
	nodes := make(map[types.NodeID]*Node)
	for _, nodeID := range validators {
		config := &NodeConfig{
			NodeID:       nodeID,
			Validators:   validators,
			Config:       consensusConfig,
			EventTracer:  tracer,
			GenesisBlock: genesisBlock,
		}
		
		node, err := NewNode(config, mockNetworks[nodeID], mockStorages[nodeID], mockCryptos[nodeID])
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create node %d: %w", nodeID, err)
		}
		
		nodes[nodeID] = node
	}
	
	// Start all nodes
	for nodeID, node := range nodes {
		if err := node.Start(); err != nil {
			return nil, nil, fmt.Errorf("failed to start node %d: %w", nodeID, err)
		}
	}
	
	// Create network manager for message processing
	networkManager := NewNetworkManager()
	networkManager.EnableLogging(enableMessageLogging)
	
	for _, node := range nodes {
		networkManager.AddNode(node)
	}
	
	// Start message processing
	networkManager.StartAll()
	
	return nodes, networkManager, nil
}