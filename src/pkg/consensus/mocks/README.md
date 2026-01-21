# Mock Infrastructure Components

This package provides mock implementations of the consensus interfaces.
These mocks enable comprehensive testing of the HotStuff consensus protocol without requiring real network, storage, or cryptographic infrastructure.

## Overview

The mock infrastructure consists of three main components:

1. **MockNetwork** - Simulates P2P network communication with configurable behavior
2. **MockStorage** - Provides in-memory persistence with failure simulation
3. **MockCrypto** - Implements deterministic cryptographic operations for testing

Additionally, the package provides:

4. **TestEnvironment** - Orchestrates multi-node test scenarios
5. **Configuration System** - Manages mock behavior and failure injection
6. **Predefined Scenarios** - Common test configurations for different scenarios

## Components

### MockNetwork

Simulates network communication between consensus nodes using Go channels.

**Key Features:**
- Configurable message delays and packet loss
- Network partition simulation
- Message duplication and corruption
- Failure injection (send/broadcast failures)
- Thread-safe operations with proper synchronization

**Usage Example:**
```go
config := mocks.DefaultNetworkConfig()
config.BaseDelay = 10 * time.Millisecond
config.PacketLossRate = 0.05

failures := mocks.DefaultNetworkFailureConfig()
network := mocks.NewMockNetwork(nodeID, config, failures)

// Send message to specific node
err := network.Send(ctx, targetNodeID, message)

// Broadcast to all peers
err := network.Broadcast(ctx, message)

// Receive messages
select {
case msg := <-network.Receive():
    // Process received message
}
```

### MockStorage

Provides **in-memory** storage with configurable persistence delays and failure simulation.

**Key Features:**
- Block and QuorumCertificate storage
- View number persistence
- Configurable read/write delays
- Data corruption simulation
- Disk full simulation
- Operation statistics tracking

**Usage Example:**
```go
config := mocks.DefaultStorageConfig()
config.PersistenceDelay = 5 * time.Millisecond

failures := mocks.DefaultStorageFailureConfig()
failures.WriteFailureRate = 0.1

storage := mocks.NewMockStorage(config, failures)

// Store and retrieve blocks
err := storage.StoreBlock(block)
retrievedBlock, err := storage.GetBlock(blockHash)
```

### MockCrypto

Implements cryptographic operations with deterministic behavior for reproducible testing (no real cryptography is used).

**Key Features:**
- Deterministic signature generation
- Configurable signature verification patterns
- Real SHA-256 hashing for consistency
- Invalid signature simulation
- Multi-node key management
- Operation statistics tracking

**Usage Example:**
```go
config := mocks.DefaultCryptoConfig()
config.EnableDeterministicSignatures = true

failures := mocks.DefaultCryptoFailureConfig()
crypto := mocks.NewMockCrypto(nodeID, config, failures)

// Sign data
signature, err := crypto.Sign(data)

// Verify signature from another node
err := crypto.Verify(data, signature, signerNodeID)

// Compute hash
hash := crypto.Hash(data)
```

### TestEnvironment

Orchestrates multi-node consensus testing with comprehensive scenario management.

**Key Features:**
- Multi-node network creation and management
- Scheduled network partitions
- Byzantine behavior configuration
- Comprehensive statistics collection
- Automatic cleanup and resource management

**Usage Example:**
```go
scenario := mocks.HappyPathScenario()
scenario.NodeCount = 5
scenario.Duration = 30 * time.Second

env := mocks.NewTestEnvironment(scenario)

// Setup and run
err := env.CreateNodes()
err = env.ConnectNodes()
env.StartAllNodes()

// Run test scenario
err = env.RunScenario()

// Get results
stats := env.GetEnvironmentStats()
fmt.Printf("Consensus reached with %d nodes", stats.RunningNodes)

// Cleanup
env.Cleanup()
```

## Configuration System

### Mock Configuration

Each mock component has its own configuration structure:

- **NetworkConfig**: Message delays, packet loss, duplication rates
- **StorageConfig**: Persistence delays, durability simulation
- **CryptoConfig**: Signature length, deterministic behavior

### Failure Configuration

Failure injection is configured separately:

- **NetworkFailureConfig**: Send failures, partition scenarios
- **StorageFailureConfig**: Read/write failures, corruption rates
- **CryptoFailureConfig**: Signature failures, verification rejection

### Predefined Scenarios

The package includes several predefined test scenarios:

- **HappyPathScenario**: Normal consensus operation
- **NetworkPartitionScenario**: Network partition recovery testing
- **ByzantineLeaderScenario**: Byzantine leader behavior
- **HighLatencyScenario**: High network latency conditions
- **StorageFailureScenario**: Storage reliability testing
- **ChaosScenario**: Multiple failure modes combined

## Testing Patterns

### Unit Testing with Mocks

```go
func TestConsensusLogic(t *testing.T) {
    mockConfig := mocks.DefaultMockConfig()
    failures := mocks.DefaultFailureConfig()

    network := mocks.NewMockNetwork(0, mockConfig.Network, failures.Network)
    storage := mocks.NewMockStorage(mockConfig.Storage, failures.Storage)
    crypto := mocks.NewMockCrypto(0, mockConfig.Crypto, failures.Crypto)

    // Test consensus logic with mocks
    consensus := NewConsensusEngine(network, storage, crypto)

    // Verify behavior
    assert.NoError(t, consensus.ProcessMessage(testMessage))
}
```

### Integration Testing

```go
func TestMultiNodeConsensus(t *testing.T) {
    scenario := mocks.HappyPathScenario()
    env := mocks.NewTestEnvironment(scenario)

    require.NoError(t, env.CreateNodes())
    require.NoError(t, env.ConnectNodes())

    env.StartAllNodes()
    defer env.Cleanup()

    // Run consensus and verify results
    time.Sleep(scenario.Duration)

    stats := env.GetEnvironmentStats()
    assert.Equal(t, scenario.NodeCount, stats.RunningNodes)
}
```

### Failure Injection Testing

```go
func TestByzantineFaultTolerance(t *testing.T) {
    scenario := mocks.ByzantineLeaderScenario()
    env := mocks.NewTestEnvironment(scenario)

    require.NoError(t, env.CreateNodes())
    require.NoError(t, env.ConnectNodes())

    // Configure Byzantine behavior
    env.SetByzantineNodes([]types.NodeID{0})

    env.StartAllNodes()
    defer env.Cleanup()

    // Verify system handles Byzantine behavior
    err := env.RunScenario()
    assert.NoError(t, err)

    validation := scenario.GetValidationConfig()
    assert.True(t, validation.ExpectedConsensus)
}
```

## Statistics and Monitoring

All mock components provide detailed statistics:

```go
// Network statistics
networkStats := mockNetwork.GetStats()
fmt.Printf("Node %d: %d peers, queue %d/%d",
    networkStats.NodeID,
    networkStats.ConnectedPeers,
    networkStats.QueueSize,
    networkStats.QueueCapacity)

// Storage statistics
storageStats := mockStorage.GetStats()
fmt.Printf("Storage: %d blocks, %d QCs, %d reads, %d writes",
    storageStats.BlockCount,
    storageStats.QCCount,
    storageStats.ReadCount,
    storageStats.WriteCount)

// Crypto statistics
cryptoStats := mockCrypto.GetStats()
fmt.Printf("Crypto: %d signs, %d verifies, %d hashes",
    cryptoStats.SignCount,
    cryptoStats.VerifyCount,
    cryptoStats.HashCount)

// Environment statistics
envStats := env.GetEnvironmentStats()
fmt.Printf("Environment: %d/%d running, %d Byzantine, runtime: %v",
    envStats.RunningNodes,
    envStats.NodeCount,
    envStats.ByzantineNodes,
    envStats.RunTime)
```

## Best Practices

### Configuration

1. **Use Default Configurations**: Start with `DefaultMockConfig()` and `DefaultFailureConfig()` and modify as needed
2. **Gradual Failure Introduction**: Start with no failures and gradually increase failure rates
3. **Realistic Parameters**: Use realistic network delays and failure rates based on target deployment environment

### Testing

1. **Deterministic Testing**: Enable deterministic signatures for reproducible test results
2. **Proper Cleanup**: Always call `env.Cleanup()` or component-specific cleanup methods
3. **Statistics Validation**: Use statistics to verify expected behavior and catch edge cases
4. **Scenario Composition**: Combine simple scenarios to build complex test cases

### Debugging

1. **Enable Logging**: The mocks integrate with the existing logging system
2. **Statistics Monitoring**: Monitor component statistics to identify bottlenecks or unexpected behavior
3. **Step-by-Step Testing**: Test individual components before integration testing
4. **Failure Isolation**: Use single failure modes before testing combined failures

## Integration with Real Components

The mock implementations are designed to be drop-in replacements for real components:

```go
// Development/Testing
network := mocks.NewMockNetwork(nodeID, mockConfig, failures)
storage := mocks.NewMockStorage(storageConfig, failures)
crypto := mocks.NewMockCrypto(nodeID, cryptoConfig, failures)

// Production
network := p2p.NewNetwork(p2pConfig)
storage := postgres.NewStorage(dbConfig)
crypto := ed25519.NewCrypto(keyConfig)

// Same interface usage
consensus := NewConsensusEngine(network, storage, crypto)
```

## Performance Considerations

- **Memory Usage**: Mock components use in-memory storage; monitor memory usage in long-running tests
- **Goroutine Management**: Network mocks create goroutines for message delivery; ensure proper cleanup
- **Channel Buffering**: Network message queues are buffered; adjust buffer sizes for high-throughput tests
- **Statistics Overhead**: Statistics collection has minimal overhead but can be disabled if needed

## Demo Application

Run the comprehensive demo to see all components in action:

```bash
go run ./cmd/demo/hotstuff/mock-infrastructure-demo/
```

The demo demonstrates:
1. Basic network message passing
2. Storage operations (blocks, QCs, view persistence)
3. Cryptographic operations (sign, verify, hash)
4. Integrated test environment
5. Failure injection capabilities
