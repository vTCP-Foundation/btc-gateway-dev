# Event-Based Consensus Testing Framework

This package implements the event-based testing infrastructure described in [ADR-007](/workspace/architecture/btc-federation/adrs/ADR-007-event-based-consensus-testing.md) for comprehensive HotStuff consensus protocol validation.

## Overview

The event-based testing framework provides:

- **Production-safe event tracing** with zero overhead in production builds
- **Comprehensive rule validation** for protocol compliance checking
- **Byzantine behavior injection** using build tags for attack vector testing
- **Flow-by-flow testing** aligned with the consensus data flow diagram

## Architecture

### Core Components

1. **Events Package** (`pkg/consensus/events/`)
   - `EventTracer` interface for production-safe event collection
   - Event type definitions and payload structures
   - Core event types for all consensus protocol phases

2. **Testing Package** (`pkg/consensus/testing/`)
   - Validation rule engine for event sequence verification
   - Byzantine behavior injection system (build tag separated)
   - Test helper functions for network setup and execution

3. **Mock Integration** (`pkg/consensus/mocks/`)
   - Event tracer implementations (NoOp and testing)
   - Mock infrastructure extended with event tracing

## Usage

### Basic Event Tracing

```go
// Production: Zero overhead
tracer := &mocks.NoOpEventTracer{}

// Testing: Full event collection
tracer := mocks.NewConsensusEventTracer()

// Record events during consensus
tracer.RecordEvent(nodeID, events.EventProposalCreated, events.EventPayload{
    "view": view,
    "block_hash": blockHash,
})

// Get events for validation
allEvents := tracer.GetEvents()
proposalEvents := tracer.GetEventsByType(events.EventProposalCreated)
```

### Happy Path Testing

```go
func TestHappyPathConsensusFlow(t *testing.T) {
    tracer := mocks.NewConsensusEventTracer()
    
    // Create 5-node network with event tracing
    config := CreateTestConsensusConfig(5)
    nodes := CreateTestNetwork(config, tracer)
    
    // Execute consensus round
    payload := []byte("test_payload")
    err := ExecuteConsensusRound(nodes, payload)
    require.NoError(t, err)
    
    // Validate events follow protocol rules
    rules := GetHappyPathRules(5, 1)
    errors := ValidateEvents(tracer.GetEvents(), rules)
    require.Empty(t, errors)
}
```

### Byzantine Behavior Testing

```go
//go:build testing

func TestEquivocationDetection(t *testing.T) {
    tracer := mocks.NewConsensusEventTracer()
    nodes := CreateTestNetwork(CreateTestConsensusConfig(5), tracer)
    
    // Inject Byzantine behavior (only in testing builds)
    byzantineNode := NewByzantineNode(types.NodeID(1), tracer)
    byzantineNode.EnableByzantineBehavior(DoubleVoteMode)
    
    // Execute consensus round
    err := ExecuteConsensusRound(nodes, []byte("byzantine_test"))
    
    // Validate Byzantine detection
    rules := GetByzantineDetectionRules()
    errors := ValidateEvents(tracer.GetEvents(), rules)
    require.Empty(t, errors, "Byzantine detection validation failed")
}
```

### Custom Validation Rules

```go
// Create custom validation rule
rule := &MustHappenBeforeRule{
    EventA:      events.EventProposalReceived,
    EventB:      events.EventVoteSent,
    Description: "Must receive proposal before voting",
}

// Validate events against rule
errors := rule.Validate(events)
for _, err := range errors {
    t.Errorf("Validation error: %v", err)
}
```

## Event Types

### Proposal Events
- `EventProposalCreated` - Block proposal created by leader
- `EventProposalReceived` - Proposal received by validator
- `EventProposalValidated` - Proposal passed validation
- `EventProposalRejected` - Proposal failed validation

### Vote Events  
- `EventVoteCreated` - Vote created by validator
- `EventVoteSent` - Vote sent to leader
- `EventVoteReceived` - Vote received by leader
- `EventVoteValidated` - Vote passed validation

### QC Events
- `EventQCFormed` - Quorum certificate formed
- `EventQCReceived` - QC received by node
- `EventQCValidated` - QC passed validation

### View Events
- `EventViewTimeout` - View timeout occurred
- `EventViewChange` - View change initiated
- `EventNewViewStarted` - New view began

### Byzantine Events
- `EventByzantineDetected` - Byzantine behavior detected
- `EventEquivocationFound` - Equivocation (double voting) found
- `EventEvidenceStored` - Cryptographic evidence stored

### Block Events
- `EventBlockCommitted` - Block committed to chain
- `EventBlockAdded` - Block added to tree
- `EventBlockValidated` - Block passed validation

## Validation Rules

### Ordering Rules
- `MustHappenBeforeRule` - Event A must occur before Event B
- `CausalDependencyRule` - Events must have causal relationships

### Quorum Rules
- `QuorumRule` - Minimum number of nodes must participate
- `CrossNodeCoordinationRule` - Multi-node coordination validation

### Role-Based Rules  
- `LeaderOnlyRule` - Events only allowed on leader nodes
- `ValidatorOnlyRule` - Events only allowed on validator nodes

### Byzantine Rules
- `MustNotHappenRule` - Forbidden events with conditions
- `StateTransitionRule` - Valid state machine transitions

## Rule Sets

### Happy Path Rules (`GetHappyPathRules`)
- Proposal must be received before voting
- Quorum threshold must be met for votes
- Only leaders should create proposals
- QC formation must precede commitment
- No double voting allowed

### Byzantine Detection Rules (`GetByzantineDetectionRules`)
- Byzantine behavior must be detected within 10 seconds
- Evidence must be stored after detection
- View change should follow Byzantine detection
- Byzantine nodes shouldn't detect own misbehavior

### Partition Recovery Rules (`GetPartitionRecoveryRules`)
- Partition mode only on partition detection
- No voting during partition mode
- State sync required before resuming consensus

## Byzantine Behaviors

The framework supports testing 15+ Byzantine attack vectors:

### Vote-Based Attacks
- `DoubleVoteMode` - Vote for multiple blocks in same phase
- `InvalidVoteMode` - Send malformed votes
- `SelectiveVotingMode` - Only vote for specific proposals
- `VoteWithholdingMode` - Refuse to vote on valid proposals

### Proposal-Based Attacks
- `ConflictingProposalMode` - Send different proposals to different nodes
- `InvalidProposalMode` - Send malformed proposals
- `SelectiveProposalMode` - Send proposals to subset of nodes

### QC-Based Attacks
- `InvalidQCMode` - Create QCs with forged signatures
- `InsufficientStakeMode` - QC with insufficient voting power

### Network-Based Attacks
- `MessageWithholdingMode` - Selectively drop messages
- `MessageReplayMode` - Replay old messages
- `TimingAttackMode` - Manipulate message timing
- `FloodingAttackMode` - Overwhelm nodes with messages

## Build Tags

Byzantine behaviors are isolated using Go build tags:

```bash
# Production build (excludes Byzantine code)
go build ./...

# Testing build (includes Byzantine behaviors)
go build -tags testing ./...

# Run tests with Byzantine capabilities
go test -tags testing ./pkg/consensus/...
```

## Performance

- **Production overhead**: Zero (NoOpEventTracer optimized away)
- **Testing performance**: Minimal impact from event collection
- **Memory efficient**: Pre-allocated event slices, efficient rule validation
- **Thread-safe**: All tracers use appropriate synchronization

## Integration with Existing Code

The event-based testing framework integrates seamlessly with existing consensus code:

1. **Interface injection**: `EventTracer` interface added to consensus components
2. **Mock infrastructure**: Existing mocks extended with event tracing
3. **Zero production impact**: No-op implementation ensures no overhead
4. **Clean separation**: Byzantine code completely isolated with build tags

## Future Extensions

The framework is designed to be extensible:

- Additional event types can be easily added
- New validation rules follow simple interface pattern
- Byzantine behaviors can be extended with new attack vectors
- Rule sets can be composed for different testing scenarios

## Testing Best Practices

1. **Start with integration tests**: Test complete flows before unit components
2. **Use event validation**: Leverage rule-based validation for protocol compliance
3. **Test Byzantine scenarios**: Use build tags to test attack vectors
4. **Measure performance**: Validate latency and throughput requirements
5. **Compose rule sets**: Combine different rule sets for comprehensive testing

This framework enables comprehensive testing of the HotStuff consensus protocol with complete path coverage, Byzantine fault tolerance validation, and production safety.

## Demos

Interactive demonstrations of the event-based testing infrastructure are available in `cmd/demo/hotstuff/`:

### Event Tracer Demo
- **Location**: [`cmd/demo/hotstuff/event-tracer-demo/`](/cmd/demo/hotstuff/event-tracer-demo/)
- **Purpose**: Complete demonstration of the event-based testing infrastructure
- **Features**: 
  - Basic event tracing capabilities
  - Production vs testing tracer performance comparison
  - Mock infrastructure integration
  - Validation rules engine in action
  - Byzantine behavior detection
  - Happy path consensus flow validation
  - Performance impact analysis

Run the demo:
```bash
go run ./cmd/demo/hotstuff/event-tracer-demo/
```

This demo validates all Task 05-02 deliverables according to workspace/policy.md requirements and proves the infrastructure is ready for flow-by-flow consensus testing.