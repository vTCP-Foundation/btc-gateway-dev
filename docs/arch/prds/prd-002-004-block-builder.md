# PRD: Block Builder

**ID**: PRD-002-004
**Scope**: 002-mempool-architecture

> **NO CODE**: PRDs describe WHAT to build, not HOW. Use prose. Code snippets only for complex data structures (3-5 lines max). Let developer agent write implementation.

## Implements

- Features: `scopes/002-mempool-architecture/feature-3-block-builder.md`
- ADRs: `003-adr-block-builder.md`, `012-adr-stack-consensus.md`
- C4 Components: BlockBuildService, LeadershipHandler, ConsensusProposer, NodeProposer

## Implementation Target

Implement block builder that drains mempool on leadership gain and proposes blocks via HotStuff consensus. Support empty blocks for liveness. Integrate with consensus view change events.

## Technical Context

Uses HotStuff consensus (ADR-012) via ConsensusProposer port (implemented by NodeProposer adapter). Block building triggered by view change when node becomes leader. Maximum block size 128MB excluding signatures (ADR-003). Propose empty blocks when mempool is empty to maintain liveness.

**Dependency**: Requires PRD-002-002 (Mempool) for Pool.Drain().

## Data Model

- **Block Payload**: msgpack-serialized TransactionBatch
- **Max Block Size**: 128 MB (134,217,728 bytes) excluding signatures

## Implementation Steps

### Step 1: Create ConsensusProposer Interface

**Files**: `internal/builder/proposer.go` (create)

**Logic**: Define ConsensusProposer interface with ProposeBlock(batch) method. Batch is TransactionBatch. This port abstracts consensus block proposal, allowing BlockBuildService to remain infrastructure-agnostic.

**Edge Cases**:
- nil batch: return error

### Step 2: Implement NodeProposer

**Files**: `internal/builder/proposer.go` (continue)

**Logic**: NodeProposer implements ConsensusProposer by wrapping HotStuff Node. ProposeBlock serializes the batch with msgpack and calls node.ProposeBlock(payload). Adapter handles serialization.

**Edge Cases**:
- Node nil: return error from constructor
- Serialization fails: return error

### Step 3: Create BlockBuildService

**Files**: `internal/builder/builder.go` (create)

**Logic**: Service takes TxPool and ConsensusProposer as dependencies. BuildBlock() method: call pool.Drain(MaxBlockSize), create TransactionBatch from returned TXs, call proposer.ProposeBlock(batch). If pool is empty, create batch with zero TXs (empty block).

**Edge Cases**:
- Empty mempool: create empty TransactionBatch, propose anyway
- Drain returns partial: include what we got, don't wait for more

### Step 4: Create LeadershipHandler

**Files**: `internal/builder/leadership.go` (create)

**Logic**: Handler subscribes to consensus events. On receiving view change event where this node is the new leader, call blockBuildService.BuildBlock(). Use existing EventTracer or add leadership callback to HotStuffCoordinator.

**Edge Cases**:
- Rapid view changes: ensure we don't build multiple blocks per view
- Leadership lost mid-build: let current build complete, next view will correct

### Step 5: Add Leadership Detection to Consensus

**Files**: `pkg/consensus/integration/hotstuff_coordinator.go` (modify)

**Logic**: Add OnLeadershipGain callback registration. When view changes and this node is elected leader (round-robin based on view number), invoke registered callbacks. Callback receives view number.

**Edge Cases**:
- No callbacks registered: proceed normally
- Callback panics: recover, log error, continue consensus

### Step 6: Wire BlockBuildService in Node Startup

**Files**: `cmd/kvnode/main.go` (modify)

**Logic**: After consensus Node and TxPool are created, create NodeProposer, BlockBuildService, and LeadershipHandler. Register LeadershipHandler with consensus. Start handler before consensus.Start().

**Edge Cases**:
- Pool not ready: builder will get empty drain, propose empty block

### Step 7: Remove Committed TXs from Mempool

**Files**: `internal/statemachine/executor.go` (modify)

**Logic**: After ApplyBlock succeeds, extract TX hashes from the block's TransactionBatch and call pool.Remove(hashes). This clears committed TXs from mempool.

**Edge Cases**:
- TX not in mempool: Remove should handle gracefully (idempotent)
- Block from other leader: still remove TXs (they're committed network-wide)

### Step 8: Add Block Size Configuration

**Files**: `internal/builder/config.go` (create)

**Logic**: Define Config struct with MaxBlockSizeMB field. Default to 128 MB. NewBlockBuildService(config, pool, proposer) uses config.MaxBlockSizeMB * 1024 * 1024 as byte limit for Drain().

**Edge Cases**:
- Zero or negative MaxBlockSizeMB: use default
- Very large value: cap at reasonable limit (256 MB)

### Step 9: Handle Leadership Loss

**Files**: `internal/builder/leadership.go` (modify)

**Logic**: Track current view number. If leadership callback fires with same view number twice, ignore second call. If view number is lower than last seen, ignore (stale event). Reset state on view change.

**Edge Cases**:
- Concurrent view changes: only process highest view number seen

## Interfaces

### Inputs
- Leadership event: from HotStuffCoordinator with view number
- `Pool.Drain(maxBytes)`: returns `[]*Transaction` up to size limit
- Config: MaxBlockSizeMB (default 128)

### Outputs
- `ConsensusProposer.ProposeBlock(batch)`: called with TransactionBatch
- Block contains 0 to N transactions depending on mempool state

### Error States
- `ErrNotLeader`: ProposeBlock called when not leader -> logged, no-op
- `ErrConsensusUnavailable`: consensus not running -> logged, retry next view
- Drain errors: log and propose empty block

## Acceptance Criteria

### Functional
- [ ] AC-001: Block is proposed when node gains leadership
- [ ] AC-002: Block contains TXs from mempool (up to 128MB limit)
- [ ] AC-003: Empty block is proposed when mempool is empty
- [ ] AC-004: Committed TXs are removed from mempool after block execution
- [ ] AC-005: Block building respects configured size limit
- [ ] AC-006: No duplicate blocks proposed for same view

### Non-Functional
- [ ] AC-NFR-001: Block building latency <100ms from leadership gain to propose
- [ ] AC-NFR-002: Block size respects 128MB limit (excluding signatures)
- [ ] AC-NFR-003: Empty blocks maintain consensus liveness

## Edge Cases & Error Handling

- **Large mempool** (must): Drain respects size limit; excess TXs stay for next block
- **Rapid leadership changes** (must): View number tracking prevents duplicate blocks
- **Concurrent block from other leader** (must): Both blocks may be proposed; consensus resolves via voting
- **Mempool modified during drain** (should): Mutex in Pool prevents race; drain gets consistent snapshot
- **Node restart as leader** (should): Empty mempool, propose empty block, receive gossip, future blocks have TXs

## Out of Scope

- TX ordering by fee priority
- Block gas/compute limits
- Parallel block building
- Speculative execution before commit
- Uncle/ommer blocks

## Testing Guidance

- **Unit**: Test BlockBuildService.BuildBlock() with mocked Pool and Node. Test LeadershipHandler event processing with mocked BlockBuildService. Test view number dedup logic.
- **Integration**: Multi-node test: submit TXs, observe blocks contain expected TXs. Test empty block proposal on empty mempool. Test 128MB limit with large TXs.
- **Manual**: Monitor block sizes and build latency under load. Verify liveness with empty mempool.
