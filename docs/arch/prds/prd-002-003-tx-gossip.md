# PRD: TX Gossip

**ID**: PRD-002-003
**Scope**: 002-mempool-architecture

> **NO CODE**: PRDs describe WHAT to build, not HOW. Use prose. Code snippets only for complex data structures (3-5 lines max). Let developer agent write implementation.

## Implements

- Features: `scopes/002-mempool-architecture/feature-2-tx-gossip.md`
- ADRs: `002-adr-tx-gossip-protocol.md`, `011-adr-stack-networking.md`
- C4 Components: TxBroadcaster, GossipSubBroadcaster, GossipHandler

## Implementation Target

Implement TX broadcast via libp2p GossipSub topic. Create TxBroadcaster interface with GossipSub implementation. Build GossipHandler to receive and process incoming gossiped TXs.

## Technical Context

Uses existing libp2p GossipSub (ADR-011) with dedicated topic `/btc-gateway/tx/1.0.0`. Serialization via msgpack for compact binary format (ADR-002). Dedup via mempool Contains() check before relay to prevent broadcast loops.

**Dependency**: Requires PRD-002-002 (Mempool and TX Validation) for TxSubmitService and Pool.

## Data Model

- **Gossip Message**
  - Topic: `/btc-gateway/tx/1.0.0`
  - Payload: msgpack-serialized Transaction

## Implementation Steps

### Step 1: Create TxBroadcaster Interface

**Files**: `internal/gossip/broadcaster.go` (create)

**Logic**: Define interface with Broadcast(tx) method. Fire-and-forget semantics; errors logged but not returned (best-effort broadcast).

**Edge Cases**:
- nil TX: log warning, return early

### Step 2: Implement GossipSubBroadcaster

**Files**: `internal/gossip/gossipsub.go` (create)

**Logic**: Implementation wraps libp2p pubsub.Topic. Constructor takes network.Manager and creates/joins topic `/btc-gateway/tx/1.0.0`. Broadcast serializes TX with msgpack and calls topic.Publish().

**Edge Cases**:
- Topic join fails: return error from constructor
- Publish fails: log error, don't propagate (best-effort)
- No peers connected: publish succeeds but goes nowhere; GossipSub handles this

### Step 3: Create GossipHandler

**Files**: `internal/gossip/handler.go` (create)

**Logic**: Handler subscribes to TX topic and processes incoming messages. For each message: deserialize TX from msgpack, check pool.Contains(tx.Hash()), if not present call TxSubmitService.Submit(tx, "gossip"). Runs in background goroutine.

**Edge Cases**:
- Malformed message: log warning, skip message
- Already in mempool: skip silently (dedup)
- Validation fails: log at debug level, skip (peer sent invalid TX)

### Step 4: Integrate Broadcaster with TxSubmitService

**Files**: `internal/service/tx_submit.go` (modify)

**Logic**: Add TxBroadcaster as dependency to TxSubmitService. After successful pool.Add(), call broadcaster.Broadcast(tx) only if source is "rest" (not re-broadcast received gossip).

**Edge Cases**:
- Broadcast after pool add: ensures only valid TXs are gossiped
- Source tracking: Submit(tx, source) uses source string ("rest" or "gossip") to distinguish origin

### Step 5: Wire GossipSub in Node Startup

**Files**: `cmd/kvnode/main.go` (modify)

**Logic**: After network.Manager is created, create GossipSubBroadcaster and GossipHandler. Inject broadcaster into TxSubmitService. Start GossipHandler before accepting API requests.

**Edge Cases**:
- GossipSub initialization fails: fatal error, node can't function

### Step 6: Add Shutdown Handling

**Files**: `internal/gossip/handler.go` (modify), `internal/gossip/gossipsub.go` (modify)

**Logic**: Add Stop() method to GossipHandler that cancels subscription and closes goroutine. Add Close() method to GossipSubBroadcaster that leaves topic. Call both on node shutdown.

**Edge Cases**:
- Shutdown during message processing: context cancellation, graceful exit

## Interfaces

### Inputs
- `Transaction` (struct): from TxSubmitService for broadcast, from GossipSub for received messages
- `pubsub.Subscription`: from libp2p for incoming messages
- `source` (string): "rest" or "gossip" in Submit() to track origin and prevent re-broadcast

### Outputs
- Published messages: msgpack-serialized TX on topic `/btc-gateway/tx/1.0.0`
- Submitted TXs: gossiped TXs passed to TxSubmitService

### Error States
- `ErrTopicJoinFailed`: can't join GossipSub topic -> fatal at startup
- Deserialization errors: logged, message skipped
- Validation errors on received TX: logged at debug, TX discarded

## Acceptance Criteria

### Functional
- [ ] AC-001: Local TX is broadcast to peers after mempool admission
- [ ] AC-002: TX received from gossip is added to local mempool (if valid)
- [ ] AC-003: Duplicate TX from gossip is silently ignored (no re-broadcast)
- [ ] AC-004: Invalid TX from gossip is rejected and logged
- [ ] AC-005: Gossip uses dedicated topic `/btc-gateway/tx/1.0.0`
- [ ] AC-006: Node gracefully shuts down gossip handler

### Non-Functional
- [ ] AC-NFR-001: TX propagates to all connected peers within 2 seconds
- [ ] AC-NFR-002: No duplicate broadcasts for same TX
- [ ] AC-NFR-003: Gossip handler processes incoming TXs without blocking

## Edge Cases & Error Handling

- **Peer disconnection** (must): GossipSub handles automatically via mesh management
- **Message flood** (should): libp2p has built-in rate limiting; additional limits deferred
- **Malformed messages** (must): Deserialize error logged, message dropped
- **Re-broadcast loop** (must): Prevented by mempool Contains() check and source tracking
- **All peers down** (should): TXs stay in local mempool; will be gossiped when peers reconnect

## Out of Scope

- Rate limiting on incoming gossip (deferred)
- TX batching in gossip messages
- Gossip protocol customization (mesh size, heartbeat)
- Priority lanes for gossip

## Testing Guidance

- **Unit**: Test GossipSubBroadcaster with mocked pubsub.Topic. Test GossipHandler message processing with mocked TxSubmitService. Test source parameter behavior.
- **Integration**: Multi-node test: submit TX on node A, verify appears in node B's mempool. Test dedup across network.
- **Manual**: Monitor gossip topic traffic with libp2p debug tools. Verify no broadcast storms.
