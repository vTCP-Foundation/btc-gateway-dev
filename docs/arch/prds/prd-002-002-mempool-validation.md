# PRD: Mempool and TX Validation

**ID**: PRD-002-002
**Scope**: 002-mempool-architecture

> **NO CODE**: PRDs describe WHAT to build, not HOW. Use prose. Code snippets only for complex data structures (3-5 lines max). Let developer agent write implementation.

## Implements

- Features: `scopes/002-mempool-architecture/feature-1-mempool.md`, `scopes/002-mempool-architecture/feature-4-tx-validation.md`
- ADRs: `001-adr-mempool-architecture.md`, `004-adr-tx-validation-pipeline.md`, `010-adr-stack-storage.md`
- C4 Components: Pool, Validator, TxSubmitService, TxHandler

## Implementation Target

Implement in-memory FIFO mempool. Create TX validation pipeline for signature verification, deduplication, and nonce checking. Build TxSubmitService to orchestrate validation and mempool admission.

## Technical Context

Mempool is in-memory FIFO (ADR-001). Validation order: signature → dedup → nonce (ADR-004). Dedup checks both mempool and CommittedTxStore port for committed TXs. Uses CryptoProvider from PRD-002-001 for signature verification. Uses AccountStore from PRD-002-001 for nonce checking.

**Dependency**: Requires PRD-002-001 (Account Model) to be implemented first.

## Data Model

- **Pool** (in-memory)
  - txs: ordered map of hash -> Transaction (FIFO)

- **TX Hash**: SHA-256 of serialized Transaction

## Implementation Steps

### Step 1: Create TxPool Interface

**Files**: `internal/mempool/pool.go` (create)

**Logic**: Define interface with Add(tx), Remove(hashes), Drain(maxBytes), Contains(hash), and Size() methods. Add returns error if TX already exists. Drain returns slice of TXs up to maxBytes limit, removes them from pool.

**Edge Cases**:
- Add duplicate: return ErrDuplicateTx

### Step 2: Implement FIFO Pool

**Files**: `internal/mempool/pool.go` (continue)

**Logic**: Implement Pool struct with sync.RWMutex for thread safety. Use a slice for FIFO ordering and a map for O(1) lookup. Add appends to slice and inserts to map. Remove deletes from both. Drain takes from front of slice.

**Edge Cases**:
- Concurrent Add calls: mutex prevents race
- Drain empty pool: return empty slice, no error

### Step 3: Create TxValidator Interface

**Files**: `internal/validation/validator.go` (create)

**Logic**: Define interface with Validate(tx) method returning error or nil. Error types indicate failure reason: ErrInvalidSignature, ErrDuplicateTx, ErrInvalidNonce.

**Edge Cases**:
- nil TX: return error immediately

### Step 4: Implement Validator

**Files**: `internal/validation/validator.go` (continue), `internal/validation/errors.go` (create)

**Logic**: Validator takes CryptoProvider, TxPool, AccountStore, and CommittedTxStore as dependencies. Validation order:

1. Verify signature using CryptoProvider.Verify(tx.Payload, tx.Signature, tx.SenderPubKey)
2. Check mempool Contains(tx.Hash())
3. Check CommittedTxStore.Contains(hash) for committed TX
4. Get sender account, compare tx.Nonce with account.Nonce. TX nonce must equal account nonce (next expected).

**Edge Cases**:
- Missing signature: return ErrInvalidSignature
- Missing sender: return ErrMissingSender
- Nonce too low (replay): return ErrNonceTooLow
- Nonce too high (gap): return ErrNonceTooHigh

### Step 5: Add Sender Field to Transaction

**Files**: `internal/statemachine/transaction.go` (modify)

**Logic**: Add Sender (address string), SenderPubKey ([]byte), and Signature ([]byte) fields to Transaction struct. Update Serialize/Deserialize to include these fields. Add Hash() method that computes SHA-256 of serialized TX (without signature field to prevent malleability).

**Edge Cases**:
- Legacy TXs without sender: handle gracefully during transition

### Step 6: Create TxSubmitService

**Files**: `internal/service/tx_submit.go` (create)

**Logic**: Service takes TxValidator and TxPool as dependencies. Submit(tx, source) method: call validator.Validate(tx), if passes call pool.Add(tx). Source indicates "rest" or "gossip" for metrics. Returns error from either step.

**Edge Cases**:
- Validation fails: return validation error, don't add to pool

### Step 7: Add TxHandler to API

**Files**: `internal/api/handler.go` (modify)

**Logic**: Modify existing handleTransaction or add POST /tx/submit endpoint. Parse Transaction from request body (JSON). Call TxSubmitService.Submit(tx, "rest"). Return success or error response with reason.

**Edge Cases**:
- Malformed JSON: return 400 Bad Request
- Validation failure: return 400 with specific error code

### Step 8: Record Committed TX Hashes

**Files**: `internal/statemachine/executor.go` (modify)

**Logic**: After applying each TX in ApplyBlock, call committedTxStore.Add(hash). Executor takes CommittedTxStore as dependency. This enables dedup lookup in validator via the CommittedTxStore port.

**Edge Cases**:
- Storage error: log but don't fail block application

## Interfaces

### Inputs
- `Transaction` (struct): from REST POST body - must include Sender, SenderPubKey, Signature, Nonce
- Validation dependencies: CryptoProvider, TxPool, AccountStore, CommittedTxStore

### Outputs
- `TxPool.Drain()`: returns `[]*Transaction` - consumed by BlockBuilder
- `TxPool.Contains()`: returns bool - consumed by GossipHandler for dedup
- HTTP response: success or error JSON - consumed by clients

### Error States
- `ErrInvalidSignature`: signature verification failed -> HTTP 400
- `ErrDuplicateTx`: TX already in mempool or committed -> HTTP 409 Conflict
- `ErrNonceTooLow`: replay attack or already processed -> HTTP 400
- `ErrNonceTooHigh`: nonce gap, missing earlier TX -> HTTP 400

## Acceptance Criteria

### Functional
- [ ] AC-001: Valid TX is added to mempool after validation
- [ ] AC-002: TX with invalid signature is rejected
- [ ] AC-003: Duplicate TX (same hash in mempool) is rejected
- [ ] AC-004: TX with hash already committed is rejected
- [ ] AC-005: TX with nonce != expected nonce is rejected
- [ ] AC-006: Pool.Drain returns TXs in FIFO order
- [ ] AC-007: Concurrent access to mempool is thread-safe

### Non-Functional
- [ ] AC-NFR-001: Validation completes in <10ms per TX (includes TiKV lookup)
- [ ] AC-NFR-002: Mempool operations (add/remove) complete in <1ms
- [ ] AC-NFR-003: No race conditions under concurrent access (verified by race detector)

## Edge Cases & Error Handling

- **TX submitted during block commit** (must): Mutex prevents race; TX validated against post-commit state
- **CommittedTxStore unavailable** (must): Fail validation, return 503
- **Nonce gap** (should): Reject with specific error; client should submit missing TX first
- **Rapid duplicate submissions** (should): Second submission rejected with 409

## Out of Scope

- TX fee prioritization (v2)
- Mempool size limits and eviction policies (v2)
- TX replacement (RBF)
- Mempool persistence across restarts
- TX batching at submission (one TX per request)

## Testing Guidance

- **Unit**: Test Pool operations (add, remove, drain, contains). Test Validator with mocked dependencies. Test thread safety with concurrent goroutines.
- **Integration**: Test TxSubmitService with real Pool and mocked external deps. Test REST endpoint with HTTP client.
- **Manual**: Submit rapid TXs, verify FIFO order in built blocks. Verify race detector passes.
