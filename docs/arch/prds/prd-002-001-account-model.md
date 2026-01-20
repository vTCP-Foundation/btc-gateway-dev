# PRD: Account Model

**ID**: PRD-002-001
**Scope**: 002-mempool-architecture

> **NO CODE**: PRDs describe WHAT to build, not HOW. Use prose. Code snippets only for complex data structures (3-5 lines max). Let developer agent write implementation.

## Implements

- Features: `scopes/002-mempool-architecture/feature-5-account-model.md`
- ADRs: `005-adr-account-model.md`, `010-adr-stack-storage.md`
- C4 Components: Account, AccountStore, TiKVAccountStore, CryptoProvider, Ed25519Provider, AccountHandler, AccountQueryService

## Implementation Target

Implement Account entity with address, nonce, and balance fields. Create AccountStore interface with TiKV implementation. Build CryptoProvider abstraction with Ed25519 test implementation. Add REST endpoint for account queries.

## Technical Context

Account state persists in TiKV (ADR-010) with key format `account:{address}`. CryptoProvider interface enables L1-agnostic crypto operations (ADR-005). Ed25519 serves as the test implementation. Account nonces increment on successful TX execution via StateMachine.

## Data Model

- **Account**
  - Address: string (validated by CryptoProvider)
  - Nonce: uint64 (starts at 0, increments per TX)
  - Balance: uint64 (arbitrary units, starts at 0)

- **TiKV Key Format**
  - Key: `account:{address}`
  - Value: msgpack-serialized Account

## Implementation Steps

### Step 1: Create CryptoProvider Interface

**Files**: `internal/crypto/provider.go` (create)

**Logic**: Define interface with four methods. Sign takes a message and returns signature bytes. Verify takes message, signature, and public key, returns bool. ValidateAddress takes address string, returns bool. DeriveAddress takes public key, returns address string.

**Edge Cases**:
- Empty inputs: return error or false appropriately
- Invalid signature length: Verify returns false

### Step 2: Implement Ed25519Provider

**Files**: `internal/crypto/ed25519/provider.go` (create)

**Logic**: Implement CryptoProvider using Go's crypto/ed25519. For ValidateAddress, check hex encoding and length (64 chars for ed25519 public key hex). For DeriveAddress, hex-encode the public key.

**Edge Cases**:
- Non-hex address: ValidateAddress returns false
- Wrong length address: ValidateAddress returns false

### Step 3: Create Account Entity

**Files**: `internal/account/account.go` (create)

**Logic**: Define Account struct with Address, Nonce, Balance fields. Add constructor NewAccount(address) that initializes with nonce=0, balance=0. Add methods IncrementNonce() and SetBalance(amount).

**Edge Cases**:
- Empty address: constructor returns error

### Step 4: Create AccountStore Interface

**Files**: `internal/account/store.go` (create)

**Logic**: Define interface with Get(address), Save(account), IncrementNonce(address), and SetBalance(address, amount) methods. Get returns account and bool (exists). IncrementNonce atomically increments nonce. SetBalance atomically updates balance.

**Edge Cases**:
- Account not found: Get returns nil, false (no error)

### Step 5: Implement TiKVAccountStore

**Files**: `internal/account/tikv/store.go` (create)

**Logic**: Implement AccountStore using existing tikv.Storage. Key format is `account:{address}`. Use msgpack for serialization. Get reads key and deserializes. Save serializes and writes. IncrementNonce reads, increments, saves (no transaction needed for single key). SetBalance reads, updates balance, saves.

**Edge Cases**:
- First access to new address: auto-create account with nonce=0, balance=0
- TiKV connection error: propagate error

### Step 6: Create AccountQueryService

**Files**: `internal/service/account_query.go` (create)

**Logic**: Service wraps AccountStore. GetAccount(address) calls store.Get(). If account doesn't exist, return new Account with nonce=0, balance=0 (don't persist).

**Edge Cases**:
- Invalid address format: return error before TiKV lookup

### Step 7: Add AccountHandler to API

**Files**: `internal/api/handler.go` (modify)

**Logic**: Add GET /account/{addr} endpoint. Extract address from path. Call AccountQueryService. Return JSON with address, nonce, balance fields.

**Edge Cases**:
- Missing address in path: return 400 Bad Request
- Invalid address format: return 400 Bad Request with error message

### Step 8: Add SetBalance TX Type

**Files**: `internal/statemachine/transaction.go` (modify), `internal/statemachine/machine.go` (modify)

**Logic**: Add TxSetBalance transaction type. Transaction includes target address and amount. StateMachine.applyTransaction handles TxSetBalance by calling accountStore.SetBalance(). No validation of fund source per scope requirements.

**Edge Cases**:
- Negative amount: reject TX (use uint64 to prevent)

### Step 9: Integrate Nonce Increment with StateMachine

**Files**: `internal/statemachine/machine.go` (modify)

**Logic**: After successfully applying a transaction, call accountStore.IncrementNonce(tx.Sender). This requires Transaction to have a Sender field (address of TX originator).

**Edge Cases**:
- TX without sender: skip nonce increment (for system TXs)

## Interfaces

### Inputs
- `address` (string): from REST path `/account/{addr}` - must pass CryptoProvider.ValidateAddress()
- `Account` (struct): from AccountStore.Get() - contains Address, Nonce, Balance

### Outputs
- JSON response: `{"address": "...", "nonce": 0, "balance": 0}` - consumed by clients
- `Account` entity: stored in TiKV - consumed by TxValidator for nonce checking

### Error States
- `ErrInvalidAddress`: address fails validation -> HTTP 400
- `ErrAccountNotFound`: treat as new account -> return nonce=0, balance=0
- `ErrStorageUnavailable`: TiKV unreachable -> HTTP 503

## Acceptance Criteria

### Functional
- [ ] AC-001: GET /account/{addr} returns account with nonce and balance
- [ ] AC-002: Non-existent account returns nonce=0, balance=0 (no error)
- [ ] AC-003: TxSetBalance updates account balance in TiKV
- [ ] AC-004: Account nonce increments after TX execution
- [ ] AC-005: CryptoProvider.ValidateAddress correctly validates ed25519 addresses
- [ ] AC-006: Invalid address format returns 400 error

### Non-Functional
- [ ] AC-NFR-001: Account query completes in <10ms (TiKV read latency)
- [ ] AC-NFR-002: CryptoProvider interface allows swapping implementations without code changes

## Edge Cases & Error Handling

- **First TX from new address** (must): Auto-create account with nonce=0, balance=0
- **TiKV unavailable** (must): Return 503 Service Unavailable, log error
- **Concurrent nonce increments** (should): TiKV's single-key atomicity sufficient
- **Balance overflow** (could): uint64 max is ~18 quintillion, unlikely in practice

## Out of Scope

- Balance transfer TX type (future PRD)
- Account deletion/pruning
- Multi-signature accounts
- Account metadata beyond nonce/balance

## Testing Guidance

- **Unit**: Test CryptoProvider implementations with known test vectors. Test Account entity methods. Mock AccountStore for service tests.
- **Integration**: Test TiKVAccountStore against real TiKV. Test REST endpoint with HTTP client.
- **Manual**: Verify account state persists across node restart.
