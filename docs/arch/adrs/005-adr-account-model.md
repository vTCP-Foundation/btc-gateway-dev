# ADR-005: Account Model

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

Per-sender nonce validation requires tracking account state. Also need balance field for future use.

## Decision

Implement Account entity in `internal/account` with address, nonce, balance fields. AccountStore interface with TiKVAccountStore implementation (ADR-010). CryptoProvider interface in `internal/crypto` for L1-agnostic address/signature handling. Ed25519Provider as test implementation. REST endpoint GET /account/{addr}. New TX type "set_balance".

## Consequences

### Positive
- Enables per-sender nonce tracking
- L1-agnostic via CryptoProvider interface
- Balance field ready for future features

### Negative
- New TikV key space for accounts
- Crypto interface adds abstraction layer
