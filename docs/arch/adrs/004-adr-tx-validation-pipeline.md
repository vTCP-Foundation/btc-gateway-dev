# ADR-004: TX Validation Pipeline

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

TXs must be validated before mempool admission and gossip relay. Need deduplication, nonce checking, and signature verification.

## Decision

Implement TxValidator in `internal/validation`. Validation order: (1) signature via CryptoProvider, (2) dedup via mempool Contains() + CommittedTxStore.Contains(), (3) nonce check via AccountStore. TxSubmitService orchestrates: validate → mempool → gossip. CommittedTxStore port abstracts committed TX hash storage.

## Consequences

### Positive
- Prevents invalid TXs from propagating
- Clear validation ordering
- Reusable CryptoProvider interface
- CommittedTxStore port keeps validator infrastructure-agnostic

### Negative
- Committed TX lookup adds latency (~10ms)
- No caching layer (port implementation could add caching)
