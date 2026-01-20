# ADR-010: Storage Stack

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

Mempool architecture requires persistent storage for account state (nonce, balance) and TX deduplication. Evaluated TikV (existing), PostgreSQL, and embedded KV stores.

## Decision

Use TikV. Already integrated for consensus storage, provides distributed KV semantics needed for account state, no new infrastructure required.

## Consequences

### Positive
- No new infrastructure
- Distributed and fault-tolerant
- Consistent with existing architecture

### Negative
- Requires TikV cluster for all deployments
- Higher latency than embedded stores
