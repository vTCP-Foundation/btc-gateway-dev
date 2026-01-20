# ADR-001: Mempool Architecture

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

Current system proposes each TX as its own block via `ProposeBlock()`. Need to batch transactions and allow non-leaders to accept TXs.

## Decision

Implement in-memory FIFO mempool in `internal/mempool`. TxPool interface with Add/Remove/Drain/Contains/Size methods. Thread-safe via mutex. Size limits deferred to v2.

## Consequences

### Positive
- Enables TX batching (multiple TXs per block)
- Any node can accept TXs
- Simple FIFO ordering

### Negative
- In-memory only (no persistence across restarts)
- No priority/fee ordering (deferred to v2)
