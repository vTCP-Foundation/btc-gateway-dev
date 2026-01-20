# ADR-002: TX Gossip Protocol

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

Pending TXs must propagate to all nodes so the leader has the full TX set. Need efficient broadcast without loops.

## Decision

Use libp2p GossipSub (ADR-011) with dedicated topic `/btc-gateway/tx/1.0.0`. Implement in `internal/gossip` with TxBroadcaster interface. Use msgpack serialization. Dedup via mempool Contains() check before relay.

## Consequences

### Positive
- Efficient mesh-based gossip
- Deduplication prevents broadcast loops
- Reuses existing libp2p infrastructure

### Negative
- Topic management overhead
- No rate limiting (deferred)
