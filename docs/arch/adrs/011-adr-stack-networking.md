# ADR-011: Networking Stack

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

TX gossip requires peer-to-peer broadcast. Evaluated libp2p GossipSub (existing), custom flooding protocol, and NATS.

## Decision

Use libp2p GossipSub. Already integrated for consensus messaging, provides efficient pub/sub with mesh topology, no new dependencies.

## Consequences

### Positive
- No new infrastructure
- Battle-tested gossip protocol
- Integrates with existing peer discovery

### Negative
- Coupled to libp2p ecosystem
- GossipSub overhead for small networks
