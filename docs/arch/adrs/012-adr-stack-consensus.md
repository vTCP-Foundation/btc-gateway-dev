# ADR-012: Consensus Stack

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

Block builder needs to detect leadership and propose blocks. Evaluated HotStuff (existing), Raft, and custom leader election.

## Decision

Use HotStuff BFT consensus. Already implemented with leader rotation, provides `ProposeBlock()` API, Byzantine fault tolerant.

## Consequences

### Positive
- No new implementation needed
- BFT guarantees for block finality
- Clear leader election per view

### Negative
- Complex protocol for simple use cases
- Requires 2f+1 validators minimum
