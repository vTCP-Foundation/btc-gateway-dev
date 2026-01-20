# ADR-003: Block Builder

**Date**: 2026-01-20
**Scope**: 002-mempool-architecture

## Context

Leader must build blocks from mempool when gaining leadership. Need to integrate with HotStuff consensus view changes.

## Decision

Implement BlockBuildService in `internal/builder`. LeadershipHandler subscribes to consensus events. On leadership gain: drain mempool (up to 128MB), create TransactionBatch, call ConsensusProposer.ProposeBlock(batch). NodeProposer adapter wraps HotStuff Node. Propose empty blocks for liveness.

## Consequences

### Positive
- Batches TXs efficiently
- Empty blocks maintain liveness
- Clean integration with HotStuff (ADR-012)
- ConsensusProposer port keeps BlockBuildService infrastructure-agnostic

### Negative
- Requires consensus event hook
- Block building latency added to consensus round
