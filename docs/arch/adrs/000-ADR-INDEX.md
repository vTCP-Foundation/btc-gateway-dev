# ADR Index

Navigation map for Architecture Decision Records.

## Recent

| ADR | Title | Date |
|-----|-------|------|
| [012](012-adr-stack-consensus.md) | Consensus Stack | 2026-01-20 |
| [011](011-adr-stack-networking.md) | Networking Stack | 2026-01-20 |
| [010](010-adr-stack-storage.md) | Storage Stack | 2026-01-20 |
| [005](005-adr-account-model.md) | Account Model | 2026-01-20 |
| [004](004-adr-tx-validation-pipeline.md) | TX Validation Pipeline | 2026-01-20 |

## Stack Decisions

### Data

| ADR | Technology | Purpose |
|-----|------------|---------|
| [010](010-adr-stack-storage.md) | TiKV | Distributed state storage |

### Processing

| ADR | Technology | Purpose |
|-----|------------|---------|
| [012](012-adr-stack-consensus.md) | HotStuff BFT | Block consensus |

### Networking

| ADR | Technology | Purpose |
|-----|------------|---------|
| [011](011-adr-stack-networking.md) | libp2p GossipSub | P2P messaging |

## Pattern Decisions

### Mempool

- [001](001-adr-mempool-architecture.md): Mempool Architecture (→ ADR-010)
- [002](002-adr-tx-gossip-protocol.md): TX Gossip Protocol (→ ADR-011)
- [004](004-adr-tx-validation-pipeline.md): TX Validation Pipeline (→ ADR-010)

### Consensus

- [003](003-adr-block-builder.md): Block Builder (→ ADR-012)

### Account

- [005](005-adr-account-model.md): Account Model (→ ADR-010)

## By Scope

<details>
<summary>002-mempool-architecture</summary>

**Stack ADRs:**
- [010](010-adr-stack-storage.md): Storage Stack
- [011](011-adr-stack-networking.md): Networking Stack
- [012](012-adr-stack-consensus.md): Consensus Stack

**Pattern ADRs:**
- [001](001-adr-mempool-architecture.md): Mempool Architecture
- [002](002-adr-tx-gossip-protocol.md): TX Gossip Protocol
- [003](003-adr-block-builder.md): Block Builder
- [004](004-adr-tx-validation-pipeline.md): TX Validation Pipeline
- [005](005-adr-account-model.md): Account Model

</details>
