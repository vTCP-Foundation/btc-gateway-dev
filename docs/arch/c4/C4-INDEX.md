# C4 Diagram Index

**Format**: D2 (https://d2lang.com)
**Render**: `d2 --layout=elk <file>.d2 <file>.svg`
**Scope**: 002-mempool-architecture

## Context (C1)

- [c1-context.d2](c1-context.d2) - BTC Gateway system boundary with external systems (TiKV, Peer Nodes)

## Containers (C2)

- [c2-containers.d2](c2-containers.d2) - KVNode application container

## Components (C3)

- [c3-kvnode-overview.d2](c3-kvnode-overview.d2) - KVNode internals: handlers, services, mempool, consensus integration

## Diagram Hierarchy

```
c1-context.d2
└── c2-containers.d2
    └── c3-kvnode-overview.d2
```

## Key Components (C3)

| Component | Type | Purpose |
|-----------|------|---------|
| TxHandler | Handler | HTTP POST /tx entry point |
| AccountHandler | Handler | HTTP GET /account/{addr} entry point |
| GossipHandler | Handler | Receive gossiped TXs via libp2p |
| LeadershipHandler | Handler | Trigger block building on leadership |
| TxSubmitService | Service | Validate → Mempool → Gossip flow |
| BlockBuildService | Service | Drain mempool, propose blocks via ConsensusProposer |
| AccountQueryService | Service | Query account state |
| TxValidator | Engine | Signature, nonce, dedup validation |
| Pool | Store | In-memory FIFO mempool |
| TiKVAccountStore | Repository | Account persistence |
| TiKVCommittedTxStore | Repository | Committed TX hash lookup for dedup |
| GossipSubBroadcaster | Client | TX broadcast via GossipSub |
| Ed25519Provider | Client | L1-agnostic crypto |
| NodeProposer | Client | ConsensusProposer impl wrapping HotStuff Node |
| Node | Engine | HotStuff consensus (existing) |
| Executor | Engine | Apply committed blocks (existing) |
| StateMachine | Engine | KV operations (existing) |

## Rendering

```bash
# Render all diagrams
for f in *.d2; do d2 --layout=elk "$f" "${f%.d2}.svg"; done

# Watch mode for development
d2 --watch --layout=elk c3-kvnode-overview.d2 c3-kvnode-overview.svg
```
