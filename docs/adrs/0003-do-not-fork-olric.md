# ADR 0003 - Do not fork Olric; reuse its primitives, build the object-storage core

**Status:** Accepted

## Context

[Olric](https://github.com/olric-data/olric) is the closest existing system to
shale by shape: an embeddable Go distributed KV with gossip membership,
consistent-hash sharding, automatic rebalancing, and no external coordinator.
When two systems share a shape this closely, "should we just fork it" is a
real question.

## Decision

**Do not fork Olric.** Reuse the foundational LIBRARIES Olric itself is built
on - `buraksezer/consistent` for the hash ring (literally the Olric author's
library) and `hashicorp/memberlist` for gossip - and build the object-storage-
native clustering on top ourselves.

## Rationale / Alternatives considered

The shapes match, but the cores are opposites:

- Olric's core is an **in-memory storage engine** plus a **data-movement
  rebalancer**, with AP / optimistic / last-write-wins semantics (a node loss
  can lose un-replicated data; replicas can diverge).
- shale needs an **object-storage core** with **zero-copy lease-handoff**
  (ADR 0005) and **single-writer / epoch-fenced / strong-per-unit** semantics
  (ADR 0004) - the opposite on every axis.

Forking Olric would mean:

1. Deleting and rewriting Olric's core (the in-memory engine + the data-
   movement rebalancer), which is most of what Olric IS, and
2. inheriting Olric's in-memory + AP assumptions, baked throughout its
   codebase, as a liability we would fight everywhere.

The parts of Olric worth keeping (membership + consistent-hash routing +
request forwarding) are thin and already library-provided - which is exactly
what we reuse directly. So a fork keeps the easy ~20% (library glue) and
inherits a liability, while we would still build the hard 80% from scratch.

- **Fork Olric and swap its storage layer.** Rejected, per the above.
- **Use Olric as-is.** Rejected: in-memory, no durability, no object storage,
  AP semantics - none of shale's requirements.

## Consequences

- (+) We reuse the same battle-tested hashing and gossip primitives Olric
  uses, without its in-memory/AP baggage.
- (+) The object-storage core (lease-handoff, fencing) is built clean, free of
  in-memory assumptions.
- (-) We own the clustering and reshard logic ourselves. But that logic IS the
  novel part of shale; the part a fork would have donated is mostly glue we get
  from libraries anyway.
- Landscape note: the systems that DO scale writes over object storage
  (WarpStream) are services, not embeddable libraries; the embeddable +
  object-storage systems (SlateDB) are single-writer (no write scale). shale's
  distinct point - an embeddable, object-storage-native KV that scales writes
  via sharded lease-handoff - is why no existing project was the right base.
