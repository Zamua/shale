# Architecture Decision Records

Short records of the load-bearing architectural decisions behind shale: the
context, the decision, the alternatives we rejected, and the consequences we
accepted. They are the "why", complementary to `docs/SPEC.md` (the "what").

Format per file: Status / Context / Decision / Alternatives considered /
Consequences. Numbered in dependency order (each tends to assume the ones
before it).

- [0001 - Object storage is the source of truth](0001-object-storage-source-of-truth.md)
- [0002 - Backend-agnostic interfaces; object-storage properties isolated in the slate backend](0002-backend-agnostic-interfaces.md)
- [0003 - Do not fork Olric; reuse its primitives, build the object-storage core](0003-do-not-fork-olric.md)
- [0004 - Single-writer-per-unit via epoch fencing, not consensus](0004-single-writer-epoch-fencing.md)
- [0005 - Zero-copy resharding via lease-handoff](0005-zero-copy-reshard-lease-handoff.md)
