# shale — contributor guide

Read this before doing substantive work on shale. Read [`docs/SPEC.md`](docs/SPEC.md) next for what shale actually does.

This project is meant to be open-sourceable from day one. Don't put environment-specific notes, operator-specific configuration, or personal identifiers anywhere in this tree.

## Workflow

### Spec-first, always

Before any code change that adds or alters product behavior:

1. Open [`docs/SPEC.md`](docs/SPEC.md).
2. Confirm the spec already describes what you're about to build. If not, edit the spec first, re-read it, confirm it still hangs together as a whole.
3. Then write the code.

In a single commit, the spec should reflect the behavior the code in that commit implements — not what existed before, not what we plan next. The spec is the source of truth for what shale does. The code is the implementation.

### Implementation discipline: DDD + TDD

**Domain-Driven Design.** Organize packages by *bounded context*, not by technical layer.

- The domain layer holds pure types + invariants. No I/O. Plain Go data + pure functions you can test without spinning anything up.
- Infrastructure adapters (concrete Backend impls, gRPC transport, membership) live in their own packages and depend on the domain.
- Application services (the public Cluster surface) orchestrate use cases by composing domain types with adapters via small interfaces.
- Don't reach for fancy patterns unless a concrete use case forces them.

**Test-Driven Development.** Tests are part of the same change as the code they cover.

- For new behavior: red → green → refactor.
- For modifying existing behavior: write a characterization test pinning current behavior first, then change code + test together.
- Prefer integration tests over unit tests where the boundary is real (multi-node cluster via in-process goroutines, real backend via temp dir or test-mode).
- A PR that "adds a feature without tests" doesn't ship.

### Commits

Conventional Commits, single line. No co-author trailers, no agent-attribution lines.

```
feat(cluster): consistent-hash routing for Put/Get
fix(ring): handle empty member set without panic
test(membership): pin node-leave triggers shard rebalance
docs(spec): clarify replication semantics for R=1
```

## Repo layout (planned; some not yet created)

```
pkg/
  backend/             abstract Backend interface
    backend.go         the interface
    memory/            in-memory impl for tests + dev
    slate/             SlateDB-on-object-storage impl
  cluster/             public API surface (what apps import)
    cluster.go         Cluster struct, Put/Get/Delete/ScanPrefix/Begin
  ring/                consistent hash ring (wraps buraksezer/consistent)
  membership/          memberlist wrapper + topology change events
  rpc/                 gRPC server + client for inter-node ops
  rebalance/           shard handoff during membership changes
cmd/
  shaled/              standalone node binary (for ops/testing)
  shale-bench/         load tester
internal/              private helpers + test fixtures
tests/
  unit/                per-package tests
  integration/         multi-node in-process cluster, full path
docs/
  SPEC.md              the canonical design doc
LICENSE                Apache 2.0
README.md              user-facing intro
CLAUDE.md              this file
```

## Local setup

Go 1.25+ required.

```
go test ./...     # all tests
go build ./...    # all packages compile
```

Integration tests spin up 3-node clusters in-process via goroutines + ephemeral ports. No external services required.

## Don'ts

- Don't commit environment-specific paths, IPs, hostnames, account IDs, or operator credentials. The repo is meant to be a clean implementation that anyone can clone and run.
- Don't add personal preferences or session-specific notes here.
- Don't bypass the spec-first rule.
- Don't introduce SQL semantics. shale is KV-only on purpose; SQL clustering is fundamentally different and well-served by Vitess / Citus.
- Don't add features for backends we don't currently ship. If you want a Postgres-replica fronting layer, that's a different project.
