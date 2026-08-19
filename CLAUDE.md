# shale - contributor guide

Read this before doing substantive work on shale. Read [`docs/SPEC.md`](docs/SPEC.md) next for what shale actually does.

This project is meant to be open-sourceable from day one. Don't put environment-specific notes, operator-specific configuration, or personal identifiers anywhere in this tree.

## Workflow

### Spec-first, always

Before any code change that adds or alters product behavior:

1. Open [`docs/SPEC.md`](docs/SPEC.md).
2. Confirm the spec already describes what you're about to build. If not, edit the spec first, re-read it, confirm it still hangs together as a whole.
3. Then write the code.

In a single commit, the spec should reflect the behavior the code in that commit implements - not what existed before, not what we plan next. The spec is the source of truth for what shale does. The code is the implementation.

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

## Repo layout (multi-module)

```
pkg/
  backend/             abstract Backend interface
    backend.go         the interface (the only thing every impl depends on)
    memory/            in-memory impl for tests + dev (lives in the core module)
  cluster/             public API surface (what apps import)
    cluster.go         Cluster struct, Put/Get/Delete/ScanPrefix/Begin
  ring/                consistent hash ring (wraps buraksezer/consistent)
  coord/               coordination PORT: who is a member, where a unit sits,
                       what transitional role each member advertises
    cas/               the ONE shipped adapter: membership as one JSON document
                       in a conditional store, CAS-mutated and polled
  storageunit/         storage-unit domain types (UnitID, UnitCount, epochs,
                       the BackendFactory mount/lease seam)
  reshard/             the CAS arbiter: declarative unit-count agreement
  blob/                value-separation plane for large values
  rpc/                 gRPC server + client for inter-node ops
  shaled/              shared run-loop helper used by every shaled-* binary
backends/              each backend is its own Go module (own go.mod, own deps)
  slate/               SlateDB-on-object-storage impl
                       module: github.com/Zamua/shale/backends/slate
    cmd/shaled-slate/  per-backend shaled binary (slate-only)
  pebble/              Pebble local-disk LSM impl
                       module: github.com/Zamua/shale/backends/pebble
    cmd/shaled-pebble/ per-backend shaled binary (pebble-only)
cmd/                   the command-line tools; its OWN Go module
                       (module: github.com/Zamua/shale/cmd). These are
                       CONSUMERS of shale, not shale - they are out of the
                       library module so it does not carry their deps,
                       notably the backend adapters the bench harness needs.
  shaled/              the reference shaled binary; memory backend only
  shale/               CLI; put/get/delete/scan/topology/stats/ping over gRPC
                       against a running node (defaults to 127.0.0.1:7947)
  shale-bench/         comparative benchmark harness (memory / pebble / slate)
internal/              private helpers + test fixtures
  decide/              coordination decisions as pure functions (no *Cluster,
                       no lock, no clock, no I/O), so each decision's state
                       space is a table rather than a hand-built fixture
  coordcontract/       the shared coord.Coordinator contract harness every
                       adapter (in-tree or forked) must pass
  coordstatic/         transport-free static coordinator for tests
tests/
  unit/                per-package tests
  integration/         multi-node in-process cluster, full path
docs/
  SPEC.md              the canonical design doc
go.work                workspace file - lets `go test ./...` from the root traverse
                       the core module + every backend module without a publish cycle
                       (release builds use GOWORK=off so each module's go.mod is
                       the source of truth for its own deps)
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

## Building the deployable shaled-slate image

`backends/slate/cmd/shaled-slate/` carries TWO Dockerfiles. Both share a 3-stage
shape (Rust `libslatedb_uniffi.so` -> Go+cgo `-tags slatedb` binary -> distroless
runtime); only how they resolve the core `github.com/Zamua/shale` module differs:

- **`Dockerfile.slatedb`** (release): build context is the `backends/slate` module
  root, `GOWORK=off`, so the core module resolves to the PUBLISHED version pinned
  in `backends/slate/go.mod`. Use this once the core changes you depend on are
  tagged/published.
- **`Dockerfile.slatedb.local`** (working tree): build context is the REPO ROOT,
  `go.work` active, so the core module resolves to the IN-TREE source. Use this to
  build an image that carries LOCAL, unpublished core-module changes (e.g.
  validating a new `pkg/cluster` feature on a real cluster before the core commits
  are tagged):

  ```
  docker build -f backends/slate/cmd/shaled-slate/Dockerfile.slatedb.local \
    --platform linux/amd64 -t <image>:<tag> .   # from the repo root
  ```

  The heavy Rust stage is byte-identical between the two and shares its build
  cache. Do NOT delete `Dockerfile.slatedb.local` "because it looks redundant": it
  is the only way to ship unreleased core changes, and it has been lost before.

### Debug / observability endpoint

The shaled run-loop exposes an OPTIONAL debug HTTP server, OFF unless the
`SHALE_DEBUG_ADDR` env var is set (so production is unaffected). When set (e.g.
`:6060` on a node under investigation) it serves `net/http/pprof` plus
`/debug/shale/state` - a per-node dump of every `ReplicaUnit`'s
desired/pendingOwner/mounted/handoff-phase (flagging the desired-but-unmounted
auto-recovery wedge) and the last swallowed acquire error. Reach it with a
port-forward; no shell needed in the distroless image.

## Don'ts

- Don't commit environment-specific paths, IPs, hostnames, account IDs, or operator credentials. The repo is meant to be a clean implementation that anyone can clone and run.
- Don't add personal preferences or session-specific notes here.
- Don't bypass the spec-first rule.
- Don't introduce SQL semantics. shale is KV-only on purpose; SQL clustering is fundamentally different and well-served by Vitess / Citus.
- Don't add features for backends we don't currently ship. If you want a Postgres-replica fronting layer, that's a different project.
