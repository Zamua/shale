# Streaming blob values in shale (design)

Status: draft / design. No code yet. This doc is the contract to poke holes in
before implementation.

## 1. Motivation

Apps built on shale (hostthis is the driving case) need to store large opaque
BLOBS (a paste body, a site's files, up to ~10 MB each) alongside small
structured METADATA (the paste row, the manifest), under two hard
requirements:

1. **Transactional**: write/delete the metadata AND the blob together, so one
   never lands without the other. No orphans, no torn reads.
2. **Streaming**: never hold a whole blob in memory; stream end to end, on both
   write and read.

Today this is solved with TWO independent stores: metadata in shale (slatedb),
blob bytes in a detached object-store bucket the app pokes directly. The two
are coordinated by ordered-writes plus a garbage-collection sweep, which is the
entire source of: orphaned blobs, a cross-shard referenced-SHA scan (that
fences on post-rebalance handles), and an app-side reservation dance to avoid
racing an in-flight write. None of that is intrinsic to the problem; it is the
cost of the two stores not sharing a transaction.

Goal: bring blobs UNDER shale so a record's metadata and its blob live on the
same shard and are committed/deleted in one transaction.

Hard non-goal: storing blob BYTES inside slatedb's LSM. Large values sit inline
in SSTables, so compaction rewrites them (10-30x write amplification) and the
memtable/cache buffer them - real RAM pressure on the 2 GB fleet boxes, layered
onto the engine that has been the fragile part of the stack. So we do
value-SEPARATION: bytes in object storage, a small transactional POINTER in
slatedb.

## 2. The model: value separation (WiscKey, one layer up)

This is the WiscKey / RocksDB-BlobDB technique (large values in a separate
value log, the LSM holds only a pointer), implemented at the SHALE layer rather
than inside slatedb - so slatedb stays vanilla and we keep every upstream
update.

- The blob BYTES are stored as an object in the SAME object store slatedb
  already runs on (no new infrastructure).
- A small POINTER record (`{objkey, size, ...}`) is stored as a slatedb value
  on the owning slug's shard. The pointer is an ordinary small value, so it
  participates in transactions and R=2 replication exactly like metadata.
- The bytes stream in and out (multipart PUT, ranged/sequential GET) and are
  never buffered whole.

Net properties: collocation (pointer + metadata on one shard), a transactional
reference (atomic create-visibility, atomic delete), streaming, and no large
LSM values.

### 2.1 The byte plane is shared; only the pointer routes

Key consequence of value-separation over a SHARED object store: the BYTES do
not have to route through the slug's owning node. The object store is reachable
from every node, so:

- **Write**: the receiving node streams the bytes straight to object storage
  under a key in the owning shard's namespace (any node can write any key), then
  commits the small POINTER via the normal routed transaction to the owning
  node.
- **Read**: the reading node fetches the pointer (a routed read), then streams
  the bytes directly from object storage.

So shale's ring forwards only small pointers; the multi-megabyte byte I/O goes
node -> object-store directly. There is NO blob streaming over gRPC between
nodes. "Blobs through shale" means shale owns the object lifecycle and the
transactional pointer - not that the bytes traverse the cluster's RPC plane.

## 3. Atomicity model (precise)

A literal single transaction CANNOT physically contain the streamed bytes: to
put them in the transaction's write-set you would buffer the whole blob, which
violates the streaming requirement, and you would hold a shard lease open for
the multi-second upload, blocking every other write to that shard. So:

- **CREATE is reader-atomic.** Stream the bytes to a durable STAGED object first
  (outside any transaction - no lease held during the upload), then commit a
  FAST transaction that writes `{metadata + the blob pointer}` together. The
  blob becomes referenced (hence reader-visible) only at the commit, so a reader
  never sees metadata without its blob. A crash before the commit leaves a
  staged object that nothing references - GC'd, never reader-visible.
- **DELETE is fully atomic.** One transaction removes `{metadata + the blob
  pointer}` together. The now-unreferenced bytes object is reclaimed by a
  SAME-SHARD orphan sweep (the owning node lists objects under its own shard
  prefix and deletes any with no live pointer). A crash between the transaction
  and the object delete leaves an orphan object the sweep reclaims - a storage
  leak, never a torn read.

So: fully-atomic delete, reader-atomic create, same-shard orphan-bytes GC. This
is the strongest guarantee achievable WITH streaming; "bytes inside the commit"
is both impossible and unnecessary - the reference is what must be
transactional, and it is.

There is no cross-shard scan and no fence: every blob's only possible
referencer is its pointer on its own shard.

## 4. Interface (DDD ports + composed types, compile-time capability)

shale composes two storage concerns. The blob concern is OPTIONAL, and the
capability is visible IN THE TYPE so that calling a blob method without
configuring a blob store is a COMPILE error, not a runtime nil-check.

(Go note: a method on a generic type exists for ALL instantiations, and phantom
types tag values but do not gate methods - so generics cannot express "PutBlob
only when blobs configured." The idiomatic Go answer is "states as distinct
types": the blob-configured cluster is a distinct type that HAS the blob
methods. That is the shape below. Confirmed against Go 1.26 and the
accepted-for-1.27 generic-methods proposal, neither of which adds conditional
method presence.)

### 4.1 Ports

```go
// MetadataStore: the small-value transactional KV concern (slatedb backend).
type MetadataStore interface { /* per-shard get/put/delete + transactions */ }

// BlobStore: streamable bytes to object storage (an object-store adapter).
// OPTIONAL - omitting it yields a metadata-only cluster.
type BlobStore interface {
    PutStream(objkey string, r io.Reader) (size int64, err error)
    GetStream(objkey string) (io.ReadCloser, error)
    Delete(objkey string) error
    List(prefix string) iter.Seq2[string, error] // for the orphan sweep
}
```

### 4.2 Composed cluster types (capability in the type)

```go
func New(meta MetadataStore) *KV                              // metadata only
func NewWithBlobs(meta MetadataStore, b BlobStore) *BlobKV    // metadata + blobs

// *KV exposes:     Get, Put, Delete, Transact(shardKey, func(*Tx) error)
// *BlobKV is a SUPERSET of *KV, and additionally:
//   StageBlob(r io.Reader) (BlobHandle, error)  // stream to a staged object; opaque handle
//   GetBlob(key string) (io.ReadCloser, error)  // stream out the blob bound at key
//   and its Transact yields a *BlobTx with one extra op:
//   (tx *BlobTx) BindBlob(key string, h BlobHandle)  // attach the staged blob to key, atomically
```

- A consumer that needs blobs takes a `*BlobKV` (or a blob-requiring
  interface). Wiring a `*KV` there does not compile. `*BlobKV` is a superset of
  `*KV`, so code needing only metadata accepts either.
- The POINTER is fully internal. The app touches only an opaque, transient
  `BlobHandle` (the token that carries a staged blob from StageBlob into
  BindBlob) and references blobs by KEY. It never sees `{objkey, size}`.
- `Delete(key)` on a `*Tx`/`*BlobTx` removes a bound blob's pointer; shale GCs
  the bytes. (Delete needs no blob-specific op - removing the pointer key is an
  ordinary KV delete, which is why DELETE works in a plain `*Tx` too once the
  pointer exists as a key.)

### 4.3 Why StageBlob is a separate step (not collapsed into the transaction)

The slow streaming must happen OUTSIDE the transaction (don't hold a shard lease
for seconds; don't buffer the blob to put it in the write-set). StageBlob does
the slow durable write and returns an opaque handle; the fast transaction then
binds it. This is the minimal seam - one opaque, transient token - and it is
load-bearing, not incidental.

If the process dies after StageBlob but before the binding transaction commits,
the staged object is unreferenced and reclaimed by the same orphan sweep.

## 5. Object key layout, collocation, durability

- Bytes object key lives under the owning shard's namespace, e.g.
  `blob/<shardKey>/<blobid>`, so the owning node can enumerate "its" blob
  objects (`BlobStore.List(prefix)`) for the shard-local orphan sweep. `blobid`
  is shale-internal (e.g. a ULID or the content sha when the app provides it for
  dedup).
- The POINTER's slatedb key lives on the same shard (co-routed by the same
  ShardKeyFn the app's metadata uses), so metadata + pointer commit in one
  per-shard transaction.
- **Durability of the bytes**: default is a SINGLE shared object with the object
  store's own durability (e.g. MinIO erasure coding), referenced by the R=2
  pointer - so no 2x byte storage, matching today's blob durability. The pointer
  is R=2 (replicated by shale), so a node loss never loses the reference. Open
  question (section 8): whether to additionally shale-replicate the bytes for
  defense in depth.

## 6. Consumer usage (hostthis)

CREATE (upload):
```
stage := blobKV.StageBlob(  tee(stdin -> sha256 + zstd)  )   // streams, opaque handle
blobKV.Transact(slug, func(tx *BlobTx) error {
    tx.BindBlob(blobKey(slug, ver), stage)   // shale stores the pointer internally
    tx.Put(pasteKey(slug), metadataBytes)    // hostthis's own metadata - same commit
    return nil
})
```
READ:
```
meta := blobKV.Get(pasteKey(slug))            // metadata (small)
rc   := blobKV.GetBlob(blobKey(slug, ver))    // stream -> decompress -> client
```
DELETE / EXPIRE:
```
blobKV.Transact(slug, func(tx *BlobTx) error {
    tx.Delete(pasteKey(slug))
    tx.Delete(blobKey(slug, ver))             // bytes GC'd by the same-shard sweep
    return nil
})
```

- Versions and site files are each their own blob key under the slug
  (`blobKey(slug, ver)` / `blobKey(slug, fileSha)`), all on the slug's shard;
  create/delete are transactions over the metadata plus the relevant blob keys.
- Within-record dedup (a paste reverting to old content; a site redeploy
  re-sending an unchanged file) is the app keying the blob by content sha:
  StageBlob is skipped when `Has(blobKey(slug, sha))`. Cross-record dedup is
  intentionally gone (it was the source of the cluster-wide GC).

## 7. What this supersedes

- The detached object-store blob bucket and hostthis's direct S3 blob client.
- The cross-shard referenced-SHA blob-GC (the #443 fence) - blob lifetime is now
  the pointer's lifetime, transactional and shard-local.
- The crash-orphan reconcile and the site reservation from the slug-scoped
  branch (`feat/slug-scoped-blobs`) - replaced by transactional create/delete +
  a same-shard orphan-bytes sweep. The slug-scoped KEY scheme
  (`blob/<...>/<slug>/...`) carries forward as the collocation key; that work is
  the bridge, not waste.

## 8. Open questions / risks

1. **gRPC vs object-store byte plane** (resolved, but verify): the bytes go
   node -> object store directly; only pointers route. Confirm shale's per-shard
   Transact already supports multi-key writes on one shard (metadata + pointer)
   - it should, it is a per-shard transaction.
2. **Bytes durability**: shared object + EC (default, lean) vs shale-replicated
   bytes (stronger, 2x storage). Start with EC; revisit if a node+object-store
   correlated failure model warrants it.
3. **Orphan-bytes sweep**: cadence, and how the owning node bounds the
   `List(prefix)` cost. A blob is orphan-eligible only if its pointer key is
   absent on the owning shard; same-shard, fence-free.
4. **Staged-object lifecycle**: where StageBlob writes (a staging prefix) and the
   TTL for abandoned uploads; the orphan sweep covers it but a TTL bounds the
   window.
5. **Pointer record + a new internal key family** in slatedb (e.g. `bptr/...`);
   ensure it routes by the same shard key as the app's record so they co-commit.
6. **Streaming compression**: zstd over the stream, flushed at the multipart
   part boundary so the staged object is the compressed representation; sha is
   over the original bytes (computed in the tee), carried in the app's metadata.
7. **Rebalance interaction**: when a shard moves, its pointers move with the
   slatedb unit (existing machinery); the bytes objects do not move (they are in
   shared storage under the shard-key prefix, which the new owner now owns). The
   new owner's orphan sweep covers them. Confirm the prefix is derived from the
   shard key, not the node, so ownership transfer is automatic.
8. **Capability surface**: confirm `*BlobKV` superset-of-`*KV` is expressed by
   embedding without inheriting `*KV`'s narrower `Transact` (so `*BlobKV.Transact`
   can yield the richer `*BlobTx`). Minor internal wiring; invisible to callers.

## 9. Phasing (implementation, spec-first per phase)

1. **shale BlobStore port + object-store adapter**: PutStream/GetStream/Delete/
   List + the pointer record + the staged-object path. Unit + integration tests
   against real MinIO.
2. **shale capability split**: `*KV` / `*BlobKV` / `*BlobTx` + `StageBlob` /
   `GetBlob` / `BindBlob` + the same-shard orphan-bytes sweep. Tests, incl.
   reader-atomic-create and atomic-delete crash-injection.
3. **hostthis integration**: rewrite the blob path to stream through `*BlobKV`;
   transactional create/delete; delete the S3 blob store + the reconcile + the
   reservation. Conformance + e2e tests; streaming verified (peak memory bounded).
4. **Migration + rollout**: one-time re-key of existing bucket blobs into
   shale-managed objects + pointers under a brief write-freeze; staging; prod.

Each phase is its own spec-first change with its own tests; this doc is the
umbrella design.

## 10. Phase-1 review resolutions (2026-06-19)

Phase 1 (the `pkg/blob` BlobStore port + minio adapter + types) is built and
green (streaming proven on a 20 MB round-trip against real MinIO). The phase-1
workflow's adversarial review surfaced two foundation decisions to resolve here
before phase 2 builds on them, plus minor hygiene. Resolutions:

### 10.1 Promotion model: STAGE-TO-FINAL-KEY, no copy (resolves review P1-2)

The design above left "how a staged object becomes the bound final object"
implicit, and phase 1 guessed a flat `blobstaging/<id>` prefix + a copy-on-bind.
Reject that: a flat staging prefix is NOT shard-keyed, so a crash between stage
and bind leaves a staged orphan that only a CROSS-shard sweep could find -
reintroducing exactly the cross-shard concern this whole design removes.

Resolution: `StageBlob` streams the bytes DIRECTLY to the final, shard-keyed key
`blob/<shardKey>/<blobid>`. The caller (hostthis) knows the slug at upload time,
so `*BlobKV.StageBlob` derives the shardKey from it (`ShardKeyFn`) and mints the
blobid before streaming. `BindBlob` then only writes the small `BlobPointer`
(referencing that already-final key) and co-commits it with the metadata in the
per-shard transaction. NO copy, no separate staging namespace.

This is reader-safe: bytes sitting at the final key before the pointer commits
are unreachable to any reader (a reader reaches bytes only via the committed
metadata -> pointer), so writing them "early" is invisible. A crash before the
bind commit leaves `blob/<shardKey>/<blobid>` with no pointer - a SHARD-LOCAL
orphan, reclaimed by the same-shard sweep. The phase-1 `Stage` helper +
`StagingKey`/`IsStagingKey` + the staging-prefix concept are SUPERSEDED and to be
removed; the phase-2 `StageBlob` is `PutStream` to the final key. `BlobHandle`
collapses into `BlobPointer` (the staged object's final key + size + optional
content hash fully determine the pointer), so the app sees only that.

### 10.2 Orphan-bytes sweep age-gates via object `LastModified`

Because bytes now land at the final key BEFORE the pointer commits, the orphan
sweep must not reclaim a blob whose bind transaction simply has not committed
yet (the same in-flight race the hostthis site-reservation hit). Resolution: the
sweep reclaims a pointer-less blob object only if its object-store `LastModified`
is older than a grace (default ~1 h, far beyond any bind window). The object's
timestamp is a DURABLE, per-object signal from the store itself - no in-memory
debounce, survives restarts, and is naturally shard-local (the sweep already
enumerates `blob/<shardKey>/` on the owning node). A just-staged blob is recent
and kept; a genuine crash-orphan ages out and is reclaimed.

### 10.3 Module placement: port in core, minio adapter in a backend module

Per shale's architecture (CLAUDE.md: the core module stays light, "no heavy
storage deps"; `cmd/shaled` is memory-only; object-store deps live in backend
modules), the minio-go dependency must NOT sit in the core module. Resolution:
`pkg/blob` (core) holds ONLY the `BlobStore` port + the pure domain types
(`BlobPointer`, the `FinalKey`/`FinalPrefixForShard` helpers, `NewBlobID`,
`ErrNotFound`); the concrete `MinioBlobStore` adapter moves to a backend module
(it reuses the exact minio client pattern already in `backends/slate/condstore.go`;
whether it lives in `backends/slate` or its own `backends/blobstore` module is a
follow-up call - own-module is cleaner since the blob byte-plane is independent
of the metadata backend). The cluster (`pkg/cluster`, core, phase 2) depends only
on the `blob.BlobStore` interface; the concrete adapter is wired at the
`cmd/shaled-*` binary, exactly as backends already are. Phase-1 code currently
has the adapter in core; to be moved before phase-1 commit.

### 10.4 Minor hygiene (from the review)

- Add `goleak.VerifyTestMain` to `pkg/blob` (repo precedent) - minio-go's
  `GetObject` spawns a ctx-bound reader goroutine, so a missed `Close` leaks
  silently; document on `GetStream` (and future `GetBlob`) that the passed `ctx`
  must OUTLIVE the returned reader, not just the call (else a phase-2 caller that
  scopes ctx to "assemble the response" truncates a large stream mid-flight).
- `List` should strip the prefix with `strings.TrimPrefix` (fail-safe), not a
  blind length-slice.
- Update section 4.1's interface sketch to the implemented signatures (methods
  take `context.Context`; `PutStream(ctx, objkey, r, size) error`; `GetStream`
  returns `(ReadCloser, size, error)`; `Has` added).

### 10.5 Phase-1 status

Built + reviewed; NOT yet committed. The structural refinements above (10.1 stage
model -> drop the staging-prefix code; 10.3 move the adapter out of core) plus the
10.4 hygiene are the immediate next step, after which phase 1 commits clean. The
`BlobStore` port, `MinioBlobStore` streaming behavior, `BlobPointer` round-trip,
`ErrNotFound` mapping, key-layout helpers, and the integration tests are sound and
carry forward unchanged.

## 11. Phase-2 cluster surface (detailed design)

This section grounds the phase-2 cluster capability split (sections 4 and 10.4)
in shale's ACTUAL `pkg/cluster` API and pins the exact signatures, flows, and
internal key rules phase 2 implements. It supersedes the conceptual sketches in
section 4.2 wherever the real API forced a change; the forced changes and why
are called out inline and summarized in 11.9.

### 11.0 What the real cluster API forced (read first)

Five facts about the implemented cluster changed the section-4 sketch. None
change the model (reader-atomic create, atomic delete, shard-local sweep); they
change the SHAPE the surface bolts onto.

1. **The constructor is `cluster.Open(cfg Config) (*Cluster, error)`, not
   `New(...)`.** There is one cluster type, `*Cluster`, configured by a `Config`
   struct (NodeID, Backend/BackendFactory, ShardKeyFn, ReplicationFactor, ...).
   So `*KV` / `*BlobKV` are NOT alternative return types of two constructors;
   they are THIN WRAPPERS over a `*Cluster`, and the blob capability is gated by
   a new optional `Config.BlobStore blob.Store` field plus a blob-aware
   constructor. The capability-in-the-type property (section 4) is preserved by
   the wrapper types, not by the cluster itself.

2. **`Transact` is `func(pinKey []byte, fn func(tx backend.Transaction) error)
   error`** (pkg/cluster/cas.go:408). The transaction handed to `fn` is the
   `backend.Transaction` INTERFACE (concretely `*clusterTx` in remote.go), not a
   concrete `*Tx`. So `*BlobTx` wraps a `backend.Transaction`; it does not embed
   a concrete cluster transaction type (there is no exported one). The shadowing
   in 4.2 ("`*BlobKV.Transact` yields `*BlobTx`") is implemented by the wrapper's
   own `Transact` method passing an adapter closure to `*Cluster.Transact`.

3. **The blob object key MUST be UNIT-keyed, not raw-shardKey-keyed.** This is
   the load-bearing correction. The routing chain is
   `key -> shardKey (ShardKeyFn) -> ShardHash (xxhash) -> UnitID (hash & (N-1))
   -> ring owner of the unit` (storageunit/unitmap.go, cluster/multibackend.go).
   A node owns a BOUNDED set of UNITS (N is a fixed power of two), each holding
   MANY shard keys; the app's shard key (a hostthis slug / id / subnet) is an
   UNBOUNDED set. The same-shard sweep must enumerate "blobs this node owns", and
   what a node owns is UNITS. Keying objects by the raw shard key
   (`blob/<shardKey>/...`) gives the sweep no finite per-owner prefix: it would
   have to list ALL of `blob/` and recompute ownership per object - a global
   scan, exactly the cross-shard cost this whole design removes. Keying objects
   by the unit (`blob/<unitID>/<blobid>`) makes the sweep list one finite prefix
   per owned unit. So phase 2 changes the object-key layout from
   `blob/<shardKey>/<blobid>` (phase-1 `FinalKey`) to a UNIT-derived prefix; see
   11.5. The phase-1 `FinalKey(shardKey, blobid)` / `FinalPrefixForShard` helpers
   are re-cut to take a unit token (11.5, 11.9).

4. **`blob.Store.List` yields only keys (`iter.Seq2[string, error]`); the sweep
   needs each object's `LastModified` for the age-gate (section 10.2).** The port
   gains a modtime-carrying listing (11.6). This is the GAP called out in the
   brief.

5. **There is no exported "units this node owns" accessor on `*Cluster`.** The
   internal `desiredGenUnits()` (multibackend.go:121) computes exactly this set
   but is unexported and multi-backend-only. The sweep needs a public, mode-aware
   accessor; phase 2 adds `*Cluster.OwnedUnits()` (11.5, 11.7).

### 11.1 Capability wiring: `Config.BlobStore` + two wrapper types

The blob store is injected at construction via a new optional `Config` field. The
concrete `*blobstore.MinioBlobStore` is wired at the `cmd/shaled-slate` binary
(exactly as the slate backend + `ConditionalStore` already are); tests pass an
in-memory `blob.Store` fake.

```go
// pkg/cluster/cluster.go - add to Config:
type Config struct {
    // ... existing fields ...

    // BlobStore is the OPTIONAL streaming byte plane (blob.Store port). nil
    // leaves the cluster metadata-only. When set, NewBlobKV exposes the blob
    // capability; the concrete object-store adapter is wired at the cmd binary.
    BlobStore blob.Store
}
```

`*KV` and `*BlobKV` are wrapper types over `*Cluster`. They exist so the blob
capability is visible IN THE TYPE (calling a blob op without a configured store
is a COMPILE error, section 4), which a single `*Cluster` with a nil field could
not give.

```go
// pkg/cluster (new file kv.go) - the metadata-only surface.
type KV struct{ c *Cluster }

// New wraps a metadata-only cluster. cfg.BlobStore MUST be nil (a configured
// blob store with the plain *KV surface is a wiring mistake: the bytes plane is
// unreachable). New returns an error if cfg.BlobStore != nil.
func New(cfg Config) (*KV, error)

func (kv *KV) Get(key []byte) ([]byte, error)
func (kv *KV) Put(key, value []byte) error
func (kv *KV) Delete(key []byte) error
func (kv *KV) Transact(pinKey []byte, fn func(*Tx) error) error
func (kv *KV) Close() error

// *Tx wraps the cluster transaction (backend.Transaction) with the SAME
// Get/Put/Delete the cluster transaction exposes. It is the metadata-only
// transaction; *BlobTx is its blob-capable superset.
type Tx struct{ tx backend.Transaction }

func (t *Tx) Get(key []byte) ([]byte, error)
func (t *Tx) Put(key, value []byte) error
func (t *Tx) Delete(key []byte) error
```

```go
// *BlobKV is a SUPERSET of *KV: it embeds *KV (inheriting Get/Put/Delete and
// the metadata Transact), SHADOWS Transact to yield the richer *BlobTx, and adds
// the streaming blob ops. Because it embeds *KV, any consumer that needs only
// metadata accepts a *BlobKV too.
type BlobKV struct {
    *KV
    blobs blob.Store
}

// NewBlobKV wraps a blob-configured cluster. cfg.BlobStore MUST be non-nil
// (NewBlobKV returns an error otherwise). This is the ONLY constructor that
// yields a value with StageBlob/GetBlob/BindBlob, so a caller that did not
// configure a blob store cannot reach those methods - a compile-time gate.
func NewBlobKV(cfg Config) (*BlobKV, error)

// Transact SHADOWS *KV.Transact to yield a *BlobTx (which adds BindBlob /
// UnbindBlob). Same pinKey + retry semantics as *Cluster.Transact.
func (b *BlobKV) Transact(pinKey []byte, fn func(*BlobTx) error) error

// StageBlob streams r to the FINAL, unit-keyed object key OUTSIDE any
// transaction (no shard lease held). See 11.2.
func (b *BlobKV) StageBlob(ctx context.Context, routeKey []byte, r io.Reader, size int64) (BlobRef, error)

// GetBlob resolves the bref for (routeKey, blobid), decodes the pointer, and
// streams the bytes out. See 11.4.
func (b *BlobKV) GetBlob(ctx context.Context, routeKey []byte, blobid string) (io.ReadCloser, int64, error)

// SweepOrphans reclaims unreferenced blob objects under the units THIS node
// owns. See 11.7.
func (b *BlobKV) SweepOrphans(ctx context.Context, now time.Time, grace time.Duration) error
```

```go
// *BlobTx embeds *Tx (Get/Put/Delete) and adds the two blob-binding ops. They
// are ordinary tx.Put / tx.Delete of the internal bref key, so they co-commit
// with the app's metadata in the SAME single-shard transaction.
type BlobTx struct {
    *Tx
    kv *BlobKV // for the route-key -> bref-key derivation
}

func (bt *BlobTx) BindBlob(ref BlobRef) error
func (bt *BlobTx) UnbindBlob(ref BlobRef) error
```

**Embed-and-shadow compiles cleanly.** `BlobKV` embeds `*KV`, so `BlobKV` has
`*KV.Transact` promoted; defining a method `Transact` directly on `*BlobKV` with
a DIFFERENT signature (`func(*BlobTx)` vs `func(*Tx)`) is allowed in Go - a
method declared on the outer type shadows the promoted one (there is no
overloading conflict because the promoted method is only a candidate when the
outer type declares none). `BlobTx` embeds `*Tx` identically. Confirmed against
the Go embedding rules; no generic-method or interface-satisfaction subtlety
applies (these are concrete types, and `*BlobKV` does NOT need to satisfy a `*KV`
interface - it embeds the concrete `*KV`, so the "narrower Transact" concern in
open question 8 is a non-issue: a shadowed concrete method does not have to keep
the embedded method's signature).

### 11.2 StageBlob (the slow streaming, outside any transaction)

```go
func (b *BlobKV) StageBlob(ctx context.Context, routeKey []byte, r io.Reader, size int64) (BlobRef, error)
```

Mechanics, grounded in the cluster:

1. `unit := b.c.OwnedUnitToken(routeKey)` - derive the UNIT TOKEN the bytes are
   keyed under. This is `genUnitForKey(routeKey)` rendered as a stable string
   (11.5). It uses the SAME `ShardKeyFn` + hash + mask the routed write uses, so
   the blob object lands under the same unit the pointer (and the app's metadata)
   route to. Note: this is a PURE function of `routeKey` + the current
   generation/count - no network, no lease.
2. `blobid, _ := blob.NewBlobID()` - fresh random id (or the app passes a
   content-sha-keyed id later for within-record dedup; out of phase-2 scope).
3. `objkey := blob.FinalKey(unit, blobid)` - `blob/<unit>/<blobid>` (11.5).
4. `err := b.blobs.PutStream(ctx, objkey, r, size)` - streams the SLOW bytes
   straight to the object store. **This goes node -> object store directly; it
   does NOT route through the ring and never crosses a gRPC boundary** (confirmed
   below). No shard lease is held for the multi-second upload.
5. Returns `BlobRef{Unit: unit, BlobID: blobid, Size: size, ContentHash: ...}`.

After StageBlob returns, the bytes are durable at `blob/<unit>/<blobid>` but
UNREFERENCED (no bref points at them). They are invisible to every reader (a
reader reaches bytes only via a committed pointer). A crash here leaves a
shard-local orphan the age-gated sweep reclaims (11.7).

**Confirmed: StageBlob's byte stream never crosses nodes.** `blob.Store`
(MinioBlobStore) is a plain object-store client constructed from endpoint +
creds (backends/slate/blobstore/minio.go); it has NO reference to the ring,
membership, or any peer client. `PutStream` calls `minio.PutObject` directly.
The only thing that routes through the cluster is the small POINTER, via the
existing `Transact` -> `commitCAS` path (remote.go:217), which forwards a tiny
`CommitCASRequest` to the unit owner over gRPC. The multi-megabyte plane is
entirely off the RPC path, exactly as section 2.1 requires.

### 11.3 BindBlob / UnbindBlob (the fast transaction, co-committed)

```go
func (bt *BlobTx) BindBlob(ref BlobRef) error {
    ptr := blob.Pointer{ObjKey: blob.FinalKey(ref.Unit, ref.BlobID), Size: ref.Size, ContentHash: ref.ContentHash}
    enc, err := ptr.Encode()
    if err != nil { return err }
    return bt.Put(brefKey(ref.Unit, ref.BlobID), enc) // an ordinary tx.Put
}

func (bt *BlobTx) UnbindBlob(ref BlobRef) error {
    return bt.Delete(brefKey(ref.Unit, ref.BlobID))   // an ordinary tx.Delete
}
```

`BindBlob` is `tx.Put(brefKey, pointer.Encode())`. Because it is a plain
`tx.Put` on the cluster transaction, it buffers into the SAME write-set as the
app's metadata `tx.Put`s and is shipped in ONE `commitCAS` to the unit owner
(remote.go:457/504/217). The cluster's existing single-shard multi-key atomicity
is the vehicle - no new commit path. The cross-shard guard
(`guardShard`, remote.go:372) enforces that the bref key and the app's metadata
keys all route to the SAME unit; the bref key is constructed (11.5) so they do
(the bref's shard key IS the unit, and the metadata's shard key hashes INTO that
unit). See 11.5 for the precise rule and 11.8 for the one subtlety this raises.

**Reader-atomic create:** the bref key becomes visible only at the transaction's
commit. `GetBlob` resolves through the bref, so it cannot see the blob until the
bind commits, even though the bytes have sat at the final key since StageBlob.

**Atomic delete:** `UnbindBlob` (or an ordinary `tx.Delete(brefKey)`) removes the
pointer in the same transaction that removes the app's metadata. The bytes are
now unreferenced and reclaimed by the sweep. `UnbindBlob` is a convenience over
`tx.Delete(brefKey(...))`; both work because deleting the pointer key is an
ordinary KV delete (section 4.2).

### 11.4 GetBlob (resolve pointer, stream out)

```go
func (b *BlobKV) GetBlob(ctx context.Context, routeKey []byte, blobid string) (io.ReadCloser, int64, error) {
    unit := b.c.OwnedUnitToken(routeKey)
    enc, err := b.c.Get(brefKey(unit, blobid))   // routed read to the unit owner
    if errors.Is(err, backend.ErrNotFound) { return nil, 0, blob.ErrNotFound }
    if err != nil { return nil, 0, err }
    ptr, err := blob.DecodePointer(enc)
    if err != nil { return nil, 0, err }
    return b.blobs.GetStream(ctx, ptr.ObjKey)     // node -> object store directly
}
```

The pointer read is a normal routed `Cluster.Get` (small value, goes to the unit
owner). The byte stream is a direct object-store `GetStream`, off the RPC path.

**ctx lifetime (carry the phase-1 warning forward):** the returned reader streams
lazily; `ctx` MUST outlive the reader (the whole duration the caller pipes the
bytes downstream), not just the `GetBlob` call - minio-go's GetObject binds a
reader goroutine to `ctx` and `Close` cancels it (blobstore/minio.go:86 LIFETIME
note). A caller that scopes `ctx` to "assemble the response" truncates a large
stream mid-flight. The caller MUST `Close` the returned reader.

### 11.5 Object-key layout + the bref shard-key rule (the load-bearing change)

Two internal namespaces, both UNIT-keyed so both are shard-local to the owner:

- **Blob bytes** (object store): `blob/<unit>/<blobid>`
- **Blob pointer** (slatedb value, an internal key on the unit's shard):
  `bref/{<unit>}/<blobid>` (SUPERSEDED by section 12: the pointer key is now
  TOKEN-FREE, `bref/{<routeShard>}/<blobid>`, so reads survive a reshard; the
  object key below is unchanged)

where `<unit>` is a STABLE STRING RENDERING of the routed unit (see below), and
the `{...}` braces around it in the bref key are the Redis-style hash-tag the
default `ring.ShardKey` honors (ring.go:260).

**Why the unit is the object-key prefix (not the raw shard key):** restated from
11.0 item 3 - the node owns units, the sweep enumerates units, the raw shard key
is unbounded. `blob/<unit>/` is a finite per-owned-unit prefix the sweep can list.

**The unit token.** `<unit>` must:
- be derivable from the route key alone, purely (no network), at stage time;
- be stable for the life of the blob (so the pointer's ObjKey and the sweep's
  prefix agree);
- survive a reshard the way the pointer does (11.8).

The natural token is the GenUnit `genUnitForKey(routeKey)` rendered as
`<gen>-<unitID>` (decimal), e.g. `0-13`. It is computed via the SAME
`shardKey` + `HashShardKey` + `resolveGenUnit` the routed write uses
(multibackend.go:161), so the blob's unit and the metadata's unit are identical
by construction. In LEGACY (non-multi) mode there are no units; the token is a
fixed sentinel (e.g. `legacy`) since the single backend owns the whole keyspace
- the sweep there enumerates the one prefix `blob/legacy/` (11.7).

**The bref shard-key rule (so the pointer co-routes with the metadata).** The
bref key is `bref/{<unit>}/<blobid>`. We need `ShardKeyFn(brefKey)` to extract a
shard key that hashes into THE SAME unit `<unit>` denotes - so the bref's
`tx.Put` lands on the same shard as the app's metadata and the cross-shard guard
admits them into one transaction. Two ways, and we pick the robust one:

- The brittle way (rejected): make the bref's shard key a string that
  `HashShardKey` happens to mask back to `<unit>`. There is no such string
  in general (the mask is many-to-one; you cannot invert it), so this does not
  work - you cannot synthesize a shard key that lands on a CHOSEN unit without a
  search. This is why the object key uses the unit directly and the pointer needs
  a different mechanism.

- **The chosen rule: route the bref by the ROUTE KEY's shard, via the app's
  ShardKeyFn, NOT by the literal bref bytes.** Phase 2 makes the bref key carry
  the original route key inside the hash tag, so the SAME `ShardKeyFn` the app
  configured extracts the SAME shard key it extracts for the app's metadata. The
  bref key is `bref/{<routeShardKey>}/<unit>/<blobid>` where `<routeShardKey> =
  ShardKeyFn(routeKey)`. The default `ring.ShardKey` returns the hash-tagged
  portion `<routeShardKey>` (ring.go:277), which hashes to the same unit the
  metadata does. For an app with a CUSTOM ShardKeyFn (hostthis), the app's
  ShardKeyFn must also extract `<routeShardKey>` from a `bref/` key - so phase 2
  REQUIRES one new `ShardKeyFn` case in the app, analogous to its existing
  `pastes/` / `versions/` cases: `bref/{<shardKey>}/...` -> the hash-tagged
  segment. To avoid imposing parsing on every app, phase 2 instead makes shale
  build the bref key with a hash tag AND ships the rule centrally: the cluster's
  `brefKey` is `bref/{` + `<routeShardKey>` + `}/<unit>/<blobid>`, and shale
  documents that an app's ShardKeyFn MUST honor hash tags for `bref/` keys (the
  default `ring.ShardKey` already does; a custom ShardKeyFn adds a leading
  `if HasPrefix(key, "bref/") { return ring.ShardKey(key) }` guard - one line).
  The brace content is `ShardKeyFn(routeKey)`, the EXACT bytes the metadata
  shards on, so they co-route by construction.

  Concretely:

  ```go
  // brefKey builds the internal pointer key for (routeKey, unit, blobid). The
  // hash-tag {sk} carries the ROUTE KEY's shard key so the pointer co-routes
  // with the app's metadata under the SAME unit, and the <unit>/<blobid> tail
  // disambiguates within the shard. sk = the cluster's ShardKeyFn(routeKey).
  func (b *BlobKV) brefKeyFor(routeKey []byte, unit, blobid string) []byte {
      sk := b.c.shardKey(routeKey) // exported-for-blob accessor; same fn metadata uses
      return []byte("bref/{" + string(sk) + "}/" + unit + "/" + blobid)
  }
  ```

  Note BlobRef carries `Unit`, but the bref key needs the route shard key too, so
  BindBlob/UnbindBlob take the route key OR BlobRef carries it. To keep the app
  API (section 6) unchanged (`tx.BindBlob(ref)`), BlobRef carries the route shard
  key it was staged with (11.6); `brefKey` is then a pure function of BlobRef.

This rule means: object bytes keyed by `<unit>` (finite sweep prefix), pointer
keyed by the route shard key inside a hash tag (co-routes with metadata). The two
keys carry DIFFERENT prefixes on purpose - the byte plane needs unit-granular
enumeration, the pointer needs metadata co-location, and a single key shape
cannot give both.

```go
// pkg/blob (re-cut helpers; replaces the phase-1 shardKey-taking versions):
const FinalPrefix = "blob/"
func FinalKey(unit, blobid string) string      { return FinalPrefix + unit + "/" + blobid }
func FinalPrefixForUnit(unit string) string    { return FinalPrefix + unit + "/" }
```

### 11.6 BlobRef + the Store-port modtime extension

```go
// pkg/cluster - BlobRef is the opaque token StageBlob returns and Bind/Unbind/
// Get consume. It carries everything brefKey + the pointer need, so the app
// never sees objkeys or the pointer record. shale never persists it; the
// persisted reference is the blob.Pointer the bref holds. (A caller MAY
// persist refs to drive crash-recovery unstaging - see 13.6 for the
// round-trip contract that creates.)
type BlobRef struct {
    Unit        string // the routed unit token <gen>-<unitID> (or "legacy")
    RouteShard  []byte // ShardKeyFn(routeKey) at stage time - drives brefKey co-routing
    BlobID      string // the blob.NewBlobID minted at stage time
    Size        int64  // the stored (possibly compressed) byte length
    ContentHash string // OPTIONAL app-supplied hash of the original bytes; carried into Pointer
}
```

**Store-port extension for the sweep's age-gate (the GAP).** The phase-1 `List`
yields only keys; the sweep (section 10.2) needs each object's `LastModified` to
age-gate. The cleaner of the two options in the brief is to make List carry the
modtime (one round of listing already returns it from S3/MinIO's `ListObjects`,
so no extra round-trips - a separate `Stat` per object would be N extra HEADs).
Phase 2 changes `List` to yield a small struct:

```go
// pkg/blob - replaces List(ctx, prefix) iter.Seq2[string, error]:
type ObjectInfo struct {
    Key      string
    Size     int64
    ModTime  time.Time // the object store's LastModified - the age-gate signal
}

// List yields every object under prefix with its size + modtime. iter.Seq2 so a
// listing error surfaces per-item and the caller can stop early.
List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]
```

The MinioBlobStore adapter already iterates `minio.ListObjects`, whose
`ObjectInfo` carries `Key`, `Size`, and `LastModified` - the adapter maps those
three fields (a near-zero change to blobstore/minio.go:136). `Stat(objkey) ->
(size, modtime, err)` is the rejected alternative: it would force one HEAD per
listed object in the sweep, turning an O(objects) listing into O(objects)
network HEADs. List-carries-modtime is strictly cheaper and is the canonical
sweep input.

### 11.7 SweepOrphans (age-gated, unit-local, owner-only)

```go
func (b *BlobKV) SweepOrphans(ctx context.Context, now time.Time, grace time.Duration) error {
    for _, unit := range b.c.OwnedUnits() {            // bounded set; new accessor (11.0 item 5)
        prefix := blob.FinalPrefixForUnit(unit)        // blob/<unit>/
        for obj, err := range b.blobs.List(ctx, prefix) {
            if err != nil { return err }
            blobid := path.Base(obj.Key)               // strip blob/<unit>/
            // Is there a live pointer? LocalGet, not routed Get: this node OWNS
            // the unit, so the bref lives locally; a routed Get would needlessly
            // round-trip and, mid-reshard, could chase the moved owner.
            _, gerr := b.c.LocalGet(b.brefKeyForUnit(unit, blobid))
            if gerr == nil { continue }                // referenced -> keep
            if !errors.Is(gerr, backend.ErrNotFound) { return gerr }
            // Unreferenced. Age-gate: only reclaim if the OBJECT is older than
            // grace, so a just-staged-not-yet-bound blob is never swept (10.2).
            if now.Sub(obj.ModTime) < grace { continue }
            if derr := b.blobs.Delete(ctx, obj.Key); derr != nil { return derr }
        }
    }
    return nil
}
```

Key points, grounded:

- **`OwnedUnits()` is the new public accessor** (11.0 item 5). It returns the
  string unit tokens this node currently owns: in multi-backend mode it wraps
  `desiredGenUnits()` rendered as `<gen>-<unitID>`; in legacy mode it returns the
  single sentinel `["legacy"]` (the node owns the whole keyspace, so the one
  `blob/legacy/` prefix). This makes the sweep mode-agnostic.
- **The bref existence check is `LocalGet`, not routed `Get`** (cluster.go:1018).
  The sweep runs ON the owner of the unit, so the bref - if it exists - is in
  this node's local backend. `LocalGet` reads it directly, bypassing ring
  routing, which is both faster and correct during a reshard window (a routed Get
  would chase the new owner of a unit mid-handoff and could see a transient
  not-found that the age-gate already protects against anyway).
- **Age-gate via `obj.ModTime`** (section 10.2): a pointer-less object younger
  than `grace` (default ~1h) is a possible in-flight stage; older is a genuine
  orphan. The object store's `LastModified` is durable + survives restarts, so no
  in-memory debounce is needed.
- **`Delete` is idempotent** (blob.go Delete contract): a double-sweep or a race
  with a concurrent unbind+manual-delete is harmless.
- **Cadence + cost** (open question 3): the caller (a background loop in the
  shaled run-loop, or an explicit admin trigger) sets the cadence; each pass is
  bounded by the owned-unit count times the per-unit object count. Phase 2 ships
  `SweepOrphans` as a single callable pass; the scheduling loop is a thin wrapper
  the cmd binary owns (deferred to the integration, like the cmd-side wiring).
- **SHIPPED SHAPE + THE SINGLE-SNAPSHOT TOPOLOGY GUARD (supersedes the sketch
  above).** The implementation enumerates MOUNTED units (not desired ownership -
  the earlier P0) and protects objects via a referenced-object-key set scanned
  from the node's local brefs, all documented on `BlobKV.SweepOrphans` itself.
  The guard this section now REQUIRES: the pass captures the mounted-unit set
  ONCE, BEFORE the referenced scan, sweeps ONLY that snapshot, and ABORTS
  fail-closed if the mounted set has changed by the end of the referenced scan
  OR if any membership transition (Joining/Draining) is in flight. Without it
  the referenced set and the object enumeration are two reads of a MOVING mount
  view: a unit acquired between them is enumerated for objects with NONE of its
  brefs in the referenced set, so every bound blob under it that is older than
  the grace is deleted - committed-data loss (observed live: a >3h-old bound
  canary blob deleted mid-rollout while its metadata and pointer stayed
  intact). A skipped pass is a storage leak retried next tick; a torn pass is
  data loss - fail closed.

### 11.8 Reshard interaction (pointer moves with the unit; bytes stay put)

When a shard (unit) moves to a new owner, two things must remain consistent: the
pointer and the bytes.

- **The pointer (`bref/{<routeShard>}/<unit>/<blobid>`) moves with the slatedb
  unit** via the existing rebalance/handoff machinery (pkg/rebalance,
  multibackend handoff). It is an ordinary slatedb value on the unit's shard, so
  it is carried in the unit's key range exactly like the app's metadata. No blob-
  specific reshard code.

- **The bytes (`blob/<unit>/<blobid>`) do NOT move.** They sit in SHARED object
  storage under a unit-derived prefix, reachable from every node. After the unit
  moves, the NEW owner's `OwnedUnits()` now includes `<unit>`, so the NEW owner's
  `SweepOrphans` enumerates `blob/<unit>/` and the NEW owner's `LocalGet` resolves
  the (now-local) bref - ownership of the byte sweep transfers automatically
  because the prefix is UNIT-derived, not node-derived. This is exactly the
  property section 5 + open question 7 require, and the unit-keyed object layout
  (11.5) is what makes it hold: a node-keyed or raw-shardKey-keyed object prefix
  would NOT transfer cleanly.

- **The reshard EDGE CASE - unit doubling (split). SUPERSEDED by section 12 for
  the READ PATH.** The "ancestor-prefix union" sketched below was a SWEEP-side
  patch and was never shipped (11.12); it does not address the read-path miss
  (#471) where `GetBlob` recomputed a NEW-generation token and missed the bref
  written under the old token. Section 12 fixes that at the root by making the
  pointer key TOKEN-FREE (`bref/{<routeShard>}/<blobid>`), so the recomputed
  lookup key is reshard-invariant. The description below is retained for the
  OBJECT-key half (the bytes stay under their old-token prefix and are resolved
  via the verbatim `Pointer.ObjKey`, which section 12 preserves) and as the
  historical record of the bug.
  unit doubling (`UnitForHash` doubling-compatible, unitmap.go:40): unit `u` under
  count N splits into two children under 2N, and the GenUnit token changes
  (`<gen>-<u>` at gen g becomes `<g+1>-<low>` / `<g+1>-<high>`). Existing bytes
  were staged under `blob/<g>-<u>/...` (the OLD token), but the pointer (carrying
  `Pointer.ObjKey`) ALSO holds the old token verbatim, so `GetBlob` still resolves
  the bytes at their old key after the split - the ObjKey in the pointer is the
  ground truth, the recomputed token is only used at STAGE time. The SWEEP,
  however, recomputes the CURRENT token from `OwnedUnits()` and would enumerate
  `blob/<g+1>-<low>/`, MISSING the pre-split `blob/<g>-<u>/` objects. Phase 2
  handles this by having `OwnedUnits()` return BOTH the current-generation tokens
  AND the still-referenced ancestor tokens whose key range this node now owns
  (the same union `desiredGenUnits` already computes across the cut-over - it
  returns the gen-(g+1) children for cut-over units; phase 2 extends the SWEEP's
  unit set to also include the parent token while any object remains under it).
  The simplest correct rule: the sweep enumerates, for each owned current unit,
  BOTH `blob/<currentToken>/` and the chain of ancestor tokens that mask into it,
  until a pass finds an ancestor prefix empty. This is bounded (the doubling chain
  is short) and is the ONLY reshard-specific blob code. Flagged as the one
  non-trivial reshard interaction; see 11.10 open questions.

### 11.9 What changed from the section-4 / phase-1 decisions, and why

| Decision (section 4 / phase 1) | Phase-2 reality | Why |
| --- | --- | --- |
| `New(meta)` / `NewWithBlobs(meta, b)` | `Open(cfg)` exists; add `Config.BlobStore` + `New(cfg) *KV` / `NewBlobKV(cfg) *BlobKV` wrappers | The real constructor is `Open(Config)`; there is one `*Cluster`. Wrappers carry the capability-in-the-type. |
| `Transact(shardKey, func(*Tx))` | `Transact(pinKey []byte, func(tx backend.Transaction))`; wrappers re-expose `func(*Tx)` / `func(*BlobTx)` | The cluster's `Transact` hands the `backend.Transaction` interface; the concrete tx type is unexported. |
| object key `blob/<shardKey>/<blobid>`; `FinalKey(shardKey, blobid)`; `FinalPrefixForShard(shardKey)` | object key `blob/<unit>/<blobid>`; `FinalKey(unit, blobid)`; `FinalPrefixForUnit(unit)` | The node owns UNITS (bounded), not raw shard keys (unbounded). The sweep needs a finite per-owner prefix. THE load-bearing change. |
| pointer key `bref/<shardKey>/<blobid>`, "routed by the same ShardKeyFn" | pointer key `bref/{<routeShardKey>}/<unit>/<blobid>`, route shard key in a hash tag | You cannot synthesize a shard key that masks back to a chosen unit; instead carry the route key's shard key in a hash tag so the SAME ShardKeyFn co-routes the pointer with the metadata. Apps with a custom ShardKeyFn add one `bref/` -> hash-tag case. |
| `List(prefix) iter.Seq2[string, error]` | `List(prefix) iter.Seq2[ObjectInfo, error]` (Key+Size+ModTime) | The sweep age-gate needs `LastModified`; carrying it in the listing avoids a HEAD per object. |
| (implicit) some way to enumerate owned shards | new `*Cluster.OwnedUnits()` accessor | No exported owned-unit accessor exists; `desiredGenUnits()` is internal + multi-only. |
| `BlobHandle` (opaque token, section 4) | `BlobRef{Unit, RouteShard, BlobID, Size, ContentHash}` | Section 10.1 already collapsed BlobHandle into the pointer; the staged ref must additionally carry the unit + route shard for brefKey + sweep. Still opaque to the app. |

### 11.10 Phase 2 vs phase 3 (hostthis) split

**Phase 2 IMPLEMENTS (this design, in shale):**
- `Config.BlobStore` field + the `New(cfg) *KV` / `NewBlobKV(cfg) *BlobKV`
  constructors and the `*KV` / `*Tx` / `*BlobKV` / `*BlobTx` wrapper types.
- `StageBlob` / `GetBlob` / `BindBlob` / `UnbindBlob` and `BlobRef`.
- The unit-keyed object layout (`FinalKey(unit, ...)` / `FinalPrefixForUnit`),
  the `brefKey` rule, and `*Cluster.OwnedUnits()` / `OwnedUnitToken()` /
  exported `shardKey` accessor.
- The `blob.Store.List` modtime extension (`ObjectInfo`) and its MinioBlobStore
  mapping.
- `SweepOrphans` as a single callable pass, including the reshard ancestor-prefix
  union (11.8).
- Tests: reader-atomic-create (bytes staged, bref not yet committed -> GetBlob
  is not-found; after commit -> resolves), atomic-delete (unbind + metadata
  delete in one tx -> bref gone, sweep reclaims bytes), crash-injection (stage
  then no bind -> sweep reclaims after grace, not before), the embed-and-shadow
  compile + dispatch, and a multi-node integration test that StageBlob on one
  node, binds the pointer that routes to ANOTHER node, and GetBlobs from a third
  - proving the byte plane stays off the RPC path while the pointer routes. A
  memory `blob.Store` fake (with a settable per-object modtime) backs the unit
  tests; the MinIO integration test reuses the phase-1 harness.

**Phase 2 DEFERS to phase 3 (hostthis integration):**
- The cmd-side wiring of the concrete `MinioBlobStore` into
  `cmd/shaled-slate` + the sweep scheduling loop in the shaled run-loop (phase 2
  ships `SweepOrphans` callable; the cadence/loop is operator wiring).
- hostthis rewriting its blob path to stream through `*BlobKV`, adding the one
  `bref/` -> hash-tag case to `shaleShardKey`, and retiring its detached S3 blob
  store + the crash-orphan reconcile + the site reservation (section 7).
- Within-record dedup (StageBlob skipped when `Has(content-keyed objkey)`):
  needs the app to key a blob by content sha (section 6.2); phase 2 leaves
  `NewBlobID`-keyed blobs only.
- Streaming compression / the tee (section 6): an app concern; `BlobRef`
  already carries the optional `ContentHash` slot for the app's sha.
- The one-time migration of existing bucket blobs into shale-managed
  objects+pointers (section 9 phase 4).

### 11.11 Open questions phase 2 could not fully close from the code

1. **Reshard ancestor-prefix sweep bound (11.8).** The union of current + ancestor
   unit prefixes the sweep must enumerate across a doubling is correct in
   principle, but the EXACT termination rule ("until an ancestor prefix lists
   empty") needs a test against a real mid-reshard cluster to confirm there is no
   window where a parent prefix is non-empty yet unowned by this node. The
   `desiredGenUnits` union already models the ownership side; the byte-prefix side
   is new. Recommend a dedicated reshard+blob integration test in phase 2 and, if
   it proves fiddly, deferring blob-during-reshard to a follow-up (steady-state
   blobs are the common case; a reshard is an explicit, rare op).

2. **`OwnedUnits()` during the handoff window.** `desiredGenUnits()` returns what
   this node SHOULD own; mid-handoff the physical mount may lag. For the SWEEP
   this is safe (a not-yet-mounted unit's `LocalGet` returns not-found, and the
   age-gate prevents premature deletion), but it is worth a test that a unit
   mid-acquire does not get its just-arrived-but-not-yet-bound blobs swept. The
   age-gate should cover it; confirm.

3. **Empty-value constraint on the pointer.** `Cluster.Put` rejects an empty
   value (`ErrEmptyValue`, cluster.go:1424). `Pointer.Encode()` always produces a
   non-empty JSON envelope, so BindBlob is safe, but this should be pinned by a
   test (a zero-value Pointer must still encode to non-empty bytes) so a future
   pointer-schema change cannot accidentally produce an empty value that the
   commit silently rejects.

4. **Legacy-mode unit sentinel collision.** Using `"legacy"` as the single unit
   token assumes no app ships a real unit token literally named `legacy`; since
   tokens are `<gen>-<unitID>` (always containing a `-`), `legacy` cannot collide.
   Confirmed safe by construction, but noted so the rendering rule
   (`<gen>-<unitID>`, never a bare word) is treated as load-bearing.

### 11.12 Implementation deltas (phase-2 build + P0 review fix)

The build diverged from the section-11 sketch in a few grounded ways; the code is
the source of truth, these are the notable ones:

- **The sweep enumerates MOUNTED units, not desired units.** The original sketch
  had `SweepOrphans` iterate `OwnedUnits()` (desired-from-ring) while the
  referenced-pointer set (`referencedObjKeys`) scans `LocalScanPrefix("bref/")`
  (MOUNTED backends only). An adversarial review found that mismatch is a P0
  data-loss path: a desired-but-not-yet-mounted unit (cold boot, or the
  rebalance-acquire window) has its blob objects listable from shared storage but
  its pointers NOT locally visible, so its already-bound blobs look unreferenced
  and age past the grace and get deleted. Fix: the sweep now uses
  `Cluster.MountedUnits()` (derived from the mount map), so the object-list loop
  and the pointer scan read the SAME ownership view. A desired-but-unmounted unit
  is simply skipped this pass (a missed sweep is a storage leak, never data loss,
  since `GetBlob` still resolves via the stored pointer's `ObjKey`). Pinned by
  `TestSweepOrphans_OnlySweepsMountedUnits` (an object under a non-mounted unit is
  never reclaimed even when old + unreferenced).
- **The reshard-ancestor machinery (`OwnedUnits` + `ancestorUnitTokens`) was
  removed**, not shipped: it was unexercised, and reshard-era reclamation of
  pre-split objects under old-gen prefixes is a documented leak-only follow-up,
  not a data-loss path. (Mid-reshard `GetBlob` still resolves via the pointer.)
- **`OwnedUnitToken` -> `RoutedUnitToken`** (it returns the routed token for any
  key with no ownership check; the old name implied a check that does not happen).
- **The referenced-set sweep is more robust to reshard than the 11.7 per-object
  `LocalGet` sketch**: it compares each listed object key byte-exactly against the
  set of `ptr.ObjKey` from the local bref scan (the pointer is ground truth), so a
  token change from a doubling never causes a bound object to be swept.
- **`BlobRef.RouteShard` is `[]byte`** (not the sketch's `string`): `ShardKeyFn`
  returns `[]byte` cluster-wide, so `brefKey` builds the hash tag byte-exact with
  no conversion. `brefKey` contract: `RouteShard` must not contain a `}` byte
  (`ring.ShardKey` stops at the first `}`); benign for slug/id route keys.
- **The in-memory `blob.Store` fake lives at `pkg/blob/blobmem`** (exported, not
  `internal/`), so an app's tests (hostthis, phase 3) can use it to unit-test blob
  paths without standing up MinIO.

## 12. Token-free bref key (reshard-transparent pointer, #471)

### 12.1 The bug the section-11 bref shape caused

Section 11.5 keyed the blob POINTER as `bref/{<routeShard>}/<unit>/<blobid>`,
embedding the routed unit token (`<gen>-<unitID>`, e.g. `0-13`) as a tail
segment. That token segment couples the pointer key to the CURRENT generation,
which breaks reads across a reshard:

1. `BindBlob` writes the bref at `bref/{<routeShard>}/<old-token>/<blobid>` where
   `<old-token>` is `RoutedUnitToken(routeKey)` at bind time (e.g. `0-13` in a
   16-unit gen-0 cluster).
2. A reshard (split `N -> 2N` or merge `2N -> N`) copies the unit's keyspace to
   its child / survivor BYTE-VERBATIM: `ScanPrefix(nil)` over every parent key,
   `tx.Put` each key UNCHANGED (the live overlap dual-write and the finalize
   copy both do this). The `{<routeShard>}` hash tag routes the bref to the
   correct child / survivor, so the pointer arrives intact - but still keyed with
   the OLD token.
3. `finalizeReshard` advances the cluster generation to `{gen+1, nextCount}`. Now
   `RoutedUnitToken(routeKey)` returns the NEW token (e.g. `1-5`).
4. `GetBlob` rebuilds the lookup key as `bref/{<routeShard>}/<new-token>/<blobid>`
   -> MISS -> `blob.ErrNotFound`. The pointer is present (under the old-token key)
   and the bytes never moved (they are at `blob/<old-token>/<blobid>`, found via
   the stored `Pointer.ObjKey`), but the bref KEY the reader reconstructs is
   unreachable. The app metadata survives because its key (`pastes/<slug>`)
   carries no token.

This is the same generation-coupling section 11.8 flagged as "the one
non-trivial reshard interaction" and 11.12 deferred as a leak-only follow-up for
the SWEEP. For the SWEEP it is leak-only (the stored ObjKey is ground truth); for
the READ PATH it is a hard miss (the lookup key is recomputed from the live
generation, not read from the pointer). The section-11.8 "ancestor-prefix union"
idea was the SWEEP-side patch; it does NOT help the read path and the
ancestor machinery was removed (11.12). The clean fix is to remove the coupling
at its root: drop the token from the pointer KEY.

### 12.2 The fix: the bref key is token-free

The pointer key becomes:

```
bref/{<routeShard>}/<blobid>
```

The `<unit-token>` tail segment is GONE. The `{<routeShard>}` hash tag already
routes the pointer to the SAME survivor / child the metadata routes to (the byte-
verbatim copy carries it there), and `<blobid>` already disambiguates within the
shard, so the unit token was redundant disambiguation that only coupled the key
to the generation. With it removed, `BindBlob`, `UnbindBlob`, and `GetBlob` build
the IDENTICAL key before and after any reshard (the key is a pure function of the
route shard + blob id, neither of which a reshard changes), so reads and deletes
are reshard-transparent.

`BlobRef.RouteShard` and `BlobRef.BlobID` are all `brefKey` consumes; the
`BlobRef.Unit` field stays (StageBlob still needs it for the OBJECT key, below)
but is no longer part of the pointer key.

### 12.3 What does NOT change (the byte location is owned solely by the Pointer)

- **The OBJECT key keeps the token: `blob/<unit-token>/<blobid>`.** `StageBlob`
  is UNCHANGED. The bytes still live under a unit-token prefix so the orphan sweep
  has one finite prefix per MOUNTED unit to enumerate (11.7); a token-free object
  key would force the sweep back to a global `blob/` scan. The byte LOCATION is
  owned SOLELY by the persisted `Pointer.ObjKey` (which carries the verbatim
  object key the bytes were staged under, including the stage-time token), NOT by
  any recomputation: `GetBlob` reads the token-free bref, decodes the Pointer, and
  streams `ptr.ObjKey` verbatim. So bytes staged at `blob/<old-token>/<blobid>`
  before a reshard are still found after it - the recomputed token is used ONLY at
  STAGE time, never at read time. This is the same ObjKey-is-ground-truth property
  11.12 already relies on for the sweep, now also the read path's invariant.
- **The reshard COPY stays byte-verbatim.** Do NOT re-key the bref inside the
  split / merge copy. The copy runs WITHOUT a write-freeze while live
  `BindBlob` / `UnbindBlob` dual-writes flow to both generations keyed verbatim;
  re-keying a bref in the copy would split one logical pointer across two keys, so
  apply-if-newer (LWW) could not order a create against a concurrent delete - a
  resurrected or lost delete. The fix is the KEY SHAPE only: a token-free key is
  carried correctly by the existing byte-verbatim copy with ZERO copy-path
  changes. This is why the fix lives in the key derivation, not the copy.
- **Delete safety.** An `UnbindBlob` tombstone is itself written at the token-free
  key, so it is reshard-transparent identically to a create: a delete issued
  before a reshard, or a read issued after one, both resolve the same key. A
  delete-after-reshard stays deleted under standard apply-if-newer LWW (token-free
  reads see the token-free tombstone). This holds for BOTH split (`N -> 2N`) and
  merge (`2N -> N`).

### 12.4 NO in-code fallback; legacy data needs an offline migration (deploy precondition)

The running code understands ONLY token-free brefs. There is deliberately NO
read-fallback to the old token-ful key, no parent-token walk, no
migrate-forward-on-read, no dual-format decode. A fallback would re-introduce the
generation coupling the fix removes (the fallback key still embeds a token to
guess) and would have to guess across the doubling chain.

Consequence: a cluster that already holds POINTERS in the legacy token-ful
format (`bref/{<routeShard>}/<unit>/<blobid>`, e.g. prod gen-0 data written by
the section-11 code) cannot serve them after deploying this code - the new
`GetBlob` looks under the token-free key and misses. Converting that data is a
ONE-TIME, OFFLINE MIGRATION the OPERATOR runs as a DEPLOY PRECONDITION:

1. Scale the app down (no live writers, so no dual-write races the migration).
2. Re-key every `bref/{<routeShard>}/<unit>/<blobid>` entry to
   `bref/{<routeShard>}/<blobid>` (strip the `<unit>` segment; the value - the
   encoded Pointer, carrying the verbatim ObjKey - is unchanged, so the bytes
   stay where they are).
3. Bring the app up on this code.

That migration is an OPERATOR-SIDE deliverable; it lives in the private infra
repo (a one-off `down` / `migrate` / `up` runbook + tooling), NOT in shale. shale
ships only the token-free product code + tests; it does not carry migration code,
a legacy-format reader, or any operator runbook. A deployment that skips the
offline migration will 404 its pre-existing blobs (the bytes and pointers are
intact, only the lookup key shape differs) until the re-key is run; a deployment
with no pre-existing token-ful brefs (a fresh cluster) needs no migration.

### 12.5 Supersedes

This section supersedes the bref KEY SHAPE in 11.5 / 11.9 (`bref/{<routeShard>}/
<unit>/<blobid>` -> `bref/{<routeShard>}/<blobid>`) and resolves the read-path
half of the reshard interaction 11.8 / 11.11 flagged. The OBJECT key shape
(`blob/<unit-token>/<blobid>`, 11.5) and the sweep's mounted-unit enumeration
(11.7 / 11.12) are UNCHANGED.

## 13. UnstageBlob (exact-ref reclamation, no scan, no quiescence)

### 13.1 Motivation

The orphan sweep (11.7) is age-gated, scan-based, and refuses to run whenever
membership is in transition or the mount view moves mid-pass. Those refusals are
correct for a mechanism that acts on ABSENCE (delete what nothing references),
but they make the sweep a poor fit for the one caller that KNOWS which staged
bytes it abandoned: a crash-recovery path that recorded its staged refs on a
durable intent before binding. Such a caller holds an exact list; making it wait
for cluster quiescence to reclaim bytes it can name is backwards.

`UnstageBlob` is the presence-acting complement: delete the object ONE ref
names. No enumeration, no referenced-set scan, no transition gate.

### 13.2 API

```go
// on BlobKV, beside StageBlob
func (b *BlobKV) UnstageBlob(ctx context.Context, ref BlobRef) error
```

Semantics, in order:

0. **Ref validation (fail loudly).** A ref whose Unit, BlobID, or RouteShard is
   missing - or whose RouteShard carries a `}` (the brefKey hash-tag contract) -
   is rejected with `blob.ErrInvalidRef` before any read. Persisted refs
   re-enter here after a round-trip through the caller's serialization; a ref
   that silently lost RouteShard would guard-read a DIFFERENT key than BindBlob
   wrote, turning a bound ref into a false "unbound". Deletion keys are derived
   from the ref, so this is the only place a lossy round-trip can be caught.
1. **Bound-ref guard (fail closed, quorum-floored).** Read `brefKey(ref)`
   through the cluster. If a pointer EXISTS, the blob is bound: committed
   metadata references these bytes, and deleting them is committed-data loss
   discovered at read time. Refuse with the typed sentinel `blob.ErrBound`. If
   the read fails with anything other than not-found, return that error WITHOUT
   deleting (cannot verify -> do not destroy).

   The guard read does NOT inherit `cfg.ReadConsistency`. At R>1 it runs at
   ReadQuorum minimum (ReadAll stays ReadAll) AND requires absence to be
   witnessed by a full read quorum of not-found legs (`guardGet`). Two distinct
   holes force this beyond a naive `Get`:
   - Under ReadNearest (the library default) the first answering leg wins; a
     replica lagging behind a WriteQuorum-acked bind answers not-found and the
     holders' values are dropped - a false "unbound" in steady state.
   - The gather loop concludes not-found from WHATEVER legs answered; with
     holders unreachable, a single miss would carry a quorum read too. The
     guard's sweep therefore refuses sub-quorum absence (errSubQuorumAbsence,
     surfaced as a retryable guard-read error, never as not-found).
   Both are acceptable staleness for an ordinary read (the next read heals);
   for a delete they are data loss, hence the dedicated read path. Single-copy
   modes (R=1, legacy, forwarded) already answer authoritatively-or-refuse
   (acquiring windows, fence guards), so plain Get is sound there.
2. **Delete the object.** `Store.Delete(ctx, blob.FinalKey(ref.Unit,
   ref.BlobID))` - the same key StageBlob wrote (ref.Unit is the stage-time
   token, carried verbatim, so this is reshard-transparent the same way GetBlob's
   ptr.ObjKey is). Store.Delete is idempotent (missing object is a no-op), so a
   recovery that re-runs a partially-unstaged list converges.

The guard is a single routed read, not a scan: brefKey is a pure function of
ref.RouteShard + ref.BlobID (section 12). As long as BlobIDs are unique-minted
at stage time, a pointer found there can only describe THIS blob; a future
content-keyed dedup id (11.6 leaves the door open) would add a FALSE-REFUSE
mode - two stagings of identical content sharing a bref key - which is
leak-side and sweep-backstopped, never loss-side.

### 13.3 The race window, and what the caller must actually build

The guard is check-then-delete: a BindBlob committing between the pointer read
and the object delete would bind bytes that are about to vanish. No lock closes
this inside shale (the pointer commit and the object delete live in different
stores).

The exclusion is the caller's, and for the crash-recovery caller this section
exists for, it is NOT automatic. Recovery is BY DESIGN a different process
reading refs a (presumed-)crashed writer persisted - and the presumed-dead
writer may be alive (partition, GC pause, wedged-then-resumed pod) and still
able to bind while recovery unstages the same ref. "Do not bind and unstage
the same ref concurrently" therefore requires a FENCE, not good intentions:

- An OWNERSHIP RECORD carries an ownership epoch (or lease). It must be
  CO-SHARDED with the bref (same route key), because shale's transaction is
  single-shard; the durable intent itself may shard elsewhere (a consumer
  that shards intents by owner identity for node-local boot scans keeps that
  layout and adds a small per-route-key ownership record instead - the epoch
  lives where the bind can see it, the intent keeps its home).
- BindBlob runs inside a transaction that CO-COMMITS a check of that
  ownership record: read it in the same Transact, abort if recovery has taken
  it over. shale's single-shard transaction gives this atomicity today; no
  new machinery is needed.
- Recovery bumps the ownership epoch FIRST (taking ownership), then unstages.
  A resumed writer's subsequent bind aborts on the ownership check instead of
  racing the delete.

A caller without such a fence has a real, if narrow, loss window; the API doc
states the requirement rather than implying the guard closes it.

### 13.4 Interaction with the sweep and UnbindBlob

- After `UnbindBlob` (pointer deleted), the bytes are again staged-but-
  unreferenced; `UnstageBlob` on the original ref succeeds and reclaims them
  immediately, without waiting for a sweep pass. Unbind-then-unstage is the
  scan-free deletion path for a caller that kept the ref.
- The sweep remains the backstop for refs NOBODY recorded (crash before the
  intent write). The two mechanisms are disjoint by construction: unstage acts
  on a recorded presence, the sweep on a computed absence.

### 13.5 Errors

Three caller-visible outcomes, three distinct shapes, all beside
`blob.ErrNotFound` in pkg/blob:

- `blob.ErrBound`: the ref has a live pointer; unstage refused. Recovery
  treats it as a SKIP (the blob got bound after all; drop it from the list).
- `blob.ErrInvalidRef`: a key-forming field is missing or malformed; retrying
  the same ref cannot succeed. A caller bug or a lossy persistence
  round-trip - surface it, do not retry.
- anything else (wrapped): the guard read or the delete failed transiently;
  RETRY the ref later. Includes the sub-quorum-absence refusal.

The refusal never crosses the wire: UnstageBlob is client-side composition (a
routed pointer read + a direct object-store delete on the calling node), so
`errors.Is` identity needs no wire decode. The one property that does cross -
the pointer read's not-found identity through peer forwarding - is the same
pre-existing property GetBlob's blob.ErrNotFound mapping already load-bears.

### 13.6 The ref persistence contract

Sections 11.6 / the BlobRef doc say shale never persists a BlobRef - still
true. But this feature's caller DOES: the durable intent records refs so
recovery can unstage them. That makes the ref's fields a de-facto persistence
contract:

- Unit, RouteShard, BlobID MUST round-trip byte-exact; UnstageBlob derives
  both the guard key and the object key from them. Size / ContentHash are not
  consumed by unstage.
- A MISSING field is caught by validation (ErrInvalidRef). A CORRUPTED-but-
  present RouteShard is NOT detectable - the guard would read a wrong key and
  conclude "unbound". Callers serializing refs by hand (rather than encoding
  the struct whole) own that risk.
