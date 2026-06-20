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
