// Package blob is the streaming byte plane of shale's value-separation model
// (docs/design/blob-values.md): large opaque blobs live as objects in the same
// object store slatedb already runs on, while a small transactional POINTER
// referencing them lives as an ordinary slatedb value on the owning shard. The
// LSM never holds blob bytes; only the pointer routes through the cluster, and
// the multi-megabyte byte I/O goes node -> object store directly.
//
// # Phase 1 scope (this package)
//
// This package is ONLY the low-level byte PORT plus its pure domain types:
//
//   - Store: the streaming object-byte PORT (PutStream / GetStream / Delete /
//     Has). Pure interface, no object-store types. Reads map a missing
//     object to ErrNotFound; Delete is idempotent (deleting a missing object is a
//     no-op). PutStream and GetStream are strictly streaming and never buffer a
//     whole blob in memory.
//   - Domain types (pure, no I/O): Pointer (the small versioned record phase 2
//     persists as a slatedb value, with Encode/DecodePointer) and the
//     object-key layout helpers (FinalKey, NewBlobID).
//
// The single object-store ADAPTER that implements Store (a MinIO/S3 client
// reusing the slate ConditionalStore's minio-go pattern) does NOT live here: it
// lives in the slate backend module at backends/slate/blobstore. The core module
// stays free of minio-go so the memory-backed reference binary pulls in no
// object-store client; this package holds only the port + the pure types.
//
// # Object-key model: stage to the final key (no staging namespace)
//
// Blob bytes are streamed DIRECTLY to their final, UNIT-keyed object key
// (FinalKey) at upload time - there is no separate staging namespace and no
// promotion/rename step. The caller knows the route key (hence the owning
// storage unit) at upload time, so it writes to blob/<unit>/<blobid> straight
// away, and the binding transaction only commits the small Pointer. Bytes
// sitting at the final key before the pointer commits are unreachable to any
// reader (a reader reaches them only via committed metadata -> pointer), so a
// crash before the bind leaves a UNIT-LOCAL orphan. Reclamation is the
// caller's, by exact ref (UnstageBlob over a durably recorded intent); bytes
// whose ref was never recorded anywhere are an accepted bounded leak
// (section 11.7).
//
// # Explicitly DEFERRED to phase 2 (the cluster capability split)
//
//   - The *KV / *BlobKV / *BlobTx capability-in-the-type split (blobs as an
//     optional, compile-time-gated cluster capability).
//   - StageBlob + BindBlob: streaming bytes to the final shard-keyed key and
//     co-committing the Pointer with the app's metadata in one per-shard
//     transaction (the reader-atomic create).
//
// # Explicitly DEFERRED to phase 3
//
//   - hostthis integration: rewriting hostthis's blob path to stream through
//     *BlobKV and retiring its detached S3 blob store, reconcile, and reservation.
//
// The DDD layering: Store is the port; Pointer + the key helpers are pure
// domain types with no I/O; the MinIO adapter (in backends/slate/blobstore) is
// the single infrastructure implementation.
package blob
