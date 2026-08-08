package blob

import (
	"context"
	"errors"
	"io"
	"iter"
	"time"
)

// ErrNotFound is returned by Store reads (GetStream) when the requested
// object does not exist. It is the package's single not-found sentinel, mapped
// from the underlying object store's NoSuchKey/NoSuchBucket response, mirroring
// how the slate ConditionalStore adapter maps NoSuchKey to its own sentinel.
// Delete deliberately does NOT surface this: deleting a missing object is a
// no-op, not an error (see Store.Delete).
var ErrNotFound = errors.New("blob: object not found")

// ErrBound is returned by UnstageBlob when the ref's pointer exists: the blob
// is committed metadata's referenced bytes, and deleting them would be
// committed-data loss surfaced only at read time. Callers branch with
// errors.Is; the recovery path treats it as "this ref was bound after all,
// drop it from the unstage list".
var ErrBound = errors.New("blob: ref is bound; unstage refused")

// SizeUnknown is the size sentinel a caller passes to PutStream when the length
// of the reader is not known up front. The adapter then streams the bytes via a
// multipart upload that does not need the total size in advance. Prefer passing
// the real (possibly compressed) length whenever it is known: a known size lets
// the object store choose a single-shot or sized multipart PUT.
const SizeUnknown int64 = -1

// Store is the streaming-bytes-to-object-storage port: the OPTIONAL byte
// plane of shale's value-separation model (the small transactional POINTER that
// references a blob is a separate, metadata-store concern - see package doc).
//
// It is a pure interface (no I/O, no object-store types) so the domain and the
// future *BlobKV split depend only on this seam; the concrete object-store
// adapter (MinioStore) is the infrastructure layer. All methods are
// STREAMING: neither PutStream nor GetStream may buffer a whole blob in memory.
//
// objkey is a fully-qualified object key in the store's namespace (built by the
// FinalKey helper in this package); the adapter does not prepend any prefix of
// its own beyond an optional bucket-internal KeyPrefix.
type Store interface {
	// PutStream streams r to the object at objkey. size is the number of bytes
	// r will yield (the compressed length when the caller compresses), letting
	// the object store choose a single-shot or sized multipart PUT. Pass
	// SizeUnknown (-1) when the length is not known up front: the adapter then
	// uses a streaming multipart upload that does not need the total in advance.
	// PutStream returns only after the object is durably written.
	PutStream(ctx context.Context, objkey string, r io.Reader, size int64) error

	// GetStream opens the object at objkey for STREAMING reads. It returns a
	// ReadCloser the caller streams from (it is NEVER read fully into memory by
	// this method) and the object's total size in bytes. The caller MUST Close
	// the reader. A missing object returns ErrNotFound.
	GetStream(ctx context.Context, objkey string) (rc io.ReadCloser, size int64, err error)

	// Delete removes the object at objkey. It is IDEMPOTENT: deleting an object
	// that does not exist is a no-op and returns nil, not ErrNotFound. This is
	// what the same-shard orphan-bytes sweep (phase 2) relies on so a double
	// delete or a delete of an already-reclaimed object is harmless.
	Delete(ctx context.Context, objkey string) error

	// Has reports whether an object exists at objkey, without transferring its
	// bytes. The future within-record dedup (phase 3) skips staging when the
	// content-keyed blob already exists.
	Has(ctx context.Context, objkey string) (bool, error)

	// List yields every object under prefix, each as an ObjectInfo (Key + Size
	// + ModTime), for the same-shard orphan-bytes sweep (phase 2). The sweep
	// age-gates a pointer-less object on its ModTime (the object store's
	// LastModified), so List MUST carry it; the object store's listing already
	// returns Key/Size/LastModified together, so no per-object HEAD is needed
	// (design section 11.6). It is an iter.Seq2 so the caller can stop early and
	// so a listing error surfaces per-item rather than buffering the whole
	// listing; once a non-nil error is yielded the sequence ends.
	List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]
}

// ObjectInfo is the per-object metadata List yields: enough for the orphan
// sweep to identify an object (Key), report its footprint (Size), and age-gate
// it (ModTime). It is the object store's own listing record mapped onto the
// fields the sweep needs - no extra round-trip beyond the listing itself.
type ObjectInfo struct {
	// Key is the object key in the same key space the other Store methods use
	// (the adapter strips any internal KeyPrefix), so a key from List feeds
	// straight back into Has / Delete.
	Key string
	// Size is the stored object byte length.
	Size int64
	// ModTime is the object store's LastModified timestamp - the durable,
	// per-object, restart-surviving signal the sweep age-gates on (a
	// pointer-less object younger than the grace may be an in-flight stage and
	// is kept; older is a genuine orphan). Design sections 10.2 + 11.6.
	ModTime time.Time
}
