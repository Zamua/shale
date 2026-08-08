package blob

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned by Store reads (GetStream) when the requested
// object does not exist. It is the package's single not-found sentinel, mapped
// from the underlying object store's NoSuchKey/NoSuchBucket response, mirroring
// how the slate ConditionalStore adapter maps NoSuchKey to its own sentinel.
// Delete deliberately does NOT surface this: deleting a missing object is a
// no-op, not an error (see Store.Delete).
var ErrNotFound = errors.New("blob: object not found")

// ErrBound is returned when unstaging is refused because the ref's pointer
// exists: the bytes are referenced by committed metadata, and deleting them
// would be committed-data loss surfaced only at read time. Callers branch with
// errors.Is; a recovery path treats it as "this ref was bound after all, drop
// it from the unstage list" - a skip, not a failure.
var ErrBound = errors.New("blob: ref is bound; unstage refused")

// ErrInvalidRef is returned when unstaging is refused because a key-forming
// ref field (unit, route shard, blob id) is missing or malformed. Distinct
// from both ErrBound (skip) and transient errors (retry): it means the
// caller's persisted ref did not round-trip faithfully, and retrying the same
// ref cannot succeed.
var ErrInvalidRef = errors.New("blob: invalid ref; unstage refused")

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
}
