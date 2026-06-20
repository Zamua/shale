package cluster

// kv.go is the capability-split surface over *Cluster (design section 11.1).
//
// shale composes two storage concerns: the small-value transactional KV plane
// (always present) and the streaming blob byte plane (OPTIONAL). The blob
// capability is made visible IN THE TYPE so that calling a blob method without
// having configured a blob store is a COMPILE error, not a runtime nil-check:
//
//   - *KV     wraps a metadata-only cluster. Get / Put / Delete / Transact.
//   - *BlobKV is a SUPERSET of *KV (it embeds *KV) and adds the blob ops. It
//     exists ONLY via NewBlobKV, which requires Config.BlobStore, so a caller
//     that did not configure a blob store cannot obtain a *BlobKV and therefore
//     cannot reach StageBlob / GetBlob / BindBlob.
//
// A single *Cluster with a nilable blob field could not give this property
// (every method would exist on the one type regardless of configuration). The
// idiomatic Go answer is "states as distinct types": the blob-configured
// cluster is a distinct type that HAS the blob methods (section 4).
//
// The wrappers are THIN: they hold a *Cluster and forward to its existing
// Get / Put / Delete / Transact. The cluster itself is unchanged; the
// capability split lives entirely here.

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/blob"
)

// KV is the metadata-only cluster surface: the small-value transactional KV
// plane with no blob capability. It is the type a consumer that needs only
// metadata takes. *BlobKV embeds *KV, so a consumer that accepts a *KV also
// accepts a *BlobKV (the superset relationship).
type KV struct {
	c *Cluster
}

// New wraps a metadata-only cluster. cfg.BlobStore MUST be nil: a configured
// blob store with the plain *KV surface is a wiring mistake (the byte plane is
// unreachable through *KV, so the bytes would be written but never bindable or
// readable). New returns an error if cfg.BlobStore != nil, surfacing the
// mistake at construction rather than silently dropping the capability. Use
// NewBlobKV when a blob store is configured.
func New(cfg Config) (*KV, error) {
	if cfg.BlobStore != nil {
		return nil, errors.New("cluster: New got a non-nil Config.BlobStore; the *KV surface cannot reach the blob plane, use NewBlobKV")
	}
	c, err := Open(cfg)
	if err != nil {
		return nil, err
	}
	return &KV{c: c}, nil
}

// Cluster returns the underlying *Cluster. It is the escape hatch for the
// cluster surface the wrappers do not re-expose (membership, admin, debug);
// the routed KV ops a consumer needs day to day are forwarded directly.
func (kv *KV) Cluster() *Cluster { return kv.c }

// Get returns the value for key (a routed read to the owning unit).
func (kv *KV) Get(key []byte) ([]byte, error) { return kv.c.Get(key) }

// Put writes key=value (a routed write to the owning unit). An empty value is
// rejected (ErrEmptyValue), as on the underlying cluster.
func (kv *KV) Put(key, value []byte) error { return kv.c.Put(key, value) }

// Delete removes key (a routed delete to the owning unit).
func (kv *KV) Delete(key []byte) error { return kv.c.Delete(key) }

// Close shuts down the underlying cluster.
func (kv *KV) Close() error { return kv.c.Close() }

// Transact runs fn inside a single-shard transaction pinned to pinKey, with the
// SAME pin + retry semantics as Cluster.Transact (a retryable commit re-runs fn
// from scratch; a conflict retries up to the budget). The *Tx handed to fn
// exposes Get / Put / Delete buffered into one atomic commit. fn returning a
// non-nil error aborts the transaction (no commit).
func (kv *KV) Transact(pinKey []byte, fn func(*Tx) error) error {
	return kv.c.Transact(pinKey, func(btx backend.Transaction) error {
		return fn(&Tx{tx: btx})
	})
}

// Tx is the metadata-only transaction handed to a KV.Transact closure. It wraps
// the cluster transaction (the backend.Transaction interface; the concrete
// cluster transaction type is unexported, so the wrapper holds the interface).
// Its Get / Put / Delete buffer into the one atomic commit. *BlobTx is its
// blob-capable superset (it embeds *Tx).
type Tx struct {
	tx backend.Transaction
}

// Get reads key within the transaction's snapshot.
func (t *Tx) Get(key []byte) ([]byte, error) { return t.tx.Get(key) }

// Put buffers key=value into the transaction's write-set.
func (t *Tx) Put(key, value []byte) error { return t.tx.Put(key, value) }

// Delete buffers a delete of key into the transaction's write-set.
func (t *Tx) Delete(key []byte) error { return t.tx.Delete(key) }

// BlobKV is the blob-capable cluster surface: a SUPERSET of *KV (it embeds *KV,
// inheriting Get / Put / Delete / Close and the metadata Transact) that
// SHADOWS Transact to yield the richer *BlobTx and adds the streaming blob ops
// (StageBlob / GetBlob / SweepOrphans, landing in the implementation phase).
// Because it embeds *KV, any consumer that needs only metadata accepts a
// *BlobKV too.
//
// Embed-and-shadow compiles cleanly: a method named Transact declared directly
// on *BlobKV with a different signature (func(*BlobTx) vs func(*Tx)) shadows the
// promoted *KV.Transact - the promoted method is only a candidate when the outer
// type declares none, so there is no overloading conflict (design section 11.1).
type BlobKV struct {
	*KV
	blobs blob.Store
}

// NewBlobKV wraps a blob-configured cluster. cfg.BlobStore MUST be non-nil:
// NewBlobKV returns an error otherwise, since the whole point of *BlobKV is the
// reachable byte plane. This is the ONLY constructor that yields a value with
// the blob methods, so a caller that did not configure a blob store cannot
// reach them - the compile-time capability gate (design section 11.1).
func NewBlobKV(cfg Config) (*BlobKV, error) {
	if cfg.BlobStore == nil {
		return nil, errors.New("cluster: NewBlobKV requires a non-nil Config.BlobStore; use New for a metadata-only cluster")
	}
	c, err := Open(cfg)
	if err != nil {
		return nil, err
	}
	return &BlobKV{KV: &KV{c: c}, blobs: cfg.BlobStore}, nil
}

// Transact SHADOWS *KV.Transact to yield a *BlobTx (which adds BindBlob /
// UnbindBlob over the embedded *Tx). Same pinKey + retry semantics as
// Cluster.Transact. The blob-binding ops are ordinary tx.Put / tx.Delete of the
// internal bref key, so they co-commit with the app's metadata in the SAME
// single-shard transaction.
func (b *BlobKV) Transact(pinKey []byte, fn func(*BlobTx) error) error {
	return b.c.Transact(pinKey, func(btx backend.Transaction) error {
		return fn(&BlobTx{Tx: &Tx{tx: btx}, kv: b})
	})
}

// BlobTx is the blob-capable transaction handed to a BlobKV.Transact closure.
// It embeds *Tx (Get / Put / Delete) and adds the two blob-binding ops. They are
// ordinary tx.Put / tx.Delete of the internal bref key, so they co-commit with
// the app's metadata in the same atomic, single-shard commit.
type BlobTx struct {
	*Tx
	kv *BlobKV // for the route-key -> bref-key derivation (and the blob store, later)
}

// BindBlob attaches a staged blob to the metadata being committed, atomically:
// it writes the small blob.Pointer (referencing the already-staged final
// object) at the internal bref key, co-committed with the app's metadata in the
// SAME transaction. After commit the blob is referenced (hence reader-visible);
// before it, the staged bytes are unreachable to any reader (reader-atomic
// create, design section 11.3).
//
// It is an ordinary tx.Put of the bref key, so it buffers into the SAME
// write-set as the app's metadata Puts and ships in one commit to the unit
// owner. The bref key derives from ref via brefKey, whose hash tag carries the
// route shard key, so the pointer routes to the SAME unit as the metadata and
// the cluster's cross-shard guard admits them into one transaction (the caller
// MUST pin the Transact on a key that routes to that same unit). ref MUST come
// from StageBlob (which populates Unit + RouteShard); a zero-value ref produces
// a malformed bref key.
func (bt *BlobTx) BindBlob(ref BlobRef) error {
	ptr := blob.Pointer{
		ObjKey:      blob.FinalKey(ref.Unit, ref.BlobID),
		Size:        ref.Size,
		ContentHash: ref.ContentHash,
	}
	enc, err := ptr.Encode()
	if err != nil {
		return err
	}
	// Pointer.Encode always yields a non-empty JSON envelope, so this Put never
	// trips the cluster's empty-value rejection (design section 11.11 item 3).
	return bt.Put(brefKey(ref), enc)
}

// UnbindBlob removes a bound blob's pointer in the SAME transaction that removes
// the app's metadata (atomic delete, design section 11.3). The now-unreferenced
// bytes are reclaimed by the same-shard orphan sweep. It is a convenience over
// tx.Delete(brefKey(ref)); both work because deleting the pointer key is an
// ordinary KV delete.
func (bt *BlobTx) UnbindBlob(ref BlobRef) error {
	return bt.Delete(brefKey(ref))
}

// StageBlob streams r to the FINAL, unit-keyed object key OUTSIDE any
// transaction (no shard lease held for the multi-second upload) and returns the
// opaque BlobRef the binding transaction consumes (design section 11.2).
//
// The bytes go node -> object store DIRECTLY (b.blobs is a plain object-store
// client with no ring/peer reference); they never cross a gRPC boundary. Only
// the small pointer (committed later by BindBlob) routes through the cluster.
//
// After StageBlob returns the bytes are durable at blob/<unit>/<blobid> but
// UNREFERENCED: no bref points at them, so they are invisible to every reader (a
// reader reaches bytes only via a committed pointer). A crash here leaves a
// unit-local orphan the age-gated sweep reclaims (SweepOrphans).
//
// routeKey is the app key whose shard the blob co-locates with (its slug / id);
// StageBlob derives the unit + the route shard from it purely (no network).
// size is the number of bytes r will yield (the compressed length when the app
// compresses), or blob.SizeUnknown (-1) when not known up front.
func (b *BlobKV) StageBlob(ctx context.Context, routeKey []byte, r io.Reader, size int64) (BlobRef, error) {
	unit := b.c.RoutedUnitToken(routeKey)
	blobid, err := blob.NewBlobID()
	if err != nil {
		return BlobRef{}, err
	}
	// Capture the route shard key at stage time so brefKey is a pure function of
	// the ref later (BindBlob / UnbindBlob / GetBlob reconstruct the same key
	// without re-resolving routing). Copy it: c.shardKey may return a sub-slice
	// of routeKey, which the caller could mutate after StageBlob returns.
	sk := b.c.shardKey(routeKey)
	routeShard := append([]byte(nil), sk...)

	objkey := blob.FinalKey(unit, blobid)
	if err := b.blobs.PutStream(ctx, objkey, r, size); err != nil {
		return BlobRef{}, err
	}
	return BlobRef{
		Unit:       unit,
		RouteShard: routeShard,
		BlobID:     blobid,
		Size:       size,
	}, nil
}

// GetBlob resolves the bref for (routeKey, blobid), decodes the pointer, and
// streams the bytes out (design section 11.4). The pointer read is a normal
// routed Cluster.Get (small value, to the unit owner); the byte stream is a
// direct object-store GetStream, OFF the RPC path.
//
// A missing bref (the blob was never bound, or was unbound/deleted) maps to
// blob.ErrNotFound, the same not-found shape the caller already handles for a
// missing object.
//
// ctx LIFETIME: the returned reader streams lazily, so ctx MUST outlive the
// reader (the whole duration the caller pipes the bytes downstream), not just
// the GetBlob call - the object-store adapter binds a reader goroutine to ctx
// and Close cancels it. A caller that scopes ctx to "assemble the response"
// truncates a large stream mid-flight (design section 11.4, blobstore/minio.go
// LIFETIME note). The caller MUST Close the returned reader.
func (b *BlobKV) GetBlob(ctx context.Context, routeKey []byte, blobid string) (io.ReadCloser, int64, error) {
	unit := b.c.RoutedUnitToken(routeKey)
	ref := BlobRef{Unit: unit, RouteShard: b.c.shardKey(routeKey), BlobID: blobid}
	enc, err := b.c.Get(brefKey(ref))
	if errors.Is(err, backend.ErrNotFound) {
		return nil, 0, blob.ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	ptr, err := blob.DecodePointer(enc)
	if err != nil {
		return nil, 0, err
	}
	return b.blobs.GetStream(ctx, ptr.ObjKey)
}

// SweepOrphans reclaims unreferenced blob objects under the units this node has
// MOUNTED (design section 11.7). It enumerates ONLY mounted units (MountedUnits,
// NOT desired ownership) so the object-listing loop and the referenced-pointer
// scan (referencedObjKeys, a scan of this node's MOUNTED brefs) read the SAME
// ownership view: a unit that is DESIRED but not yet MOUNTED (cold boot / mid
// rebalance-acquire) is never swept, so its bound-but-not-locally-visible blobs
// are never mis-classified as unreferenced and deleted (the P0 fix). The cost is
// that a desired-but-unmounted unit is skipped this pass - a missed sweep is a
// storage leak, never data loss, since GetBlob still resolves via the stored
// pointer's ObjKey. Reshard-era objects sitting under an OLD-generation token
// prefix (after a doubling re-keys a unit) are likewise a documented leak-only
// follow-up, NOT reclaimed here. It is a single callable pass; the scheduling
// loop / cadence is operator wiring the cmd binary owns (deferred to phase 3).
//
// Mechanics, per mounted unit (blob/<unit>/):
//
//   - Build the set of OBJECT KEYS this node's local pointers reference, by
//     scanning the local bref/ keys and decoding each pointer's ObjKey. This is
//     shard-LOCAL (the node holds only the brefs for units it owns), exact, and
//     fail-closed; it does NOT require reconstructing a point bref key from an
//     object key (which is impossible: the object key blob/<unit>/<blobid>
//     carries no route shard, but the bref key embeds the route shard in its
//     hash tag - see design section 11.5). Using the pointer's own ObjKey as
//     ground truth also survives a reshard token change (the pointer holds the
//     verbatim ObjKey, design section 11.8).
//   - List blob/<unit>/. An object whose key is NOT in the referenced set is a
//     candidate orphan.
//   - AGE-GATE: reclaim a candidate only if its object-store ModTime is older
//     than now.Add(-grace), so a just-staged-not-yet-bound blob (recent
//     ModTime) is never swept (design section 10.2 / 11.7). A genuine
//     crash-orphan ages out past grace and is reclaimed.
//
// FAIL-CLOSED: any List or local-scan error aborts the pass with the error and
// reclaims NOTHING further (it keeps the bytes - a missed sweep is a storage
// leak, never data loss, since GetBlob still resolves via the stored pointer).
// Delete is idempotent (a double sweep or a race with a manual delete is
// harmless).
func (b *BlobKV) SweepOrphans(ctx context.Context, now time.Time, grace time.Duration) error {
	// The referenced-object set is computed ONCE per pass from the node's local
	// pointers; it covers every unit this node has MOUNTED (the local scan sees
	// only the brefs for mounted units), the SAME view MountedUnits enumerates
	// below. Fail-closed: a scan error keeps every object.
	referenced, err := b.referencedObjKeys()
	if err != nil {
		return err
	}
	cutoff := now.Add(-grace)
	for _, unit := range b.c.MountedUnits() {
		prefix := blob.FinalPrefixForUnit(unit)
		for obj, lerr := range b.blobs.List(ctx, prefix) {
			if lerr != nil {
				return lerr // fail-closed: keep the bytes
			}
			if _, ok := referenced[obj.Key]; ok {
				continue // a live pointer references it -> keep
			}
			// Unreferenced. Only reclaim once the object is older than the grace,
			// so an in-flight stage (recent) is never swept.
			if !obj.ModTime.Before(cutoff) {
				continue
			}
			if derr := b.blobs.Delete(ctx, obj.Key); derr != nil {
				return derr // fail-closed: stop on the first delete error
			}
		}
	}
	return nil
}

// referencedObjKeys returns the set of blob object keys this node's LOCAL
// pointers reference: it scans the local bref/ keyspace and decodes each
// pointer's ObjKey. The node holds only the brefs for the units it owns, so the
// scan is shard-local (bounded by this node's pointer count), NOT a global
// cross-shard scan. It is the exact, fail-closed input to the orphan sweep: an
// object is orphan-eligible only if its key is absent from this set (design
// section 11.7).
//
// A pointer that fails to decode cannot have its ObjKey read, so we cannot tell
// which object it protects; rather than risk deleting an object a
// malformed-but-present pointer references, a decode error fails the whole scan
// closed (the sweep keeps every object).
func (b *BlobKV) referencedObjKeys() (map[string]struct{}, error) {
	it, err := b.c.LocalScanPrefix([]byte(brefPrefix))
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	referenced := make(map[string]struct{})
	for {
		k, v, err := it.Next()
		if err != nil {
			return nil, err
		}
		if k == nil {
			break
		}
		ptr, derr := blob.DecodePointer(v)
		if derr != nil {
			return nil, derr // fail-closed: an undecodable pointer aborts the scan
		}
		referenced[ptr.ObjKey] = struct{}{}
	}
	return referenced, nil
}

// BlobRef is the opaque token StageBlob returns and Bind / Unbind / Get consume.
// It carries everything brefKey + the persisted blob.Pointer need, so the app
// never sees object keys or the pointer record (design section 11.6). It is NOT
// persisted - the persisted reference is the blob.Pointer the bref holds; the
// BlobRef is a transient handle that threads a staged blob from StageBlob into
// the binding transaction.
type BlobRef struct {
	// Unit is the routed storage-unit token (<gen>-<unitID>, e.g. "0-13", or
	// the "legacy" sentinel). It is the object-key prefix the bytes live under
	// (blob/<unit>/<blobid>) and the finite per-owner prefix the orphan sweep
	// enumerates.
	Unit string
	// RouteShard is ShardKeyFn(routeKey) captured at stage time. It drives the
	// bref key's hash tag so the pointer co-routes with the app's metadata under
	// the same unit (design section 11.5). Carried on the ref so brefKey is a
	// pure function of the ref and the app API stays tx.BindBlob(ref).
	RouteShard []byte
	// BlobID is the blob.NewBlobID minted at stage time (or a content-sha-keyed
	// id later for within-record dedup).
	BlobID string
	// Size is the stored (possibly compressed) byte length.
	Size int64
	// ContentHash is an OPTIONAL app-supplied hash of the ORIGINAL bytes, carried
	// verbatim into the blob.Pointer. Empty when the app supplies none.
	ContentHash string
}

// brefKey builds the internal pointer key for ref:
//
//	bref/{<routeShard>}/<unit>/<blobid>
//
// The hash tag {<routeShard>} carries the ROUTE KEY's shard key so the default
// ring.ShardKey (and an app's ShardKeyFn that honors hash tags for bref/ keys)
// extracts the SAME shard key it extracts for the app's metadata - so the
// pointer co-routes with the metadata under the same unit and the cross-shard
// guard admits them into one transaction. The <unit>/<blobid> tail
// disambiguates within the shard (design section 11.5).
//
// It is a pure function of the BlobRef (which carries both RouteShard and Unit),
// so BindBlob / UnbindBlob / GetBlob derive the key without re-resolving routing.
//
// CONTRACT: ref.RouteShard MUST NOT contain a '}' byte. The default
// ring.ShardKey reads the hash tag up to the FIRST '}', so an embedded '}' in the
// route shard would truncate the extracted tag and mis-route the pointer away
// from the app's metadata shard. This is benign for slug / id route keys (no
// '}'); no validation is enforced here, this is the documented caller contract.
func brefKey(ref BlobRef) []byte {
	out := make([]byte, 0, len(brefPrefix)+len(ref.RouteShard)+len(ref.Unit)+len(ref.BlobID)+4)
	out = append(out, brefPrefix...)
	out = append(out, '{')
	out = append(out, ref.RouteShard...)
	out = append(out, '}', '/')
	out = append(out, ref.Unit...)
	out = append(out, '/')
	out = append(out, ref.BlobID...)
	return out
}

// brefPrefix is the namespace root for the internal blob-pointer keys. An app's
// custom ShardKeyFn MUST honor hash tags for keys under this prefix (the default
// ring.ShardKey already does) so the pointer co-routes with the app's metadata.
const brefPrefix = "bref/"
