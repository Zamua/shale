// Package slate implements backend.Backend on top of SlateDB, an
// embedded LSM-tree KV store with object-storage durability. Use this
// backend when you want durable, replication-friendly KV storage that
// lives on S3-compatible object storage (AWS S3, MinIO, R2, GCS via
// S3-compat, etc.) rather than local disk.
//
// Build/runtime requirements: this package is gated behind the
// `slatedb` build tag because it depends on the cgo-backed
// slatedb.io/slatedb-go binding. The default `go build ./...` (with
// CGO_ENABLED=0) excludes this package so consumers who only want the
// memory backend pay no cgo cost. To build with this backend:
//
//	CGO_ENABLED=1 \
//	CGO_LDFLAGS="-L/path/to/slatedb/target/release" \
//	DYLD_LIBRARY_PATH=/path/to/slatedb/target/release \
//	go build -tags slatedb ./...
//
// # Object store configuration
//
// SlateDB's underlying OpenDAL/object_store crate reads AWS_* process
// env vars; it does not honor URL query parameters. New() writes the
// relevant env vars from Config before calling ObjectStoreResolve.
// Don't open two Slate backends pointing at different buckets in the
// same process; the env-var writes would collide. A construction whose
// endpoint/region/credentials/SSL mode conflicts with what this process
// already applied fails fast with a config-conflict error (envguard.go)
// rather than silently clobbering the earlier env.

//go:build slatedb

package slate

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"

	"github.com/Zamua/shale/pkg/backend"
)

// Slate is a SlateDB-backed Backend. It owns the underlying *slatedb.Db
// and *slatedb.ObjectStore; both are released on Close.
//
// writeOpts, when non-nil, is applied per-call to PutWithOptions /
// DeleteWithOptions on the db and to CommitWithOptions on transactions.
// Nil routes through the plain Put/Delete/Commit calls (slatedb's
// internal default = AwaitDurable=true), keeping behavior byte-exact
// with the pre-WriteOptions slate backend for callers that don't opt in.
// See Config.WriteOptions doc comment for the durability semantics.
type Slate struct {
	db        *slatedb.Db
	store     *slatedb.ObjectStore
	writeOpts *slatedb.WriteOptions
}

// New opens a SlateDB instance backed by the configured object store.
// The caller must Close() the returned *Slate to flush + shut down
// cleanly. See package doc for the env-var caveat around running two
// Slate instances in one process.
//
// If cfg.Settings is non-nil, it is applied to the DbBuilder before
// Build (pass-through, no merging with shale defaults). Nil falls
// back to slatedb's own defaults.
//
// If cfg.WriteOptions is non-nil, every Put/Delete on the returned
// *Slate routes through PutWithOptions/DeleteWithOptions with the
// supplied value, and every transaction Commit routes through
// CommitWithOptions. Nil leaves the plain Put/Delete/Commit paths
// in place (slatedb default AwaitDurable=true).
func New(cfg Config) (*Slate, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.applyEnv(); err != nil {
		return nil, err
	}

	url := "s3://" + cfg.Bucket + "/"
	store, err := slatedb.ObjectStoreResolve(url)
	if err != nil {
		return nil, fmt.Errorf("slate: resolve object store %q: %w", url, err)
	}
	db, err := buildDb(cfg.DbName, store, cfg.Settings, cfg.Cache)
	if err != nil {
		store.Destroy()
		return nil, fmt.Errorf("slate: open db %q: %w", cfg.DbName, err)
	}
	return &Slate{db: db, store: store, writeOpts: cfg.WriteOptions}, nil
}

// NewWithStore opens a SlateDB instance against an already-resolved
// ObjectStore. Useful for tests that want to point at a non-S3 store
// (e.g. "memory:///") without touching the AWS_* env vars. The Slate
// instance takes ownership of the store and will Destroy it on Close.
//
// settings, if non-nil, is forwarded verbatim to the DbBuilder before
// Build (same pass-through semantics as Config.Settings in New).
func NewWithStore(dbName string, store *slatedb.ObjectStore, settings ...*slatedb.Settings) (*Slate, error) {
	if len(settings) > 1 {
		return nil, errors.New("slate: at most one Settings may be passed")
	}
	var s *slatedb.Settings
	if len(settings) == 1 {
		s = settings[0]
	}
	return NewWithStoreOpts(dbName, store, s, nil)
}

// NewWithStoreOpts is the long-form NewWithStore that also accepts a
// per-write *WriteOptions (same pass-through semantics as
// Config.WriteOptions in New). Nil writeOpts leaves the plain
// Put/Delete/Commit paths in place; non-nil routes every Put/Delete to
// the binding's *WithOptions APIs and every transaction Commit to
// CommitWithOptions.
//
// Same test-targeted constructor as NewWithStore: useful for memory:///
// stores without the AWS_* env-var dance. New is the production entry
// point.
func NewWithStoreOpts(dbName string, store *slatedb.ObjectStore, settings *slatedb.Settings, writeOpts *slatedb.WriteOptions) (*Slate, error) {
	return NewWithStoreCache(dbName, store, settings, writeOpts, nil)
}

// NewWithStoreCache is NewWithStoreOpts plus an explicit slatedb DbCache
// (the SST block + metadata cache) handed to the DbBuilder via
// WithDbCache. nil cache leaves slatedb's default (no block cache: every
// read re-fetches SST blocks from the store). Same store-ownership and
// pass-through semantics as NewWithStoreOpts; useful for memory:/// stores
// in tests that want to exercise the cache path.
func NewWithStoreCache(dbName string, store *slatedb.ObjectStore, settings *slatedb.Settings, writeOpts *slatedb.WriteOptions, cache *slatedb.DbCache) (*Slate, error) {
	if dbName == "" {
		return nil, errors.New("slate: dbName required")
	}
	if store == nil {
		return nil, errors.New("slate: store required")
	}
	db, err := buildDb(dbName, store, settings, cache)
	if err != nil {
		return nil, fmt.Errorf("slate: open db %q: %w", dbName, err)
	}
	return &Slate{db: db, store: store, writeOpts: writeOpts}, nil
}

// defaultPutOptions returns the no-op PutOptions slate hands to
// PutWithOptions when WriteOptions is set: TtlDefault means "no
// override," equivalent to plain Db.Put on the TTL axis. We need to
// pass *something* because the binding's PutWithOptions takes both a
// PutOptions and a WriteOptions; the WriteOptions carries the
// durability knob shale exposes, the PutOptions stays at defaults.
func defaultPutOptions() slatedb.PutOptions {
	return slatedb.PutOptions{Ttl: slatedb.TtlDefault{}}
}

// buildDb runs the DbBuilder pipeline shared by New + NewWithStore.
// settings is forwarded verbatim to WithSettings if non-nil; absent
// settings, slatedb's own defaults apply. cache, if non-nil, is handed
// to WithDbCache so the SST block + metadata cache is shared by this Db
// (and any other Db built with the same handle); nil leaves slatedb's
// default (no block cache - every read re-fetches SST blocks from the
// object store).
func buildDb(dbName string, store *slatedb.ObjectStore, settings *slatedb.Settings, cache *slatedb.DbCache) (*slatedb.Db, error) {
	// slatedb's open (DbBuilder.Build) does object-store I/O with NO internal
	// timeout, so a stalled request against a busy/degraded distributed object
	// store hangs the open FOREVER. That is catastrophic for the multi-backend
	// cold start, which opens many unit DBs SEQUENTIALLY (cluster mount loop):
	// one stalled open wedges the whole Open and the node never becomes ready.
	// (Observed on a 3-node distributed MinIO under load; a single-backend node
	// opens one DB so it almost never hits it, which is why this lay latent.)
	//
	// Bound the open: if it does not complete within openTimeout, return an
	// error so the caller (e.g. the cluster mount loop) can fail cleanly and the
	// process restart + re-mount from a fresh state - far better than hanging.
	//
	// RETRY policy (v0.8 Phase 2g): a RETURNED transient error - "empty SSTable",
	// a too-small SST/WAL object that a momentarily-overloaded object store can
	// hand back under a concurrent-open CPU spike - IS retried (bounded, with
	// backoff), so a transient read-truncation self-heals instead of degrading
	// the position. A TIMEOUT is NEVER retried: the stalled un-cancellable Build
	// goroutine is still live, and a re-open of the same dbName would race its
	// writer-epoch fence. A genuinely corrupt object returns the same error every
	// attempt and falls through after the retries, unchanged.
	return buildDbRetry(realOpenAttempt, dbName, store, settings, cache)
}

// openAttemptFunc does ONE timeout-bounded slatedb open, returning (db, err,
// timedOut). Injected into buildDbRetry so the retry control flow is unit-tested
// without a real slatedb open.
type openAttemptFunc func(dbName string, store *slatedb.ObjectStore, settings *slatedb.Settings, cache *slatedb.DbCache) (*slatedb.Db, error, bool)

// realOpenAttempt is the production openAttemptFunc: the timeout-bounded
// DbBuilder pipeline.
func realOpenAttempt(dbName string, store *slatedb.ObjectStore, settings *slatedb.Settings, cache *slatedb.DbCache) (*slatedb.Db, error, bool) {
	r, timedOut := runWithTimeout(openTimeout(), func() buildResult {
		db, err := openDbBlocking(dbName, store, settings, cache)
		return buildResult{db: db, err: err}
	})
	return r.db, r.err, timedOut
}

// buildDbRetry opens dbName via attempt, retrying a RETURNED transient error up
// to openRetries() extra times with backoff. A timeout (still-running open) is
// surfaced immediately without retry; a non-transient error returns immediately;
// a transient error retries until it clears or the budget is exhausted (then the
// last error is returned, so degraded boot sees the underlying "empty SSTable").
func buildDbRetry(attempt openAttemptFunc, dbName string, store *slatedb.ObjectStore, settings *slatedb.Settings, cache *slatedb.DbCache) (*slatedb.Db, error) {
	retries := openRetries()
	var lastErr error
	for i := 0; ; i++ {
		db, err, timedOut := attempt(dbName, store, settings, cache)
		if timedOut {
			return nil, fmt.Errorf("slate: open %q timed out after %s (slatedb open stalled on object-store I/O)", dbName, openTimeout())
		}
		if err == nil {
			return db, nil
		}
		lastErr = err
		if !isTransientOpenError(err) || i >= retries {
			return nil, lastErr
		}
		time.Sleep(openRetryBackoff(i))
	}
}

// isTransientOpenError reports whether a RETURNED slatedb open error is a
// transient read-truncation worth retrying. "empty SSTable" is slatedb's report
// of a too-small SST/WAL object, which a momentarily-overloaded/CPU-throttled
// object store can return under a concurrent-open burst (the 2026-06-18 root
// cause). A genuinely corrupt object yields the same error on every attempt and
// thus exhausts the retries -> degraded boot, unchanged.
func isTransientOpenError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "empty SSTable")
}

// defaultOpenRetries is the number of EXTRA open attempts on a transient error.
const defaultOpenRetries = 3

// openRetries is the transient-open retry budget, overridable via
// SLATE_DB_OPEN_RETRIES (a non-negative int). 0 disables retry.
func openRetries() int {
	if v := strings.TrimSpace(os.Getenv("SLATE_DB_OPEN_RETRIES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return defaultOpenRetries
}

// openRetryBackoff is the sleep before retry attempt i (0-indexed): 200ms,
// 400ms, 800ms ... capped at 2s, to let a momentarily-overloaded store recover.
// A package var so tests can zero it for speed.
var openRetryBackoff = func(i int) time.Duration {
	d := 200 * time.Millisecond << uint(i)
	if d > 2*time.Second || d <= 0 {
		d = 2 * time.Second
	}
	return d
}

// buildResult carries the (db, err) pair across the runWithTimeout boundary.
type buildResult struct {
	db  *slatedb.Db
	err error
}

// openDbBlocking is the actual (blocking, un-cancellable) DbBuilder pipeline.
func openDbBlocking(dbName string, store *slatedb.ObjectStore, settings *slatedb.Settings, cache *slatedb.DbCache) (*slatedb.Db, error) {
	builder := slatedb.NewDbBuilder(dbName, store)
	defer builder.Destroy()
	if settings != nil {
		if err := builder.WithSettings(settings); err != nil {
			return nil, fmt.Errorf("apply settings: %w", err)
		}
	}
	if cache != nil {
		if err := builder.WithDbCache(cache); err != nil {
			return nil, fmt.Errorf("apply db cache: %w", err)
		}
	}
	db, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// defaultDbOpenTimeout bounds a single slatedb DB open. Generous enough that a
// healthy open never trips it (a cold open is sub-second to a few seconds even
// against object storage), tight enough that a stalled open fails fast so the
// caller can restart + retry instead of hanging forever.
const defaultDbOpenTimeout = 25 * time.Second

// openTimeout is the per-open bound, overridable via SLATE_DB_OPEN_TIMEOUT (a Go
// duration string, e.g. "40s"). An empty/invalid/non-positive value uses the
// default. Operators tune this up only if a deployment's object store is
// legitimately slow to open (vs stalled).
func openTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("SLATE_DB_OPEN_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultDbOpenTimeout
}

// runWithTimeout runs fn in a goroutine and returns (result, false) if it
// completes within timeout, or (zero, true) on timeout. On timeout the fn
// goroutine KEEPS RUNNING until fn returns - fn here is an un-cancellable uniffi
// call, so callers must tolerate the leak (it is reaped on process restart). The
// result channel is buffered so a late completion never blocks that goroutine.
func runWithTimeout[T any](timeout time.Duration, fn func() T) (T, bool) {
	done := make(chan T, 1)
	go func() { done <- fn() }()
	select {
	case r := <-done:
		return r, false
	case <-time.After(timeout):
		var zero T
		return zero, true
	}
}

// Put stores value under key. When Config.WriteOptions is non-nil,
// routes through PutWithOptions with default PutOptions (TtlDefault)
// and the operator-supplied WriteOptions; otherwise plain Db.Put.
func (s *Slate) Put(key, value []byte) error {
	if s.db == nil {
		return backend.ErrClosed
	}
	if s.writeOpts != nil {
		if _, err := s.db.PutWithOptions(key, value, defaultPutOptions(), *s.writeOpts); err != nil {
			return fenceTag("slate: put", err)
		}
		return nil
	}
	if _, err := s.db.Put(key, value); err != nil {
		return fenceTag("slate: put", err)
	}
	return nil
}

// Get returns the value for key, or backend.ErrNotFound if absent.
// SlateDB signals "not present" by returning a nil *[]byte from Get;
// we translate that into the backend's sentinel.
func (s *Slate) Get(key []byte) ([]byte, error) {
	if s.db == nil {
		return nil, backend.ErrClosed
	}
	raw, err := s.db.Get(key)
	if err != nil {
		return nil, fenceTag("slate: get", err)
	}
	if raw == nil {
		return nil, backend.ErrNotFound
	}
	return append([]byte(nil), (*raw)...), nil
}

// Delete removes key. Idempotent at the SlateDB level too. When
// Config.WriteOptions is non-nil, routes through DeleteWithOptions;
// otherwise plain Db.Delete.
func (s *Slate) Delete(key []byte) error {
	if s.db == nil {
		return backend.ErrClosed
	}
	if s.writeOpts != nil {
		if _, err := s.db.DeleteWithOptions(key, *s.writeOpts); err != nil {
			return fenceTag("slate: delete", err)
		}
		return nil
	}
	if _, err := s.db.Delete(key); err != nil {
		return fenceTag("slate: delete", err)
	}
	return nil
}

// Flush forces the active memtable (and any immutable memtables) durable
// to L0 SSTs in the object store WITHOUT closing the backend, so a
// subsequent open of this database recovers a minimal WAL tail. It
// implements the OPTIONAL backend.Flusher capability the cluster's
// displacement flush uses at handoff edges (docs/SPEC.md "Displacement
// flush"). Once a higher-epoch writer has fenced this instance the flush
// fails with the fence error; callers treat that as a best-effort miss
// (the successor already owns the database), never as data loss.
func (s *Slate) Flush() error {
	if s.db == nil {
		return backend.ErrClosed
	}
	if err := s.db.FlushWithOptions(slatedb.FlushOptions{FlushType: slatedb.FlushTypeMemTable}); err != nil {
		return fenceTag("slate: flush", err)
	}
	return nil
}

// Compile-time capability assertion: the slate backend supports the
// cluster's displacement flush.
var _ backend.Flusher = (*Slate)(nil)

// ScanPrefix returns an iterator over all keys starting with prefix,
// in key-ascending order. Caller MUST Close the iterator to release
// the underlying SlateDB cursor.
func (s *Slate) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if s.db == nil {
		return nil, backend.ErrClosed
	}
	// The zero KeyRange (no bounds) scans the whole prefix - the pre-v0.14
	// single-argument behavior.
	it, err := s.db.ScanPrefix(prefix, slatedb.KeyRange{})
	if err != nil {
		return nil, fenceTag("slate: scan prefix", err)
	}
	return &iterator{it: it}, nil
}

// Begin starts a SlateDB transaction at snapshot isolation. SlateDB's
// only supported level today is SnapshotIsolation; serializable-snapshot
// is rejected up-front so callers don't get silent downgrades.
//
// The transaction inherits the backend's writeOpts: Commit routes
// through CommitWithOptions when non-nil, plain Commit otherwise. The
// per-write knobs on slatedb-go's transaction Put/Delete (PutOptions)
// are independent of WriteOptions (a TTL knob, not a durability knob),
// so transaction writes use the plain Put/Delete paths regardless;
// durability is only honored at Commit time.
func (s *Slate) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if s.db == nil {
		return nil, backend.ErrClosed
	}
	if level != backend.SnapshotIsolation {
		return nil, fmt.Errorf("slate: isolation level %d not supported (only SnapshotIsolation)", level)
	}
	tx, err := s.db.Begin(slatedb.IsolationLevelSnapshot)
	if err != nil {
		return nil, fmt.Errorf("slate: begin: %w", err)
	}
	return &transaction{tx: tx, writeOpts: s.writeOpts}, nil
}

// Close shuts the SlateDB down (flushing pending writes) and destroys
// the underlying object store handle. Idempotent.
func (s *Slate) Close() error {
	if s.db == nil {
		return nil
	}
	db := s.db
	store := s.store
	s.db = nil
	s.store = nil
	if err := db.Shutdown(); err != nil {
		if store != nil {
			store.Destroy()
		}
		return fmt.Errorf("slate: shutdown: %w", err)
	}
	if store != nil {
		store.Destroy()
	}
	return nil
}

// IsFenced reports whether err is a slatedb WRITER-EPOCH FENCE: the engine
// rejecting an operation because a NEWER writer opened the same (bucket,
// dbName) and bumped the manifest writer-epoch, locking this (now-stale)
// writer out. It is the typed, binding-stable way to recognize the fence
// the v0.8 lease-handoff model relies on (a handed-off unit's old owner is
// fenced the instant the new owner opens it), instead of matching the
// error's rendered string.
//
// It matches via errors.As against slatedb.ErrorClosed with
// Reason == CloseReasonFenced. The slate backend wraps every slatedb error
// with %w, so a fence surfacing from Put/Delete/Get/Commit/ScanPrefix on a
// *Slate is reachable here. A nil error is not a fence.
//
// NOTE this typed check only works IN-PROCESS, where the wrapped Go error
// is intact. A fence that has crossed a gRPC boundary arrives as a status
// STRING and is no longer an *ErrorClosed; a caller on the far side of the
// wire must match the stable message fragments (e.g. "detected newer DB
// client") instead. The slate-backed chaos harness does exactly that for
// gRPC-transported fences; this helper is for the local path.
func IsFenced(err error) bool {
	if err == nil {
		return false
	}
	var closed *slatedb.ErrorClosed
	return errors.As(err, &closed) && closed.Reason == slatedb.CloseReasonFenced
}

// fenceTag wraps a slatedb operation error under prefix and, when it is a
// writer-epoch fence, ADDITIONALLY tags it with backend.ErrFenced (multi-%w) so
// the cluster layer recognizes the fence backend-agnostically (errors.Is(err,
// backend.ErrFenced)) and recodes the leg to the TRANSIENT acquiring-window
// error instead of hard-failing the write OR the cross-shard scan. EVERY op
// routes through here - the NON-transactional backend Put/Get/Delete/ScanPrefix
// and the iterator's Next (the path the cross-shard Aggregate / LocalScan reads
// a MOUNTED unit through), AND every transactional op, NOT just Commit: the
// writer-epoch fence can surface at whichever op runs first against a now-stale
// writer (slatedb's background manifest-epoch poll lands it on the next op), so
// the apply sequence Begin->Get->Put->Commit, or a bare ScanPrefix on a
// superseded mount, could fence at any step. errors.As
// against *ErrorClosed still resolves through the multi-wrap, so IsFenced and the
// rendered slatedb reason are both preserved. A non-fence error keeps the plain
// %w wrap.
func fenceTag(prefix string, err error) error {
	if IsFenced(err) {
		return fmt.Errorf("%s: %w: %w", prefix, err, backend.ErrFenced)
	}
	return fmt.Errorf("%s: %w", prefix, err)
}

// -- iterator -------------------------------------------------------

// iterator wraps a *slatedb.DbIterator, translating its (KeyValue,
// nil-on-exhaust) protocol to the backend.Iterator contract.
type iterator struct {
	it *slatedb.DbIterator
}

func (i *iterator) Next() ([]byte, []byte, error) {
	if i.it == nil {
		return nil, nil, nil
	}
	kv, err := i.it.Next()
	if err != nil {
		return nil, nil, fenceTag("slate: scan next", err)
	}
	if kv == nil {
		return nil, nil, nil
	}
	// Copy out of SlateDB-owned memory so the caller can hold these
	// past the next Next()/Close() call.
	key := append([]byte(nil), kv.Key...)
	val := append([]byte(nil), kv.Value...)
	return key, val, nil
}

func (i *iterator) Close() error {
	if i.it == nil {
		return nil
	}
	i.it.Destroy()
	i.it = nil
	return nil
}

// -- transaction ----------------------------------------------------

// transaction wraps a *slatedb.DbTransaction. Commit and Rollback are
// terminal; subsequent calls return backend.ErrClosed.
//
// writeOpts is inherited from the parent *Slate at Begin time. Commit
// routes through CommitWithOptions when non-nil.
type transaction struct {
	tx        *slatedb.DbTransaction
	writeOpts *slatedb.WriteOptions
	done      bool
}

func (t *transaction) Get(key []byte) ([]byte, error) {
	if t.done || t.tx == nil {
		return nil, backend.ErrClosed
	}
	raw, err := t.tx.Get(key)
	if err != nil {
		return nil, fenceTag("slate: tx get", err)
	}
	if raw == nil {
		return nil, backend.ErrNotFound
	}
	return append([]byte(nil), (*raw)...), nil
}

func (t *transaction) Put(key, value []byte) error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	if err := t.tx.Put(key, value); err != nil {
		return fenceTag("slate: tx put", err)
	}
	return nil
}

func (t *transaction) Delete(key []byte) error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	if err := t.tx.Delete(key); err != nil {
		return fenceTag("slate: tx delete", err)
	}
	return nil
}

func (t *transaction) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if t.done || t.tx == nil {
		return nil, backend.ErrClosed
	}
	it, err := t.tx.ScanPrefix(prefix, slatedb.KeyRange{})
	if err != nil {
		return nil, fenceTag("slate: tx scan prefix", err)
	}
	return &iterator{it: it}, nil
}

func (t *transaction) Commit() error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	t.done = true
	if t.writeOpts != nil {
		if _, err := t.tx.CommitWithOptions(*t.writeOpts); err != nil {
			return fenceTag("slate: tx commit", err)
		}
		return nil
	}
	if _, err := t.tx.Commit(); err != nil {
		return fenceTag("slate: tx commit", err)
	}
	return nil
}

func (t *transaction) Rollback() error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	t.done = true
	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("slate: tx rollback: %w", err)
	}
	return nil
}
