// factory.go: the slatedb-backed storageunit.BackendFactory - the
// DEPLOYABLE multi-backend backing the v0.8 lease-handoff + reshard model
// runs against in production (the real version of
// internal/sharedfactory's in-process test double). See the "slatedb
// BackendFactory (deployable multi-backend backing)" section in
// docs/SPEC.md for the full design.
//
// Shape (mirrors sharedfactory's Backing/Handle split):
//
//   - A Backing owns the connection to ONE shared MinIO/S3 bucket. Every
//     node's factory points at the SAME bucket, so a unit's bytes live at
//     a fixed object-store prefix that whichever node currently owns the
//     lease opens - that is what makes a lease handoff copy-free.
//   - A Handle (one per node) implements storageunit.BackendFactory. Many
//     Handles share one Backing. Each Handle tracks which units IT
//     currently has open (its in-process view); the AUTHORITATIVE
//     writer-epoch lives in each unit's durable slatedb manifest in the
//     bucket.
//   - One slatedb instance PER UNIT, keyed by GenUnit: the DbName (the
//     key-prefix within the bucket) is derived deterministically from the
//     GenUnit, so OpenUnit(gu) opens THAT unit's database. gen-g unit K
//     and gen-(g+1) unit K are DISTINCT databases at DISTINCT prefixes
//     that coexist during a doubling bisect.
//
// Epoch fencing IS slatedb's own writer-epoch protocol: opening a unit's
// database fences any prior writer that still holds it at a lower epoch.
// The durable manifest writer-epoch (read via the Admin surface WITHOUT
// opening the db, hence without fencing) is the cross-node source of
// truth; OpenUnit fences strictly above it. See fenceEpoch + OpenUnit.
//
// Durability: every unit opens with WriteOptions{AwaitDurable: true}, so
// every acked write is durable in the bucket before the ack. Combined
// with flush-before-release in CloseUnit, that upholds the
// NO-ACKED-WRITE-LOST invariant across a handoff.

//go:build slatedb

package slate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	slatedb "slatedb.io/slatedb-go/uniffi"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// Logf is the slate backend's operator log hook for the rare, operator-
// critical open events (the per-open fence-read/build phase split a handoff
// investigation needs). Default log.Printf (stderr with timestamps, which a
// containerized deployment captures); an embedding application may replace
// it or set it to nil to silence the backend.
var Logf func(format string, args ...any) = log.Printf

// logf guards the nil hook.
func logf(format string, args ...any) {
	if Logf != nil {
		Logf(format, args...)
	}
}

// BackingConfig captures the shared-bucket connection parameters. Every
// node's factory in one cluster uses the SAME values (one shared
// backing); the per-unit DbName is what isolates one unit's database from
// another inside the bucket.
type BackingConfig struct {
	// Bucket is the shared S3 bucket name. Required.
	Bucket string

	// Endpoint is the S3 endpoint URL (e.g. "http://127.0.0.1:9000").
	// Empty for AWS S3.
	Endpoint string

	// Region defaults to "us-east-1" when empty.
	Region string

	// AccessKey + SecretKey are S3 credentials.
	AccessKey string
	SecretKey string

	// UseSSL mirrors slate.Config.UseSSL: false (default) tells the
	// object_store crate plaintext HTTP is OK (MinIO at http://...).
	UseSSL bool

	// KeyPrefix is prepended ahead of the "u/" unit namespace, so one
	// bucket can host multiple unrelated shale clusters. Default empty.
	// When set it should end in "/".
	KeyPrefix string

	// Settings is forwarded verbatim to every unit's slatedb instance
	// (e.g. a small memtable so many units fit one node). Nil = slatedb
	// defaults. Same pass-through semantics as slate.Config.Settings.
	Settings *slatedb.Settings

	// Cache, if non-nil, is the slatedb SST block + metadata cache shared
	// by EVERY unit this Backing opens (WithDbCache clones the Arc, so one
	// cache fronts all units' reads). Nil = no block cache (every read
	// re-fetches SST blocks from the object store). See slate.Config.Cache
	// for how to build one; the operator owns its lifecycle and Destroys
	// it once on shutdown.
	Cache *slatedb.DbCache

	// RelaxedReplicaDurability, when true, opens every R>1 REPLICA unit with
	// WriteOptions{AwaitDurable:false}: a write acks at memtable insert and
	// becomes durable via the background WAL flush, instead of blocking on
	// the object-store flush per write. This is the relaxed-durability mode
	// where durability comes from REPLICATION (the peer replica's memtable
	// holds the write if one replica crashes pre-flush), not from a per-write
	// object-store round-trip - so it is SAFE ONLY at R>=2.
	//
	// It affects openSlateReplica ONLY (the R>=2 path the cluster opens replica
	// units through); the R=1 openSlate path stays AwaitDurable=true
	// unconditionally, because relaxed durability at R=1 loses any un-flushed
	// write on a single-replica crash. Graceful perturbations stay lossless
	// regardless: CloseUnit/Shutdown forces a flush before the unit releases.
	//
	// Default false (strict per-write durability, byte-exact with the
	// pre-flag behavior). See docs/SPEC.md "Relaxed durability at R>=2
	// (multi-backend)".
	RelaxedReplicaDurability bool
}

func (c BackingConfig) validate() error {
	if c.Bucket == "" {
		return errors.New("slate: BackingConfig.Bucket required")
	}
	return nil
}

func (c *BackingConfig) applyDefaults() {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
}

// dbNameReplica maps a ReplicaUnit to its deterministic slatedb DbName (R>1
// multi-backend). Delegates to the PURE dbNameReplicaFor (dbname.go); see
// that function for the prefix-disjointness durability guarantee. The
// production paths resolve through dbNameRef (the single R=1/R>1 encoding
// switch); this spelling survives only for the `slatedb && integration`
// bench harness, which addresses a replica prefix directly.
func (c BackingConfig) dbNameReplica(ru storageunit.ReplicaUnit) string {
	return dbNameReplicaFor(c.KeyPrefix, ru)
}

// The ref's String() lives on storageunit.MountRef and renders "unit g1/u5" on
// the sole path, "replica g1/u5/r0" on the per-replica path - the same strings
// the hand-written R=1 and R>1 error messages produced before they were
// consolidated, so every "%s over the ref" message below is unchanged.

// dbNameRef resolves the ref to its slatedb DbName. Delegates to the PURE
// dbNameForRef (dbname.go), which is the SINGLE place the R=1 / R>1 on-disk
// encoding split is decided and is pinned tagless by dbname_test.go.
func (c BackingConfig) dbNameRef(r unitRef) string {
	return dbNameForRef(c.KeyPrefix, r)
}

// Backing owns the shared-bucket connection parameters that every per-node
// Handle references. One Backing per CLUSTER; one Handle per NODE off it.
// It is the production analogue of sharedfactory.Backing: the shared
// object storage all nodes point at.
type Backing struct {
	cfg BackingConfig
	url string // "s3://<bucket>/"
}

// NewBacking validates the shared-bucket config and writes the AWS_*
// process env vars the slatedb object_store crate reads. Because every
// unit in this process shares ONE bucket + ONE set of credentials, the
// env writes are identical across units and are set ONCE here. A second
// construction whose endpoint/region/credentials/SSL mode CONFLICTS with
// what this process already applied fails fast here with a config-conflict
// error (envguard.go) rather than silently clobbering the earlier env.
// The BUCKET is deliberately NOT part of that guarded tuple: it travels
// in the s3:// URL, not the env, so a second Backing differing only by
// bucket cannot collide in env and registers cleanly (the cluster design
// still gives a node exactly ONE Backing; the guard just has nothing to
// enforce for that case). The guard is write-once per process: the env
// vars are never unset (Close does not un-apply them), so changing the
// object-store config requires a process restart - see registerEnvConfig.
func NewBacking(cfg BackingConfig) (*Backing, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	// Reuse slate.Config.applyEnv so the env-var contract stays identical
	// to the single-instance backend (path-style, AWS_ALLOW_HTTP, etc.).
	envCfg := Config{
		Bucket:    cfg.Bucket,
		DbName:    "_backing", // unused; applyEnv ignores DbName
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey,
		UseSSL:    cfg.UseSSL,
	}
	if err := envCfg.applyEnv(); err != nil {
		return nil, err
	}
	return &Backing{cfg: cfg, url: "s3://" + cfg.Bucket + "/"}, nil
}

// resolveStore resolves a fresh slatedb ObjectStore against the shared
// bucket. slatedb takes ownership of the returned store per db/admin
// handle, so each open resolves its own.
func (b *Backing) resolveStore() (*slatedb.ObjectStore, error) {
	store, err := slatedb.ObjectStoreResolve(b.url)
	if err != nil {
		return nil, fmt.Errorf("slate: resolve object store %q: %w", b.url, err)
	}
	return store, nil
}

// DurableEpoch is the exported view of durableEpoch: it reads the unit's
// durable manifest writer-epoch (the cross-node source of truth a handoff
// fences against) WITHOUT opening the database. Exposed so a test can pin
// the factory's epoch arithmetic directly - read the durable epoch, drive
// an acquire with a deliberately-stale intended floor, and assert the open
// landed strictly above the durable epoch (proving fenceEpoch's
// max(intended, durable+1) and not relying on slatedb's own fencing to mask
// a regression). Returns (0, nil) when the db does not yet exist.
func (b *Backing) DurableEpoch(gu storageunit.GenUnit) (storageunit.Epoch, error) {
	return b.durableEpoch(gu)
}

// durableEpoch reads the unit's durable manifest writer-epoch WITHOUT
// opening the database (hence without fencing). It is the cross-node
// source of truth a handoff fences against. A nil manifest means the unit
// has never been created (durable epoch 0, no prior writer to fence).
//
// Uses the slatedb Admin surface (NewAdminBuilder + ReadManifest), which
// reads the manifest object directly. Returns (0, nil) when the db does
// not yet exist.
type durableEpochResult struct {
	epoch storageunit.Epoch
	err   error
}

// readDurableEpochBounded reads dbName's durable manifest writer-epoch via the
// slatedb Admin surface (NewAdminBuilder + ReadManifest), BOUNDED by openTimeout().
// The admin Build + ReadManifest are un-cancellable cgo calls; without a bound a
// slow/hung manifest read on a bloated unit wedges the boot-fence path FOREVER
// (the DB-open path has runWithTimeout, but this durable-epoch read historically
// did NOT), which is exactly the cold-start mount hang. On timeout we abandon the
// admin goroutine (same contract as runWithTimeout for the DB open: it keeps
// running until the cgo returns, then runs its own Destroy()s; the result channel
// is buffered so the late send never blocks) and return a timeout error so the
// caller boots DEGRADED instead of hanging. The store is created + destroyed
// INSIDE the goroutine so a timeout never Destroys a store the abandoned
// goroutine is still using.
func (b *Backing) readDurableEpochBounded(dbName string) (storageunit.Epoch, error) {
	r, timedOut := runWithTimeout(openTimeout(), func() durableEpochResult {
		store, err := b.resolveStore()
		if err != nil {
			return durableEpochResult{err: err}
		}
		defer store.Destroy()
		adminBuilder := slatedb.NewAdminBuilder(dbName, store)
		defer adminBuilder.Destroy()
		admin, err := adminBuilder.Build()
		if err != nil {
			return durableEpochResult{err: fmt.Errorf("slate: build admin for %s: %w", dbName, err)}
		}
		defer admin.Destroy()
		manifest, err := admin.ReadManifest(nil)
		if err != nil {
			return durableEpochResult{err: fmt.Errorf("slate: read manifest for %s: %w", dbName, err)}
		}
		if manifest == nil {
			// Db not yet created: no prior writer, durable epoch 0.
			return durableEpochResult{epoch: 0}
		}
		return durableEpochResult{epoch: storageunit.Epoch(manifest.WriterEpoch)}
	})
	if timedOut {
		return 0, fmt.Errorf("slate: durable-epoch read for %s timed out after %s (un-cancellable slatedb admin read abandoned; boots degraded)", dbName, openTimeout())
	}
	return r.epoch, r.err
}

// durableEpochRef reads the ref's durable manifest writer-epoch WITHOUT opening
// the database. It is the one implementation behind both durableEpoch (R=1) and
// durableEpochReplica (R>1): identical read, the ONLY difference being which
// prefix dbNameRef resolves to.
func (b *Backing) durableEpochRef(r unitRef) (storageunit.Epoch, error) {
	return b.readDurableEpochBounded(b.cfg.dbNameRef(r))
}

func (b *Backing) durableEpoch(gu storageunit.GenUnit) (storageunit.Epoch, error) {
	return b.durableEpochRef(refUnit(gu))
}

// DurableEpochReplica is the R>1 analogue of DurableEpoch: it reads the
// durable manifest writer-epoch for replica position ru WITHOUT opening the
// database. Exposed so a test can pin the per-replica epoch arithmetic
// directly (read the position's durable epoch, drive an acquire with a
// deliberately-stale intended floor, assert the open landed strictly above).
// Returns (0, nil) when the position's database does not yet exist.
func (b *Backing) DurableEpochReplica(ru storageunit.ReplicaUnit) (storageunit.Epoch, error) {
	return b.durableEpochReplica(ru)
}

// durableEpochReplica reads replica position ru's durable manifest
// writer-epoch WITHOUT opening the database (hence without fencing). It is
// the per-replica cross-node source of truth a handoff of THAT position
// fences against. Because each replica position is its own slatedb database
// at dbNameReplica(ru), reading r1's manifest never observes or touches r0's
// epoch: replica positions fence INDEPENDENTLY. A nil manifest means the
// position has never been created (durable epoch 0). Mirrors durableEpoch.
func (b *Backing) durableEpochReplica(ru storageunit.ReplicaUnit) (storageunit.Epoch, error) {
	return b.durableEpochRef(refReplica(ru))
}

// minioClient builds a fresh minio S3 client against the configured endpoint +
// credentials. Shared by PresentUnits and the serving-marker read/write (the
// marker is a plain object the S3 client GETs/PUTs directly, NOT a slatedb key,
// so it does not go through the slatedb ObjectStore). The caller does not need
// to close it (minio clients hold no persistent connection).
func (b *Backing) minioClient() (*minio.Client, error) {
	mc, err := minio.New(b.minioEndpointHost(), &minio.Options{
		Creds:  credentials.NewStaticV4(b.cfg.AccessKey, b.cfg.SecretKey, ""),
		Secure: b.cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("slate: minio client: %w", err)
	}
	return mc, nil
}

// writeServingMarker writes replica ru's durable SERVING MARKER object (v0.8
// Phase 2e): a tiny object at servingMarkerKeyFor(ru) carrying the new owner's
// open epoch as a decimal string. The new (gaining) owner calls it EXACTLY ONCE
// at its Acquiring -> Ready mount flip; the old (draining) owner polls it via
// readServingMarker as the POLL-ONLY release signal.
//
// It is a sibling OBJECT of the position's slatedb database, written WITHOUT
// opening the database, so it does not perturb the position's WAL/manifest and
// the old owner can read it without mounting. It is MONOTONIC: it reads any
// existing marker first and refuses to LOWER the recorded epoch (a stale write
// from a fenced prior owner must not roll the marker back below a live
// higher-epoch owner's value). Idempotent at the same-or-higher epoch.
//
// NB this read-then-write is NOT atomic against a concurrent marker write, but
// the marker has a SINGLE legitimate writer at a time (the one live owner that
// just reached Ready), so the non-atomicity only matters against a stale fenced
// writer racing the live one - and the monotonic floor is exactly what blocks
// that stale write from winning. A CAS (If-None-Match / version) would tighten
// it; the floor is sufficient for the single-live-writer invariant.
func (b *Backing) writeServingMarker(r unitRef, epoch storageunit.Epoch) error {
	cur, ok, err := b.readServingMarker(r)
	if err != nil {
		return err
	}
	if ok && cur >= epoch {
		return nil // monotonic: never lower a recorded marker.
	}
	mc, err := b.minioClient()
	if err != nil {
		return err
	}
	key := servingMarkerKeyForRef(b.cfg.KeyPrefix, r)
	payload := []byte(strconv.FormatUint(uint64(epoch), 10))
	_, err = mc.PutObject(context.Background(), b.cfg.Bucket, key,
		bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		return fmt.Errorf("slate: write serving marker %s: %w", r, err)
	}
	return nil
}

// readServingMarker reads replica ru's durable SERVING MARKER object WITHOUT
// opening the database (v0.8 Phase 2e). It returns (epoch, true, nil) when the
// marker exists, (0, false, nil) when it has never been written (no live owner
// has reached Ready for this position), and a non-nil err only on a real I/O
// failure. It is the point-in-time liveness observation the old owner's
// drainCheck polls: it releases ONLY on ok == true AND epoch >= its own open
// epoch.
func (b *Backing) readServingMarker(r unitRef) (storageunit.Epoch, bool, error) {
	mc, err := b.minioClient()
	if err != nil {
		return 0, false, err
	}
	key := servingMarkerKeyForRef(b.cfg.KeyPrefix, r)
	obj, err := mc.GetObject(context.Background(), b.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return 0, false, fmt.Errorf("slate: read serving marker %s: %w", r, err)
	}
	defer obj.Close()
	raw, err := io.ReadAll(obj)
	if err != nil {
		// A missing object surfaces here (or on Stat) as a NoSuchKey error,
		// which is NOT a failure: it means no marker has been written yet.
		if isNotFound(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("slate: read serving marker %s: %w", r, err)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("slate: parse serving marker %s (%q): %w", r, raw, err)
	}
	return storageunit.Epoch(parsed), true, nil
}

// isNotFound reports whether err is the S3 "object does not exist" error, which
// the serving-marker read treats as ok == false rather than a failure.
func isNotFound(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.StatusCode == 404
}

// PresentUnits enumerates the GenUnits whose databases EXIST in the bucket
// (regardless of which node currently has them mounted), ascending by
// (Generation, UnitID). It is the production analogue of the test double's
// enumerable shared map: the cluster uses it to learn what the backing
// contains (generation/owner derivation, reconcile against units no node
// mounts).
//
// It is DISTINCT from Handle.OpenUnits (the locally-mounted set), which is
// what the BackendFactory interface returns. The slatedb-go binding's
// ObjectStore exposes no list API, so this scans the bucket with the S3
// client (minio-go) under the "<KeyPrefix>u/" prefix and parses each
// "g<gen>/u<id>/" object key segment back to a GenUnit.
func (b *Backing) PresentUnits(ctx context.Context) ([]storageunit.GenUnit, error) {
	mc, err := minio.New(b.minioEndpointHost(), &minio.Options{
		Creds:  credentials.NewStaticV4(b.cfg.AccessKey, b.cfg.SecretKey, ""),
		Secure: b.cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("slate: minio client: %w", err)
	}

	prefix := b.cfg.KeyPrefix + unitPrefix
	seen := make(map[storageunit.GenUnit]struct{})
	for obj := range mc.ListObjects(ctx, b.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("slate: list units: %w", obj.Err)
		}
		gu, ok := parseUnitKey(strings.TrimPrefix(obj.Key, prefix))
		if ok {
			seen[gu] = struct{}{}
		}
	}

	out := make([]storageunit.GenUnit, 0, len(seen))
	for gu := range seen {
		out = append(out, gu)
	}
	sortGenUnits(out)
	return out, nil
}

// minioEndpointHost strips the scheme from the configured Endpoint, which
// minio.New wants as a bare host:port. Empty Endpoint (AWS S3) yields the
// default AWS host.
func (b *Backing) minioEndpointHost() string {
	ep := b.cfg.Endpoint
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	if ep == "" {
		return "s3.amazonaws.com"
	}
	return ep
}

// parseUnitKey extracts the GenUnit from an object key RELATIVE to the
// "<KeyPrefix>u/" prefix, i.e. a key shaped "g<gen>/u<id>/<...slatedb
// internals...>". Returns ok=false for any key that does not start with
// the "g<gen>/u<id>/" shape (so unrelated objects under the prefix are
// ignored).
func parseUnitKey(rel string) (storageunit.GenUnit, bool) {
	segs := strings.SplitN(rel, "/", 3)
	if len(segs) < 2 {
		return storageunit.GenUnit{}, false
	}
	genSeg, unitSeg := segs[0], segs[1]
	if !strings.HasPrefix(genSeg, "g") || !strings.HasPrefix(unitSeg, "u") {
		return storageunit.GenUnit{}, false
	}
	gen, err := strconv.ParseUint(genSeg[1:], 10, 64)
	if err != nil {
		return storageunit.GenUnit{}, false
	}
	id, err := strconv.ParseUint(unitSeg[1:], 10, 32)
	if err != nil {
		return storageunit.GenUnit{}, false
	}
	return storageunit.NewGenUnit(storageunit.Generation(gen), storageunit.UnitID(id)), true
}

func sortGenUnits(s []storageunit.GenUnit) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Gen != s[j].Gen {
			return s[i].Gen < s[j].Gen
		}
		return s[i].ID < s[j].ID
	})
}

// Handle is a per-node factory handle over a shared Backing. It implements
// storageunit.BackendFactory. Several Handles (one per node) share one
// Backing, which is what makes a handoff copy-free + fence-correct.
type Handle struct {
	backing *Backing

	mu sync.Mutex
	// open holds the positions THIS handle has mounted, keyed by unitRef so
	// the R=1 (GenUnit) and R>1 (ReplicaUnit) surfaces share one map WITHOUT
	// aliasing: the ref carries the replicated flag, so refUnit(gu) and
	// refReplica(gu, 0) are different keys just as their two on-disk prefixes
	// are different strings. A cluster runs R=1 OR R>1 (fixed at Open), so in
	// practice only one family is ever populated; the flag is what guarantees
	// the merge stays safe if that ever stops holding.
	open map[unitRef]*mountedUnit
	// openLatch holds a per-ref mutex so the WHOLE open of one position
	// (held-check, durable-manifest fence read, slatedb open, map insert)
	// runs in a single critical section per position. Without it openRef would
	// release h.mu around the slow slatedb open, leaving a window where two
	// goroutines opening the SAME position could both pass the held-check and
	// both open. Keyed per ref so opens of DIFFERENT positions still proceed
	// concurrently (the slatedb open is the slow part and is position-local).
	openLatch map[unitRef]*sync.Mutex
}

// mountedUnit bundles a unit's live slatedb instance with the epoch this
// handle opened it at.
type mountedUnit struct {
	slate *Slate
	epoch storageunit.Epoch
}

// Handle returns a fresh per-node handle over this backing. Each node in a
// cluster gets its own Handle; they all share b.
func (b *Backing) Handle() *Handle {
	return &Handle{
		backing:   b,
		open:      make(map[unitRef]*mountedUnit),
		openLatch: make(map[unitRef]*sync.Mutex),
	}
}

// latchFor returns the per-ref open latch, creating it on first use. The latch
// map itself is guarded by h.mu (briefly), so two goroutines opening the same
// position get the SAME *sync.Mutex and serialize on it; goroutines opening
// different positions get distinct latches and do not contend. Because the ref
// carries the R=1/R>1 flag, an R=1 unit and its R>1 replica-0 position latch
// independently, matching their independent on-disk prefixes.
func (h *Handle) latchFor(r unitRef) *sync.Mutex {
	h.mu.Lock()
	defer h.mu.Unlock()
	l, ok := h.openLatch[r]
	if !ok {
		l = &sync.Mutex{}
		h.openLatch[r] = l
	}
	return l
}

// OpenUnit opens (mounts) the slatedb instance for the mount m in the shared
// bucket and returns it ready to serve, together with the EXACT epoch the open
// landed at. Opening the database IS the fence: slatedb's writer-epoch protocol
// bumps the manifest so any prior writer still holding the mount at a lower
// epoch is locked out (its next Put/Delete/Commit fails with CloseReasonFenced).
//
// It is the storageunit.BackendFactory entry point, and m carries the layout:
// a sole mount resolves to the unit's own prefix, a replica mount to the
// position's child prefix. See dbNameForRef.
//
//   - This handle ALREADY holds m open: a double-open error at ANY epoch
//     (one live writer per handle). The caller must CloseUnit(m) first,
//     then OpenUnit again. We do NOT support a strictly-higher same-node
//     re-open by closing + reopening the same slatedb db in-process: that
//     trips an internal "stored epoch is lower than local epoch" assertion
//     in a slatedb async task (a process-level panic), and the SUT never
//     needs it (the reconcile RELEASEs before any same-node re-acquire). So
//     we reject it closed.
//   - This handle does NOT hold m (the cold-acquire / handoff case): the
//     intended epoch is a best-effort floor; the factory fences
//     AUTHORITATIVELY against the durable manifest at
//     max(intended, durableEpoch+1). This NEVER rejects for being too low -
//     a new owner must always be able to take the lease.
//
// The whole open runs under a per-mount latch so a same-mount concurrent open
// SERIALIZES (the second caller sees the mount held and is rejected) rather
// than both passing a stale held-check around the slow slatedb open.
func (h *Handle) OpenUnit(m storageunit.MountRef, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	s, opened, err := h.openRef(m, epoch)
	if err != nil {
		return nil, 0, err
	}
	return s, opened, nil
}

// openRef is the ONE open sequence behind both OpenUnit (R=1) and
// OpenReplicaUnit (R>1): per-ref latch, held-check under that latch, fence
// against the ref's durable manifest, slatedb open, mount-map insert. Keeping
// it single-sourced is the point of unitRef: a guard added here (or a fix to
// the fence ordering) lands on BOTH surfaces, instead of on whichever copy the
// author happened to be editing.
//
// It returns the epoch the open landed at as well as the backend, because the
// R>1 caller must use that EXACT value as its open epoch (gate + serving
// marker) and must never re-read the durable to recover it. The R=1 surface
// discards it, matching the narrower BackendFactory signature.
//
// Two behavioural properties are ref-DEPENDENT and preserved verbatim:
//   - the durability flag: openSlateRef keeps AwaitDurable pinned true on the
//     R=1 path regardless of RelaxedReplicaDurability (see there);
//   - the timing log: only the R>1 path emits the fence-read/build breakdown
//     (see logOpen).
func (h *Handle) openRef(r unitRef, epoch storageunit.Epoch) (*Slate, storageunit.Epoch, error) {
	latch := h.latchFor(r)
	latch.Lock()
	defer latch.Unlock()

	// Held-check under the latch: a concurrent same-position open holds the
	// latch, so by the time we get here either it finished (position held ->
	// we reject) or it has not started (not held -> we proceed and it rejects).
	h.mu.Lock()
	cur, held := h.open[r]
	h.mu.Unlock()
	if held {
		return nil, 0, fmt.Errorf("slate: %s already open on this handle at epoch %d; CloseUnit first to re-open", r, cur.epoch)
	}

	// FENCE <= RECOVERY ORDERING INVARIANT (v0.8 Phase 2e, NEW-P1-4). The new
	// owner's recovery snapshot (the durable WAL/manifest tail it mounts) MUST
	// be taken AT-OR-AFTER the manifest-epoch fence is durably effective, so no
	// write the OLD owner can still ack escapes the recovered tail. Because
	// forwarding continues until the mount flip (strictly AFTER this open
	// completes), a forwarded write can be acked on the old owner AFTER this
	// recovery snapshot but still BELOW the fence epoch E; to keep every such
	// write inside the recovered tail the fence must be effective no later than
	// the recovery cutoff. The spec's required order is therefore: make the
	// fence effective FIRST, THEN take the recovery snapshot.
	//
	// WHAT THE GO LAYER CAN GUARANTEE, AND WHAT IT CANNOT (the precise
	// assumption). At the Go level we order fenceEpochRef BEFORE openSlateRef,
	// which is the correct fence-then-recover sequence as far as Go can express
	// it. BUT fenceEpochRef is ARITHMETIC ONLY: it reads the ref's durable
	// manifest writer-epoch and computes opened = max(intended, durable+1); it
	// does NOT itself durably WRITE the bumped epoch. The actual fence AND the
	// WAL recovery both happen INSIDE slatedb's DbBuilder.Build() (openSlateRef
	// -> buildDb), which is a single opaque uniffi FFI call over the Rust core.
	// The slatedb-go v0.13.1 binding exposes NO WithEpoch setter on DbBuilder
	// and NO hook between the fence and the recovery snapshot, so from Go we can
	// neither inject the computed epoch into the open NOR observe whether the
	// Rust core fences-then-recovers or recovers-then-fences internally. We
	// therefore CANNOT PROVE the fence<=recovery ordering from the module; it is
	// an EXPLICIT ASSUMPTION about slatedb's open path (consistent with its
	// independent WalReader surface and writer-epoch protocol, but not verified
	// in Go source).
	//
	// THE RELEASE GATE. This assumption is PINNED by the two real-slate tests in
	// factory_servingmarker_slatedb_test.go (build tag slatedb), run in the
	// staging acceptance run, NOT here: (1) a write acked on the old owner JUST
	// BEFORE the fence is readable on the new owner AFTER the handoff; (2) a
	// write driven THROUGH THE FORWARD PATH concurrently with the flip (acked
	// below E while the new owner's open/recovery races) is readable after. If
	// either fails, the Rust core recovers-then-fences (or does not replay the
	// full durable WAL tail) and this open must be adjusted to RE-SCAN the WAL
	// tail after the fence (via the WalReader surface) before serving; until the
	// pins pass on real slate, Phase 2e is BLOCKED per the spec's P0 gate.
	//
	// The intended epoch is only a floor; the durable manifest governs so a
	// stale floor cannot under-fence.
	fenceStart := time.Now()
	opened, err := h.backing.fenceEpochRef(r, epoch)
	if err != nil {
		logOpen(r, "slate: open %s: fence-read FAILED after %s: %v", r, time.Since(fenceStart).Round(time.Millisecond), err)
		return nil, 0, err
	}
	fenceDur := time.Since(fenceStart)

	buildStart := time.Now()
	s, err := h.backing.openSlateRef(r)
	if err != nil {
		logOpen(r, "slate: open %s: fence-read %s, build FAILED after %s: %v",
			r, fenceDur.Round(time.Millisecond), time.Since(buildStart).Round(time.Millisecond), err)
		return nil, 0, err
	}
	logOpen(r, "slate: open %s: fence-read %s, build %s, epoch %d",
		r, fenceDur.Round(time.Millisecond), time.Since(buildStart).Round(time.Millisecond), opened)

	h.mu.Lock()
	h.open[r] = &mountedUnit{slate: s, epoch: opened}
	h.mu.Unlock()
	return s, opened, nil
}

// logOpen emits openRef's per-open fence-read/build timing breakdown, but ONLY
// on the R>1 replica path.
//
// DIVERGENCE PRESERVED DELIBERATELY. Before this consolidation the R=1 OpenUnit
// had no instrumentation at all and the R>1 OpenReplicaUnit had all three lines
// (fence-fail, build-fail, success). Routing both through openRef would have
// started emitting operator log lines on every R=1 mount, which is a visible
// behaviour change to a deployed R=1 cluster's log volume, so the gate stays.
// Turning it on for R=1 is a deliberate, separate decision (arguably the right
// one, since the fence/build split is exactly what a cold-start mount-hang
// investigation needs on either path) and not something a mechanical extraction
// should smuggle in.
func logOpen(r unitRef, format string, args ...any) {
	if !r.Replicated() {
		return
	}
	logf(format, args...)
}

// fenceEpochRef computes the epoch the open of r will land at: strictly above
// r's durable manifest writer-epoch, and at least the cluster's intended floor.
// Opening at that epoch fences any lower-epoch writer of the SAME ref.
//
// This max(intended, durable+1) arithmetic is the single fence rule for BOTH
// R=1 and R>1; the ONLY thing the ref changes is which manifest is read. At R>1
// that manifest is the position's own (dbNameReplica(ru)), so opening replica 1
// reads r1's manifest only: a fence of r0 never bumps r1, and re-acquiring r0
// at a higher epoch leaves r1 untouched. Replica positions fence INDEPENDENTLY.
func (b *Backing) fenceEpochRef(r unitRef, intended storageunit.Epoch) (storageunit.Epoch, error) {
	durable, err := b.durableEpochRef(r)
	if err != nil {
		return 0, err
	}
	opened := intended
	if floor := durable + 1; floor > opened {
		opened = floor
	}
	return opened, nil
}

// openSlateRef opens the slatedb instance for r at its resolved DbName with the
// backing's Settings/Cache. Because the DbName encodes the position, the
// instance's WAL/LSM/manifest live at a prefix disjoint from every other unit's
// and (at R>1) every other replica's, which is the structural
// replica-independence guarantee.
//
// DURABILITY DIVERGENCE, PRESERVED DELIBERATELY. AwaitDurable is:
//   - ALWAYS true on the R=1 path, regardless of the operator's
//     RelaxedReplicaDurability setting. Relaxed durability at R=1 loses any
//     un-flushed write on a single-replica crash, because there is no peer
//     memtable holding the write. The flag is pinned here, not operator-tunable,
//     exactly as the pre-consolidation openSlate pinned it.
//   - true by default on the R>1 path, and false when the operator set
//     RelaxedReplicaDurability: a write then acks at memtable insert and the
//     background WAL flush carries durability, with the peer replica's memtable
//     as the safety net for a single-replica pre-flush crash.
//
// Reading the flag off the ref (r.replicated) rather than off a call-site is
// what keeps that R=1 protection from being lost the next time this path is
// edited. See BackingConfig.RelaxedReplicaDurability.
func (b *Backing) openSlateRef(r unitRef) (*Slate, error) {
	store, err := b.resolveStore()
	if err != nil {
		return nil, err
	}
	// Relaxed durability is reachable ONLY on the R>1 path; at R=1 there is no
	// peer memtable to act as the safety net, so the operator's flag is ignored.
	relaxed := r.Replicated() && b.cfg.RelaxedReplicaDurability
	awaitDurable := !relaxed
	wopts := &slatedb.WriteOptions{AwaitDurable: awaitDurable}
	db, err := buildDb(b.cfg.dbNameRef(r), store, b.cfg.Settings, b.cfg.Cache)
	if err != nil {
		store.Destroy()
		return nil, fmt.Errorf("slate: open %s: %w", r, err)
	}
	return &Slate{db: db, store: store, writeOpts: wopts}, nil
}

// closeRef is the ONE release sequence behind both CloseUnit (R=1) and
// CloseReplicaUnit (R>1): drop the position from this handle's mount map, then
// flush + shut down its slatedb instance (Db.Shutdown forces pending writes
// durable) WITHOUT affecting any other position and WITHOUT deleting the bucket
// bytes. The data stays durable at the position's prefix for the next owner.
// Idempotent: releasing a position this handle does not hold is a no-op
// returning nil.
func (h *Handle) closeRef(r unitRef) error {
	h.mu.Lock()
	mu, held := h.open[r]
	if held {
		delete(h.open, r)
	}
	h.mu.Unlock()
	if !held {
		return nil
	}
	if err := mu.slate.Close(); err != nil {
		return fmt.Errorf("slate: close %s: %w", r, err)
	}
	return nil
}

// CloseUnit releases gu from THIS handle: flushes (Db.Shutdown forces
// pending writes durable) then shuts down the unit's slatedb instance,
// WITHOUT affecting any other unit and WITHOUT deleting the bucket bytes.
// The data stays durable at the unit's prefix for the next owner.
// Idempotent: closing a unit this handle does not hold is a no-op
// returning nil.
func (h *Handle) CloseUnit(m storageunit.MountRef) error {
	return h.closeRef(m)
}

// OpenReplicaUnit opens (mounts) the slatedb instance for replica position
// ru.Replica of unit ru.Unit at dbNameReplica(ru) and returns it ready to
// serve. It is the R>1 analogue of OpenUnit, structurally identical but keyed
// by ReplicaUnit. Opening the database IS the fence: slatedb's writer-epoch
// protocol bumps the manifest at ru's OWN prefix, so any prior writer of the
// SAME replica position still holding it at a lower epoch is locked out; a
// writer of a DIFFERENT position (or a different unit) is never touched -
// distinct positions are independent databases at disjoint prefixes.
//
//   - This handle ALREADY holds ru open: a double-open error at ANY epoch (one
//     live writer per position per handle). The caller must CloseReplicaUnit(ru)
//     first. We do NOT support a strictly-higher same-node re-open in-process
//     for the same reason OpenUnit doesn't (it trips a slatedb async-task
//     assertion); the reconcile RELEASEs before any same-node re-acquire.
//   - This handle does NOT hold ru (the cold-acquire / handoff case): the
//     intended epoch is a best-effort floor; the factory fences AUTHORITATIVELY
//     against ru's durable manifest at max(intended, durableEpochReplica+1).
//     This NEVER rejects for being too low - a new owner must always be able to
//     take the position's lease.
//
// The whole open runs under a per-ru latch so a same-position concurrent open
// SERIALIZES rather than both passing a stale held-check around the slow
// slatedb open. Opens of different positions proceed concurrently.
// It is a CONVENIENCE spelling of OpenUnit(refReplica(ru), epoch), not a port
// method: shale declares exactly one storage interface and never asks whether
// an adapter has this one. It survives because the slatedb-tagged integration
// and bench harnesses address a replica position directly.
func (h *Handle) OpenReplicaUnit(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	s, opened, err := h.openRef(refReplica(ru), epoch)
	if err != nil {
		return nil, 0, err
	}
	// opened is the EXACT fence epoch this open landed at; the caller uses it as
	// this node's open epoch (gate + serving marker), never re-reading the durable.
	return s, opened, nil
}

// WriteServingMarker writes replica ru's durable serving marker carrying epoch
// (v0.8 Phase 2e, Option B overlap handoff). The new (gaining) owner calls it
// EXACTLY ONCE at its Acquiring -> Ready mount flip, AFTER OpenReplicaUnit
// returned and mountMap[ru] was inserted. It is the POLL-ONLY release signal
// the old (draining) owner polls via ReadServingMarker; there is NO push RPC.
// Delegates to the Backing (the marker is a shared-storage object every node's
// Handle reaches, exactly like the position's bytes).
func (h *Handle) WriteServingMarker(m storageunit.MountRef, epoch storageunit.Epoch) error {
	return h.backing.writeServingMarker(m, epoch)
}

// ReadServingMarker reads replica ru's durable serving marker WITHOUT opening
// the database (v0.8 Phase 2e). It is the cross-node, point-in-time liveness
// observation the old owner's drainCheck polls: it releases ONLY on ok == true
// AND epoch >= its own open epoch. ok == false means no live owner has reached
// Ready for this position yet (the old owner stays Draining + keeps serving).
func (h *Handle) ReadServingMarker(m storageunit.MountRef) (storageunit.Epoch, bool, error) {
	return h.backing.readServingMarker(m)
}

// DurableEpochReplica reads replica position ru's durable manifest writer-epoch
// WITHOUT opening the database. TEST-ONLY convenience for
// DurableEpoch(refReplica(ru)); see OpenReplicaUnit. The old (draining) owner
// uses it as the LIVENESS HINT that
// arms drainCheck; it is NOT the release trigger (a bare fence-epoch advance
// never releases - only a serving marker strictly above the open epoch does).
// Delegates to the Backing, which reads the shared-storage manifest every
// node's Handle reaches.
func (h *Handle) DurableEpochReplica(ru storageunit.ReplicaUnit) (storageunit.Epoch, error) {
	return h.backing.durableEpochReplica(ru)
}

// DurableEpoch reads m's durable manifest writer-epoch WITHOUT opening the
// database, satisfying the storageunit.BackendFactory seam. It is the
// cross-node source of truth OpenUnit fences above, and the LIVENESS HINT that
// arms the old (draining) owner's drainCheck; it is NOT the release trigger (a
// bare fence-epoch advance never releases - only a serving marker strictly
// above the open epoch does). Delegates to the Backing, which reads the
// shared-storage manifest every node's Handle reaches.
func (h *Handle) DurableEpoch(m storageunit.MountRef) (storageunit.Epoch, error) {
	return h.backing.durableEpochRef(m)
}

// CloseReplicaUnit releases replica position ru from THIS handle: flushes
// (Db.Shutdown forces pending writes durable) then shuts down the position's
// slatedb instance, WITHOUT affecting any other replica or unit and WITHOUT
// deleting the bucket bytes. The data stays durable at dbNameReplica(ru) for
// the next owner. Idempotent: closing a position this handle does not hold is
// a no-op returning nil. It is the R>1 analogue of CloseUnit.
// It is a CONVENIENCE spelling of CloseUnit(refReplica(ru)); see
// OpenReplicaUnit.
func (h *Handle) CloseReplicaUnit(ru storageunit.ReplicaUnit) error {
	return h.closeRef(refReplica(ru))
}

// CurrentEpoch reports the epoch THIS handle currently holds gu open at,
// and ok=false if this handle does not have gu open. LOCAL in-process view
// only, per the BackendFactory contract: the cross-node source of truth is
// the durable manifest writer-epoch, which OpenUnit fences against.
func (h *Handle) CurrentEpoch(m storageunit.MountRef) (storageunit.Epoch, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	mu, ok := h.open[m]
	if !ok {
		return 0, false
	}
	return mu.epoch, true
}

// OpenUnits returns the mounts THIS handle currently holds, both layouts, in
// storageunit.CompareMountRefs order. A fresh copy the caller may retain. This
// is the locally-mounted set the reconcile diffs against desired; see
// Backing.PresentUnits for the present-in-bucket set.
//
// It no longer filters to the sole mounts. The pre-collapse signature could
// only name GenUnits, so it had to drop the replica mounts and an R>1 handle
// reported an empty set; MountRef can name both, so the enumerator is now
// complete.
func (h *Handle) OpenUnits() []storageunit.MountRef {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]storageunit.MountRef, 0, len(h.open))
	for r := range h.open {
		out = append(out, r)
	}
	slices.SortFunc(out, storageunit.CompareMountRefs)
	return out
}

// Close releases every position this handle still has mounted, R=1 and R>1
// alike (best-effort flush + shutdown). Used on node shutdown; idempotent.
func (h *Handle) Close() error {
	h.mu.Lock()
	mounted := make([]*mountedUnit, 0, len(h.open))
	for r, mu := range h.open {
		mounted = append(mounted, mu)
		delete(h.open, r)
	}
	h.mu.Unlock()
	var firstErr error
	for _, mu := range mounted {
		if err := mu.slate.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// compile-time assertion that Handle satisfies the one storage port, at every
// ReplicationFactor. There is no second interface to assert.
var _ storageunit.BackendFactory = (*Handle)(nil)
