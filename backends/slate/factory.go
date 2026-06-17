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
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	slatedb "slatedb.io/slatedb-go/uniffi"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

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

// dbName maps a GenUnit to its deterministic slatedb DbName (the key-prefix
// within the shared bucket). Delegates to the PURE dbNameFor so the encoding
// is shared with the tagless unit tests (dbname.go) and cannot drift.
func (c BackingConfig) dbName(gu storageunit.GenUnit) string {
	return dbNameFor(c.KeyPrefix, gu)
}

// dbNameReplica maps a ReplicaUnit to its deterministic slatedb DbName (R>1
// multi-backend). Delegates to the PURE dbNameReplicaFor (dbname.go); see
// that function for the prefix-disjointness durability guarantee.
func (c BackingConfig) dbNameReplica(ru storageunit.ReplicaUnit) string {
	return dbNameReplicaFor(c.KeyPrefix, ru)
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
// env writes are identical across units and are set ONCE here. Two
// Backings in one process pointing at DIFFERENT buckets is unsupported
// (the env writes would collide) and is not a configuration the cluster
// produces - a node has exactly one Backing.
func NewBacking(cfg BackingConfig) (*Backing, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	// Reuse slate.Config.applyEnv so the env-var contract stays identical
	// to the single-instance backend (path-style, AWS_ALLOW_HTTP, etc.).
	Config{
		Bucket:    cfg.Bucket,
		DbName:    "_backing", // unused; applyEnv ignores DbName
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey,
		UseSSL:    cfg.UseSSL,
	}.applyEnv()
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
func (b *Backing) durableEpoch(gu storageunit.GenUnit) (storageunit.Epoch, error) {
	store, err := b.resolveStore()
	if err != nil {
		return 0, err
	}
	defer store.Destroy()

	adminBuilder := slatedb.NewAdminBuilder(b.cfg.dbName(gu), store)
	defer adminBuilder.Destroy()
	admin, err := adminBuilder.Build()
	if err != nil {
		return 0, fmt.Errorf("slate: build admin for %s: %w", gu, err)
	}
	defer admin.Destroy()

	manifest, err := admin.ReadManifest(nil)
	if err != nil {
		return 0, fmt.Errorf("slate: read manifest for %s: %w", gu, err)
	}
	if manifest == nil {
		// Db not yet created: no prior writer, durable epoch 0.
		return 0, nil
	}
	return storageunit.Epoch(manifest.WriterEpoch), nil
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
	store, err := b.resolveStore()
	if err != nil {
		return 0, err
	}
	defer store.Destroy()

	adminBuilder := slatedb.NewAdminBuilder(b.cfg.dbNameReplica(ru), store)
	defer adminBuilder.Destroy()
	admin, err := adminBuilder.Build()
	if err != nil {
		return 0, fmt.Errorf("slate: build admin for %s: %w", ru, err)
	}
	defer admin.Destroy()

	manifest, err := admin.ReadManifest(nil)
	if err != nil {
		return 0, fmt.Errorf("slate: read manifest for %s: %w", ru, err)
	}
	if manifest == nil {
		return 0, nil
	}
	return storageunit.Epoch(manifest.WriterEpoch), nil
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
func (b *Backing) writeServingMarker(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) error {
	cur, ok, err := b.readServingMarker(ru)
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
	key := servingMarkerKeyFor(b.cfg.KeyPrefix, ru)
	payload := []byte(strconv.FormatUint(uint64(epoch), 10))
	_, err = mc.PutObject(context.Background(), b.cfg.Bucket, key,
		bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		return fmt.Errorf("slate: write serving marker %s: %w", ru, err)
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
func (b *Backing) readServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool, error) {
	mc, err := b.minioClient()
	if err != nil {
		return 0, false, err
	}
	key := servingMarkerKeyFor(b.cfg.KeyPrefix, ru)
	obj, err := mc.GetObject(context.Background(), b.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return 0, false, fmt.Errorf("slate: read serving marker %s: %w", ru, err)
	}
	defer obj.Close()
	raw, err := io.ReadAll(obj)
	if err != nil {
		// A missing object surfaces here (or on Stat) as a NoSuchKey error,
		// which is NOT a failure: it means no marker has been written yet.
		if isNotFound(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("slate: read serving marker %s: %w", ru, err)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("slate: parse serving marker %s (%q): %w", ru, raw, err)
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

	mu   sync.Mutex
	open map[storageunit.GenUnit]*mountedUnit // units THIS handle has open
	// openLatch holds a per-GenUnit mutex so the WHOLE open of one unit
	// (held-check, durable-manifest fence read, slatedb open, map insert)
	// runs in a single critical section per unit. Without it OpenUnit would
	// release h.mu around the slow slatedb open, leaving a window where two
	// goroutines opening the SAME unit could both pass the held-check and
	// both open. Keyed per unit so opens of DIFFERENT units still proceed
	// concurrently (the slatedb open is the slow part and is unit-local).
	openLatch map[storageunit.GenUnit]*sync.Mutex

	// openReplica + openLatchReplica are the R>1 siblings of open + openLatch,
	// keyed by ReplicaUnit so each (gen, unit, replica) position is tracked +
	// serialized INDEPENDENTLY. A Handle never mixes the R=1 and R>1 maps (a
	// cluster runs R=1 OR R>1, fixed at Open); keeping them separate keeps the
	// R=1 path byte-for-byte unchanged.
	openReplica      map[storageunit.ReplicaUnit]*mountedUnit
	openLatchReplica map[storageunit.ReplicaUnit]*sync.Mutex
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
		backing:          b,
		open:             make(map[storageunit.GenUnit]*mountedUnit),
		openLatch:        make(map[storageunit.GenUnit]*sync.Mutex),
		openReplica:      make(map[storageunit.ReplicaUnit]*mountedUnit),
		openLatchReplica: make(map[storageunit.ReplicaUnit]*sync.Mutex),
	}
}

// latchFor returns the per-GenUnit open latch, creating it on first use. The
// latch map itself is guarded by h.mu (briefly), so two goroutines opening
// the same unit get the SAME *sync.Mutex and serialize on it; goroutines
// opening different units get distinct latches and do not contend.
func (h *Handle) latchFor(gu storageunit.GenUnit) *sync.Mutex {
	h.mu.Lock()
	defer h.mu.Unlock()
	l, ok := h.openLatch[gu]
	if !ok {
		l = &sync.Mutex{}
		h.openLatch[gu] = l
	}
	return l
}

// latchForReplica is the R>1 sibling of latchFor: it returns the
// per-ReplicaUnit open latch, creating it on first use. Two goroutines
// opening the SAME replica position get the SAME *sync.Mutex and serialize;
// opens of different positions (or different units) get distinct latches and
// do not contend.
func (h *Handle) latchForReplica(ru storageunit.ReplicaUnit) *sync.Mutex {
	h.mu.Lock()
	defer h.mu.Unlock()
	l, ok := h.openLatchReplica[ru]
	if !ok {
		l = &sync.Mutex{}
		h.openLatchReplica[ru] = l
	}
	return l
}

// OpenUnit opens (mounts) the slatedb instance for gu in the shared
// bucket and returns it ready to serve. Opening the database IS the fence:
// slatedb's writer-epoch protocol bumps the manifest so any prior writer
// still holding the unit at a lower epoch is locked out (its next
// Put/Delete/Commit fails with CloseReasonFenced).
//
//   - This handle ALREADY holds gu open: a double-open error at ANY epoch
//     (one live writer per handle). The caller must CloseUnit(gu) first,
//     then OpenUnit again. We do NOT support a strictly-higher same-node
//     re-open by closing + reopening the same slatedb db in-process: that
//     trips an internal "stored epoch is lower than local epoch" assertion
//     in a slatedb async task (a process-level panic), and the SUT never
//     needs it (the reconcile RELEASEs before any same-node re-acquire). So
//     we reject it closed.
//   - This handle does NOT hold gu (the cold-acquire / handoff case): the
//     intended epoch is a best-effort floor; the factory fences
//     AUTHORITATIVELY against the durable manifest at
//     max(intended, durableEpoch+1). This NEVER rejects for being too low -
//     a new owner must always be able to take the lease.
//
// The whole open runs under a per-gu latch so a same-unit concurrent open
// SERIALIZES (the second caller sees the unit held and is rejected) rather
// than both passing a stale held-check around the slow slatedb open.
func (h *Handle) OpenUnit(gu storageunit.GenUnit, epoch storageunit.Epoch) (backend.Backend, error) {
	latch := h.latchFor(gu)
	latch.Lock()
	defer latch.Unlock()

	// Held-check under the latch: a concurrent same-unit open holds the latch,
	// so by the time we get here either it finished (unit held -> we reject) or
	// it has not started (unit not held -> we proceed and it rejects).
	h.mu.Lock()
	cur, held := h.open[gu]
	h.mu.Unlock()
	if held {
		return nil, fmt.Errorf("slate: unit %s already open on this handle at epoch %d; CloseUnit first to re-open", gu, cur.epoch)
	}

	// Fence authoritatively above the durable manifest epoch. The intended
	// epoch is only a floor; the durable manifest governs so a stale floor
	// cannot under-fence.
	opened, err := h.backing.fenceEpoch(gu, epoch)
	if err != nil {
		return nil, err
	}

	s, err := h.backing.openSlate(gu)
	if err != nil {
		return nil, err
	}

	h.mu.Lock()
	h.open[gu] = &mountedUnit{slate: s, epoch: opened}
	h.mu.Unlock()
	return s, nil
}

// fenceEpoch computes the epoch the open will land at: strictly above the
// unit's durable manifest writer-epoch, and at least the cluster's
// intended floor. Opening at that epoch fences any lower-epoch writer.
func (b *Backing) fenceEpoch(gu storageunit.GenUnit, intended storageunit.Epoch) (storageunit.Epoch, error) {
	durable, err := b.durableEpoch(gu)
	if err != nil {
		return 0, err
	}
	opened := intended
	if floor := durable + 1; floor > opened {
		opened = floor
	}
	return opened, nil
}

// fenceEpochReplica is the R>1 analogue of fenceEpoch: it computes the epoch
// the open of replica position ru will land at - strictly above ru's
// PER-REPLICA durable manifest writer-epoch, and at least the cluster's
// intended floor. Identical arithmetic to fenceEpoch (max(intended,
// durable+1)), but reading the per-replica manifest at dbNameReplica(ru), so
// opening replica 1 reads r1's manifest only: a fence of r0 never bumps r1,
// and re-acquiring r0 at a higher epoch leaves r1 untouched.
func (b *Backing) fenceEpochReplica(ru storageunit.ReplicaUnit, intended storageunit.Epoch) (storageunit.Epoch, error) {
	durable, err := b.durableEpochReplica(ru)
	if err != nil {
		return 0, err
	}
	opened := intended
	if floor := durable + 1; floor > opened {
		opened = floor
	}
	return opened, nil
}

// openSlate opens the per-unit slatedb instance with AwaitDurable=true
// (the multi-backend durability invariant) and the backing's Settings.
func (b *Backing) openSlate(gu storageunit.GenUnit) (*Slate, error) {
	store, err := b.resolveStore()
	if err != nil {
		return nil, err
	}
	// AwaitDurable=true: every acked write durable in the bucket before
	// ack, per unit. Pinned here (not operator-tunable) because the
	// multi-backend model is R=1; relaxed durability needs R>=2.
	wopts := &slatedb.WriteOptions{AwaitDurable: true}
	db, err := buildDb(b.cfg.dbName(gu), store, b.cfg.Settings, b.cfg.Cache)
	if err != nil {
		store.Destroy()
		return nil, fmt.Errorf("slate: open unit %s: %w", gu, err)
	}
	return &Slate{db: db, store: store, writeOpts: wopts}, nil
}

// openSlateReplica is the R>1 analogue of openSlate: it opens the per-replica
// slatedb instance at dbNameReplica(ru) with AwaitDurable=true and the
// backing's Settings/Cache. Because the DbName encodes the replica position,
// the instance's WAL/LSM/manifest live at a prefix disjoint from every other
// replica's, which is the structural replica-independence guarantee.
func (b *Backing) openSlateReplica(ru storageunit.ReplicaUnit) (*Slate, error) {
	store, err := b.resolveStore()
	if err != nil {
		return nil, err
	}
	wopts := &slatedb.WriteOptions{AwaitDurable: true}
	db, err := buildDb(b.cfg.dbNameReplica(ru), store, b.cfg.Settings, b.cfg.Cache)
	if err != nil {
		store.Destroy()
		return nil, fmt.Errorf("slate: open replica %s: %w", ru, err)
	}
	return &Slate{db: db, store: store, writeOpts: wopts}, nil
}

// CloseUnit releases gu from THIS handle: flushes (Db.Shutdown forces
// pending writes durable) then shuts down the unit's slatedb instance,
// WITHOUT affecting any other unit and WITHOUT deleting the bucket bytes.
// The data stays durable at the unit's prefix for the next owner.
// Idempotent: closing a unit this handle does not hold is a no-op
// returning nil.
func (h *Handle) CloseUnit(gu storageunit.GenUnit) error {
	h.mu.Lock()
	mu, held := h.open[gu]
	if held {
		delete(h.open, gu)
	}
	h.mu.Unlock()
	if !held {
		return nil
	}
	if err := mu.slate.Close(); err != nil {
		return fmt.Errorf("slate: close unit %s: %w", gu, err)
	}
	return nil
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
func (h *Handle) OpenReplicaUnit(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	latch := h.latchForReplica(ru)
	latch.Lock()
	defer latch.Unlock()

	h.mu.Lock()
	cur, held := h.openReplica[ru]
	h.mu.Unlock()
	if held {
		return nil, 0, fmt.Errorf("slate: replica %s already open on this handle at epoch %d; CloseReplicaUnit first to re-open", ru, cur.epoch)
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
	// assumption). At the Go level we order fenceEpochReplica BEFORE
	// openSlateReplica, which is the correct fence-then-recover sequence as far
	// as Go can express it. BUT fenceEpochReplica is ARITHMETIC ONLY: it reads
	// ru's durable manifest writer-epoch and computes opened = max(intended,
	// durable+1); it does NOT itself durably WRITE the bumped epoch. The actual
	// fence AND the WAL recovery both happen INSIDE slatedb's DbBuilder.Build()
	// (openSlateReplica -> buildDb), which is a single opaque uniffi FFI call
	// over the Rust core. The slatedb-go v0.13.1 binding exposes NO WithEpoch
	// setter on DbBuilder and NO hook between the fence and the recovery
	// snapshot, so from Go we can neither inject the computed epoch into the
	// open NOR observe whether the Rust core fences-then-recovers or
	// recovers-then-fences internally. We therefore CANNOT PROVE the
	// fence<=recovery ordering from the module; it is an EXPLICIT ASSUMPTION
	// about slatedb's open path (consistent with its independent WalReader
	// surface and writer-epoch protocol, but not verified in Go source).
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
	opened, err := h.backing.fenceEpochReplica(ru, epoch)
	if err != nil {
		return nil, 0, err
	}

	s, err := h.backing.openSlateReplica(ru)
	if err != nil {
		return nil, 0, err
	}

	h.mu.Lock()
	h.openReplica[ru] = &mountedUnit{slate: s, epoch: opened}
	h.mu.Unlock()
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
func (h *Handle) WriteServingMarker(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) error {
	return h.backing.writeServingMarker(ru, epoch)
}

// ReadServingMarker reads replica ru's durable serving marker WITHOUT opening
// the database (v0.8 Phase 2e). It is the cross-node, point-in-time liveness
// observation the old owner's drainCheck polls: it releases ONLY on ok == true
// AND epoch >= its own open epoch. ok == false means no live owner has reached
// Ready for this position yet (the old owner stays Draining + keeps serving).
func (h *Handle) ReadServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool, error) {
	return h.backing.readServingMarker(ru)
}

// DurableEpochReplica reads replica position ru's durable manifest writer-epoch
// WITHOUT opening the database, satisfying the ReplicaBackendFactory seam
// (v0.8 Phase 2e). The old (draining) owner uses it as the LIVENESS HINT that
// arms drainCheck; it is NOT the release trigger (a bare fence-epoch advance
// never releases - only a serving marker strictly above the open epoch does).
// Delegates to the Backing, which reads the shared-storage manifest every
// node's Handle reaches.
func (h *Handle) DurableEpochReplica(ru storageunit.ReplicaUnit) (storageunit.Epoch, error) {
	return h.backing.durableEpochReplica(ru)
}

// CloseReplicaUnit releases replica position ru from THIS handle: flushes
// (Db.Shutdown forces pending writes durable) then shuts down the position's
// slatedb instance, WITHOUT affecting any other replica or unit and WITHOUT
// deleting the bucket bytes. The data stays durable at dbNameReplica(ru) for
// the next owner. Idempotent: closing a position this handle does not hold is
// a no-op returning nil. It is the R>1 analogue of CloseUnit.
func (h *Handle) CloseReplicaUnit(ru storageunit.ReplicaUnit) error {
	h.mu.Lock()
	mu, held := h.openReplica[ru]
	if held {
		delete(h.openReplica, ru)
	}
	h.mu.Unlock()
	if !held {
		return nil
	}
	if err := mu.slate.Close(); err != nil {
		return fmt.Errorf("slate: close replica %s: %w", ru, err)
	}
	return nil
}

// CurrentEpoch reports the epoch THIS handle currently holds gu open at,
// and ok=false if this handle does not have gu open. LOCAL in-process view
// only, per the BackendFactory contract: the cross-node source of truth is
// the durable manifest writer-epoch, which OpenUnit fences against.
func (h *Handle) CurrentEpoch(gu storageunit.GenUnit) (storageunit.Epoch, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	mu, ok := h.open[gu]
	if !ok {
		return 0, false
	}
	return mu.epoch, true
}

// OpenUnits returns the units THIS handle currently has mounted, ascending
// by (Generation, UnitID). A fresh copy the caller may retain. This is the
// locally-mounted set the reconcile diffs against desired; see
// Backing.PresentUnits for the present-in-bucket set.
func (h *Handle) OpenUnits() []storageunit.GenUnit {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]storageunit.GenUnit, 0, len(h.open))
	for gu := range h.open {
		out = append(out, gu)
	}
	sortGenUnits(out)
	return out
}

// Close releases every unit this handle still has mounted (best-effort
// flush + shutdown). Used on node shutdown; idempotent.
func (h *Handle) Close() error {
	h.mu.Lock()
	mounted := make([]*mountedUnit, 0, len(h.open)+len(h.openReplica))
	for gu, mu := range h.open {
		mounted = append(mounted, mu)
		delete(h.open, gu)
	}
	for ru, mu := range h.openReplica {
		mounted = append(mounted, mu)
		delete(h.openReplica, ru)
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

// compile-time assertions that Handle satisfies the domain interfaces. The
// ReplicaBackendFactory assertion is what lets cluster.Open accept
// multi-backend mode at ReplicationFactor > 1 with a slate Handle.
var _ storageunit.BackendFactory = (*Handle)(nil)
var _ storageunit.ReplicaBackendFactory = (*Handle)(nil)
