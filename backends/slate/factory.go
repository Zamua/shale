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
	"context"
	"errors"
	"fmt"
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

// unitPrefix namespaces the per-unit databases away from any unrelated
// object in the same bucket and gives PresentUnits a single list-prefix
// to scan. A GenUnit gu maps to the DbName "<KeyPrefix>u/g<gen>/u<id>".
const unitPrefix = "u/"

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

// dbName maps a GenUnit to its deterministic slatedb DbName (the
// key-prefix within the shared bucket). The generation segment is ahead
// of the unit segment, matching the ring's genUnitBytes ordering; the
// pair is collision-free, so two GenUnits never share a database.
func (c BackingConfig) dbName(gu storageunit.GenUnit) string {
	return c.KeyPrefix + unitPrefix + fmt.Sprintf("g%d/u%d", gu.Gen, gu.ID)
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
		open:      make(map[storageunit.GenUnit]*mountedUnit),
		openLatch: make(map[storageunit.GenUnit]*sync.Mutex),
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
	s, err := NewWithStoreOpts(b.cfg.dbName(gu), store, b.cfg.Settings, wopts)
	if err != nil {
		store.Destroy()
		return nil, fmt.Errorf("slate: open unit %s: %w", gu, err)
	}
	return s, nil
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
	mounted := make([]*mountedUnit, 0, len(h.open))
	for gu, mu := range h.open {
		mounted = append(mounted, mu)
		delete(h.open, gu)
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

// compile-time assertion that Handle satisfies the domain interface.
var _ storageunit.BackendFactory = (*Handle)(nil)
