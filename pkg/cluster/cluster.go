// Package cluster is the public API surface of shale. Apps import
// this package + call cluster.Open(cfg) to get a Cluster handle that
// routes KV operations across nodes.
//
// Two modes:
//
//   - single-node: cfg.BindAddr empty. Cluster delegates every op
//     directly to the local Backend. Useful for tests, embedded apps
//     that don't yet want gossip, and the v0.1 baseline.
//   - multi-node:  cfg.BindAddr non-empty. Cluster brings up a
//     membership.Membership on BindAddr (joining via cfg.Seeds) + a
//     ring.Ring populated from membership events. Single-key ops
//     hash the key through the ring; if the owner is the local node
//     they hit the local Backend, otherwise they are forwarded to
//     the owner's gRPC service via a cached rpc.Client.
//
// v0.2 static topology: membership join/leave events update the ring
// but no key data moves between nodes. A node returning to an already-
// populated cluster sees only newly-written keys. Rebalancing lands
// in v0.3.
//
// See docs/SPEC.md for the full model.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// reconcileInterval is how often the cluster polls membership for a
// full snapshot and reapplies any add/remove the event-channel layer
// may have missed under backpressure. Small enough that a dropped
// event recovers within seconds; large enough to be free in steady
// state. Exposed as a var (not const) so tests can shrink it.
var reconcileInterval = 5 * time.Second

// ErrEmptyValue is returned by Put when value is nil or zero-length.
// Use Delete to remove a key; the envelope's empty-payload shape is
// reserved for Delete tombstones and storing one via Put would surface
// as NotFound on subsequent Get calls (R>1) or empty bytes (R=1),
// silently splitting Put-with-empty into two different semantics by
// replication factor.
var ErrEmptyValue = errors.New("cluster: Put with empty value; use Delete to remove a key")

// WriteConsistency picks how many replica acks a Put / Delete waits
// for before returning. iota+1 so the zero value is "unset" + Open
// normalizes to the v0.4 default (WriteQuorum).
type WriteConsistency int

const (
	// WriteOne returns success as soon as the primary acks. Loosest
	// setting + lowest write latency; tolerates the most replica
	// failures but offers the weakest durability.
	WriteOne WriteConsistency = iota + 1
	// WriteQuorum waits for floor(R/2)+1 acks. The v0.4 default:
	// survives the loss of a minority of replicas without losing the
	// write.
	WriteQuorum
	// WriteAll waits for every replica to ack. Any down replica fails
	// the write.
	WriteAll
)

// ReadConsistency picks how many replicas a Get reads from. iota+1
// so the zero value is "unset" + Open normalizes to the v0.4 default
// (ReadNearest).
type ReadConsistency int

const (
	// ReadNearest reads from the primary only (one hop in the common
	// case; matches v0.3 R=1 behavior). No read-repair fires on
	// ReadNearest since there is nothing to compare against.
	ReadNearest ReadConsistency = iota + 1
	// ReadQuorum reads from floor(R/2)+1 replicas + returns the LWW
	// winner. Triggers async read-repair on lagging replicas.
	ReadQuorum
	// ReadAll reads from every replica. Strongest read freshness;
	// triggers read-repair on any disagreement.
	ReadAll
)

// Config configures a Cluster. NodeID + Backend are always required.
// The peer-discovery fields (BindAddr, Seeds, GRPCAddr) are required
// only for multi-node mode; in single-node mode they may be empty.
type Config struct {
	// NodeID is this node's stable identity. Used in membership +
	// ring placement. MUST be unique within the cluster.
	NodeID string

	// Backend is the local KV engine this node owns. Required in the
	// legacy per-node mode; MUST be nil in multi-backend mode (set
	// BackendFactory + UnitCount instead). See "Two modes" below.
	Backend backend.Backend

	// BackendFactory + UnitCount select MULTI-BACKEND mode (v0.8 Phase
	// 2). When BOTH are set (and Backend is nil), the node mounts one
	// backend per OWNED storage unit and routes each key to its unit's
	// owner (key -> shardKey -> unit -> owner) instead of a single
	// Backend. The factory opens / closes per-unit backends; UnitCount
	// is the fixed power-of-two number of units N the unit space is
	// partitioned into.
	//
	// Two modes, mutually exclusive (validated in Open):
	//   - legacy per-node: Backend set, BackendFactory + UnitCount unset.
	//     The default; unchanged byte-for-byte.
	//   - multi-backend:   BackendFactory + UnitCount set, Backend unset.
	//
	// Setting both modes, or neither, is a config error. Multi-backend
	// mode is STATIC in Phase 2: the unit set this node owns is fixed at
	// Open (no rebalance / lease handoff on membership change; that is
	// Phase 3). It also requires ReplicationFactor == 1 in Phase 2 (the
	// per-unit replication interplay is out of scope).
	BackendFactory storageunit.BackendFactory
	UnitCount      storageunit.UnitCount

	// ConditionalStore is the create-if-absent + compare-and-set object
	// store backing the DECLARATIVE, DECENTRALIZED reshard agreement (v0.9;
	// see docs/decentralized-reshard-design.md). When set on an R>1
	// multi-backend cluster, Open constructs and seeds a reshard.Arbiter over
	// it: the cluster's agreed reshard epoch ({count, target, plan}) lives in
	// a single durable object every node reads and advances by a
	// conditional-write race, replacing an elected coordinator. The same
	// MinIO/S3 If-None-Match / If-Match primitive slatedb already uses for
	// manifest fencing (backends/slate supplies MinioConditionalStore; tests
	// use storageunit.MemConditionalStore).
	//
	// Optional and decoupled: nil leaves the arbiter unconstructed and the
	// cluster on the existing static / coordinated-freeze reshard paths
	// (byte-for-byte unchanged). It is a no-op outside R>1 multi-backend mode
	// (the decentralized online reshard is R>1 only; the R=1 multi-node path
	// keeps the coordinated freeze barrier).
	ConditionalStore storageunit.ConditionalStore

	// DeclarativeReshard makes the cluster DRIVE the arbiter target from the
	// cluster-wide gossiped DECLARED unit count (UnitCount, advertised in
	// membership metadata): on every reconcile tick, when the cluster is steady
	// AND every live member agrees on the same declared count, the cluster
	// CAS-retargets the arbiter to it and the decentralized driver converges
	// online. This is the "declare SHALE_UNIT_COUNT in the deployment and
	// apply" operator surface for the homogeneous deployment (see
	// observeDeclaredReshardTarget + docs/SPEC.md "Declarative reshard").
	//
	// Default false: the arbiter target is then set ONLY externally (the
	// imperative Reshard path, or a test driving reshard.Arbiter.Retarget
	// directly), and the gossiped declared count is informational. Tests that
	// drive a reshard imperatively (the lossless split/merge gate) MUST leave
	// this false so their target is not reconciled back to the founded count.
	// Requires ConditionalStore (an arbiter); a no-op without one.
	DeclarativeReshard bool

	// BlobStore is the OPTIONAL streaming byte plane (the blob.Store port -
	// docs/design/blob-values.md). nil leaves the cluster metadata-only; the
	// plain *KV surface is all that is reachable. When set, NewBlobKV wraps the
	// cluster in the blob-capable *BlobKV surface (StageBlob / GetBlob /
	// BindBlob / SweepOrphans). The concrete object-store adapter
	// (blobstore.MinioBlobStore) is wired at the cmd binary, exactly as the
	// slate Backend + ConditionalStore are; tests pass an in-memory blob.Store.
	//
	// The capability is gated IN THE TYPE, not by this field's nil-ness: New
	// (the *KV constructor) rejects a non-nil BlobStore (it would be unreachable
	// through *KV), and NewBlobKV (the *BlobKV constructor) requires it. So a
	// caller cannot reach a blob method without having configured the store -
	// the wiring mistake is a constructor error, and the missing method is a
	// compile error. See kv.go.
	BlobStore blob.Store

	// BindAddr is the host:port memberlist listens on (UDP + TCP).
	// Non-empty enables multi-node mode. Format: "host:port" (host
	// may be empty for "all interfaces").
	BindAddr string

	// GRPCAddr is this node's gRPC service address, broadcast to
	// peers as their forwarding target. Required in multi-node mode.
	// Format: "host:port" (host should be a routable address; an
	// empty host works for loopback tests but won't reach peers on
	// other machines).
	GRPCAddr string

	// Seeds are addresses of already-running cluster nodes (their
	// BindAddr). Bootstrap gossip contacts these to discover the
	// rest of the membership. Empty means this node is the seed.
	Seeds []string

	// ShardKeyFn lets the app extract a shard key from a full key.
	// Default = hashTagged identity (full key, honoring `{tag}`
	// hash tags). Override for custom shard key shapes.
	ShardKeyFn func(key []byte) []byte

	// LogOutput is where membership's internal logger writes. nil
	// means memberlist's default (stderr). Tests pass io.Discard.
	LogOutput io.Writer

	// RebalanceSettleDelay is how long the cluster waits after a
	// membership event before kicking off Evaluate. Each subsequent
	// event in the window resets the timer (debounce). Zero falls
	// back to the package default (5s; matches docs/SPEC.md).
	RebalanceSettleDelay time.Duration

	// RebalanceRetryAfterMs is the hint returned in the
	// FailedPrecondition error when a Put / Delete lands on a key
	// whose partition is migrating out. Clients SHOULD wait this
	// many ms before retrying. Zero falls back to 50ms (matches
	// rebalance.DefaultOptions).
	RebalanceRetryAfterMs int

	// RebalanceGraceDuration is how long a HandedOff range stays on
	// the source before the sweep deletes its now-foreign keys. Zero
	// falls back to rebalance.DefaultOptions (30s, matching
	// docs/SPEC.md "Cutover" T_drain). Integration tests + faster-
	// feedback fixtures pass small values.
	RebalanceGraceDuration time.Duration

	// RebalanceHandoffTimeout bounds how long the source's runSend
	// waits for the destination to ack the gRPC stream. Past the
	// timeout the partition transitions Done-with-error (the next
	// Evaluate retries). Zero falls back to the package default
	// (rebalance.DefaultOptions, 5 minutes). Integration tests
	// shrink it so a "destination never asks" scenario fails fast.
	RebalanceHandoffTimeout time.Duration

	// ReplicationFactor is the number of nodes that hold a copy of
	// each key. Zero is normalized to 1 by Open (v0.3 behavior:
	// single owner per key, no replicas, no LWW envelope cost on the
	// read path). Values > 1 are clamped at fan-out time to the
	// number of distinct members in the ring (see ring.LocateKeyN).
	// HA + LWW conflict resolution is opt-in via ReplicationFactor > 1.
	ReplicationFactor int

	// OpenConcurrency bounds how many replica-position opens run IN
	// PARALLEL on this node - BOTH the boot-time mount pool
	// (mountReplicaUnits) AND the background overlap acquires the
	// reconcile spawns during a membership transition
	// (acquireReplicaUnitOverlap), which take a node-wide permit around
	// each OpenReplicaUnit. Each (unit, replica) position is an
	// independent durable database, so concurrent opens of DISTINCT
	// positions never fence one another; the cap exists because
	// concurrent REAL-DATA opens are unsafe in the shipped slatedb-go
	// binding (see defaultOpenConcurrency) and, secondarily, so an open
	// burst cannot overwhelm the shared object store. Zero (or negative)
	// is normalized to defaultOpenConcurrency (1 = strictly sequential,
	// the proven-safe mode). Raising it is a deliberate, revalidated act
	// via this one knob. Only consulted in multi-backend R>1 mode.
	OpenConcurrency int

	// WriteConsistency picks how many replica acks a Put / Delete
	// waits for. Zero is normalized to WriteQuorum by Open (the v0.4
	// default). See WriteConsistency for the per-value semantics.
	WriteConsistency WriteConsistency

	// ReadConsistency picks how many replicas a Get reads from. Zero
	// is normalized to ReadNearest by Open (the v0.4 default; matches
	// v0.3 single-replica read behavior). See ReadConsistency for the
	// per-value semantics.
	ReadConsistency ReadConsistency

	// WriteTimeout bounds the wall-clock budget for a Put / Delete
	// fanout. Each replica dispatch inherits this deadline through
	// context.WithTimeout, so a blackholed peer (hung gRPC, half-open
	// TCP) cancels at deadline rather than blocking forever. The
	// fanout still returns deterministically when the deadline fires:
	// any replica that hasn't replied is counted as a failure for the
	// purposes of the success/failure budget. Zero falls back to 5s.
	WriteTimeout time.Duration

	// ReadTimeout bounds the wall-clock budget for a Get fanout.
	// Same shape as WriteTimeout: per-dispatch deadline so a hung
	// peer doesn't block the call beyond ReadTimeout, even if the
	// configured consistency would otherwise wait for that replica.
	// Zero falls back to 5s.
	ReadTimeout time.Duration

	// PeerConnectTimeout bounds how long a cross-shard Aggregate (the
	// snapshotPeer fan-out) waits for a peer's gRPC connection to reach
	// READY before recording that peer's AggregateResult.Err. The peer
	// gRPC client is FAIL-FAST by default (gRPC's WaitForReady=false): if
	// a peer's gRPC server is momentarily not-ready at first-connect (a
	// heavy cold-start where the process is busy mounting units), the
	// CACHED ClientConn enters TRANSIENT_FAILURE + backoff and every
	// subsequent fan-out RPC on that cached client fails INSTANTLY with
	// "error reading server preface: use of closed network connection"
	// for the whole backoff window. To absorb that transient, snapshotPeer
	// explicitly waits (up to this bound) for the connection to become
	// READY before opening the scan stream, so a peer that comes up within
	// the window is scanned instead of spuriously erroring. Zero falls
	// back to 30s (generous: a cold-starting peer can take tens of seconds
	// to begin serving gRPC; the wait returns AS SOON AS the peer is ready,
	// so a healthy fan-out pays ~nothing).
	PeerConnectTimeout time.Duration

	// GenLearnBudget bounds the total wall-clock a JOINER spends re-sweeping its
	// seeds for the cluster generation at Open (learnGenerationFromSeed) before
	// failing closed. This is the cold-start SIBLING of PeerConnectTimeout: a
	// seed that is itself cold-starting has bound its gRPC port but does not
	// begin SERVING it until after its own Open returns (i.e. after it has
	// mounted every replicated unit, an object-storage-bound step that on a
	// loaded backend takes tens of seconds). During that window the joiner's
	// GenState dial times out; a single attempt that fails closed becomes a
	// CRASH-LOOP under a supervisor. Re-sweeping for this budget WAITS the seed
	// out instead. Bounded (not infinite) so a joiner whose seeds are truly dead
	// still fails Open for a supervised restart. Zero falls back to
	// defaultGenLearnBudget (180s). The caller env-plumbs this for deployments
	// whose mounts run longer (and tests set it tiny to assert fail-closed fast).
	GenLearnBudget time.Duration

	// TestingBlockPeerDials, when true, refuses every clientFor call
	// from the moment Open returns. Must be set at construction time
	// (not after) because the bootstrap Evaluate runs synchronously
	// inside Open for a joiner with already-visible peers + launches
	// FetchRange goroutines that grab clients immediately; a post-Open
	// setter would race those goroutines and let some streams complete
	// before the block engaged. Used by the destination-crash failure
	// tests to make the wired-handoff assertion robust to fast
	// loopbacks (the source's runSendWired times out waiting for an
	// ack the destination cannot deliver). Test-only; no production
	// code path reads this.
	TestingBlockPeerDials bool

	// TestingMountDelay, when >0, sleeps for that long inside Open right before
	// the unit mount, SIMULATING a slow cold-start mount (a loaded object store
	// taking tens of seconds). Because the caller starts this node's gRPC server
	// only after Open returns, the delay also delays when this node begins
	// SERVING gRPC - so a founder with this set reproduces the window in which a
	// joiner's GenState dial times out, exercising the patient learnGenerationFromSeed.
	// A debug/repro lever (env-plumbed as SHALE_DEBUG_MOUNT_DELAY); no production
	// path sets it. Named Testing* for the Config convention; it is also used to
	// reproduce the cold-start failure on a real staging cluster.
	TestingMountDelay time.Duration

	// TestingForceCleanCut, when true, BREAKS the Option B overlap handoff
	// back to the pre-2e clean-cut RELEASE-then-ACQUIRE on the R>1 reconcile
	// AND disables the Option-A WriteTimeout-bounded retry. It exists solely
	// to keep the overlap-handoff acceptance gate honest: the break-demo
	// (docs/SPEC.md "v0.8 Phase 2e", THE OVERLAP-HANDOFF AVAILABILITY GATE)
	// runs the SAME slow-mount membership change with this set and asserts the
	// ack rate COLLAPSES, proving the overlap forward is what holds
	// availability rather than rubber-stamping it. Disabling BOTH (not just
	// overlap) is required: leaving Option-A retry on would let it partially
	// absorb the slow mount and muddy the signal. Test-only; no production
	// code path sets this. See multibackend_overlap.go (the clean-cut branch)
	// and multibackend_handoff_retry.go (the single-attempt branch).
	TestingForceCleanCut bool

	// TestingForceUncleanReshard, when true, BREAKS the v0.9 decentralized split's
	// no-acked-write-loss safeguard: the driver publishes a unit's caught-up +
	// cut-over markers WITHOUT first copying the parent into its children, and
	// finalize skips its final clean-copy backstop before retiring the parents. So
	// the cluster flips routing to children + retires parents that never received
	// the parents' data, and acked writes are dropped. It exists solely to keep the
	// lossless-split oracle honest: the break-demo runs the SAME split with this
	// set and asserts the oracle CATCHES the loss, proving the copy + caught-up
	// gate is load-bearing rather than rubber-stamped. Test-only; no production
	// code path sets this. See multibackend_reshard_driver.go.
	TestingForceUncleanReshard bool

	// TestingFinalizeRetireDelay, when > 0, makes finalizeSplit sleep this long
	// between a parent's final catch-up copy and retiring it. It deterministically
	// WIDENS the post-final-scan / pre-retire window so a concurrent parent-leg
	// write provably lands inside it - the regime that exposes the acked-write
	// loss when the finalize retire boundary is NOT write-quiesced. The lossless
	// gate sets it to prove the per-unit pause across finalize is load-bearing
	// (without the pause the gate loses writes; with it the blocked write is
	// rejected transient and re-routed). Test-only; no production code path sets
	// this. See multibackend_reshard_driver.go.
	TestingFinalizeRetireDelay time.Duration

	// GracefulLeaveDrainTimeout bounds the graceful-leave drain on shutdown
	// (v0.8 Phase 2e, scale-down). When > 0 AND the cluster runs the
	// multi-backend overlap path (multiReplicated()), Close() FIRST broadcasts
	// the memberlist leave and BLOCKS until every position this node owns has
	// been handed off to its successor (the overlap drain seen from the losing
	// side, for all positions at once) OR this timeout fires - all BEFORE any
	// teardown, while the reconcile loop / serving / drainCheck / forward path
	// are still running. Then Close proceeds with the normal teardown. This
	// closes the scale-down availability gap: without it, Close tears down the
	// mounts the instant the leave is broadcast, so the leaving node's
	// positions are unserved from that instant until the survivors finish their
	// (slow) Acquiring.
	//
	// 0 = DISABLED = exactly today's behavior (Close does not drain; the gap
	// remains). This is also the break-demo state for the acceptance gate.
	// The field is a no-op outside multi-backend overlap (the R=1 lease-handoff
	// and legacy per-node paths are untouched, consistent with the rest of
	// Phase 2e living behind multiReplicated()).
	//
	// OPERATOR CONCERN: the orchestrator's termination grace period (k8s
	// terminationGracePeriodSeconds) MUST exceed this, or the orchestrator
	// SIGKILLs the process mid-drain and reopens the gap. The code enforces
	// only its own timeout, not the orchestrator's.
	GracefulLeaveDrainTimeout time.Duration
}

// validateBackendMode enforces the legacy-vs-multi-backend XOR and
// returns whether the cluster runs in multi-backend mode (v0.8 Phase 2).
//
// Exactly one mode must be selected:
//   - legacy per-node: Backend set, BackendFactory + UnitCount unset.
//   - multi-backend:   BackendFactory + UnitCount set, Backend unset.
//
// Setting both, or neither, is an error (fail closed: an operator who
// half-configures multi-backend mode must hear about it, not silently get
// the legacy path). Multi-backend mode additionally requires
// ReplicationFactor in {0, 1}: the per-unit replication interplay is out
// of Phase 2 scope, so a multi-backend cluster is single-replica.
func validateBackendMode(cfg *Config) (multi bool, err error) {
	hasFactory := cfg.BackendFactory != nil
	hasUnitCount := !cfg.UnitCount.IsZero()
	hasBackend := cfg.Backend != nil

	switch {
	case hasBackend && (hasFactory || hasUnitCount):
		return false, errors.New("cluster: set EITHER Backend (legacy per-node) OR BackendFactory+UnitCount (multi-backend), not both")
	case hasFactory != hasUnitCount:
		return false, errors.New("cluster: multi-backend mode requires BOTH BackendFactory and UnitCount")
	case hasFactory && hasUnitCount:
		// Multi-backend mode. At R>1 (replicated multi-backend, v0.8 Phase 2b)
		// the factory MUST be a ReplicaBackendFactory: a unit's R replicas are
		// independent durable databases keyed by replica position, which the
		// base BackendFactory cannot open. A single-replica factory at R>1 is a
		// configuration error caught here, not a runtime nil-deref.
		if cfg.ReplicationFactor > 1 {
			if _, ok := cfg.BackendFactory.(storageunit.ReplicaBackendFactory); !ok {
				return false, fmt.Errorf("cluster: multi-backend mode at ReplicationFactor %d requires a ReplicaBackendFactory (the per-replica independent durable databases R>1 needs)", cfg.ReplicationFactor)
			}
		}
		return true, nil
	case hasBackend:
		return false, nil
	default:
		return false, errors.New("cluster: Backend is required (or BackendFactory+UnitCount for multi-backend mode)")
	}
}

// normalizeConfig fills in v0.4 default values for any zero-valued
// fields that have a defined default. Called once at the top of Open
// so the rest of the package can rely on the normalized values
// without re-checking zero everywhere. Mutates cfg in place via the
// pointer; the caller's local Config value is left as supplied (Open
// works against the normalized copy held in c.cfg).
func normalizeConfig(cfg *Config) {
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 1
	}
	if cfg.WriteConsistency == 0 {
		cfg.WriteConsistency = WriteQuorum
	}
	if cfg.ReadConsistency == 0 {
		cfg.ReadConsistency = ReadNearest
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.PeerConnectTimeout <= 0 {
		cfg.PeerConnectTimeout = 30 * time.Second
	}
}

// Cluster is the public handle apps use. All operations are
// goroutine safe.
type Cluster struct {
	cfg     Config
	backend backend.Backend

	// multi-backend mode (v0.8). multi is true when the cluster runs in
	// multi-backend mode (BackendFactory + UnitCount set); in that mode
	// c.backend is nil and every op resolves a per-unit backend from
	// mountMap instead. In legacy mode multi is false, mountMap is nil, and
	// none of this is touched. factory opens / closes per-unit backends;
	// unitCount is the fixed N AT THE STARTING GENERATION (the live count +
	// generation that routing uses are in genState, which a doubling reshard
	// advances). genOwner answers "which node owns this generation-qualified
	// unit" off the ring (default c.genUnitOwner; tests override it). mountMap
	// holds the units this node has mounted, KEYED BY GenUnit so a gen-g unit
	// K and a gen-(g+1) unit K (the doubling's old + new) coexist without
	// colliding. The map is initialized at Open, then mutated by the Phase 3
	// lease-handoff reconcile on every membership change (acquire newly-owned
	// / release no-longer-owned) and by the Phase 4 resharder (bisect creates
	// the gen-(g+1) children, cut-over retires the gen-g unit). mountMu guards
	// mountMap. See multibackend.go and multibackend_rebalance.go.
	//
	// v0.8 Phase 2e: mountMap is keyed by ReplicaUnit = (GenUnit, replica
	// position), NOT bare GenUnit, so a node can hold the OLD position
	// (draining) AND the NEW position (acquiring) of the SAME unit at once
	// during an overlap handoff. At R=1 (and in the legacy / reshard paths) a
	// node holds at most one position per unit and uses position 0, so a
	// ReplicaUnit-keyed map is equivalent to the old per-GenUnit map: the
	// non-replicated paths key via replica0(gu). The separate replicaPos map is
	// folded into the key (the position is ru.Replica).
	multi     bool
	factory   storageunit.BackendFactory
	unitCount storageunit.UnitCount
	genOwner  func(storageunit.GenUnit) (storageunit.NodeID, bool)
	mountMu   sync.RWMutex
	mountMap  map[storageunit.ReplicaUnit]backend.Backend

	// lastAcquireErr records, per ReplicaUnit, the most recent OpenReplicaUnit
	// failure during an acquire (clean-cut or overlap). Diagnostic ONLY: the
	// acquire paths otherwise swallow the error and retry next tick, so a
	// position that fails to mount forever is invisible. Surfaced via DebugState.
	// sync.Map so it needs no Open-time init and no extra lock ordering.
	lastAcquireErr sync.Map // storageunit.ReplicaUnit -> string

	// myOpenEpoch records the EXACT epoch THIS node opened each mounted
	// ReplicaUnit at, captured from OpenReplicaUnit's RETURN VALUE (not a re-read
	// of the shared, climbing DurableEpochReplica). It is the STABLE drain-release
	// gate (beginDrain) AND the epoch the serving marker carries: both must be
	// THIS node's exact open epoch, captured once, immutable for the mount's life,
	// so a successor's marker (at the successor's open epoch == leaver's + 1) is
	// permanently strictly above the leaver's gate. Recorded just before
	// mountMap[ru] at every open site (so a beginDrain that sees the mount also
	// sees the epoch); cleared on release. See docs/SPEC.md v0.8 Phase 2e. sync.Map
	// (no Open-time init).
	myOpenEpoch sync.Map // storageunit.ReplicaUnit -> storageunit.Epoch

	// handoffPhase holds, per IN-FLIGHT ReplicaUnit, the pure
	// ownership-transition state of an overlap handoff (v0.8 Phase 2e,
	// Option B). Absence means steady state: Owned if mountMap[ru] is present,
	// Absent otherwise. Only positions mid-transition carry a HandoffState:
	//
	//   - on the GAINER (taking over a position): PhaseAcquiring -> PhaseReady.
	//     While Acquiring the new owner is still mounting and FORWARDS routed
	//     ops to the recorded Predecessor (the position-addressed forward); at
	//     Ready its mountMap[ru] entry is inserted (the mount flip) and it
	//     serves locally.
	//   - on the LOSER (giving up a position): PhaseDraining -> PhaseReleasing.
	//     While Draining the old owner KEEPS SERVING (directly and for the new
	//     owner's forwards); at Releasing it tears down its mount exactly once
	//     after the new owner is proven Ready via the durable serving marker.
	//
	// Guarded by mountMu (the same lock as mountMap), so the phase value + the
	// mount entry move together: the complete state of an in-flight position is
	// the phase value + the mount entry + (Acquiring) the recorded predecessor,
	// all keyed by ReplicaUnit. NO scattered isDraining/hasFlipped booleans.
	// Nil outside the R>1 (multiReplicated) paths.
	handoffPhase map[storageunit.ReplicaUnit]storageunit.HandoffState

	// acquireInFlight tracks ReplicaUnits whose overlap mount (the slow
	// OpenReplicaUnit) is currently running in a background goroutine. The
	// overlap acquire opens asynchronously so a node gaining MANY positions at
	// once (a graceful scale-down hands a survivor all the leaving node's
	// positions) mounts them CONCURRENTLY instead of serializing one slow open
	// after another inside the reconcile - which would make the drain exceed its
	// budget and the leaving node depart before the hand-off completes. The set
	// is the idempotency guard: a reconcile that re-drives an already-in-flight
	// acquire must NOT spawn a second open for the same position. Guarded by
	// mountMu.
	acquireInFlight map[storageunit.ReplicaUnit]struct{}

	// openSem is the node-wide permit gate bounding how many replica-unit
	// opens run CONCURRENTLY on this node's factory, sized by
	// Config.OpenConcurrency (normalized via openConcurrency; default 1).
	// The background overlap acquires (acquireReplicaUnitOverlap goroutines)
	// take a permit around each OpenReplicaUnit, so a node gaining many
	// positions at once queues the opens at the SAME bound the boot mount
	// pool (mountReplicaUnits) enforces - one knob governs every real-data
	// open on the node. The queued positions stay PhaseAcquiring (the union
	// covers them) so bounding costs availability nothing; it only sequences
	// the FFI opens, which is required while concurrent real-data opens are
	// unsafe in the shipped binding (see defaultOpenConcurrency). Created
	// LAZILY under mountMu on first use so its size reflects the config at
	// first acquire (mirrors mountReplicaUnits sizing its pool at call time).
	openSem chan struct{}

	// drainPollerActive guards the at-most-one background fast drain poller
	// (ensureDrainPoller): while any position is Draining the poller re-runs
	// the release checks every displacedDrainPollInterval so a displaced
	// owner releases within ~half a second of its successor's serving marker
	// instead of waiting for the periodic reconcile tick.
	drainPollerActive atomic.Bool

	// draining is a TEST-ONLY override for the gossiped Draining set: when
	// non-nil, drainingIDs returns it directly instead of reading the membership
	// snapshot. It lets the white-box pending-ranges tests inject a transition
	// without a memberlist. Nil in production (the snapshot is authoritative).
	draining map[string]struct{}

	// joining is a TEST-ONLY override for the gossiped Joining set: when non-nil,
	// joiningIDs returns it directly instead of reading the membership snapshot.
	// It lets the white-box pending-ranges tests inject a JOIN transition (and
	// exercise the quorum floor) without a memberlist. Nil in production (the
	// snapshot is authoritative). The entry-side mirror of draining above.
	joining map[string]struct{}

	// selfJoining tracks whether THIS node currently advertises the Joining bit.
	// Set true at boot when mountReplicaUnits boot-defers one or more owned
	// positions (a peer is serving them); cleared by the reconcile once every
	// owned position is mounted. Kept as a local atomic so the reconcile's
	// clear-decision does not race a self-snapshot; the authoritative gossiped bit
	// is driven through membership.SetJoining alongside this flag.
	selfJoining atomic.Bool

	// replicaFactory is the R>1 (replicated multi-backend, v0.8 Phase 2b)
	// capability view of factory: non-nil iff the factory implements
	// ReplicaBackendFactory AND R>1. The replicated paths open each owned unit
	// at its replica POSITION (an independent durable database) through this.
	// Nil at R=1 and in legacy mode. The per-unit replica POSITION is the
	// mountMap key's Replica field (Phase 2e re-keying); there is no separate
	// replicaPos map.
	replicaFactory storageunit.ReplicaBackendFactory

	// genState is the generation-aware routing state (v0.8 Phase 4): the
	// CURRENT generation, the unit count at that generation, the doubled
	// count at the next generation, and the per-old-unit cut-over set. A key
	// resolves to GenUnit{gen, UnitForHash(h, count)} unless its old unit has
	// cut over, in which case it resolves to GenUnit{gen+1, UnitForHash(h,
	// nextCount)}. genMu guards it (RWMutex: every routed op takes the read
	// lock; the resharder takes the write lock to flip a cut-over flag or
	// advance the generation). pauseUnits holds, per OLD unit being cut over,
	// a mutex the routed write path briefly blocks on so the catch-up drain
	// has a clean write-pause boundary (the NO-ACKED-WRITE-LOST cut-over).
	// reshardMu serializes whole reshards (one Reshard at a time). All zero /
	// nil in legacy mode.
	genMu      sync.RWMutex
	genState   genState
	reshardMu  sync.Mutex
	pauseMu    sync.Mutex
	pauseUnits map[storageunit.UnitID]*sync.RWMutex

	// arbiter is the DECENTRALIZED reshard agreement (v0.9): the cluster's
	// agreed reshard epoch ({count, target, plan}) in a single CAS-guarded
	// durable object every node reads + advances by a conditional-write race
	// (no elected coordinator; see docs/decentralized-reshard-design.md). Non-
	// nil only when cfg.ConditionalStore is set AND this is an R>1 multi-backend
	// cluster (initReshardArbiter); nil leaves the cluster on the existing
	// static / coordinated-freeze reshard paths. It holds no per-node state, so
	// no extra lock: the durable object's CAS version is the concurrency
	// control. Reads/retargets/advances go through reshard.Arbiter.
	arbiter *reshard.Arbiter

	// freeze is the cluster-wide WRITE-FREEZE flag for the v0.8 multi-node
	// reshard (cluster-wide freeze barrier; see multibackend_reshard_barrier.go).
	// While a node is frozen, every WRITE path (Put / Delete / the CAS commit
	// write path) returns the retryable acquiring-window error (codes.Unavailable)
	// so no write is acked during the static bisect; READS continue. The
	// coordinator's FREEZE phase sets it on every node, RESUME / ABORT clears it.
	// freezeMu guards both fields. All zero / nil in legacy mode (the barrier
	// never runs there); zero-value (frozen=false) is the steady state in multi
	// mode, so steady-state writes pay only one cheap atomic-free locked read.
	freezeMu        sync.Mutex
	frozen          bool
	freezeTargetGen storageunit.Generation
	// frozenAt records when this node most recently ENTERED the freeze (the
	// false->true edge in reshardFreeze). The self-heal loop uses it to age out a
	// STALE freeze: a node that FLIPPED to the target generation but never got
	// RESUME (a dropped RESUME RPC) would otherwise reject every write forever, so
	// once it has been frozen-past-its-flip longer than staleFreezeGrace the
	// self-heal clears the freeze. Zero in legacy mode.
	frozenAt time.Time

	// testingAfterFreezeHook, when non-nil, runs on the COORDINATOR exactly once
	// after the FREEZE barrier completes and BEFORE the pre-BISECT membership
	// re-check. Test-only seam (nil in every production path, so it costs one nil
	// compare): it lets a test inject a membership change across the barrier and
	// assert the coordinator ABORTS on it deterministically, without racing a real
	// node join/leave against the phase timing. See TestingSetAfterFreezeHook.
	testingAfterFreezeHook func()

	// reconcileMu serializes the Phase 3 lease-handoff reconcile
	// (multibackend_rebalance.go): at most one reconcile mutates the mount
	// map at a time, so two membership changes whose settle timers fire
	// close together cannot interleave mounts. It is the multi-backend
	// analogue of the legacy single-flight settle-timer Evaluate. Nil work
	// in legacy mode (the reconcile never runs there). It is DISTINCT from
	// mountMu: mountMu guards individual mount-map reads/writes (taken by
	// every KV op); reconcileMu serializes whole reconcile PASSES so the
	// acquire/release diff sees a coherent before-state.
	reconcileMu sync.Mutex

	// Populated in multi-node mode; nil in single-node mode.
	ring       *ring.Ring
	membership *membership.Membership

	clientsMu sync.RWMutex
	clients   map[string]*peerClient // peer gRPC addr -> client

	// casCommitMu serializes the owner-side CAS validate-and-apply
	// (CommitCASApply) so the read-set validation and the write-set apply
	// are atomic relative to other CAS commits on this node. Without it,
	// two concurrent commits could both pass validation against the same
	// observed value and both apply (a lost update): the memory backend's
	// transaction provides snapshot-isolation reads but NOT write-write
	// conflict detection, so OCC correctness depends on the owner
	// serializing the check-and-apply step here. This is a coarse
	// per-node lock; the commit window is short (one local tx with no
	// network inside it), so contention is bounded. A future refinement
	// could stripe it per shard / per partition.
	casCommitMu sync.Mutex

	// applyMu serializes the apply-if-newer LWW-on-write check on every
	// REPLICA-RECEIVING write path (dispatchReplicaPut's local-self
	// branch, LocalReplicaPut for a forwarded single-key Put, and
	// ApplyBatchLocal for a CAS write-set fan-out) at R>1. Each of those
	// paths reads the stored envelope's stamp, compares it against the
	// incoming stamp, and Puts the incoming bytes only if strictly newer
	// (or no stored value). That get-compare-put MUST be atomic per key:
	// the memory backend's transaction has snapshot-isolation reads but
	// NO write-write conflict detection, so two concurrent applies on the
	// same key could both read the old stamp and one could lose. This
	// node-wide lock serializes the window (one local backend op, no
	// network inside it, so contention is bounded). It is DISTINCT from
	// casCommitMu: the owner's OWN CAS local commit is authoritative by
	// construction (validated under casCommitMu) and writes the freshest
	// stamp directly without the apply-if-newer check; only the replica-
	// receiving paths take applyMu. A future refinement could stripe it
	// per key. The R=1 path never takes this lock (raw values, no
	// envelopes, no LWW).
	applyMu sync.Mutex

	// peerClientsBlocked, when true, makes clientFor return an error
	// instead of dialing. Test-only seam used by the destination-
	// crash failure tests to guarantee a node cannot reach any peer
	// regardless of loopback speed (see Config.TestingBlockPeerDials).
	// Wired from Config at Open time so the block is in effect before
	// the bootstrap Evaluate fires; runtime mutation isn't supported.
	peerClientsBlocked bool

	// closeCh is closed by Close exactly once to signal the events
	// loop (and any other lifecycle goroutines) to exit. closeOnce
	// guards the close so concurrent / repeated Close calls don't
	// panic on close-of-closed. closed mirrors the open/closed state
	// for fast no-lock checks from the KV path.
	closeCh   chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	loopWG    sync.WaitGroup

	// drainOnce guards the graceful-leave drain (v0.8 Phase 2e, scale-down) so
	// it runs exactly once at the top of Close even under concurrent Close
	// calls. The winner runs DrainForLeave while closed is still false; the
	// loser falls through to the closed CAS and waits on loopWG.
	drainOnce sync.Once

	// Rebalance state (multi-node only; rebalance is empty in
	// single-node mode). rebalance is the per-range Coordinator, held
	// behind an atomic.Pointer so the events / reconcile loops can
	// observe a coherent value without serializing against the rare
	// replaceCoordinator path (--cancel). ringGen is the monotonic
	// generation counter bumped on every membership-driven ring
	// change (events loop + reconcile loop both call bumpRingGen).
	// lastEvalRing is the ring snapshot from the most recent
	// Evaluate, paired with the live ring on the next tick to compute
	// the delta plan. settleMu + settleTimer drive the debounce:
	// every ring change (re)arms the timer; when it fires,
	// runEvaluate computes the plan. rebalanceCtx / rebalanceCancel
	// drive the background sweep loop.
	rebalance   atomic.Pointer[rebalance.Coordinator]
	ringGen     atomic.Uint64
	settleMu    sync.Mutex
	settleTimer *time.Timer
	// settlePending counts debounce-scheduled evaluations that have
	// been armed but not yet completed (the timer is live, or its
	// AfterFunc callback is running but has not yet returned). It makes
	// a scheduled-but-unrun evaluation visible to WaitForRebalanceIdle:
	// a node with settlePending > 0 is NOT rebalance-idle even though
	// the Coordinator's range table may still be empty. Incremented when
	// arming a FRESH timer (decided under settleMu so the "was there a
	// live timer" read pairs atomically with the increment), decremented
	// in a defer at the end of the runEvaluate / runReconcile callback
	// once the Coordinator has registered its ranges. A re-arm of a
	// still-live timer does NOT double-count. See docs/SPEC.md "Trigger".
	settlePending   atomic.Int64
	lastEvalRing    *ring.Ring
	rebalanceCtx    context.Context
	rebalanceCancel context.CancelFunc

	// repairCtx / repairCancel govern the lifetime of async read-
	// repair goroutines. Close cancels repairCtx so any in-flight
	// repair sees a canceled gRPC context + bails out; repairWG
	// blocks Close until every spawned repair has exited so a Close-
	// concurrent repair cannot try to dial through a torn-down
	// peerClient after Close has cleared c.clients. Initialized
	// unconditionally (single-node mode is a degenerate case with
	// no peers to repair against, so the goroutine count stays at 0
	// but the context still exists for safe Close coordination).
	repairCtx    context.Context
	repairCancel context.CancelFunc
	repairWG     sync.WaitGroup
}

// Open initializes a Cluster from cfg. In single-node mode it just
// records the cfg + returns the wrapper. In multi-node mode (when
// cfg.BindAddr is non-empty) it additionally brings up memberlist,
// joins seeds, seeds the ring with the local node + any peers already
// seen, and starts a goroutine that mirrors membership events into the
// ring.
func Open(cfg Config) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("cluster: NodeID is required")
	}
	if len(cfg.NodeID) > MaxNodeIDLen {
		return nil, fmt.Errorf("cluster: NodeID length %d exceeds MaxNodeIDLen %d; the envelope's uint16 length prefix would silently truncate",
			len(cfg.NodeID), MaxNodeIDLen)
	}
	multi, err := validateBackendMode(&cfg)
	if err != nil {
		return nil, err
	}
	normalizeConfig(&cfg)

	c := &Cluster{
		cfg:                cfg,
		backend:            cfg.Backend, // nil in multi-backend mode
		multi:              multi,
		factory:            cfg.BackendFactory,
		unitCount:          cfg.UnitCount,
		clients:            make(map[string]*peerClient),
		closeCh:            make(chan struct{}),
		peerClientsBlocked: cfg.TestingBlockPeerDials,
	}
	c.repairCtx, c.repairCancel = context.WithCancel(context.Background())

	if cfg.BindAddr == "" {
		// Single-node mode: no membership, no ring, every op is local.
		// In multi-backend mode this node still owns the whole unit
		// space (genUnitOwner reports the local owner on an empty ring),
		// so mount it now.
		if multi {
			if err := c.initMultiBackend(); err != nil {
				return nil, err
			}
		}
		return c, nil
	}

	if cfg.GRPCAddr == "" {
		return nil, errors.New("cluster: GRPCAddr is required in multi-node mode")
	}

	// JOIN write-transparency (v0.8 Phase 2e, entry side): a node JOINING an
	// existing REPLICATED cluster (it has seeds, and R>1) advertises the Joining
	// bit in its VERY FIRST gossip Meta - atomically with its ring presence - so a
	// peer never learns the newcomer is a member (and clean-cut releases a position
	// it displaces) BEFORE it learns the newcomer is Joining (which tells it to HOLD
	// + drain instead). A founder (no seeds) or an R=1 node leaves it false, so cold
	// first-cluster formation and the single-replica path are unchanged. The
	// reconcile clears the bit once this node has mounted every position it owns.
	startJoining := multi && cfg.ReplicationFactor > 1 && len(cfg.Seeds) > 0
	c.selfJoining.Store(startJoining)

	mem, err := membership.Open(membership.Config{
		NodeID:    cfg.NodeID,
		BindAddr:  cfg.BindAddr,
		GRPCAddr:  cfg.GRPCAddr,
		Seeds:     cfg.Seeds,
		Joining:   startJoining,
		LogOutput: cfg.LogOutput,
		// Gossip this node's standing declared shard count (SHALE_UNIT_COUNT)
		// so the cluster can detect cluster-wide AGREEMENT on a desired count
		// and drive a declarative reshard toward it (observeDeclaredReshardTarget).
		// A zero UnitCount (legacy / non-multi-backend mode) yields 0 = "do not
		// advertise", so peers treat this node as UNKNOWN and never auto-reshard
		// on its account.
		DeclaredUnitCount: cfg.UnitCount.N(),
		// Periodic seed re-Join heals a post-startup gossip split (e.g. a
		// mass rolling restart fragmenting the ring). Wired with the
		// production default; only active when Seeds is non-empty, so a
		// founder (no seeds) and in-process tests that pass no seeds run no
		// loop. See membership.Config.RejoinInterval.
		RejoinInterval: membership.DefaultRejoinInterval,
		// Periodic local-Meta re-broadcast heals a STALE-Meta peer view after
		// a STAGGERED rolling restart (a restarted pod resets its memberlist
		// incarnation, so peers reject its fresh declared count as stale until
		// the bumped-incarnation re-broadcast overtakes the remembered value).
		// Wired with the production default; the gossiped declared count is
		// what the declarative-reshard unanimity gate reads, so it must
		// converge deterministically rather than race. See
		// membership.Config.MetaRefreshInterval.
		MetaRefreshInterval: membership.DefaultMetaRefreshInterval,
		// Homogeneous bootstrap: when a shared ConditionalStore is wired, every
		// node carries the SAME seed list (a headless Service) and the first one
		// up reaches no peer. AllowSoloStart lets it come up solo and contend to
		// form via the __cluster/init marker, instead of failing Open. Without a
		// ConditionalStore (no durable form-lock) keep the strict behavior: a
		// joiner with unreachable seeds fails rather than silently fork.
		AllowSoloStart: cfg.ConditionalStore != nil,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: membership: %w", err)
	}

	c.membership = mem
	c.ring = ring.New()

	// Seed the ring with whatever membership knows about right now
	// (always at least the local node; possibly peers if Join already
	// returned a populated snapshot).
	for _, m := range mem.Members() {
		c.ring.Add(ring.Member{ID: m.ID, Addr: m.Addr})
	}

	if multi {
		// Multi-backend mode (v0.8). Derive the owned units from the
		// now-seeded ring + mount them at Open. On every later membership
		// change the events / reconcile loops below call bumpRingGen, which
		// in multi mode arms the COPY-FREE lease-handoff reconcile (Phase 3,
		// multibackend_rebalance.go) instead of the v0.3 Coordinator: a unit
		// whose owner moved is released by the old owner (CloseUnit, flush)
		// and acquired by the new owner (OpenUnit at a higher epoch,
		// fencing the old). The v0.3 Coordinator is intentionally NOT
		// started here (c.rebalance stays nil); the reconcile is its
		// multi-backend replacement.
		if err := c.initMultiBackend(); err != nil {
			_ = mem.Close()
			return nil, err
		}
		c.loopWG.Add(2)
		go c.runEventsLoop()
		go c.runReconcileLoop()
		return c, nil
	}

	// Rebalance machinery: Coordinator + settle-timer + sweep.
	// initRebalance MUST run BEFORE the events / reconcile goroutines
	// spawn. Both of those call bumpRingGen -> scheduleEvaluate, which
	// reads c.rebalance + (under settleMu) c.lastEvalRing. Initializing
	// after the spawn races on those fields the very first time a
	// membership event arrives. initRebalance only touches local fields
	// from the calling goroutine, so doing it here is the safest
	// publish point.
	c.initRebalance()

	// Run the events loop so future joins / leaves keep the ring in
	// sync with membership. Also run a slower reconciliation loop
	// that re-syncs the ring against the authoritative membership
	// snapshot on a fixed cadence - belt + suspenders against the
	// rare event-channel drop (membership uses a non-blocking send
	// to preserve no-deadlock guarantees; if a consumer ever falls
	// behind a join/leave could be lost, leaving the ring stale
	// forever without this loop).
	c.loopWG.Add(2)
	go c.runEventsLoop()
	go c.runReconcileLoop()

	return c, nil
}

// runEventsLoop mirrors membership join/leave events into the ring.
// It exits when the membership events channel is closed (Close drives
// that) or closeCh is signalled. The two-channel select keeps Close
// from blocking on a slow producer.
func (c *Cluster) runEventsLoop() {
	defer c.loopWG.Done()
	events := c.membership.Events()
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch ev.Type {
			case membership.EventJoin:
				// v0.8 Phase 2e (pending ranges): a DRAINING member STAYS a
				// current owner in the consistent-hash ring - it is NOT excluded.
				// The current/pending split (current = ring INCLUDING draining;
				// pending = ring EXCLUDING draining) is computed PER-OP inside
				// replicasForKey, not baked into the ring. So the event loop Adds a
				// draining member normally (it keeps its positions + keeps serving
				// the union); the drain ends only when its successors are provably
				// serving (the serving-marker gate), and the real memberlist.Leave()
				// then fires an EventLeave that removes it. (This REVERSES the
				// superseded draining-exclusion stopgap.)
				//
				// If the addr changed (NotifyUpdate path - same ID,
				// different Meta payload), evict the cached client
				// for the OLD addr so the next dial picks up the
				// new endpoint. Look up the prior addr via the ring
				// snapshot before Add overwrites it.
				oldAddr := c.priorAddrForID(ev.Member.ID)
				c.ring.Add(ring.Member{ID: ev.Member.ID, Addr: ev.Member.Addr})
				if oldAddr != "" && oldAddr != ev.Member.Addr {
					c.evictClient(oldAddr)
				}
				c.bumpRingGen()
			case membership.EventLeave:
				c.ring.Remove(ev.Member.ID)
				// Drop any cached client for this departed peer so
				// the next dial picks up a fresh connection if the
				// same node ever returns on a different address.
				c.evictClient(ev.Member.Addr)
				c.bumpRingGen()
			}
		case <-c.closeCh:
			return
		}
	}
}

// runReconcileLoop re-syncs the ring against membership.Members() on
// a fixed cadence. The event-channel layer in membership drops events
// on backpressure (intentional - keeps memberlist's callback goroutine
// unblocked); without this loop, a single dropped join would leave the
// ring permanently missing that node. With it, ring divergence
// auto-heals within one tick.
func (c *Cluster) runReconcileLoop() {
	defer c.loopWG.Done()
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-t.C:
			c.reconcileRingFromMembership()
			if c.multi {
				// Self-heal (v0.8 Phase 3): a transient OpenUnit/CloseUnit
				// failure during an earlier reconcile would otherwise strand
				// a unit (owned but not mounted, or mounted but not owned)
				// until the next membership change, since membership-driven
				// reconciles only fire on a ring change. The unit reconcile
				// is idempotent and cheap when the mounted set already matches
				// desired, so run it every tick to re-acquire/release any
				// drifted unit. This runs inside the loopWG-tracked loop, so
				// Close awaits it.
				c.runReconcile()
			}
		}
	}
}

// reconcileRingFromMembership reads the authoritative membership snapshot
// and applies any missed adds/removes to the ring. Idempotent: an
// already-present member's Add is a no-op (ring.Add overwrites the
// same Member with itself) and a non-member's absence is the desired
// state.
func (c *Cluster) reconcileRingFromMembership() {
	if c.membership == nil || c.ring == nil {
		return
	}
	snap := c.membership.Snapshot()
	want := make(map[string]ring.Member, len(snap))
	for _, m := range snap {
		// v0.8 Phase 2e (pending ranges): a DRAINING member STAYS a CURRENT
		// OWNER in the ring - do NOT exclude it. The current/pending split is
		// computed per-op inside replicasForKey (current = ring INCLUDING
		// draining members; pending = the same ring with draining members
		// removed from the located chain), so the ring carries every alive
		// member and the leaver keeps its positions + keeps serving the routed
		// union until its successors are provably serving. (This REVERSES the
		// superseded draining-exclusion: dropping a draining member here is what
		// collapsed its snapshot and stranded the hand-off.)
		want[m.ID] = ring.Member{ID: m.ID, Addr: m.Addr}
	}
	changed := false
	// Add anyone missing from the ring (or with a stale Addr).
	for id, m := range want {
		oldAddr := c.priorAddrForID(id)
		if oldAddr != m.Addr {
			c.ring.Add(m)
			if oldAddr != "" {
				c.evictClient(oldAddr)
			}
			changed = true
		}
	}
	// Remove anyone the ring still has but membership has dropped.
	for _, m := range c.ring.Members() {
		if _, ok := want[m.ID]; !ok {
			c.ring.Remove(m.ID)
			c.evictClient(m.Addr)
			changed = true
		}
	}
	if changed {
		// A reconcile-driven ring change is no different from an
		// event-driven one as far as the rebalance protocol cares;
		// arm the settle timer so the missed delta gets folded into
		// the next Evaluate.
		c.bumpRingGen()
	}
}

// priorAddrForID returns the Addr currently recorded in the ring for
// the given ID, or "" if absent. Used by the reconciliation path to
// evict a now-stale cached client when a peer's Addr changes.
func (c *Cluster) priorAddrForID(id string) string {
	if c.ring == nil {
		return ""
	}
	for _, m := range c.ring.Members() {
		if m.ID == id {
			return m.Addr
		}
	}
	return ""
}

// NodeID returns this node's stable identity, as supplied in Config.
func (c *Cluster) NodeID() string { return c.cfg.NodeID }

// Members returns a snapshot of the cluster's current ring membership.
// In single-node mode this is a single-element slice containing the
// local node with Addr=cfg.GRPCAddr (which can be empty if the caller
// didn't set GRPCAddr; no gRPC peer routing is set up in single-node
// mode regardless).
func (c *Cluster) Members() []ring.Member {
	if c.ring == nil {
		return []ring.Member{{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}}
	}
	return c.ring.Members()
}

// OwnsKey reports whether the local node is the RING owner of key.
// Used by the gRPC server's forwarding-loop guard: a request that
// arrives with forwarded=true but does NOT belong here is refused
// rather than re-forwarded (which would loop A->B->A on diverged
// rings).
//
// In multi-backend mode (v0.8 Phase 3) the guard is UNIT-RING-OWNERSHIP
// based: a key belongs to this node iff the ring places the key's storage
// UNIT on this node, WHETHER OR NOT it is mounted yet. This is the Phase 3
// correction of the Phase 2 mount-based guard: during a lease handoff the
// new owner is the ring owner BEFORE it has mounted the unit (the acquire
// is in flight). A mount-based guard would refuse a forwarded op there with
// FailedPrecondition ("refresh your ring"), but the originator's ring is
// already correct - refreshing changes nothing and it would re-forward to
// the same node in a tight loop. Checking RING ownership instead lets the
// forwarded op fall through to the local apply path, which returns the
// retryable acquiring-window error (errUnitAcquiring, codes.Unavailable) so
// the originator backs off and succeeds once the reconcile mounts the unit.
// A node that is NOT the ring owner still returns false here, so the
// classic loop-guard (FailedPrecondition, refresh + re-route) is preserved
// for a genuinely diverged ring.
func (c *Cluster) OwnsKey(key []byte) bool {
	if c.multi {
		_, isLocal := c.unitOwnerOf(key)
		return isLocal
	}
	_, local := c.ownerOf(key)
	return local
}

// LocalScanPrefix returns an iterator over the LOCAL backend's keys
// with the given prefix, bypassing ring routing entirely. Use this
// for admin-style operations (peer snapshotting, per-node counters)
// where the receiver explicitly wants to see what's physically on
// this node's storage, not "what the ring says belongs here".
//
// In multi-backend mode (v0.8 Phase 2) "the local backend" is the union
// of every MOUNTED unit, so the scan walks all mounted units in unit
// order and concatenates their iterators. keysHeld + Aggregate's
// per-node snapshot rely on this seeing the node's whole physical
// keyspace, not one unit.
func (c *Cluster) LocalScanPrefix(prefix []byte) (backend.Iterator, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	if c.multi {
		return c.localScanMounted(prefix)
	}
	return c.backend.ScanPrefix(prefix)
}

// LocalGet returns the value for key directly from the local backend,
// bypassing the cluster's owner routing. Used by the gRPC Get
// handler in the v0.3 receive-window read forwarder: when the
// destination forwards a read to us (the source) because we still
// hold the authoritative copy mid-migration, we want to serve from
// our local backend even though our own ring has moved the
// partition off us. Returns backend.ErrNotFound if the key is
// absent locally.
func (c *Cluster) LocalGet(key []byte) ([]byte, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	if c.multi {
		// PHYSICAL mount resolution (any replica index), not the ring index:
		// LocalGet exists to serve a key this node physically holds even though
		// the ring moved it off us (the v0.3 receive-window forwarder AND a v0.8
		// DRAINING node, which is excluded from the ownership ring but still holds
		// the unit mounted while it hands off). localBackendForKey would disclaim
		// it via the ring index; scan the mountMap by unit instead.
		b, ok := c.localMountedBackendForKey(key)
		if !ok {
			return nil, backend.ErrNotFound
		}
		// b is the fence-self-healing mount (storeMount): a fenced forwarded read
		// recodes to transient + evicts the stale mount on the node that physically
		// holds it, so it self-heals instead of returning the raw fence forever.
		return b.Get(key)
	}
	return c.backend.Get(key)
}

// LocalBegin opens a transaction directly against the local Backend at
// the given isolation level, bypassing ring routing. It is the owner-
// local primitive the CAS validate-and-apply commit (CommitCASApply)
// opens its single short transaction on; the caller is responsible for
// having verified ownership of the pin key first. Returns
// backend.ErrClosed if the cluster is shutting down.
//
// Legacy mode only: in multi-backend mode there is no single backend to
// begin against (the right backend depends on the pin key's unit), so the
// CAS path uses localBeginForKey instead. Calling LocalBegin in multi
// mode is a programming error and returns an error.
func (c *Cluster) LocalBegin(level backend.IsolationLevel) (backend.Transaction, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	if c.multi {
		return nil, errors.New("cluster: LocalBegin is not valid in multi-backend mode; use localBeginForKey")
	}
	return c.backend.Begin(level)
}

// localBeginForKey opens a transaction against the mounted backend that
// owns key's unit (legacy mode: the single backend). It is the CAS
// validate-and-apply primitive that works in BOTH modes: the caller has
// already verified this node owns the pin key, so in multi-backend mode a
// missing mount means the unit's lease is HANDING OFF to this node (Phase 3
// window) - the commit is refused with the retryable acquiring-window error
// (codes.Unavailable), never applied against a wrong / unmounted engine. The
// originator retries and the commit succeeds once the reconcile has acquired
// the unit.
//
// RESHARD SAFETY (Phase 4): in multi mode the transaction is opened while
// holding the pin unit's write-pause read-lock (localWriteBackendForKey), and
// the returned transaction is wrapped (pausedTx) so the pause is released only
// when the transaction commits or rolls back. This keeps the whole validate-
// and-apply on the SAME generation: a reshard cut-over for the pin unit
// (which needs the pause WRITE side) cannot land between resolve and commit,
// so a CAS write never lands in a unit that was retired mid-transaction.
func (c *Cluster) localBeginForKey(key []byte, level backend.IsolationLevel) (backend.Transaction, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	if c.multi {
		// FREEZE gate (v0.8 multi-node reshard): the CAS commit write path is a
		// WRITE, so a frozen node refuses it with the retryable error - even a
		// transaction opened BEFORE the freeze is refused at commit, so no CAS
		// write is acked during the static bisect. The client retries the whole
		// transaction (Transact's loop) after RESUME, at the new generation.
		if c.isFrozen() {
			return nil, errWriteFrozen("CommitCASApply")
		}
		b, ru, unlock, ok := c.localWriteBackendForKey(key)
		if !ok {
			unlock()
			return nil, errUnitAcquiring("CommitCASApply")
		}
		tx, err := b.Begin(level)
		if err != nil {
			// Stale mount (lease moved): evict + retryable so the CAS commit
			// retries and lands on the freshly re-acquired mount.
			c.evictStaleMount(ru, b)
			unlock()
			return nil, errUnitAcquiring("CommitCASApply")
		}
		return &pausedTx{Transaction: tx, unlock: unlock, ru: ru, b: b}, nil
	}
	return c.backend.Begin(level)
}

// pausedTx wraps a backend.Transaction so the reshard write-pause read-lock
// held while the transaction was opened is released exactly once, when the
// transaction terminates (Commit or Rollback). It keeps a CAS validate-and-
// apply bound to a single generation across the whole commit window.
//
// ru + b carry the resolved pin-unit position and its mounted backend so the
// CAS validate-and-apply (CommitCASApply) can recode a FENCED tx op (Get / Put
// / Commit, which surface on real slatedb AFTER a successful Begin) to the
// TRANSIENT acquiring-window error via c.fenceToTransient(ru, b, ...) - the
// same recode the single-key Put fan-out applies. They are zero-valued on the
// R=1 / non-multi branch (a bare c.backend.Begin tx, no pausedTx wrapper), so a
// fence recode is a no-op there: that branch has a single backend with no epoch
// handoff, so no mounted-unit fence can surface.
type pausedTx struct {
	backend.Transaction
	unlock   func()
	released bool
	ru       storageunit.ReplicaUnit
	b        backend.Backend
}

func (t *pausedTx) Commit() error {
	err := t.Transaction.Commit()
	t.release()
	return err
}

func (t *pausedTx) Rollback() error {
	err := t.Transaction.Rollback()
	t.release()
	return err
}

func (t *pausedTx) release() {
	if !t.released {
		t.released = true
		t.unlock()
	}
}

// Close releases all cluster resources. After Close, no other method
// may be called. Idempotent + safe to call concurrently with Put/Get/
// Delete/ScanPrefix - in-flight KV ops still race-finish against the
// pre-Close backend, but ops STARTING after closed=true return
// backend.ErrClosed instead of panicking on a torn-down backend.
func (c *Cluster) Close() error {
	// Graceful-leave drain (v0.8 Phase 2e, scale-down) runs at the TOP of
	// Close, BEFORE the closed CAS and BEFORE any teardown. It must run while
	// closed is still false: the reconcile loop, drainCheck, the serving path,
	// and the acquire/forward path all short-circuit on c.closed.Load(), so the
	// hand-off can only converge while those paths are live. Gated on config +
	// multiReplicated; the drainOnce guard makes it run exactly once even under
	// concurrent Close calls (the loser of drainOnce falls straight through to
	// the closed CAS below, where it waits on loopWG). DrainForLeave itself is a
	// no-op outside multiReplicated, so the only observable effect when the
	// feature is disabled (timeout 0) is skipping this block entirely.
	if c.cfg.GracefulLeaveDrainTimeout > 0 {
		c.drainOnce.Do(func() {
			if !c.multiReplicated() {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GracefulLeaveDrainTimeout)
			defer cancel()
			_ = c.DrainForLeave(ctx)
		})
	}

	if !c.closed.CompareAndSwap(false, true) {
		// Another Close already ran (or is running). Wait for the
		// background loops to wind down before returning so callers
		// see a fully-stopped cluster on Close return regardless of
		// which call won the CAS.
		c.loopWG.Wait()
		return nil
	}

	var firstErr error

	// Stop the settle timer so it can't fire a fresh Evaluate after
	// the membership is torn down. Held in settleMu to coordinate
	// with scheduleEvaluate.
	c.settleMu.Lock()
	if c.settleTimer != nil {
		// A live timer owns one pending obligation that will now never
		// fire its callback (which is what would otherwise decrement).
		// Stop returns true iff we stopped it before it fired; in that
		// case release the obligation here so settlePending does not
		// leak past Close. If Stop returns false the callback already
		// fired and runEvaluate / runReconcile owns the decrement.
		if c.settleTimer.Stop() {
			c.settlePending.Add(-1)
		}
		c.settleTimer = nil
	}
	c.settleMu.Unlock()

	// Stop the Coordinator + cancel the sweep context. Coordinator.Stop
	// closes its internal stopCh which terminates in-flight FetchRange
	// goroutines; cancel of rebalanceCtx terminates the sweep loop. Both
	// are idempotent.
	if rb := c.rebalance.Load(); rb != nil {
		rb.Stop()
	}
	if c.rebalanceCancel != nil {
		c.rebalanceCancel()
	}

	// Cancel any in-flight read-repair goroutines + wait for them to
	// exit BEFORE tearing down the peer-client cache. A repair that's
	// mid-PutForwarded against a cached peerClient would deadlock /
	// race / segfault when we close the conn below; canceling the
	// repair context first lets the gRPC call return cleanly, and the
	// short wait drains them with a budget so a wedged peer doesn't
	// stall Close indefinitely. 5s is enough for any in-flight repair
	// to honor ctx.Done; past that we give up and proceed, accepting
	// that the goroutine may log a "use of closed connection" error
	// (which is swallowed by the repair path anyway).
	if c.repairCancel != nil {
		c.repairCancel()
	}
	repairDone := make(chan struct{})
	go func() {
		c.repairWG.Wait()
		close(repairDone)
	}()
	select {
	case <-repairDone:
	case <-time.After(5 * time.Second):
	}

	// Signal the events + reconcile loops to exit + wait for them
	// before tearing down membership so we don't race with a late
	// ring.Add on closed ring. The sweep goroutine also drains via
	// loopWG (initRebalance adds 1 to the wait group for it).
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.loopWG.Wait()

	if c.membership != nil {
		if err := c.membership.Close(); err != nil {
			firstErr = err
		}
	}

	// Tear down all cached peer clients.
	c.clientsMu.Lock()
	for addr, cli := range c.clients {
		_ = cli.Close()
		delete(c.clients, addr)
	}
	c.clientsMu.Unlock()

	// Multi-backend mode: close every mounted unit via the factory
	// (CloseUnit each) instead of a single backend. c.closed is already
	// set, so KV ops starting after this point short-circuit on notReady
	// before touching a unit. Done before returning so a unit's resources
	// are released on Close.
	if c.multi {
		if err := c.closeMountedUnits(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	// Close the backend but DO NOT nil it out: a Put racing with
	// Close could have already loaded c.backend before this point,
	// and nil-ing it under their feet would either panic or trip
	// the race detector. The backend's own Close marks it closed
	// internally + subsequent ops return backend.ErrClosed cleanly.
	// c.closed.Load() is the primary guard; this is the safety net.
	if c.backend != nil {
		if err := c.backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// shardKey extracts the shard key for routing per cfg.ShardKeyFn, or
// the default (ring.ShardKey, honoring `{hash-tag}`) if unset.
func (c *Cluster) shardKey(key []byte) []byte {
	if c.cfg.ShardKeyFn != nil {
		return c.cfg.ShardKeyFn(key)
	}
	return ring.ShardKey(key)
}

// ownerOf returns the ring member that owns key + a bool indicating
// whether the local node is that owner. In single-node mode (no ring)
// it reports local-ownership unconditionally.
//
// In multi-backend mode (v0.8 Phase 2) ownership goes through the key's
// storage UNIT, not the raw key: the unit id is placed on the ring, so
// the owner of a key is the owner of its unit. This is the SAME ring used
// for legacy routing, re-keyed onto unit ids (no second ring).
func (c *Cluster) ownerOf(key []byte) (owner ring.Member, isLocal bool) {
	if c.multi {
		return c.unitOwnerOf(key)
	}
	if c.ring == nil || c.ring.Empty() {
		return ring.Member{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}, true
	}
	// LocateKey applies ShardKey internally; passing the raw key here
	// would double-extract for ShardKeyFn callers. Use the resolved
	// shard key as the hashing input.
	owner = c.ring.LocateKey(c.shardKey(key))
	return owner, owner.ID == c.cfg.NodeID
}

// clientFor returns a cached peerClient for addr, dialing on miss.
func (c *Cluster) clientFor(addr string) (*peerClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("cluster: peer has empty gRPC address")
	}
	c.clientsMu.RLock()
	if c.peerClientsBlocked {
		c.clientsMu.RUnlock()
		return nil, fmt.Errorf("cluster: peer clients blocked (test seam)")
	}
	cli, ok := c.clients[addr]
	c.clientsMu.RUnlock()
	if ok {
		return cli, nil
	}
	c.clientsMu.Lock()
	defer c.clientsMu.Unlock()
	if c.peerClientsBlocked {
		return nil, fmt.Errorf("cluster: peer clients blocked (test seam)")
	}
	if cli, ok := c.clients[addr]; ok {
		return cli, nil
	}
	cli, err := newPeerClient(addr)
	if err != nil {
		return nil, fmt.Errorf("cluster: dial %s: %w", addr, err)
	}
	c.clients[addr] = cli
	return cli, nil
}

// TestingDropAllPeerClients force-closes every cached peer-gRPC
// client without touching membership / the Coordinator / the local
// gRPC server. Used by the destination-crash failure tests to
// SEVER outgoing connections (the dest-initiated MigrateRange
// stream is a client connection; tearing down the client cancels
// the stream + the source-side handler returns the
// "transport-closed" error, which the wired-handoff path then
// routes to Done-with-error). Membership stays intact so the
// source's plan is not torn down before the rejection lands.
//
// Test-only; not exported in the public API surface. Name is
// camelCase with the Testing prefix because Go's "Testing*"
// convention earmarks white-box hooks that survive a Test-only
// build constraint without the constraint itself (the cluster
// package's tests live in the same package; integration tests
// import + call this directly).
func (c *Cluster) TestingDropAllPeerClients() {
	c.clientsMu.Lock()
	for addr, cli := range c.clients {
		_ = cli.Close()
		delete(c.clients, addr)
	}
	c.clientsMu.Unlock()
}

// evictClient closes + drops the cached client for addr, if present.
// Called when a peer leaves the ring so a returning node on the same
// address gets a fresh connection.
func (c *Cluster) evictClient(addr string) {
	if addr == "" {
		return
	}
	c.clientsMu.Lock()
	cli, ok := c.clients[addr]
	if ok {
		delete(c.clients, addr)
	}
	c.clientsMu.Unlock()
	if ok {
		_ = cli.Close()
	}
}

// -- KV surface (mirrors backend.Backend) ----------------------------------

// Put stores value under key, routing to the owning node.
//
// v0.3/v0.4 rebalancing: if the key's partition is mid-migration on
// this node (either streaming out OR being received), the local Put
// is rejected with codes.ResourceExhausted + a retry-after hint (per
// docs/SPEC.md "Cutover"). Clients should retry with backoff per the
// hint; the SDK wraps this transparently with a bounded retry budget.
// codes.FailedPrecondition is reserved for the forwarding loop-guard
// (docs/SPEC.md "Failure handling"); codes.Unavailable is reserved
// for genuine peer-down failures (so the fanout's failure budget can
// short-circuit on dead nodes). The three codes have different retry
// semantics so they must not be conflated.
//
// v0.4 replication: when ReplicationFactor > 1 the originator stamps
// the payload (time.Now().UnixNano() + NodeID) once, wraps it in an
// LWW envelope, and fans out to R replicas. The call returns once W
// acks land per WriteConsistency. Migration-guard rejections from
// individual replicas are treated as transient (don't count toward
// either ack or failure budget) so a single mid-handoff replica
// doesn't fail an otherwise-quorum write. See docs/SPEC.md "Fan-out
// + ack accounting".
//
// Empty-value rejection: Put refuses nil + zero-length value with
// ErrEmptyValue. The envelope's empty-payload shape is reserved for
// Delete tombstones; allowing Put(key, nil) would silently store a
// tombstone at R>1 (looking like a Delete on subsequent reads) while
// at R=1 the same call would store an empty value (looking like a
// successful Put). The asymmetry is a foot-gun. Apps that want to
// remove a key must call Delete explicitly.
func (c *Cluster) Put(key, value []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if len(value) == 0 {
		return ErrEmptyValue
	}
	if c.multiReplicated() {
		// R>1 (replicated multi-backend, v0.8 Phase 2b): stamp + envelope +
		// fan out to the unit's R replica nodes, waiting for W acks. The legacy
		// envelope / fan-out machinery, re-keyed per unit. Static topology, so
		// no freeze / reshard window applies here.
		return c.putReplicatedUnit(key, value)
	}
	if c.multi {
		// FREEZE gate (v0.8 multi-node reshard): while a cluster-wide reshard
		// has this node write-frozen, refuse with the retryable error so the
		// write is NEVER acked. It succeeds on the client's retry after RESUME,
		// at the new generation. This is what makes the static bisect safe: no
		// write is in flight during it. Checked before routing so a frozen node
		// does no work; reads are never gated.
		if c.isFrozen() {
			return errWriteFrozen("Put")
		}
		owner, local := c.ownerOf(key)
		if local {
			// Resolve the local backend UNDER the reshard write-pause (Phase 4):
			// if a cut-over for this key's old unit is in flight, this blocks
			// until it completes and then resolves to the new gen-(g+1) child,
			// so the write never lands in a retired unit (NO ACKED WRITE LOST).
			b, ru, unlock, ok := c.localWriteBackendForKey(key)
			defer unlock()
			if !ok {
				// This node IS the ring owner but has not mounted the
				// unit yet: the Phase 3 handoff is landing on us. Return
				// the RETRYABLE acquiring-window error so the originator
				// retries and succeeds once the reconcile has acquired
				// (never lose the write, never serve from a wrong engine).
				return errUnitAcquiring("Put")
			}
			if err := b.Put(key, value); err != nil {
				// A failed local write on an owned+mounted unit means the mount
				// is stale (its lease moved to a higher epoch - the reshard
				// redistribution window). Evict it so the next reconcile
				// re-acquires fresh, and return the RETRYABLE acquiring-window
				// error so the originator retries (never ack a write that did
				// not land).
				c.evictStaleMount(ru, b)
				return errUnitAcquiring("Put")
			}
			return nil
		}
		cli, err := c.clientFor(owner.Addr)
		if err != nil {
			return err
		}
		return c.putForwarded(cli, key, value)
	}
	if c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty() {
		return c.putReplicated(key, value)
	}
	owner, local := c.ownerOf(key)
	if local {
		if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
			return migrationGuardError(c.retryAfterMs())
		}
		return c.backend.Put(key, value)
	}
	cli, err := c.clientFor(owner.Addr)
	if err != nil {
		return err
	}
	return c.putForwarded(cli, key, value)
}

// Get returns the value for key, routing to the owning node.
//
// v0.3 rebalancing: if the key's partition is being received into
// this node (StateReceiving), the local data is not yet authoritative
// + the source still owns reads. Per docs/SPEC.md "Cutover" the
// destination transparently forwards the read back to the source's
// gRPC; the source still serves the key from its local copy until
// the destination ack flips it HandedOff. Callers see a normal
// successful Get rather than a transient error. Source-side
// IsMigrating (StateSending / StateHandedOff) is fine: we still
// have the data locally + serve the read normally.
//
// v0.4 replication: when ReplicationFactor > 1 the Get reads from N
// replicas per ReadConsistency (1 / quorum / R), picks the LWW winner
// across returned envelopes, and (on Quorum / All) asynchronously
// pushes the winner back to any lagging replica. Tombstones (empty
// payload) surface as backend.ErrNotFound. See docs/SPEC.md "Read path".
func (c *Cluster) Get(key []byte) ([]byte, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	if c.multiReplicated() {
		// R>1 (v0.8 Phase 2b): read N-of-R across the unit's replica nodes per
		// ReadConsistency, pick the LWW winner, read-repair laggers.
		return c.getReplicatedUnit(key)
	}
	if c.multi {
		owner, local := c.ownerOf(key)
		if local {
			b, ok := c.localBackendForKey(key)
			if !ok {
				// Owner-but-unmounted: handoff landing on us. Retryable
				// acquiring-window error (never serve a stale result).
				return nil, errUnitAcquiring("Get")
			}
			// b is the fence-self-healing mount (storeMount): a fenced read
			// self-heals here, so the simple Get is safe.
			return b.Get(key)
		}
		return c.forwardGet(owner.Addr, key)
	}
	if c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty() {
		return c.getReplicated(key)
	}
	owner, local := c.ownerOf(key)
	if local {
		if rb := c.rebalance.Load(); rb != nil {
			if mv, ok := rb.ReceivingMove(key); ok && mv.From.Addr != "" {
				return c.forwardGet(mv.From.Addr, key)
			}
		}
		return c.backend.Get(key)
	}
	return c.forwardGet(owner.Addr, key)
}

// forwardGet dials addr (a peer's gRPC address) + issues a Get with
// the cluster-internal forwarded=true marker. Used by the routed-Get
// path AND by the v0.3 receiving-window read forwarder: a read that
// lands on the destination during its StateReceiving window is
// transparently forwarded back to the source so the caller sees a
// successful read rather than a transient error.
func (c *Cluster) forwardGet(addr string, key []byte) ([]byte, error) {
	cli, err := c.clientFor(addr)
	if err != nil {
		return nil, err
	}
	// Bound the forwarded read: a peer that is alive-but-frozen (or whose
	// link is half-open) must surface a deadline error the caller can act
	// on, not block this goroutine forever. Mirrors replicate.go's pattern.
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ReadTimeout)
	defer cancel()
	val, found, err := cli.GetForwarded(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, backend.ErrNotFound
	}
	return val, nil
}

// putForwarded / deleteForwarded wrap the forwarded write RPCs with a bounded
// context, for the same reason as forwardGet: an unresponsive-but-alive peer
// must time out rather than wedge the caller indefinitely.
func (c *Cluster) putForwarded(cli *peerClient, key, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	defer cancel()
	return cli.PutForwarded(ctx, key, value)
}

func (c *Cluster) deleteForwarded(cli *peerClient, key []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	defer cancel()
	return cli.DeleteForwarded(ctx, key)
}

// Delete removes key, routing to the owning node.
//
// v0.3/v0.4 rebalancing: same write-guard semantics as Put. Mid-
// migration keys are rejected with ResourceExhausted + retry-after;
// the client retries once the range hands off cleanly.
//
// v0.4 replication: Delete writes a tombstone (empty-payload
// envelope) carrying the current Stamp + fans out to R replicas with
// the same Put accounting. The tombstone participates in LWW like
// any other write, so a Delete that races with a concurrent Put
// resolves by timestamp.
func (c *Cluster) Delete(key []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if c.multiReplicated() {
		// R>1 (v0.8 Phase 2b): Delete is a tombstone (empty-payload stamped
		// envelope) fanned out to the unit's R replica nodes, same path as Put.
		return c.putReplicatedUnit(key, nil)
	}
	if c.multi {
		// FREEZE gate (v0.8 multi-node reshard): a frozen node refuses Delete
		// with the retryable error so it is never acked, succeeding on retry
		// after RESUME. See Put for the full rationale.
		if c.isFrozen() {
			return errWriteFrozen("Delete")
		}
		owner, local := c.ownerOf(key)
		if local {
			// Delete is a write: resolve under the reshard write-pause so a
			// mid-flight cut-over routes it to the new child, not a retired
			// unit (NO ACKED WRITE LOST; see Put).
			b, ru, unlock, ok := c.localWriteBackendForKey(key)
			defer unlock()
			if !ok {
				// Owner-but-unmounted: handoff landing on us. Retryable
				// acquiring-window error (never lose the delete).
				return errUnitAcquiring("Delete")
			}
			if err := b.Delete(key); err != nil {
				// Stale mount (lease moved): evict + retryable, same as Put.
				c.evictStaleMount(ru, b)
				return errUnitAcquiring("Delete")
			}
			return nil
		}
		cli, err := c.clientFor(owner.Addr)
		if err != nil {
			return err
		}
		return c.deleteForwarded(cli, key)
	}
	if c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty() {
		return c.putReplicated(key, nil)
	}
	owner, local := c.ownerOf(key)
	if local {
		if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
			return migrationGuardError(c.retryAfterMs())
		}
		return c.backend.Delete(key)
	}
	cli, err := c.clientFor(owner.Addr)
	if err != nil {
		return err
	}
	return c.deleteForwarded(cli, key)
}

// ScanPrefix returns an iterator over keys with the given prefix on
// the owning shard. The prefix is treated as a shard key + routed to
// the owning node; the scan runs entirely on that node's backend. For
// cross-shard scans, use Aggregate.
func (c *Cluster) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	owner, local := c.ownerOf(prefix)
	if local {
		if c.multi {
			b, ok := c.localBackendForKey(prefix)
			if !ok {
				// Owner-but-unmounted: handoff landing on us. Retryable
				// acquiring-window error (never serve a stale scan).
				return nil, errUnitAcquiring("ScanPrefix")
			}
			return b.ScanPrefix(prefix)
		}
		return c.backend.ScanPrefix(prefix)
	}
	cli, err := c.clientFor(owner.Addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cli.ScanPrefixForwarded(ctx, prefix)
	if err != nil {
		cancel()
		return nil, err
	}
	return &remoteIterator{stream: stream, cancel: cancel}, nil
}

// Begin starts a CAS-buffered (optimistic-concurrency) transaction. The
// returned Transaction is a BUFFER, not a live backend session: Get does
// a routed read (recording a read-check), Put / Delete buffer write-ops,
// and Commit ships the read-set + write-set to the pinned shard owner in
// ONE validate-and-apply call (an in-process fast-path when the owner is
// local). The shard is pinned lazily on the first key touched; any
// subsequent key sharding to a different owner returns
// backend.ErrCrossShard at the offending op (the cross-shard guard).
// Commit returns backend.ErrCASConflict if the owner found a read-set key
// changed; the caller retries (Transact wraps that loop).
//
// This is uniform OCC everywhere: single-node, local-pin, and remote-pin
// transactions all run through the same validate-and-apply path. It is a
// behavior change from the pre-v0.6 Begin contract (which ran ops against
// a live local backend.Transaction and returned ErrCrossShard on a remote
// pin); changing it pre-1.0 is intentional. See docs/SPEC.md
// "Single-shard transactions (CAS / OCC)" + "Begin vs Transact".
func (c *Cluster) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	// FREEZE gate (v0.8 multi-node reshard): a frozen node refuses Begin with
	// the retryable error so a client does not buffer a transaction that is
	// doomed to fail at Commit (the CAS commit write path is itself frozen). The
	// client retries Begin after RESUME and commits at the new generation. The
	// CAS commit write path (localBeginForKey, below) is ALSO gated, so a tx
	// that was opened before the freeze and commits during it is refused too -
	// belt and suspenders, since this gate alone cannot cover a pre-freeze
	// Begin. Reads outside a tx are never gated.
	if c.multi && c.isFrozen() {
		return nil, errWriteFrozen("Begin")
	}
	return c.newCASTx(level), nil
}

// AggregateResult is one entry in the slice returned by Aggregate:
// either Value (whatever fn returned for that peer) or Err (a
// transport / snapshotting failure that prevented fn from running for
// that peer). Exactly one of the two is meaningful per entry; the
// other is the zero value. Splitting them keeps Err distinct from a
// peer that legitimately returned an error VALUE (fn might return
// errors as part of its normal API).
type AggregateResult struct {
	// Value is what fn returned for this peer. Nil iff Err is set.
	Value any
	// Err is the cross-shard failure that prevented fn from running
	// (snapshot transport failed, peer unreachable, etc.). Nil on
	// success.
	Err error
}

// Aggregate runs fn locally on each node's Backend in parallel +
// collects the per-node results. Use for cross-shard operations
// (admin lists, full-table scans, computed aggregates). NOT for
// hot-path queries: use shard-aware key design for those.
//
// Each entry in the returned slice is an AggregateResult: Err is set
// if shale couldn't even run fn for that peer (snapshot transport
// failure, dial failure), otherwise Value holds whatever fn returned.
// Order of results is unspecified.
//
// v0.2: peer fan-out walks the ring's member list and, for each peer
// other than the local node, streams the peer's LOCAL backend over
// gRPC (via the admin-only LocalScan RPC, which bypasses ring
// routing) into a transient in-memory backend snapshot that fn sees.
// The local node runs fn against its own backend directly.
func (c *Cluster) Aggregate(fn func(b backend.Backend) any) []AggregateResult {
	if c.notReady() {
		return nil
	}
	members := c.Members()
	results := make([]AggregateResult, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Go(func() {
			if m.ID == c.cfg.NodeID {
				if c.multi {
					// Multi-backend: no single c.backend. Give fn a
					// read-only view spanning this node's mounted units
					// (same snapshot shape snapshotPeer builds for peers).
					snap, err := c.localMountedSnapshot()
					if err != nil {
						results[i] = AggregateResult{Err: err}
						return
					}
					results[i] = AggregateResult{Value: fn(snap)}
					return
				}
				results[i] = AggregateResult{Value: fn(c.backend)}
				return
			}
			snap, err := c.snapshotPeer(m.Addr)
			if err != nil {
				results[i] = AggregateResult{Err: err}
				return
			}
			// snap is a streaming view holding a live gRPC stream/ctx;
			// Close tears it down. The stream MUST stay open across fn
			// (fn iterates it), so close only after fn returns.
			defer func() { _ = snap.Close() }()
			results[i] = AggregateResult{Value: fn(snap)}
		})
	}
	wg.Wait()
	return results
}

// snapshotPeer returns a streaming read-only view of a peer's LOCAL
// backend for Aggregate's scan/count fns. It does NOT drain the peer's
// keyspace into memory: each ScanPrefix the fn issues opens a fresh
// LocalScan stream and pulls pairs straight off the wire, so peak
// memory is one in-flight pair per peer rather than the peer's whole
// keyspace (with all peers concurrent, the old path's peak was the SUM
// of every peer's keyspace resident at once). The view holds a live
// gRPC stream context; the caller MUST Close it after fn returns.
//
// Uses LocalScan (admin path) rather than ScanPrefix so the receiving
// node hands us its own keys directly. ScanPrefix would route the
// prefix through ownerOf - hashing it to whichever shard owns it - and
// we'd get a single shard back N times instead of each shard's slice
// once. The peer applies the prefix filter server-side, so the view
// replays the same key/value pairs in the same key-ascending order the
// old materializing path observed; scan/count fns are unaffected.
//
// Consumers needing random-access Get are not supported here (a stream
// cannot seek); such a caller must switch to the materializing
// snapshotBackend path. All current Aggregate consumers scan only.
//
// snapshotPeer opens the full-keyspace LocalScan stream eagerly and
// primes its first Recv before returning. This preserves the documented
// Aggregate contract that a dial / transport failure surfaces as the
// peer's AggregateResult.Err (set "if shale couldn't even run fn"),
// independent of whether fn happens to scan: gRPC dial is lazy, so the
// transport error only materializes on the first Recv, and the old
// materializing path caught it during its eager drain. The primed first
// message is replayed by the view's initial full-keyspace ScanPrefix.
func (c *Cluster) snapshotPeer(addr string) (backend.Backend, error) {
	cli, err := c.clientFor(addr)
	if err != nil {
		return nil, err
	}
	// Wait (bounded by PeerConnectTimeout) for the peer's gRPC connection to be
	// READY before opening the scan stream. The peer client is lazy + fail-fast,
	// so without this a peer momentarily not-serving-gRPC at cold-start would
	// fail-fast the stream's first Recv AND leave this CACHED client in backoff,
	// poisoning every later fan-out RPC for the whole backoff window. Waiting
	// here absorbs that transient; the long-lived stream below then opens over an
	// already-READY connection, so its (unbounded) ctx cannot hang on connect.
	readyCtx, readyCancel := context.WithTimeout(context.Background(), c.cfg.PeerConnectTimeout)
	err = cli.waitReady(readyCtx)
	readyCancel()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cli.LocalScan(ctx, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	// Prime: force the first round-trip so dial / transport failures
	// surface here rather than (silently) inside fn.
	first, firstErr := stream.Recv()
	if firstErr != nil && !errors.Is(firstErr, io.EOF) {
		cancel()
		return nil, firstErr
	}
	primed := &primedStream{stream: stream}
	if errors.Is(firstErr, io.EOF) {
		primed.done = true
	} else {
		primed.first = first
		primed.hasFirst = true
	}
	return &streamBackend{cli: cli, ctx: ctx, cancel: cancel, primed: primed}, nil
}
