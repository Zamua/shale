// Package cas implements shale's coordination port (pkg/coord) over ONE
// membership document in a storageunit.ConditionalStore: the same
// PutIfAbsent / CompareAndSet / Get seam the reshard arbiter runs on,
// implemented by slate's MinIO adapter and the in-process
// MemConditionalStore.
//
// ONE TRUTH CHANNEL. All coordination state - presence, dial addresses,
// roles, declared unit counts, liveness - is one JSON document at a
// well-known key. Every mutation (join, lease renewal, role change,
// expired-member GC, graceful leave) is a read-modify-write CompareAndSet,
// retried on ErrPrecondition with bounded backoff: one racer wins, the loser
// re-reads the winner's document and re-applies its OWN edit to the fresh
// copy, so concurrent editors interleave without ever losing each other's
// rows. There is no gossip mesh, no seed list, no extra open port; the store
// is this adapter's one required peer.
//
// LEASES ARE COUNTERS, NOT CLOCKS. Each node renews by CAS-incrementing its
// own row's leaseGen on a fixed cadence. Failure detection is OBSERVER-SIDE:
// each node polls the document and expires a member whose leaseGen has not
// advanced for ExpiryPolls consecutive observed polls. The judgment compares
// two rates the observer itself witnesses (its polls vs the member's
// renewals), never two wall clocks, so clock skew cannot false-expire anyone
// - and a stalled observer cannot either, because its poll ticks stall with
// it. Any live member may GC an expired member's row via CAS (idempotent;
// racing GCs resolve by CAS). Detection latency is ExpiryPolls*PollInterval,
// deliberately more conservative than SWIM; the port makes even a mis-tuned
// expiry an availability cost, never a correctness one.
//
// DUPLICATE IDENTITY IS DISPLACEMENT, AND THE OLD PROCESS STEPS DOWN. Self
// records the incarnation nonce it minted for its own row; every renewal
// verifies the row still carries it. A row wearing a DIFFERENT inc means a
// replacement process with the same stable ID took the identity over (the
// k8s StatefulSet overlap: the old pod still running behind a partitioned
// kubelet while its replacement bootstraps). The old process must not merge
// into the new row - blindly incrementing it would keep a dead
// replacement's lease advancing forever, defeating expiry - so it steps
// down: marks itself displaced (surfaced via Health), stops renewing,
// polling and GC'ing, keeps serving reads from its last snapshot, and
// never mutates the document again (Close skips the row removal too: the
// row is the replacement's membership).
//
// THE STORE OUTLIVES THE CLUSTER: RUN THIS ADAPTER WITH THE MARKER PATH.
// A full-cluster crash (SIGKILL, power loss) removes no rows and leaves no
// live observer to expire them, so every restarting node finds the prior
// generation's rows and bootstrap reports BootstrapJoined even though every
// "incumbent" is dead. The CAS coordinator is therefore DESIGNED to run with
// cluster.Config.ConditionalStore wired to the SAME store: on that path a
// joiner learns the generation from the durable bootstrap marker instead of
// dialing a live incumbent, which makes all-nodes-crash recovery BOUNDED -
// the zombie rows pollute the view for at most ExpiryPolls*PollInterval,
// because bootstrap seeds every incumbent row's expiry baseline at Start
// itself (observation begins at Start, not at the first poll), then expiry
// drops them and GC reaps them. WITHOUT the marker path (ConditionalStore
// nil), a joiner must learn the generation from a live peer that only serves
// after its own Open returns: after a full-cluster crash all N nodes
// mutually wait, each fails Open on the generation-learn budget, and
// recovery degrades to the supervisor-restart cycle escaping only if some
// node's bootstrap read lands in an all-rows-removed window. See the CAS
// section of docs/SPEC.md.
//
// ONE SNAPSHOT, ONE BASIS - THE DUAL-BASIS CLASS IS STRUCTURALLY ABSENT.
// The poll derives the live member set and rebuilds the placement ring from
// it, publishing {view, ring} as ONE immutable pair behind an atomic pointer
// swap. Every per-op query (View, Populated, TransitionSets,
// PlacementMembers, Locate) reads the currently published pair: no locks, no
// I/O, per the port. Because view and ring are one artifact derived from one
// source, PlacementMembers() equals View()'s member set ALWAYS: the gossip
// adapter's documented window (ring trails the snapshot, heals on the
// reconcile cadence) cannot exist here.
//
// ROLE VISIBILITY RIDES THE POLL, NEVER THE HINT. SetRole CAS-updates self's
// row and returns; the flip reaches the query methods when a poll next
// refreshes the snapshot (this node's own within one poll interval, each
// peer's within theirs). The poll IS this mechanism's delivery, and the
// change hint plays no part in it, so a dropped hint can never freeze a
// stale role answer.
//
// ADVISORY, NOT AUTHORITATIVE. Exactly like every adapter of this port:
// nothing here fences anything; a stale view costs availability, never
// correctness - the durable store's own higher-epoch-open fencing is what
// makes two nodes disagreeing about an owner safe.
package cas

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/internal/decide"
	"github.com/Zamua/shale/internal/placement"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

const (
	// DefaultKey is the well-known document key when Config.Key is empty.
	DefaultKey = "__coord/members"

	// DefaultRenewalInterval is the production lease-renewal cadence: how
	// often this node CAS-increments its own leaseGen.
	DefaultRenewalInterval = 2 * time.Second

	// DefaultPollInterval is the production observation cadence: how often
	// this node re-reads the document and refreshes its snapshot.
	DefaultPollInterval = 1 * time.Second

	// DefaultExpiryPolls is K: a member whose leaseGen has not advanced for
	// K consecutive observed polls is expired. With the default intervals
	// that is 5s of observed silence against a 2s renewal - a 2.5x margin.
	DefaultExpiryPolls = 5

	// casAttempts bounds every read-modify-write retry loop. A CAS loser
	// re-reads and re-applies; losing this many times in a row means the
	// document is being hammered far beyond this adapter's design point.
	casAttempts = 8
)

// Config holds the CAS-adapter knobs. Store is required; everything else
// defaults to the production values above. Intervals follow the package
// convention: zero means the default, negative disables the loop (tests
// drive the corresponding Testing* seam manually instead).
type Config struct {
	// Store is the conditional store the membership document lives in.
	// Required.
	Store storageunit.ConditionalStore

	// Key overrides DefaultKey, e.g. to isolate clusters sharing one store.
	Key string

	// RenewalInterval overrides DefaultRenewalInterval (0 = default,
	// negative = no renewal loop).
	RenewalInterval time.Duration

	// PollInterval overrides DefaultPollInterval (0 = default, negative =
	// no poll loop).
	PollInterval time.Duration

	// ExpiryPolls overrides DefaultExpiryPolls (0 = default). Tests that
	// need membership pinned stable park it very high.
	ExpiryPolls int

	// LogOutput is where the adapter's own diagnostics write - a
	// rate-limited line when consecutive store failures cross the
	// degradation threshold, and the matching recovery line. nil means
	// stderr (the gossip adapter's LogOutput convention); tests pass
	// io.Discard.
	LogOutput io.Writer
}

// degradedThreshold is the consecutive-failure count at which a loop logs
// its degradation (and, on the next success, its recovery). Five failed
// polls at the production cadence is ~5s of serving a stale view - the same
// order as the expiry window, i.e. long enough to be a real outage signal
// and short enough to catch a creds rotation / bucket policy / network ACL
// break before peers have long since expired this node.
const degradedThreshold = 5

// docRow is one member's row in the membership document. The JSON field
// names are a WIRE FORMAT shared by every node reading the document: changing
// them breaks mixed-version clusters (pinned by TestDocument_WireFormat).
//
// Inc is the row's INCARNATION nonce, minted fresh every time a row is
// CREATED (bootstrap, or the renewal re-add after a GC) and carried
// unchanged by every in-place edit (renewal increments, role changes). It
// closes an ABA hole in observer-side expiry: a member GC'd and rejoining
// between two of an observer's polls restarts leaseGen at 1, which can
// EQUAL the counter the observer last tracked - without the nonce the
// observer reads "still not advancing", keeps the member expired, and its
// GC reaps every fresh row the member writes. Comparing incarnations for
// INEQUALITY only (never ordering them) keeps the no-clocks property.
type docRow struct {
	ID                string `json:"id"`
	Addr              string `json:"addr,omitempty"`
	Roles             uint8  `json:"roles,omitempty"`
	DeclaredUnitCount uint32 `json:"declaredUnitCount,omitempty"`
	LeaseGen          uint64 `json:"leaseGen"`
	Inc               string `json:"inc"`
}

// newInc mints an incarnation nonce: 8 random bytes, hex. Observers only
// ever compare nonces for inequality, so collision odds are all that
// matter, and 64 bits is far beyond this adapter's fleet sizes.
func newInc() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read does not fail on supported platforms
	return hex.EncodeToString(b[:])
}

// member converts the row to the port's member type.
func (r docRow) member() coord.Member {
	return coord.Member{
		Node:              coord.Node{ID: storageunit.NodeID(r.ID), Addr: r.Addr},
		Roles:             coord.Role(r.Roles),
		DeclaredUnitCount: r.DeclaredUnitCount,
	}
}

// document is the whole membership document.
type document struct {
	Members []docRow `json:"members"`
}

func decodeDocument(data []byte) (*document, error) {
	var d document
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("cas: decoding membership document: %w", err)
	}
	return &d, nil
}

// encodeDocument renders the document with rows sorted by ID, so two nodes
// producing the same membership produce byte-identical documents (diffable,
// and a no-op CAS rewrite stays a genuine no-op content-wise).
func encodeDocument(d *document) ([]byte, error) {
	sort.Slice(d.Members, func(i, j int) bool { return d.Members[i].ID < d.Members[j].ID })
	data, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("cas: encoding membership document: %w", err)
	}
	return data, nil
}

func (d *document) find(id storageunit.NodeID) int {
	for i := range d.Members {
		if d.Members[i].ID == string(id) {
			return i
		}
	}
	return -1
}

// upsert inserts row, replacing any existing row with the same ID. A
// replaced row's leaseGen always ADVANCES past the old one, so an observer
// tracking the stale incarnation's counter sees fresh activity immediately
// instead of counting the new incarnation toward expiry.
func (d *document) upsert(row docRow) {
	if i := d.find(storageunit.NodeID(row.ID)); i >= 0 {
		if old := d.Members[i].LeaseGen; old >= row.LeaseGen {
			row.LeaseGen = old + 1
		}
		d.Members[i] = row
		return
	}
	d.Members = append(d.Members, row)
}

// snapshot is the ONE atomically published {view, ring} pair every per-op
// query reads. Immutable after publication; successive snapshots may share
// the ring pointer when membership (ids + addrs) did not change.
type snapshot struct {
	view coord.View
	ring *ring.Ring
}

// Coordinator is the CAS/lease implementation of coord.Coordinator.
type Coordinator struct {
	cfg Config

	// self and declaredUnitCount are the identity published at Start.
	// Written once, before any loop exists, then read-only.
	self              coord.Node
	declaredUnitCount uint32

	// snap is the published {view, ring} pair. Valid-and-empty from
	// construction, so every pre-Start query answers safely.
	snap atomic.Pointer[snapshot]

	// epoch is the local monotone view counter, bumped on every published
	// snapshot change.
	epoch atomic.Uint64

	// changed is the coalescing change hint. Capacity 1: a send that would
	// block is dropped, because the contract is a hint, not a queue.
	changed chan struct{}

	// roles is the role set this node last asked to advertise. rolesMu
	// guards it; the renewal loop reads it when re-adding a GC'd self row.
	rolesMu sync.Mutex
	roles   coord.Role

	// selfInc is the incarnation nonce this process last WROTE for its own
	// row (minted at bootstrap, re-minted by a renewal re-add). The renewal
	// verifies the row still carries it: a different inc means a
	// replacement process with the same stable ID displaced this one.
	// incMu guards it.
	incMu   sync.Mutex
	selfInc string

	// displaced is latched by stepDown when a renewal finds self's row
	// wearing another process's incarnation. Once set, this process never
	// mutates the document again and serves reads from its last snapshot.
	displaced atomic.Bool

	// pollMu serializes observation passes (the background loop vs the
	// Testing seam) and guards the observer state below: the per-member lease
	// record decide.ExpireSilent folds each pass into its successor.
	pollMu  sync.Mutex
	tracker map[string]decide.LeaseObservation

	// reduced memoizes the last hypothetical-placement ring (see Locate),
	// keyed by the exclusion set and the snapshot epoch it was built from.
	reducedMu    sync.Mutex
	reducedKey   string
	reducedEpoch uint64
	reduced      *ring.Ring

	// log receives the adapter's own diagnostics (see Config.LogOutput).
	log *log.Logger

	// I/O health watermarks (see Health). Success counters are monotone;
	// failure counters count the run currently in progress and reset to 0
	// on any success.
	pollOK    atomic.Uint64
	renewOK   atomic.Uint64
	pollFail  atomic.Uint64
	renewFail atomic.Uint64

	closeCh     chan struct{}
	closeOnce   sync.Once
	loopWG      sync.WaitGroup
	startMu     sync.Mutex
	started     atomic.Bool
	closed      atomic.Bool
	hardStopped atomic.Bool
}

// Health is the adapter's I/O health surface: the two store loops' success
// watermarks and current failure runs. The success counters are MONOTONE
// watermarks in the adapter's own no-clocks idiom - an operator (or a
// debug endpoint) samples Health twice and judges liveness by whether the
// counters ADVANCED, never by comparing a timestamp. Counters count
// pollOnce / renewOnce passes (background loops and Testing seams alike);
// Start's synchronous first observation is not a poll and is not counted.
type Health struct {
	// PollSuccesses / RenewSuccesses are the monotone success watermarks.
	// A PollSuccesses that stops advancing means the served view is frozen
	// at its last snapshot; a stalled RenewSuccesses means peers will
	// expire this node after their ExpiryPolls windows.
	PollSuccesses  uint64
	RenewSuccesses uint64
	// ConsecutivePollFailures / ConsecutiveRenewFailures are the failure
	// run currently in progress; any success resets them to 0. Crossing
	// degradedThreshold also emits the rate-limited log line.
	ConsecutivePollFailures  uint64
	ConsecutiveRenewFailures uint64
	// Displaced reports that a replacement process with this node's stable
	// ID took the membership row over (a renewal found the row wearing a
	// different incarnation nonce). This process has stepped down: it
	// serves reads from its last snapshot and never mutates the document
	// again. Terminal: the honest exit is for the supervisor to retire
	// this process.
	Displaced bool
}

// Health returns the current I/O health watermarks. Safe concurrently; no
// locks, no I/O.
func (c *Coordinator) Health() Health {
	return Health{
		PollSuccesses:            c.pollOK.Load(),
		RenewSuccesses:           c.renewOK.Load(),
		ConsecutivePollFailures:  c.pollFail.Load(),
		ConsecutiveRenewFailures: c.renewFail.Load(),
		Displaced:                c.displaced.Load(),
	}
}

// New returns an unstarted Coordinator. Nothing touches the store until
// Start; the operator constructs the value, the storage layer starts and
// closes it.
func New(cfg Config) *Coordinator {
	if cfg.Key == "" {
		cfg.Key = DefaultKey
	}
	if cfg.ExpiryPolls <= 0 {
		cfg.ExpiryPolls = DefaultExpiryPolls
	}
	out := cfg.LogOutput
	if out == nil {
		out = os.Stderr
	}
	c := &Coordinator{
		cfg:     cfg,
		log:     log.New(out, "", log.LstdFlags),
		changed: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
		tracker: make(map[string]decide.LeaseObservation),
	}
	c.snap.Store(&snapshot{})
	return c
}

// Compile-time proof the adapter satisfies the port, plus the one optional
// seam it offers. coord.MemberSetter is DELIBERATELY not implemented: it
// exists to model a placement basis diverged from the view, and this
// adapter's single-snapshot design makes that divergence structurally
// absent - offering the seam would let tests manufacture exactly the state
// the design rules out. A test that needs a diverged basis therefore runs
// on internal/coordstatic instead; asking this adapter for the seam PANICS
// (cluster.TestingSetRingMembers) rather than silently doing nothing.
var (
	_ coord.Coordinator    = (*Coordinator)(nil)
	_ coord.HardShutdowner = (*Coordinator)(nil)
)

// intervalOr returns def when v is zero, and disables (returns 0) when v is
// negative - the same convention the gossip adapter uses.
func intervalOr(v, def time.Duration) time.Duration {
	switch {
	case v == 0:
		return def
	case v < 0:
		return 0
	default:
		return v
	}
}

// casBackoff sleeps briefly before a CAS retry: doubling from 1ms, capped
// small - enough to separate two racers, cheap enough for tests.
func casBackoff(attempt int) {
	d := time.Duration(1<<uint(attempt)) * time.Millisecond
	if d > 25*time.Millisecond {
		d = 25 * time.Millisecond
	}
	time.Sleep(d)
}

// Start discovers or founds the cluster through the store and reports which.
//
// BOOTSTRAP IS DISCOVERY, AND IT IS EXACT: PutIfAbsent of a document holding
// only self succeeding IS founding; ErrPrecondition means an incumbent
// document exists, so Start CAS-appends self's row and reports joined.
// Params.InitialRoles ride in that first row, atomic with this node's first
// appearance (the same atomicity gossip gets from the first Meta payload),
// and a FOUNDER drops a requested RoleJoining per the port: there is no
// incumbent to warm against. A document that exists but holds ZERO rows also
// founds: everyone left gracefully, so there is no incumbent to learn a
// generation from and "did anyone else already exist" is answered no.
//
// SoloStart collapses to store reachability: the store is this adapter's one
// required peer, so an unreachable store fails Start regardless of the flag,
// and the conditional write makes founded-vs-joined definitive without it
// (no tentative-then-correct dance, no silent-fork risk for it to guard).
func (c *Coordinator) Start(p coord.Params) (coord.Bootstrap, error) {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.started.Load() {
		return 0, errors.New("cas: Start called twice")
	}
	if c.cfg.Store == nil {
		return 0, errors.New("cas: Config.Store is required")
	}
	if p.Self.ID == "" {
		return 0, errors.New("cas: Params.Self.ID is required")
	}
	if p.Self.Addr == "" {
		return 0, errors.New("cas: Params.Self.Addr is required (peers dial it)")
	}

	c.self = p.Self
	c.declaredUnitCount = p.DeclaredUnitCount

	bootstrap, doc, ver, err := c.bootstrap(p)
	if err != nil {
		return 0, err
	}

	roles := p.InitialRoles
	if bootstrap == coord.BootstrapFounded {
		roles &^= coord.RoleJoining
	}
	c.rolesMu.Lock()
	c.roles = roles
	c.rolesMu.Unlock()

	// Synchronous first observation: the view this node serves the moment
	// Start returns is the document its own bootstrap write produced (self
	// row included, plus every incumbent).
	c.observe(doc, ver)

	c.started.Store(true)
	if iv := intervalOr(c.cfg.RenewalInterval, DefaultRenewalInterval); iv > 0 {
		c.loopWG.Add(1)
		go c.runRenewalLoop(iv)
	}
	if iv := intervalOr(c.cfg.PollInterval, DefaultPollInterval); iv > 0 {
		c.loopWG.Add(1)
		go c.runPollLoop(iv)
	}
	return bootstrap, nil
}

// bootstrap runs the founding/joining discovery loop. It returns the
// document and version its winning write produced, so Start can seed the
// first snapshot without a second read.
func (c *Coordinator) bootstrap(p coord.Params) (coord.Bootstrap, *document, string, error) {
	// The founder's row: a founder never advertises Joining (see Start).
	founderRow := docRow{
		ID:                string(p.Self.ID),
		Addr:              p.Self.Addr,
		Roles:             uint8(p.InitialRoles &^ coord.RoleJoining),
		DeclaredUnitCount: p.DeclaredUnitCount,
		LeaseGen:          1,
		Inc:               newInc(),
	}
	// Record the incarnation this process is about to write: renewals
	// verify the row still carries it, and a mismatch means a replacement
	// process with the same stable ID displaced us (see renewOnce).
	c.setSelfInc(founderRow.Inc)
	for attempt := 0; attempt < casAttempts; attempt++ {
		// Attempt the founding write: a document holding only self.
		d := &document{Members: []docRow{founderRow}}
		data, err := encodeDocument(d)
		if err != nil {
			return 0, nil, "", err
		}
		ver, err := c.cfg.Store.PutIfAbsent(c.cfg.Key, data)
		if err == nil {
			return coord.BootstrapFounded, d, ver, nil
		}
		if !errors.Is(err, storageunit.ErrPrecondition) {
			return 0, nil, "", fmt.Errorf("cas: bootstrap: %w", err)
		}

		// An incumbent document exists: read it and CAS our row in.
		data, ver, err = c.cfg.Store.Get(c.cfg.Key)
		if errors.Is(err, storageunit.ErrCondNotFound) {
			casBackoff(attempt)
			continue // vanished between the writes: retry the founding write
		}
		if err != nil {
			return 0, nil, "", fmt.Errorf("cas: bootstrap: %w", err)
		}
		cur, err := decodeDocument(data)
		if err != nil {
			return 0, nil, "", err
		}

		row := founderRow
		bootstrap := coord.BootstrapFounded
		if len(cur.Members) > 0 {
			// Incumbents exist: JOIN, keeping InitialRoles whole (Joining
			// included) so the warming stance is atomic with first
			// appearance. An empty member list keeps the founding stance:
			// the document survived its members, nobody exists to join.
			row.Roles = uint8(p.InitialRoles)
			bootstrap = coord.BootstrapJoined
		}
		cur.upsert(row)
		data, err = encodeDocument(cur)
		if err != nil {
			return 0, nil, "", err
		}
		newVer, err := c.cfg.Store.CompareAndSet(c.cfg.Key, data, ver)
		if err == nil {
			return bootstrap, cur, newVer, nil
		}
		if !errors.Is(err, storageunit.ErrPrecondition) {
			return 0, nil, "", fmt.Errorf("cas: bootstrap: %w", err)
		}
		casBackoff(attempt)
	}
	return 0, nil, "", errors.New("cas: bootstrap: CAS retries exhausted joining the membership document")
}

// mutate is the shared read-modify-write cycle every document edit goes
// through: read the current document, apply edit to a decoded copy, CAS it
// back, and on ErrPrecondition re-read the winner's document and re-apply -
// so concurrent editors interleave without losing each other's rows. An
// absent document is edited from empty and created with PutIfAbsent (the
// renewal loop uses that to re-found after an over-eager GC).
//
// edit reports whether the document it saw needs writing: false skips the
// write entirely (the document already carries the intended state, or the
// edit no longer applies), keeping a no-op from churning the version - every
// churned write is a CAS conflict some peer must retry through. Because edit
// re-runs on every CAS retry against the winner's fresh document, closures
// must derive what they write from CURRENT state (their own records, the
// re-read document), never from values captured before the loop.
func (c *Coordinator) mutate(edit func(d *document) bool) error {
	for attempt := 0; attempt < casAttempts; attempt++ {
		data, ver, err := c.cfg.Store.Get(c.cfg.Key)
		var d *document
		absent := false
		switch {
		case errors.Is(err, storageunit.ErrCondNotFound):
			d, absent = &document{}, true
		case err != nil:
			return fmt.Errorf("cas: reading membership document: %w", err)
		default:
			if d, err = decodeDocument(data); err != nil {
				return err
			}
		}
		if !edit(d) {
			return nil
		}
		out, err := encodeDocument(d)
		if err != nil {
			return err
		}
		if absent {
			_, err = c.cfg.Store.PutIfAbsent(c.cfg.Key, out)
		} else {
			_, err = c.cfg.Store.CompareAndSet(c.cfg.Key, out, ver)
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, storageunit.ErrPrecondition) {
			return fmt.Errorf("cas: writing membership document: %w", err)
		}
		casBackoff(attempt)
	}
	return errors.New("cas: CAS retries exhausted updating the membership document")
}

// runRenewalLoop CAS-advances this node's leaseGen on the renewal cadence.
// One of the adapter's two I/O loops; stops at Close.
func (c *Coordinator) runRenewalLoop(iv time.Duration) {
	defer c.loopWG.Done()
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-t.C:
			// The next tick retries a failed renewal; the failure itself is
			// not swallowed - renewOnce advanced the Health watermark and
			// logs when a run crosses the degradation threshold.
			_ = c.renewOnce()
			if c.displaced.Load() {
				return // stepped down: this process never writes the document again
			}
		}
	}
}

// errDisplaced is returned by mutating entry points after stepDown: a
// replacement process with this node's stable ID owns the membership row,
// and this process must never write the document again.
var errDisplaced = errors.New("cas: displaced: a newer process with this node's ID owns the membership row")

// selfIncarnation returns the incarnation nonce this process last wrote for
// its own row.
func (c *Coordinator) selfIncarnation() string {
	c.incMu.Lock()
	defer c.incMu.Unlock()
	return c.selfInc
}

func (c *Coordinator) setSelfInc(inc string) {
	c.incMu.Lock()
	c.selfInc = inc
	c.incMu.Unlock()
}

// stepDown latches the displaced state (idempotent) and logs it once. A
// displaced process keeps serving reads from its last published snapshot -
// tearing the view down would turn an identity handover into an outage for
// in-flight readers - but stops renewing, polling, GC'ing and role-writing:
// the document, row included, belongs to the replacement now.
func (c *Coordinator) stepDown() {
	if !c.displaced.CompareAndSwap(false, true) {
		return
	}
	c.log.Printf("cas: node %s: membership row carries a different incarnation - a replacement process with this ID displaced us; stepping down (serving the last snapshot, no further document writes)", c.self.ID)
}

// renewOnce advances self's leaseGen by one read-modify-write cycle. If a
// peer GC'd this node's row (an over-eager expiry, e.g. after a long stall),
// the renewal RE-ADDS it with the currently desired roles: renewal is also
// the self-heal path back into the membership. The in-place renewal also
// REPAIRS a row whose roles drifted from the locally desired set (the local
// record is the single source of truth for self's roles; the document is a
// projection of it), so any divergence this adapter has not imagined heals
// on the renewal cadence instead of sticking until an unrelated role change.
//
// RENEWAL IS ALSO WHERE DISPLACEMENT IS DETECTED. The renewal verifies the
// row still carries the incarnation THIS process wrote; a different inc
// means a replacement process with the same stable ID took the identity
// over (the k8s StatefulSet overlap: this pod still running behind a
// partitioned kubelet while its replacement bootstraps). Blindly
// incrementing that row would merge the two processes into one lease - and
// if the replacement then dies non-gracefully, this zombie keeps the row's
// counter advancing forever, so no observer ever expires a row whose Addr
// points at a dead pod. The old process steps down instead (see stepDown).
func (c *Coordinator) renewOnce() error {
	if c.displaced.Load() {
		return errDisplaced
	}
	var (
		displaced bool
		addedInc  string
	)
	err := c.mutate(func(d *document) bool {
		displaced, addedInc = false, "" // reset: the closure re-runs on CAS retries
		c.rolesMu.Lock()
		roles := c.roles
		c.rolesMu.Unlock()
		if i := d.find(c.self.ID); i >= 0 {
			if d.Members[i].Inc != c.selfIncarnation() {
				displaced = true
				return false // the row is the replacement's: do not touch it
			}
			d.Members[i].LeaseGen++
			d.Members[i].Roles = uint8(roles) // drift heal: desired is authoritative
			return true
		}
		addedInc = newInc() // a fresh incarnation: observers reset their expiry count
		d.upsert(docRow{
			ID:                string(c.self.ID),
			Addr:              c.self.Addr,
			Roles:             uint8(roles),
			DeclaredUnitCount: c.declaredUnitCount,
			LeaseGen:          1,
			Inc:               addedInc,
		})
		return true
	})
	if displaced {
		c.stepDown()
		return errDisplaced
	}
	if err != nil {
		if n := c.renewFail.Add(1); n == degradedThreshold {
			c.log.Printf("cas: %d consecutive renewal failures; lease not advancing since renewal success #%d (peers expire this node after their ExpiryPolls windows): %v", n, c.renewOK.Load(), err)
		}
		return err
	}
	if addedInc != "" {
		c.setSelfInc(addedInc) // the re-add landed: that inc is ours now
	}
	c.renewOK.Add(1)
	if n := c.renewFail.Swap(0); n >= degradedThreshold {
		c.log.Printf("cas: renewal recovered after %d consecutive failures", n)
	}
	return nil
}

// runPollLoop re-reads the document on the poll cadence. The adapter's other
// I/O loop; stops at Close.
func (c *Coordinator) runPollLoop(iv time.Duration) {
	defer c.loopWG.Done()
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-t.C:
			if c.displaced.Load() {
				return // stepped down: the last snapshot is frozen (see stepDown)
			}
			c.pollOnce()
		}
	}
}

// pollOnce runs one observation pass: read the document, judge expiry,
// publish the snapshot, GC, hint. A transient read error (or a corrupt
// document, or a document that unexpectedly vanished - the store has no
// delete, so that cannot normally happen) keeps serving the LAST snapshot:
// stale answers cost availability, never correctness, and tearing the view
// down on a blip would. What a failure does NOT do is stay invisible: it
// advances the consecutive-failure watermark (see Health) and, when the run
// crosses degradedThreshold, logs once - a node whose store access degrades
// must be distinguishable from a quiet cluster.
func (c *Coordinator) pollOnce() {
	if c.displaced.Load() {
		// Stepped down: the document (self's row included) belongs to the
		// replacement process. Observing it would adopt the replacement's
		// row as "self" and GC on its behalf; the honest behavior is a
		// frozen last snapshot and no document access at all.
		return
	}
	data, ver, err := c.cfg.Store.Get(c.cfg.Key)
	if err != nil {
		c.pollFailed(err)
		return
	}
	d, err := decodeDocument(data)
	if err != nil {
		c.pollFailed(err)
		return
	}
	c.pollOK.Add(1)
	if n := c.pollFail.Swap(0); n >= degradedThreshold {
		c.log.Printf("cas: poll recovered after %d consecutive failures; view is fresh again", n)
	}
	c.observe(d, ver)
}

// pollFailed records one failed observation pass, logging exactly once per
// degradation episode (when the run crosses degradedThreshold).
func (c *Coordinator) pollFailed(err error) {
	if n := c.pollFail.Add(1); n == degradedThreshold {
		c.log.Printf("cas: %d consecutive poll failures; serving a stale view since poll success #%d: %v", n, c.pollOK.Load(), err)
	}
}

// observe is the single observation step both Start and the poll go
// through: track lease advancement, expire the silent, publish one {view,
// ring} snapshot, GC expired rows, and fire the change hint if the PUBLISHED
// SNAPSHOT moved - never on document-version movement alone. Lease renewals
// move the version on every renewal interval without changing the view, so a
// hint keyed on the version would fire on essentially every poll; a consumer
// that debounces view changes with extend-on-burst semantics (the cluster's
// settle timer does) would then have its debounced response re-armed forever
// and never run it. The snapshot is the view; only its movement is a "view
// may have changed" fact worth hinting.
func (c *Coordinator) observe(d *document, ver string) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()

	// Judge liveness observer-side, in decide.ExpireSilent: the rules (self
	// exempt, a baseline pass for a member new to the view, an incarnation or
	// counter change resetting the count, K consecutive unchanged passes
	// expiring) are a fold over scalars and live there with their reasoning.
	// This pass carries the fold's output into the document's shape.
	rows := make([]decide.LeaseRow, 0, len(d.Members))
	for _, row := range d.Members {
		rows = append(rows, decide.LeaseRow{ID: row.ID, Inc: row.Inc, Gen: row.LeaseGen})
	}
	next, expiredIDs := decide.ExpireSilent(c.tracker, rows, c.cfg.ExpiryPolls, string(c.self.ID))
	c.tracker = next

	expired := make(map[string]struct{}, len(expiredIDs))
	for _, id := range expiredIDs {
		expired[id] = struct{}{}
	}
	live := make([]docRow, 0, len(d.Members)+1)
	selfSeen := false
	for _, row := range d.Members {
		if _, gone := expired[row.ID]; gone {
			continue
		}
		if storageunit.NodeID(row.ID) == c.self.ID {
			selfSeen = true
		}
		live = append(live, row)
	}

	// SELF-EXEMPTION MUST SURVIVE A MISSING ROW. A peer that judged this
	// node expired (an over-eager expiry after a stall) GC's its row, so the
	// document this poll read may lack self entirely - and the row-driven
	// loop above then produces a view WITHOUT Self, breaking the port's
	// "View always contains Self once started" shape (consumers compute
	// their own position from the view: a self-less view silently drops a
	// Draining stance and releases every mounted unit). Synthesize self from
	// local knowledge - identity, locally-desired roles, declared count -
	// and let the renewal loop re-insert the document row (the self-heal
	// path renewOnce already owns).
	if !selfSeen {
		c.rolesMu.Lock()
		roles := c.roles
		c.rolesMu.Unlock()
		live = append(live, docRow{
			ID:                string(c.self.ID),
			Addr:              c.self.Addr,
			Roles:             uint8(roles),
			DeclaredUnitCount: c.declaredUnitCount,
		})
	}

	snapMoved := c.publish(live)

	if len(expired) > 0 {
		c.gcExpired(expired, d, ver)
	}

	if snapMoved {
		c.signal()
	}
}

// publish swaps in a new {view, ring} snapshot if the live membership
// differs from the published one, and reports whether it did. The ring is
// rebuilt only when placement inputs (ids + addrs) changed; a roles-only
// change republishes the view with the same ring - still ONE pair, still
// atomically swapped.
func (c *Coordinator) publish(rows []docRow) bool {
	cur := c.snap.Load()
	members := make([]coord.Member, 0, len(rows))
	for _, r := range rows {
		members = append(members, r.member())
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	if membersEqual(cur.view.Members, members) {
		return false
	}
	r := cur.ring
	if r == nil || !nodesEqual(cur.view.Members, members) {
		r = ring.New()
		for _, m := range members {
			r.Add(ring.Member{ID: string(m.ID), Addr: m.Addr})
		}
	}
	epoch := c.epoch.Add(1)
	c.snap.Store(&snapshot{
		view: coord.View{Self: c.self, Members: members, Epoch: epoch},
		ring: r,
	})
	return true
}

// membersEqual reports whether two ID-sorted member slices are identical in
// every coordination fact (identity, address, roles, declared count).
func membersEqual(a, b []coord.Member) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// nodesEqual reports whether two ID-sorted member slices agree on placement
// inputs only (ids + addrs).
func nodesEqual(a, b []coord.Member) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Node != b[i].Node {
			return false
		}
	}
	return true
}

// gcExpired best-effort CAS-removes expired rows, using the version this
// observation pass read. ONE attempt per pass: losing the race just means
// another member's document is already correct (or will be re-judged next
// poll) - GC is idempotent and racing GCs resolve by CAS.
func (c *Coordinator) gcExpired(expired map[string]struct{}, d *document, ver string) {
	next := &document{Members: make([]docRow, 0, len(d.Members))}
	for _, row := range d.Members {
		if _, ex := expired[row.ID]; !ex {
			next.Members = append(next.Members, row)
		}
	}
	data, err := encodeDocument(next)
	if err != nil {
		return
	}
	_, _ = c.cfg.Store.CompareAndSet(c.cfg.Key, data, ver)
}

// signal delivers a coalescing change hint. A full channel means an
// undelivered hint is already pending, so dropping this one loses nothing.
func (c *Coordinator) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// Changed returns the coalescing change-hint channel: the poll fires it when
// the published snapshot changed - membership, addresses, roles, declared
// counts, INCLUDING an expiry (which changes the view without any document
// write). A document write that leaves the snapshot unchanged (a peer's lease
// renewal - one lands on virtually every poll) deliberately fires nothing:
// renewals are the mechanism's heartbeat, not a view change, and hinting them
// would turn the channel into a poll-cadence metronome that keeps a consumer's
// change-debounce permanently re-armed. Lossy by contract.
func (c *Coordinator) Changed() <-chan struct{} { return c.changed }

// View returns the published snapshot's view. No locks, no I/O: the pair is
// prebuilt by the poll.
func (c *Coordinator) View() coord.View { return c.snap.Load().view }

// Populated reports whether the view holds at least one member. It reads the
// published snapshot directly - nothing is constructed per call, honoring
// the port's per-op cost clause.
func (c *Coordinator) Populated() bool { return len(c.snap.Load().view.Members) > 0 }

// PlacementMembers returns the member set Locate computes placement over.
// UNDER THIS ADAPTER IT IS ALWAYS VIEW'S MEMBER SET: view and ring are one
// atomically-swapped snapshot derived from one source, so the dual-basis
// divergence the method exists to expose is structurally absent here - the
// bases cannot deviate, and guards reading both always see agreement.
func (c *Coordinator) PlacementMembers() []coord.Node {
	ms := c.snap.Load().view.Members
	if len(ms) == 0 {
		return nil
	}
	out := make([]coord.Node, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Node)
	}
	return out
}

// TransitionSets returns the joining / draining member-ID sets from ONE scan
// of the published snapshot's members - no view construction, nil maps in
// the steady state, fresh maps every call (the caller's to keep).
func (c *Coordinator) TransitionSets() (joining, draining map[storageunit.NodeID]struct{}) {
	for _, m := range c.snap.Load().view.Members {
		if m.Joining() {
			if joining == nil {
				joining = make(map[storageunit.NodeID]struct{}, 2)
			}
			joining[m.ID] = struct{}{}
		}
		if m.Draining() {
			if draining == nil {
				draining = make(map[storageunit.NodeID]struct{}, 2)
			}
			draining[m.ID] = struct{}{}
		}
	}
	return joining, draining
}

// Locate returns up to n nodes for u, primary first, off the published
// snapshot's ring - the same pkg/ring math and the same GenUnitBytes input
// as the gossip adapter, so the two adapters place identically over the
// same live member set.
//
// An exclusion is honored by locating over a ring GENUINELY REBUILT from
// the non-excluded members, never by filtering the full answer: bounded-load
// consistent hashing is not removal-invariant, so the filtered answer would
// be a different (wrong) placement - the port's exactness contract.
func (c *Coordinator) Locate(u storageunit.GenUnit, n int, p coord.Placement) []coord.Node {
	if n <= 0 {
		return nil
	}
	s := c.snap.Load()
	r := s.ring
	if r == nil {
		return nil
	}
	if len(p.Exclude) > 0 {
		r = c.reducedRing(s, p.Exclude)
		if r.Empty() {
			// Every member is excluded: no hypothetical placement to answer
			// with. nil lets the caller fall back to the real one.
			return nil
		}
	}
	ms := r.LocateKeyN(placement.GenUnitBytes(u), n)
	if len(ms) == 0 {
		return nil
	}
	out := make([]coord.Node, 0, len(ms))
	for _, m := range ms {
		out = append(out, coord.Node{ID: storageunit.NodeID(m.ID), Addr: m.Addr})
	}
	return out
}

// reducedRing returns a ring holding every member of snapshot s except the
// excluded ids, memoized on (exclusion set, snapshot epoch) exactly like the
// gossip adapter: a reconcile pass asks the same exclusion once per unit,
// and the epoch key guarantees a stale reduced ring can never outlive the
// membership it was built from.
func (c *Coordinator) reducedRing(s *snapshot, exclude map[storageunit.NodeID]struct{}) *ring.Ring {
	key := excludeKey(exclude)
	epoch := s.view.Epoch

	c.reducedMu.Lock()
	defer c.reducedMu.Unlock()
	if c.reduced != nil && c.reducedKey == key && c.reducedEpoch == epoch {
		return c.reduced
	}
	r := ring.New()
	for _, m := range s.view.Members {
		if _, ex := exclude[m.ID]; ex {
			continue
		}
		r.Add(ring.Member{ID: string(m.ID), Addr: m.Addr})
	}
	c.reduced, c.reducedKey, c.reducedEpoch = r, key, epoch
	return r
}

// excludeKey renders an exclusion set as an order-independent string, so two
// callers passing the same members in different map-iteration orders hit the
// same memo entry.
func excludeKey(exclude map[storageunit.NodeID]struct{}) string {
	ids := make([]string, 0, len(exclude))
	for id := range exclude {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	return strings.Join(ids, "\x00")
}

// SetRole publishes this node's complete role set. The LOCAL DESIRED record
// (c.roles) is the single source of truth: SetRole records the desire under
// the mutex, then projects it into the document - and the mutate closure
// RE-READS the current desired value on every execution (initial and each
// CAS retry), so unordered concurrent SetRoles converge the document to the
// LAST desired state instead of whichever mutate happened to land last. The
// dedup compares desired-vs-desired only; whether the document agrees is the
// renewal loop's business (its drift heal repairs a diverged row), so a
// failed or skipped write can delay a flip by a renewal interval but can
// never make a wrong advertised role stick. The flip becomes visible to the
// query methods when a poll next refreshes the snapshot (roles ride the
// poll, never the change hint).
func (c *Coordinator) SetRole(want coord.Role) error {
	if c.displaced.Load() {
		return errDisplaced // stepped down: the row is the replacement's to write
	}
	c.rolesMu.Lock()
	if c.roles == want || !c.started.Load() {
		c.roles = want
		c.rolesMu.Unlock()
		return nil
	}
	c.roles = want
	c.rolesMu.Unlock()

	return c.mutate(func(d *document) bool {
		i := d.find(c.self.ID)
		if i < 0 {
			// Row absent (a peer GC'd us): nothing to edit - the renewal's
			// re-add carries the desired roles from the local record.
			return false
		}
		c.rolesMu.Lock()
		cur := uint8(c.roles)
		c.rolesMu.Unlock()
		if d.Members[i].Roles == cur {
			// Already converged: a racing setter's write landed the current
			// desired state while this one waited out a CAS retry.
			return false
		}
		d.Members[i].Roles = cur
		return true
	})
}

// Generation reports no opinion: generation agreement stays with the
// cluster-side arbiter (which already runs over the same ConditionalStore,
// above the port). See the spec's CAS-coordinator section.
func (c *Coordinator) Generation() storageunit.Generation { return 0 }

// ProposeGeneration always refuses. See Generation.
func (c *Coordinator) ProposeGeneration(storageunit.Generation) error {
	return coord.ErrGenerationUnsupported
}

// Close stops both loops, then best-effort CAS-removes self's row (the
// graceful fast path; a crash, partition or failed removal falls through to
// observer-side expiry). Idempotent, and a safe no-op before Start. After a
// TestingShutdownNoLeave the removal is skipped: a SIGKILLed process runs
// nothing, so its row must linger for peers to reap. After a displacement
// step-down the removal is skipped too: the row wearing the replacement's
// incarnation is the REPLACEMENT's membership, and removing it would tear
// the live process out of the cluster on the dead one's way out.
func (c *Coordinator) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		c.loopWG.Wait()
		return nil
	}
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.loopWG.Wait()
	if c.started.Load() && !c.hardStopped.Load() && !c.displaced.Load() {
		_ = c.mutate(func(d *document) bool {
			i := d.find(c.self.ID)
			if i < 0 {
				return false // already gone (a peer's GC): nothing to remove
			}
			d.Members = append(d.Members[:i], d.Members[i+1:]...)
			return true
		})
	}
	return nil
}
