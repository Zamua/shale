package integration

// THE LOSSLESS-RESHARD GATE TEST for v0.8 Phase 4 (the data-loss oracle).
//
// This is the safety net the whole doubling-resharder phase is built to
// satisfy:
//
//	NO ACKED WRITE MAY BE LOST DURING THE BISECT.
//
// Phase 4 GROWS the cluster by DOUBLING the unit count N -> 2N at a new
// cluster generation. Each old gen-g unit K bisects ONLINE into EXACTLY its
// two gen-(g+1) children K and K+N by one additional hash bit. The old unit
// keeps serving reads + writes while its data is background-copied into the
// two children; then a brief per-key-space write-pause drains the last
// writes and an atomic cut-over flips routing for that unit's key-space.
// Bytes are copied ONCE per unit (online, per-unit, bounded), never globally
// re-partitioned. The hazard: a write to old-unit-K that arrives DURING the
// copy or the catch-up must not be lost.
//
// The gate below stands up an in-process multi-backend cluster on the
// SHARED-BACKING factory (internal/sharedfactory - one Backing per cluster,
// per-node Handles share it, with a per-(generation,unit) durable writer-
// epoch) and asserts SIX things across a live doubling reshard, WITH A
// CONCURRENT WRITER hammering acked keys through the whole reshard:
//
//  1. THE ORACLE (zero loss). Every key written before the reshard AND every
//     key acked by the concurrent probe DURING the reshard is still readable,
//     with its EXACT recorded value, after the cluster reaches gen 1 (2N
//     units). Hundreds of baseline keys, including co-located {tag} sets, plus
//     a stream of probe writes (the Phase-4 analogue of Phase 3's concurrent
//     write probe).
//  2. BISECT CORRECTNESS. A key that lived in old gen-0 unit K now lives in
//     new gen-1 unit UnitForShardKey(key, 2N), which is provably K or K+N (the
//     right child by the new hash bit, ChildUnit). The data PHYSICALLY MOVED:
//     the value is present in the shared backing's gen-1 child store and the
//     retired gen-0 unit store is gone.
//  3. ROUTING post-reshard. The cluster's live generation advanced to 1 and a
//     fresh Get/Put routes to the gen-1 unit under N=2N - it does not resolve
//     against a stale gen-0 unit.
//  4. CO-LOCATION SURVIVES. Every key in a {tag} set lands on ONE gen-1 unit
//     after the bisect (a co-located set is never split across two children),
//     because the whole set shares one ShardHash and one ShardHash bisects to
//     exactly one child.
//  5. CLEAN GENERATION. After the reshard the node mounts exactly the 2N gen-1
//     units and none of the retired gen-0 units (the cut-over set is cleared,
//     every unit resolves under the gen-1 map).
//  6. BREAK DEMONSTRATION (TestReshardGateCatchesLostWrite, below). A
//     deliberately broken bisect (a factory wrapper that DROPS the per-unit
//     write-pause so a write landing in the copy window is never caught up)
//     makes the oracle FAIL, proving the gate actually catches a lost write
//     rather than rubber-stamping.
//
// If this test ever passes while a write is silently lost, the gate is
// broken; the break-demonstration test exists to keep it honest.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// genUnitBytesForTest mirrors the cluster's internal genUnitBytes (8-byte
// big-endian generation, then 4-byte big-endian unit id) for ANY generation,
// so a test-side ring placement of a gen-(g) unit agrees with cluster routing.
// gen0UnitBytes is the gen-0 specialization; this is the general form the
// post-reshard routing assertions need (they place gen-1 units).
func genUnitBytesForTest(gu storageunit.GenUnit) []byte {
	var b [12]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(gu.Gen))
	binary.BigEndian.PutUint32(b[8:12], uint32(gu.ID))
	return b[:]
}

// genUnitOwnerOnRing previews which node owns a generation-qualified unit
// under the ring built from c.Members() - the same deterministic hashing +
// 12-byte encoding the cluster routes units with. Used to assert that a fresh
// op for a key routes to the gen-1 owner after a reshard.
func genUnitOwnerOnRing(c *cluster.Cluster, gu storageunit.GenUnit) string {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	return r.LocateKey(genUnitBytesForTest(gu)).ID
}

// childUnitForKey returns the gen-(g+1) child UnitID a key bisects into when
// the count doubles from oldCount to 2*oldCount, via the SAME doubling math
// the resharder uses (storageunit.ChildUnit). It is, by the doubling
// property, exactly K or K+N where K is the key's old unit.
func childUnitForKey(t *testing.T, key string, oldCount int) storageunit.UnitID {
	t.Helper()
	uc := storageunit.MustUnitCount(oldCount)
	h := storageunit.HashShardKey(ring.ShardKey([]byte(key)))
	child, err := storageunit.ChildUnit(h, uc)
	if err != nil {
		t.Fatalf("childUnitForKey: ChildUnit(%q): %v", key, err)
	}
	return child
}

// sharedStoreHas reports whether the shared backing's store for gu currently
// holds key=want. ok=false means no store exists for gu at all (never opened /
// retired). Lets the bisect-correctness assertion read the PHYSICAL gen-1
// child store directly, bypassing the cluster, to prove the data moved.
func sharedStoreHas(backing *sharedfactory.Backing, gu storageunit.GenUnit, key string, want []byte) (found, ok bool) {
	s, ok := backing.UnitStore(gu)
	if !ok {
		return false, false
	}
	got, err := s.Get([]byte(key))
	if err != nil || !bytes.Equal(got, want) {
		return false, true
	}
	return true, true
}

// TestLosslessReshardGate is THE data-loss oracle for v0.8 Phase 4. See the
// file header for the six properties it pins. It runs on a single
// shared-backing node (the right shape for the bisect-correctness +
// physical-move assertions: no cross-node ring noise, so the gen-1 child store
// is deterministic), writes a recorded dataset spanning the whole unit space
// plus co-located {tag} sets, starts a CONCURRENT PROBE that keeps acking new
// keys, doubles N -> 2N online via Cluster.Reshard(), and asserts the whole
// dataset (baseline + probe) survived with exact values, the bisect moved each
// key to its correct gen-1 child, routing advanced to gen 1, co-location held,
// and the node ends on a clean gen-1 unit set.
func TestLosslessReshardGate(t *testing.T) {
	const unitCount = 8 // N; doubles to 16
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "rgate", "", unitCount, backing)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster}, 1, 10*time.Second); err != nil {
		t.Fatalf("founder up: %v", err)
	}

	// === Step 1: write a known dataset spanning MANY units, recording
	// key -> value. Two shapes:
	//   - plainKeys: hundreds of distinct keys routed by their whole key,
	//     spreading across all N units.
	//   - coLocated: several {tag} sets, every key in a set sharing one tag so
	//     they all land on ONE unit (Redis-cluster hash-tag co-location). ===
	want := make(map[string][]byte) // the recorded dataset: key -> exact value

	const nPlain = 500
	for i := range nPlain {
		k := fmt.Sprintf("rec-%05d", i)
		v := fmt.Appendf(nil, "val-%05d-payload", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Co-located {tag} sets: 12 tags x 8 members each. Every member of a tag
	// MUST share a unit, before AND after the bisect.
	const nTags, perTag = 12, 8
	tagSets := make(map[string][]string) // tag -> its keys
	for ti := range nTags {
		tag := fmt.Sprintf("acct%02d", ti)
		set := make([]string, 0, perTag)
		for mi := range perTag {
			k := fmt.Sprintf("{%s}/field-%d", tag, mi)
			v := fmt.Appendf(nil, "co-%s-%d", tag, mi)
			if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
				t.Fatalf("co-located Put %q: %v", k, err)
			}
			want[k] = v
			set = append(set, k)
		}
		tagSets[tag] = set
	}

	// Pre-condition: every co-located set is on ONE gen-0 unit, and the dataset
	// spans the WHOLE unit space (otherwise the bisect is not exercised on every
	// unit). Record each tag's gen-0 unit so we can prove it bisects cleanly.
	oldUC := storageunit.MustUnitCount(unitCount)
	tagUnit := make(map[string]storageunit.UnitID, nTags)
	for tag, set := range tagSets {
		u0 := storageunit.UnitForShardKey(ring.ShardKey([]byte(set[0])), oldUC)
		for _, k := range set {
			if u := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), oldUC); u != u0 {
				t.Fatalf("precondition: co-located set %q split across units %d and %d", tag, u0, u)
			}
		}
		tagUnit[tag] = u0
	}
	spanned := make(map[storageunit.UnitID]struct{})
	for k := range want {
		spanned[storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), oldUC)] = struct{}{}
	}
	if len(spanned) != unitCount {
		t.Fatalf("precondition: baseline dataset spans only %d/%d units; widen the key set so every unit bisects", len(spanned), unitCount)
	}

	// Sanity: confirm everything is readable BEFORE the reshard (proves the
	// baseline is genuinely committed + routable at gen 0).
	for k, v := range want {
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 5*time.Second)
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("baseline %q unreadable/wrong before reshard: got=%q err=%v", k, got, err)
		}
	}

	// === Step 2: start a CONCURRENT background probe. It keeps adding +
	// recording ACKED keys (a key counts as acked only once Put returns nil),
	// the Phase-4 analogue of Phase 3's concurrent-write probe. It writes
	// THROUGH the full cluster surface (routed Put), so it exercises the routed
	// write path's per-unit write-pause integration across the cut-over
	// boundary, not just the in-process backend. ===
	var probeMu sync.Mutex
	probeAcked := make(map[string][]byte)
	var stop atomic.Bool
	var wg sync.WaitGroup
	const probeWriters = 4
	// Each probe writer closes its own ready channel after its FIRST acked write
	// so the barrier below can hold the reshard until every writer is in flight.
	probeReady := make([]chan struct{}, probeWriters)
	for w := range probeReady {
		probeReady[w] = make(chan struct{})
	}
	for w := range probeWriters {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("probe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "pv-%d-%07d", w, i)
				// Retry the transient acquiring-window error; any other error is a
				// real failure. Record the key ONLY after Put returns nil.
				if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 5*time.Second); err != nil {
					t.Errorf("probe Put %q during reshard: %v", k, err)
					return
				}
				probeMu.Lock()
				probeAcked[k] = v
				probeMu.Unlock()
				if i == 0 {
					close(probeReady[w])
				}
				i++
			}
		}(w)
	}
	// BARRIER: hold the reshard until every probe writer has provably landed an
	// acked write. The writers keep looping through the reshard that follows, so
	// the acked set spans it (and lands in copy windows) by construction rather
	// than by timing luck.
	awaitFirstAckBarrier(t, probeReady, 30*time.Second, &stop, &wg)

	// === Step 3: trigger the doubling reshard. The cluster bisects each old
	// gen-0 unit K into its gen-1 children K and K+N, online. ===
	if err := n1.Cluster.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("Reshard: %v", err)
	}

	// === Step 4: stop the probe and let everything settle. ===
	stop.Store(true)
	wg.Wait()

	probeMu.Lock()
	if len(probeAcked) < probeWriters {
		probeMu.Unlock()
		t.Fatalf("only %d probe writes acked; the concurrent writer did not run", len(probeAcked))
	}
	t.Logf("concurrent probe acked %d keys across the reshard", len(probeAcked))
	// Snapshot the probe set so we can assert without holding the lock.
	probeSnapshot := make(map[string][]byte, len(probeAcked))
	maps.Copy(probeSnapshot, probeAcked)
	probeMu.Unlock()

	newCount := 2 * unitCount
	newUC := storageunit.MustUnitCount(newCount)

	// === ASSERTION (Step 4 oracle): EVERY baseline key AND every acked probe
	// key is readable with its EXACT value. ZERO loss. ===
	assertReadable := func(label string, ds map[string][]byte) {
		for k, v := range ds {
			got, err := getWithRetryUnavailable(t, n1.Cluster, k, 8*time.Second)
			if err != nil {
				t.Fatalf("ORACLE VIOLATED (%s): key %q unreadable after reshard: %v (NO ACKED WRITE LOST)", label, k, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("ORACLE VIOLATED (%s): key %q = %q, want %q (value corrupted across reshard)", label, k, got, v)
			}
		}
	}
	assertReadable("baseline", want)
	assertReadable("probe", probeSnapshot)

	// === ASSERTION (Step 5): BISECT CORRECTNESS. A key that was in old gen-0
	// unit K now lives in new gen-1 unit UnitForShardKey(key, 2N), which is
	// provably K or K+N by the new hash bit. AND the data PHYSICALLY moved: its
	// value is present in the shared backing's gen-1 child store, and the
	// retired gen-0 unit store no longer serves it (the cut-over moved the
	// key-space to the children). We check a representative spread of baseline
	// keys (every Nth) plus the full probe set is implicitly covered by the
	// oracle reads above. ===
	checked := 0
	for k, v := range want {
		oldK := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), oldUC)
		// The gen-1 unit the key maps to under 2N must equal ChildUnit(old) and
		// be one of {oldK, oldK+N}.
		newU := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), newUC)
		child := childUnitForKey(t, k, unitCount)
		if newU != child {
			t.Fatalf("BISECT VIOLATED: key %q maps to gen-1 unit %d under 2N but ChildUnit says %d", k, newU, child)
		}
		if newU != oldK && newU != oldK+storageunit.UnitID(unitCount) {
			t.Fatalf("BISECT VIOLATED: key %q gen-1 unit %d is neither old unit %d nor its sibling %d", k, newU, oldK, oldK+storageunit.UnitID(unitCount))
		}
		// PHYSICAL MOVE: the value lives in the gen-1 child store.
		newGU := storageunit.NewGenUnit(1, newU)
		found, ok := sharedStoreHas(backing, newGU, k, v)
		if !ok {
			t.Fatalf("BISECT VIOLATED: key %q - shared backing has no gen-1 store for child %s (data did not move to the new generation)", k, newGU)
		}
		if !found {
			t.Fatalf("BISECT VIOLATED: key %q absent (or wrong) in its gen-1 child store %s (data did not physically move)", k, newGU)
		}
		checked++
	}
	t.Logf("bisect correctness: verified %d baseline keys moved to their correct gen-1 child store", checked)

	// === ASSERTION (Step 6): ROUTING post-reshard. The live generation is 1; a
	// FRESH Put/Get for a brand-new key routes to its gen-1 unit under N=2N (and
	// is served), and the cluster keeps serving at the doubled unit count. ===
	freshKey := "post-reshard-fresh"
	freshVal := []byte("served-at-gen-1")
	if err := putWithRetryUnavailable(t, n1.Cluster, freshKey, string(freshVal), 8*time.Second); err != nil {
		t.Fatalf("ROUTING: fresh Put after reshard: %v", err)
	}
	got, err := getWithRetryUnavailable(t, n1.Cluster, freshKey, 8*time.Second)
	if err != nil || !bytes.Equal(got, freshVal) {
		t.Fatalf("ROUTING: fresh key after reshard: got=%q err=%v", got, err)
	}
	// The fresh key must physically land in its gen-1 unit store, NOT a gen-0
	// one - proof the live generation actually advanced.
	freshU := storageunit.UnitForShardKey(ring.ShardKey([]byte(freshKey)), newUC)
	freshGU := storageunit.NewGenUnit(1, freshU)
	if found, ok := sharedStoreHas(backing, freshGU, freshKey, freshVal); !ok || !found {
		t.Fatalf("ROUTING VIOLATED: fresh key %q did not land in its gen-1 unit %s (routing did not advance to the new generation): ok=%v found=%v", freshKey, freshGU, ok, found)
	}
	// And on a single node the gen-1 owner of that unit is self (sanity that the
	// ring places gen-1 units, and they all resolve to the one node).
	if owner := genUnitOwnerOnRing(n1.Cluster, freshGU); owner != n1.ID {
		t.Fatalf("ROUTING VIOLATED: gen-1 unit %s owner is %q, want self %q", freshGU, owner, n1.ID)
	}

	// === ASSERTION (Step extra-1): CO-LOCATION SURVIVES. Each {tag} set is
	// still entirely on ONE gen-1 unit (a set shares one ShardHash, and one
	// ShardHash bisects to exactly one child), and that child is one of the
	// recorded gen-0 unit's two children. ===
	for tag, set := range tagSets {
		childOf := childUnitForKey(t, set[0], unitCount)
		for _, k := range set {
			if c := childUnitForKey(t, k, unitCount); c != childOf {
				t.Fatalf("CO-LOCATION VIOLATED: set %q split across gen-1 children %d and %d after bisect", tag, childOf, c)
			}
		}
		parent := tagUnit[tag]
		if childOf != parent && childOf != parent+storageunit.UnitID(unitCount) {
			t.Fatalf("CO-LOCATION VIOLATED: set %q child %d is not a bisect of recorded gen-0 unit %d", tag, childOf, parent)
		}
	}

	// === ASSERTION (Step extra-2): CLEAN GENERATION. After the reshard the node
	// mounts EXACTLY the 2N gen-1 units and NONE of the retired gen-0 units. ===
	if !waitUntil(8*time.Second, func() bool {
		open := n1.Handle.OpenUnits()
		if len(open) != newCount {
			return false
		}
		seen := make(map[storageunit.UnitID]bool, newCount)
		for _, gu := range open {
			if gu.Gen != 1 {
				return false
			}
			seen[gu.ID] = true
		}
		return len(seen) == newCount
	}) {
		t.Fatalf("CLEAN GENERATION VIOLATED: node did not settle to exactly %d gen-1 units: open=%v", newCount, n1.Handle.OpenUnits())
	}
}

// --- BREAK DEMONSTRATION (Step 6) ---------------------------------------
//
// To prove the gate is not a rubber stamp we deliberately BREAK the bisect so
// a write that lands in the copy window is dropped, exactly the failure the
// catch-up drain exists to prevent, then show the oracle FAILS (catches the
// loss). The break is reachable through the factory boundary: we wrap the
// gen-1 CHILD backends and SKIP THE CATCH-UP re-copy of late writes.
//
// How the resharder's bisect drives the wrapped child (see
// pkg/cluster/multibackend_reshard.go):
//
//   - BULK COPY: copyUnitInto scans the old unit and Put()s each key into the
//     child. The old unit keeps serving, so a concurrent write can land in it
//     AFTER the bulk scan passes that key - a "copy-window" write.
//   - CATCH-UP: under the per-unit write-pause, clearBackend() Delete()s the
//     child empty, then copyUnitInto re-Put()s the FROZEN old unit's current
//     contents (which now include the copy-window writes). This is what makes
//     no acked write lost.
//
// The breaking child wrapper:
//
//   - Records every key the BULK pass Put()s (the "bulk set").
//   - Detects the catch-up start: clearBackend is the only thing that Delete()s
//     the child, so the first Delete flips a "catching up" flag.
//   - During catch-up, it lets the re-copy restore bulk-set keys (so the
//     happy-path data survives) but DROPS the re-Put of any key NOT in the bulk
//     set - i.e. precisely the copy-window writes the catch-up is supposed to
//     rescue. Those acked writes vanish.
//
// The oracle (re-run in detection mode) must then observe at least one acked
// copy-window write that is now unreadable / wrong. If it does not, the gate is
// blind to a lost reshard write and the test fails loudly.

// skipCatchupHandle wraps a real sharedfactory.Handle and, for gen-1 child
// units (the bisect destinations), returns a backend that skips the catch-up
// re-copy of copy-window writes. It satisfies storageunit.BackendFactory by
// embedding *sharedfactory.Handle and overriding only OpenUnit.
type skipCatchupHandle struct {
	*sharedfactory.Handle
}

func (h *skipCatchupHandle) OpenUnit(gu storageunit.GenUnit, epoch storageunit.Epoch) (backend.Backend, error) {
	be, err := h.Handle.OpenUnit(gu, epoch)
	if err != nil {
		return nil, err
	}
	// Only sabotage the gen-1 children (the bisect targets). The gen-0 old units
	// and any post-reshard opens are left untouched.
	if gu.Gen != 1 {
		return be, nil
	}
	return &skipCatchupBackend{Backend: be, bulk: make(map[string]struct{})}, nil
}

// skipCatchupBackend drops the catch-up re-Put of copy-window writes. See the
// break-demo header. It is a deliberately broken backend; no production code
// path behaves like this.
type skipCatchupBackend struct {
	backend.Backend
	mu        sync.Mutex
	bulk      map[string]struct{} // keys the bulk copy pass Put()
	catchingU bool                // flipped true by the first Delete (clearBackend)
}

func (b *skipCatchupBackend) Put(key, value []byte) error {
	b.mu.Lock()
	if !b.catchingU {
		// Bulk pass: record the key and let it through.
		b.bulk[string(key)] = struct{}{}
		b.mu.Unlock()
		return b.Backend.Put(key, value)
	}
	// Catch-up re-copy: restore bulk-set keys, but DROP keys that were NOT in
	// the bulk pass - those are exactly the copy-window writes the catch-up is
	// supposed to rescue. Dropping them is the lost-write bug.
	_, wasBulk := b.bulk[string(key)]
	b.mu.Unlock()
	if !wasBulk {
		return nil // silently drop the late write (the break)
	}
	return b.Backend.Put(key, value)
}

func (b *skipCatchupBackend) Delete(key []byte) error {
	// clearBackend (catch-up) is the only caller that Delete()s the child;
	// the first Delete marks the catch-up as begun.
	b.mu.Lock()
	b.catchingU = true
	b.mu.Unlock()
	return b.Backend.Delete(key)
}

// TestReshardGateCatchesLostWrite is the BREAK DEMONSTRATION. It re-runs the
// reshard gate's core (write a baseline, run a concurrent probe, double N -> 2N)
// but with the skip-catch-up factory wrapper, so copy-window writes are dropped
// during the bisect. The oracle MUST then observe the loss.
//
// This test PASSES iff the broken bisect is CAUGHT (some acked probe key whose
// write landed in a copy window becomes unreadable / wrong). If the break
// produced no detectable loss, the test fails loudly: that would mean the
// oracle is blind to a write lost during a reshard, which is exactly what the
// gate must never be.
func TestReshardGateCatchesLostWrite(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	broken := &skipCatchupHandle{Handle: backing.Handle()}
	n1 := startSharedNodeWithFactory(t, "rbrk", "", unitCount, broken)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster}, 1, 10*time.Second); err != nil {
		t.Fatalf("founder up: %v", err)
	}

	// Seed so every unit has data to bulk-copy (and so the catch-up has a
	// non-trivial frozen old unit to re-scan).
	const nSeed = 400
	for i := range nSeed {
		k := fmt.Sprintf("seed-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, fmt.Sprintf("sv-%05d", i), 8*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}

	// Concurrent probe: keeps acking copy-window writes throughout the reshard.
	// These are the writes the (broken) catch-up will drop.
	var probeMu sync.Mutex
	probeAcked := make(map[string][]byte)
	var stop atomic.Bool
	var wg sync.WaitGroup
	const probeWriters = 6
	// Each probe writer closes its own ready channel after its FIRST acked write.
	// The barrier below then holds the reshard until every writer is mid-loop, so
	// the copy windows the broken catch-up must drop writes from are provably
	// occupied. Without it the reshard can finish before a writer is scheduled,
	// leaving nothing for the break to lose and turning this demonstration into a
	// spurious "BREAK NOT CAUGHT".
	probeReady := make([]chan struct{}, probeWriters)
	for w := range probeReady {
		probeReady[w] = make(chan struct{})
	}
	for w := range probeWriters {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("probe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "pv-%d-%07d", w, i)
				if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 5*time.Second); err != nil {
					// A dropped write is silent (Put still returns nil); a real
					// transport error here would be a different failure. Returning
					// without closing this writer's ready channel makes the barrier
					// below fail loudly rather than hang.
					return
				}
				probeMu.Lock()
				probeAcked[k] = v
				probeMu.Unlock()
				if i == 0 {
					close(probeReady[w])
				}
				i++
			}
		}(w)
	}
	// BARRIER: hold the broken reshard until every probe writer has provably
	// landed an acked write.
	awaitFirstAckBarrier(t, probeReady, 30*time.Second, &stop, &wg)

	if err := n1.Cluster.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("Reshard (broken bisect): %v", err)
	}
	stop.Store(true)
	wg.Wait()

	probeMu.Lock()
	snapshot := make(map[string][]byte, len(probeAcked))
	maps.Copy(snapshot, probeAcked)
	probeMu.Unlock()
	if len(snapshot) < probeWriters {
		t.Fatalf("only %d probe writes acked; the concurrent writer did not run", len(snapshot))
	}

	// THE ORACLE, run in detection mode: at least one acked probe write must now
	// be lost (a copy-window write the broken catch-up dropped). The gate is
	// proven to CATCH the break iff we observe such a loss.
	lostObserved := false
	var lostKey string
	lostCount := 0
	for k, v := range snapshot {
		got, err := n1.Cluster.Get([]byte(k))
		if err != nil || !bytes.Equal(got, v) {
			lostObserved = true
			if lostKey == "" {
				lostKey = k
			}
			lostCount++
		}
	}
	if !lostObserved {
		// If we get here, the broken bisect produced NO detectable loss, which
		// would mean the oracle is blind to a write dropped during a reshard.
		// That is the failure the break-demonstration exists to surface.
		t.Fatalf("BREAK NOT CAUGHT: skipped the catch-up re-copy of copy-window writes but every acked key still reads correctly - the oracle is blind to a lost reshard write")
	}
	t.Logf("break demonstration: skipping the catch-up dropped %d acked copy-window write(s) (e.g. %q); the oracle catches it", lostCount, lostKey)
}
