package integration

// CAS JOIN WRITE-TRANSPARENCY GATE (workstream B). Plain Put/Get/Delete became
// join-transparent via the Joining-bit union; the CAS surface (Transact / Begin,
// the If-None-Match shape hostthis is built on) did NOT: the write-set fan-out
// used the raw full-ring replica set with W over its size, and the owner
// dispatch + server gate resolved the raw full-ring head - the warming newcomer -
// whose owner-local begin refuses mid-acquire. So during a join every CAS commit
// whose pin unit was moving wedged for the whole mount window.
//
// PRE-FIX measurement (this exact test, before the CAS union fix): the engage
// gate never succeeded - 0 CAS commits acked on the probe moving unit across a
// 20s window while the newcomer warmed (every commit refused Unavailable
// mid-acquire; stable-unit commits acked fine). That is the CAS mirror of the
// 24/24 plain-put wedge the join fix closed.
//
// POST-FIX this test asserts CAS commits on MOVING units stay AVAILABLE during
// the newcomer's slow mount (gated on the mount genuinely in progress via
// desiredButUnmounted > 0), stable units stay available, and every acked CAS
// value is durably readable afterward.

import (
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// casKeysForUnit returns n distinct fresh keys that route to unit u (never
// touched before; suitable for If-None-Match probes).
func casKeysForUnit(u storageunit.UnitID, unitCount, n int, salt string) []string {
	out := make([]string, 0, n)
	for i := 0; len(out) < n; i++ {
		k := fmt.Sprintf("casj-%s-%07d", salt, i)
		if unitOf(k, unitCount) == u {
			out = append(out, k)
		}
	}
	return out
}

// casProbe runs ONE If-None-Match CAS commit through entry: Begin, Get (records
// the absent read-check on a fresh key), buffered Put, Commit. Returns the
// commit error (nil = acked).
func casProbe(entry *cluster.Cluster, key, val string) error {
	tx, err := entry.Begin(backend.SnapshotIsolation)
	if err != nil {
		return err
	}
	if _, gerr := tx.Get([]byte(key)); gerr != nil && !errors.Is(gerr, backend.ErrNotFound) {
		_ = tx.Rollback()
		return gerr
	}
	if perr := tx.Put([]byte(key), []byte(val)); perr != nil {
		_ = tx.Rollback()
		return perr
	}
	return tx.Commit()
}

// casWedged reports whether err is the join-wedge family for a CAS commit: the
// retryable Unavailable (owner-local mid-acquire refusal, or the fan-out ack
// shortfall) or the re-pin FailedPrecondition.
func casWedged(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.FailedPrecondition, codes.ResourceExhausted:
		return true
	}
	return false
}

// TestCASJoinTransparency_MovingUnitsStayAvailable is the workstream-B gate:
// CAS commits against MOVING units stay available through the newcomer's slow
// mount, exactly as plain puts do.
func TestCASJoinTransparency_MovingUnitsStayAvailable(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence-at-completion timing): this gate pins the
	// CAS union mechanism in ISOLATION - the displaced owner must stay
	// unfenced through cj-d's modeled mount so it can serve the union,
	// exactly as the plain-put mirror TestJoinIsWriteTransparent_
	// MovingShardsStayAvailable opts out. Under the backing's default eager
	// fence (real slatedb timing) the displaced owner is fenced at
	// open-START and these same moving-unit commits WEDGE, the KNOWN
	// residual TestJoinResidual_FenceAtOpenStart_MovingShardsWedge
	// reproduces deliberately under the default.
	backing.SetEagerFence(false)
	mutate := func(cfg *cluster.Config) { cfg.WriteTimeout = 300 * time.Millisecond }

	n1 := startReplicatedNodeCfg(t, "cj-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "cj-b", n1.ClusterToken, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "cj-c", n1.ClusterToken, uc, rf, backing, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)
	plantAllServingMarkers(t, n1.Handle, uc, rf)

	old3 := []string{"cj-a", "cj-b", "cj-c"}
	new4 := []string{"cj-a", "cj-b", "cj-c", "cj-d"}
	var moving, stable []storageunit.UnitID
	for u := range uk {
		if contains(replicaIDsForMembers(new4, u, rf), "cj-d") && !contains(replicaIDsForMembers(old3, u, rf), "cj-d") {
			moving = append(moving, u)
		} else {
			stable = append(stable, u)
		}
	}
	sort.Slice(moving, func(i, j int) bool { return moving[i] < moving[j] })
	sort.Slice(stable, func(i, j int) bool { return stable[i] < stable[j] })
	if len(moving) == 0 {
		t.Skip("no unit moves onto the 4th node under this hashing")
	}
	t.Logf("of %d units: %d MOVE onto cj-d, %d stay put", uc, len(moving), len(stable))

	// Join cj-d: markers planted -> boot-defers -> Joining bit -> slow overlap
	// acquire (delay armed before Open), so the moving units sit owned-but-
	// unmounted on cj-d for the window.
	const mountDelay = 25 * time.Second
	n4 := startReplicatedNodeSlowAcquire(t, "cj-d", n1.ClusterToken, uc, rf, backing, mountDelay, 300*time.Millisecond)
	cs4 := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(cs4, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}

	// ENGAGE GATE: poll a CAS commit on the first moving unit until one ACKS while
	// cj-d is provably still mid-mount. Pre-fix this NEVER succeeds (the wedge).
	probeUnit := moving[0]
	engaged := false
	var lastErr error
	engageKeys := casKeysForUnit(probeUnit, uc, 200, "engage")
	engageDeadline := time.Now().Add(20 * time.Second)
	for i := 0; time.Now().Before(engageDeadline) && i < len(engageKeys); i++ {
		if desiredButUnmounted(n4.Cluster) == 0 {
			break // cj-d finished mounting before we observed the window
		}
		err := casProbe(n1.Cluster, engageKeys[i], "engage")
		if err == nil {
			engaged = true
			break
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	unmountedAtEngage := desiredButUnmounted(n4.Cluster)
	if !engaged {
		t.Fatalf("CAS WEDGE (pre-fix behavior): no CAS commit on moving unit %d acked while cj-d was mid-mount "+
			"(lastErr=%v, cj-d unmounted=%d) - the CAS surface is not join-transparent", probeUnit, lastErr, unmountedAtEngage)
	}
	if unmountedAtEngage == 0 {
		t.Fatalf("cj-d finished mounting before the transition could be observed - raise mountDelay")
	}
	t.Logf("CAS transparency ENGAGED while cj-d still has %d owned-but-unmounted positions", unmountedAtEngage)

	// MEASURE while cj-d is still mid-mount: If-None-Match commits on every moving
	// + stable unit, fresh keys each round. Record acked values for durability.
	ackedVals := map[string]string{}
	var movOK, movWedge, movOther, stOK, stWedge, stOther int
	rounds := 0
	for r := 0; r < 4 && desiredButUnmounted(n4.Cluster) > 0; r++ {
		for _, u := range moving {
			k := casKeysForUnit(u, uc, 1, fmt.Sprintf("m%d-%d", u, r))[0]
			err := casProbe(n1.Cluster, k, "mv")
			switch {
			case err == nil:
				movOK++
				ackedVals[k] = "mv"
			case casWedged(err):
				movWedge++
			default:
				movOther++
				t.Logf("moving unit %d unexpected CAS error: %v", u, err)
			}
		}
		for _, u := range stable {
			k := casKeysForUnit(u, uc, 1, fmt.Sprintf("s%d-%d", u, r))[0]
			err := casProbe(n1.Cluster, k, "st")
			switch {
			case err == nil:
				stOK++
				ackedVals[k] = "st"
			case casWedged(err):
				stWedge++
			default:
				stOther++
				t.Logf("stable unit %d unexpected CAS error: %v", u, err)
			}
		}
		rounds++
		time.Sleep(400 * time.Millisecond)
	}
	movTotal := movOK + movWedge + movOther
	stTotal := stOK + stWedge + stOther
	t.Logf("CAS during cj-d's mount: MOVING ok=%d wedged=%d other=%d (total=%d); STABLE ok=%d wedged=%d other=%d",
		movOK, movWedge, movOther, movTotal, stOK, stWedge, stOther)

	for _, n := range []*sharedNode{n1, n2, n3, n4} {
		n.Handle.SetAcquireDelay(0)
	}

	if rounds == 0 || movTotal == 0 {
		t.Fatalf("no during-mount CAS probes ran (cj-d mounted too fast; raise mountDelay)")
	}
	if movOther > 0 || stOther > 0 {
		t.Fatalf("unexpected non-wedge CAS errors during the join (moving=%d stable=%d)", movOther, stOther)
	}
	if rate := float64(movOK) / float64(movTotal); rate < 0.9 {
		t.Fatalf("CAS NOT join-transparent: only %.0f%% of moving-unit CAS commits acked during cj-d's mount "+
			"(ok=%d wedged=%d; want >=90%%)", rate*100, movOK, movWedge)
	}
	if stWedge > 0 {
		t.Fatalf("unexpected: %d STABLE-unit CAS commits wedged during the join (only moving units transition)", stWedge)
	}

	// DURABILITY: every acked CAS value must be readable with its exact value.
	for k, v := range ackedVals {
		got, err := getWithRetryUnavailable(t, n2.Cluster, k, 15*time.Second)
		if err != nil || string(got) != v {
			t.Fatalf("acked CAS value lost: key %q got %q err %v want %q", k, got, err, v)
		}
	}
	t.Logf("CONFIRMED: CAS is join-transparent - %d/%d moving-unit commits acked during the mount "+
		"(stable %d/%d), and all %d acked values durably readable", movOK, movTotal, stOK, stTotal, len(ackedVals))
}
