package storageunit

import (
	"errors"
	"strings"
	"testing"
)

func TestHandoffPhase_String(t *testing.T) {
	cases := map[HandoffPhase]string{
		PhaseAcquiring:   "Acquiring",
		PhaseReady:       "Ready",
		PhaseDraining:    "Draining",
		PhaseReleasing:   "Releasing",
		HandoffPhase(0):  "HandoffPhase(0)",
		HandoffPhase(99): "HandoffPhase(99)",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("HandoffPhase(%d).String() = %q, want %q", uint8(p), got, want)
		}
	}
}

func TestHandoffPhase_sides(t *testing.T) {
	cases := []struct {
		p      HandoffPhase
		gainer bool
		loser  bool
		valid  bool
	}{
		{PhaseAcquiring, true, false, true},
		{PhaseReady, true, false, true},
		{PhaseDraining, false, true, true},
		{PhaseReleasing, false, true, true},
		{HandoffPhase(0), false, false, false},
		{HandoffPhase(42), false, false, false},
	}
	for _, c := range cases {
		if got := c.p.IsGainer(); got != c.gainer {
			t.Errorf("%s.IsGainer() = %v, want %v", c.p, got, c.gainer)
		}
		if got := c.p.IsLoser(); got != c.loser {
			t.Errorf("%s.IsLoser() = %v, want %v", c.p, got, c.loser)
		}
		if got := c.p.phaseValid(); got != c.valid {
			t.Errorf("%s.phaseValid() = %v, want %v", c.p, got, c.valid)
		}
	}
}

func TestHandoffState_HasPredecessor(t *testing.T) {
	if (HandoffState{Predecessor: "node-a"}).HasPredecessor() != true {
		t.Errorf("non-empty Predecessor should report HasPredecessor() true")
	}
	if (HandoffState{Predecessor: ""}).HasPredecessor() != false {
		t.Errorf("empty Predecessor should report HasPredecessor() false")
	}
}

// TestHandoffState_PredecessorAddr pins that the Acquiring entry carries the
// predecessor's dial address alongside its NodeID (the graceful-leave fix: the
// address is stored so the forward survives the predecessor leaving the ring).
func TestHandoffState_PredecessorAddr(t *testing.T) {
	s := HandoffState{Phase: PhaseAcquiring, Predecessor: "old", PredecessorAddr: "10.0.0.9:7947"}
	if !s.HasPredecessor() {
		t.Fatalf("entry with a predecessor must report HasPredecessor()")
	}
	if s.PredecessorAddr != "10.0.0.9:7947" {
		t.Fatalf("PredecessorAddr = %q, want the stored dial address", s.PredecessorAddr)
	}
	// Once Ready the node serves locally and no longer forwards, so the carried
	// predecessor address is dropped along with the NodeID.
	ready, err := NextOnReady(s, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ready.PredecessorAddr != "" {
		t.Fatalf("Ready state must not retain a predecessor address, got %q", ready.PredecessorAddr)
	}
}

// TestNextOnReady covers the single legal gainer edge Acquiring->Ready and
// rejects every other predecessor phase.
func TestNextOnReady(t *testing.T) {
	t.Run("legal Acquiring->Ready", func(t *testing.T) {
		from := HandoffState{Phase: PhaseAcquiring, Predecessor: "old"}
		got, err := NextOnReady(from, 7)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Phase != PhaseReady {
			t.Errorf("phase = %s, want Ready", got.Phase)
		}
		if got.OpenEpoch != 7 {
			t.Errorf("OpenEpoch = %d, want 7", got.OpenEpoch)
		}
		// Predecessor dropped once Ready (no longer forwarding).
		if got.HasPredecessor() {
			t.Errorf("Ready state should not retain a predecessor, got %q", got.Predecessor)
		}
	})

	illegalFrom := []HandoffPhase{PhaseReady, PhaseDraining, PhaseReleasing, HandoffPhase(0)}
	for _, p := range illegalFrom {
		t.Run("illegal from "+p.String(), func(t *testing.T) {
			_, err := NextOnReady(HandoffState{Phase: p}, 3)
			assertIllegalEdge(t, err)
		})
	}
}

// TestNextOnDrain covers the single legal loser edge Owned->Draining (Owned is
// the absent/zero phase) and rejects driving a drain from any in-flight phase.
func TestNextOnDrain(t *testing.T) {
	t.Run("legal Owned->Draining", func(t *testing.T) {
		got, err := NextOnDrain(HandoffState{}) // zero == Owned/absent
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Phase != PhaseDraining {
			t.Errorf("phase = %s, want Draining", got.Phase)
		}
	})

	illegalFrom := []HandoffPhase{PhaseAcquiring, PhaseReady, PhaseDraining, PhaseReleasing}
	for _, p := range illegalFrom {
		t.Run("illegal from "+p.String(), func(t *testing.T) {
			_, err := NextOnDrain(HandoffState{Phase: p})
			assertIllegalEdge(t, err)
		})
	}
}

// TestNextOnRelease covers the single legal loser edge Draining->Releasing and
// rejects every other predecessor phase (incl. a second Releasing edge).
func TestNextOnRelease(t *testing.T) {
	t.Run("legal Draining->Releasing", func(t *testing.T) {
		from := HandoffState{Phase: PhaseDraining, OpenEpoch: 4}
		got, err := NextOnRelease(from)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Phase != PhaseReleasing {
			t.Errorf("phase = %s, want Releasing", got.Phase)
		}
		if got.OpenEpoch != 4 {
			t.Errorf("OpenEpoch = %d, want 4 (carried through)", got.OpenEpoch)
		}
	})

	illegalFrom := []HandoffPhase{PhaseAcquiring, PhaseReady, PhaseReleasing, HandoffPhase(0)}
	for _, p := range illegalFrom {
		t.Run("illegal from "+p.String(), func(t *testing.T) {
			_, err := NextOnRelease(HandoffState{Phase: p})
			assertIllegalEdge(t, err)
		})
	}
}

// TestCanRelease pins the durable-epoch boundary: release-hint trips only when
// durable is STRICTLY above the loser's own open epoch.
func TestCanRelease(t *testing.T) {
	cases := []struct {
		name          string
		open, durable Epoch
		want          bool
	}{
		{"durable below open", 5, 4, false},
		{"durable equals open (boundary, not releasable)", 5, 5, false},
		{"durable one above open (boundary, releasable)", 5, 6, true},
		{"durable far above open", 5, 99, true},
		{"both zero", 0, 0, false},
		{"open zero, durable one", 0, 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CanRelease(c.open, c.durable); got != c.want {
				t.Errorf("CanRelease(open=%d, durable=%d) = %v, want %v", c.open, c.durable, got, c.want)
			}
		})
	}
}

// TestReleasable pins the higher-level release predicate: Draining AND a
// positive readiness. A bare epoch advance (ready==false) is never releasable.
func TestReleasable(t *testing.T) {
	cases := []struct {
		name  string
		state HandoffState
		ready bool
		want  bool
	}{
		{"draining + ready", HandoffState{Phase: PhaseDraining}, true, true},
		{"draining + not ready (bare epoch hint, crash case 1)", HandoffState{Phase: PhaseDraining}, false, false},
		{"acquiring + ready (wrong side)", HandoffState{Phase: PhaseAcquiring}, true, false},
		{"ready phase + ready (gainer never releases)", HandoffState{Phase: PhaseReady}, true, false},
		{"releasing + ready (already releasing)", HandoffState{Phase: PhaseReleasing}, true, false},
		{"owned/absent + ready", HandoffState{}, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Releasable(c.state, c.ready); got != c.want {
				t.Errorf("Releasable(%s, ready=%v) = %v, want %v", c.state.Phase, c.ready, got, c.want)
			}
		})
	}
}

// TestHandoffSequences walks the two full legal side-sequences end to end:
// gainer Owned/Absent -> Acquiring -> Ready, loser Owned -> Draining ->
// Releasing. (The Acquiring entry itself is set by the controller's reconcile,
// not a transition function; the FSM exposes only the edges out of it.)
func TestHandoffSequences(t *testing.T) {
	t.Run("loser Owned->Draining->Releasing", func(t *testing.T) {
		s, err := NextOnDrain(HandoffState{})
		if err != nil || s.Phase != PhaseDraining {
			t.Fatalf("drain: phase=%s err=%v", s.Phase, err)
		}
		s.OpenEpoch = 10 // loser opened this position at 10
		s, err = NextOnRelease(s)
		if err != nil || s.Phase != PhaseReleasing {
			t.Fatalf("release: phase=%s err=%v", s.Phase, err)
		}
		if s.OpenEpoch != 10 {
			t.Errorf("OpenEpoch lost across drain->release: got %d", s.OpenEpoch)
		}
	})

	t.Run("gainer Acquiring->Ready", func(t *testing.T) {
		// Controller would set this entry on reconcile with the predecessor.
		s := HandoffState{Phase: PhaseAcquiring, Predecessor: "old-owner"}
		s, err := NextOnReady(s, 11)
		if err != nil || s.Phase != PhaseReady {
			t.Fatalf("ready: phase=%s err=%v", s.Phase, err)
		}
		if s.OpenEpoch != 11 {
			t.Errorf("OpenEpoch = %d, want 11", s.OpenEpoch)
		}
	})
}

func assertIllegalEdge(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an illegal-edge error, got nil")
	}
	if !strings.Contains(err.Error(), "illegal handoff edge") {
		t.Fatalf("error %v does not look like an illegal-edge error", err)
	}
	// Sanity: the constructor returns a non-wrapped fmt error; ensure it is not
	// accidentally a sentinel that callers might Is-match unexpectedly.
	if errors.Unwrap(err) != nil {
		t.Errorf("illegal-edge error unexpectedly wraps another error: %v", err)
	}
}
