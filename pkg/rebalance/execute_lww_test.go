package rebalance_test

// Deterministic regression for the P1 fix at the pkg/rebalance seam:
// inProcessDestination.FetchRange, when built with
// NewInProcessDestinationLWW, routes each migrated (key, value) through
// the injected apply-if-newer applier instead of a raw local.Put. An
// older streamed value loses the per-key compare and is a committed
// no-op, never a clobber, and is NOT recorded in the rollback set (so a
// later stream failure cannot delete a key the apply did not actually
// write - which would itself clobber the newer concurrent value).
//
// The cluster layer injects its real applyEnvelopeIfNewer here in
// production; this test injects a tiny stamp-comparing stand-in (the
// rebalance package must not import pkg/cluster) so it can prove the
// routing + rollback-set behavior without the full envelope codec.

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/rebalance"
)

// stampedSource is a MigrateSource that streams a fixed list of pairs.
// errAfter, when non-nil, is sent on the error channel AFTER the pairs
// drain, modeling a mid-stream failure that triggers rollback.
type stampedSource struct {
	pairs    []rebalance.KeyValue
	errAfter error
}

func (s *stampedSource) OpenRange(_ []uint64, _ uint64) (<-chan rebalance.KeyValue, <-chan error) {
	out := make(chan rebalance.KeyValue)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		for _, kv := range s.pairs {
			out <- kv
		}
		errCh <- s.errAfter
	}()
	return out, errCh
}

// stampValue tags a payload with an 8-byte big-endian timestamp prefix
// so the test applier can compare "newer vs older" without the cluster
// envelope codec. Layout: ts(8) | payload.
func stampValue(ts uint64, payload string) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(b[:8], ts)
	copy(b[8:], payload)
	return b
}

func stampOf(v []byte) uint64 {
	if len(v) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v[:8])
}

// TestInProcessLWW_DoesNotClobberNewer drives the execute.go path: the
// destination's local backend already holds a NEWER value for K, the
// migration stream carries an OLDER value for K, and the injected
// apply-if-newer must keep the newer value and report wrote=false.
func TestInProcessLWW_DoesNotClobberNewer(t *testing.T) {
	dst := memory.New()
	defer func() { _ = dst.Close() }()

	key := []byte("K")
	newer := stampValue(2_000_000, "newer")
	older := stampValue(1_000_000, "older")

	// The live concurrent write already landed the newer value.
	if err := dst.Put(key, newer); err != nil {
		t.Fatalf("seed newer: %v", err)
	}

	// apply-if-newer: write only when the incoming stamp strictly beats
	// the stored one; report whether it actually wrote.
	applyIfNewer := func(k, v []byte) (bool, error) {
		stored, err := dst.Get(k)
		if err != nil { // NotFound -> treat as no stored value, apply.
			if perr := dst.Put(k, v); perr != nil {
				return false, perr
			}
			return true, nil
		}
		if stampOf(v) > stampOf(stored) {
			if perr := dst.Put(k, v); perr != nil {
				return false, perr
			}
			return true, nil
		}
		return false, nil
	}

	src := &stampedSource{pairs: []rebalance.KeyValue{{Key: key, Value: older}}}
	dest := rebalance.NewInProcessDestinationLWW(dst, func(_ rebalance.Member) (rebalance.MigrateSource, error) {
		return src, nil
	}, applyIfNewer)

	// totalKeys counts the SOURCE-streamed pairs (the wire count), not
	// the locally-applied ones: a no-op apply still counts toward the
	// terminal total. So we expect 1 here even though nothing was
	// written.
	n, err := dest.FetchRange(context.Background(), rebalance.Member{ID: "src"}, []uint64{0}, 1)
	if err != nil {
		t.Fatalf("FetchRange: %v", err)
	}
	if n != 1 {
		t.Fatalf("FetchRange returned %d, want 1 (the wire count includes the no-op apply)", n)
	}

	got, err := dst.Get(key)
	if err != nil {
		t.Fatalf("Get K: %v", err)
	}
	if stampOf(got) != stampOf(newer) {
		t.Fatalf("K clobbered by older streamed value (stamp %d, want %d): execute.go raw-Put LWW violation",
			stampOf(got), stampOf(newer))
	}
}

// TestInProcessLWW_RollbackDoesNotDeleteNewer is the rollback-honesty
// half: a stream that lands an OLDER (no-op) value for an
// already-newer key K and then FAILS its terminal check must NOT delete
// K on rollback (the apply did not write it, so the newer value must
// survive the rollback). This pins the "record only keys actually
// written" rule the report-returning applier enables.
func TestInProcessLWW_RollbackDoesNotDeleteNewer(t *testing.T) {
	dst := memory.New()
	defer func() { _ = dst.Close() }()

	key := []byte("K")
	newer := stampValue(2_000_000, "newer")
	older := stampValue(1_000_000, "older")
	if err := dst.Put(key, newer); err != nil {
		t.Fatalf("seed newer: %v", err)
	}

	applyIfNewer := func(k, v []byte) (bool, error) {
		stored, err := dst.Get(k)
		if err != nil {
			if perr := dst.Put(k, v); perr != nil {
				return false, perr
			}
			return true, nil
		}
		if stampOf(v) > stampOf(stored) {
			if perr := dst.Put(k, v); perr != nil {
				return false, perr
			}
			return true, nil
		}
		return false, nil
	}

	// Source streams the no-op older value, then fails. FetchRange must
	// roll back; the rollback set should be EMPTY (the apply wrote
	// nothing), so K keeps its newer value.
	src := &stampedSource{
		pairs:    []rebalance.KeyValue{{Key: key, Value: older}},
		errAfter: errors.New("simulated mid-stream failure"),
	}
	dest := rebalance.NewInProcessDestinationLWW(dst, func(_ rebalance.Member) (rebalance.MigrateSource, error) {
		return src, nil
	}, applyIfNewer)

	_, err := dest.FetchRange(context.Background(), rebalance.Member{ID: "src"}, []uint64{0}, 1)
	if err == nil {
		t.Fatalf("FetchRange should have surfaced the source error")
	}

	got, gerr := dst.Get(key)
	if gerr != nil {
		t.Fatalf("K was deleted by rollback even though the apply never wrote it: %v", gerr)
	}
	if stampOf(got) != stampOf(newer) {
		t.Fatalf("K stamp %d after rollback, want %d (rollback must not clobber the newer concurrent value)",
			stampOf(got), stampOf(newer))
	}
}
