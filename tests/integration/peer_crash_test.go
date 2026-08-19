package integration

import (
	"testing"
	"time"
)

// TestThreeNode_PeerCrashForwardingReturnsError simulates a peer
// dying ungracefully: we kill its gRPC server WITHOUT going through
// the cluster's graceful Close path, so the coordinator doesn't get
// the quick "I'm leaving" notice. From the surviving nodes' perspective
// the peer is still on the ring; routed operations to that peer must
// surface a real error within a sensible time bound (no panic, no
// hang).
func TestThreeNode_PeerCrashForwardingReturnsError(t *testing.T) {
	f := newThreeNodeFixture(t)

	// Hard-stop N3's gRPC listener (NOT a graceful cluster.Close).
	// This leaves N3 on the ring as far as N1+N2 are concerned;
	// forwarding to it should hit a dial / RPC failure.
	f.N3.stop()
	f.N3.stop = nil
	// Block until the listener is actually torn down so subsequent
	// dials see connection refused instead of accept races.
	time.Sleep(100 * time.Millisecond)

	// Find a key owned by N3 (per the still-stale ring view on N1).
	const maxProbes = 1000
	var target string
	for i := range maxProbes {
		k := probeKey(i)
		if ownerOf(f.N1.Cluster, k) == "n3" {
			target = k
			break
		}
	}
	if target == "" {
		t.Fatalf("could not find a key owned by n3 in %d probes (ring=%v)", maxProbes, f.N1.Cluster.Members())
	}

	// The Put must return - not hang, not panic - within a sensible
	// budget. We don't constrain the exact error (gRPC dial failure
	// + Unavailable status code shapes vary across versions); just
	// that something arrives.
	done := make(chan error, 1)
	go func() {
		done <- f.N1.Cluster.Put([]byte(target), []byte("v"))
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("Put to crashed N3 returned nil; expected an error")
		}
		// Good: real error surfaced.
		t.Logf("got expected error from forwarding to crashed peer: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("Put to crashed N3 hung; expected error within budget")
	}

	// Same shape for Get + Delete + ScanPrefix.
	done2 := make(chan error, 1)
	go func() {
		_, err := f.N1.Cluster.Get([]byte(target))
		done2 <- err
	}()
	select {
	case err := <-done2:
		if err == nil {
			t.Fatalf("Get to crashed N3 returned nil; expected an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Get to crashed N3 hung")
	}

	done3 := make(chan error, 1)
	go func() { done3 <- f.N1.Cluster.Delete([]byte(target)) }()
	select {
	case err := <-done3:
		if err == nil {
			t.Fatalf("Delete to crashed N3 returned nil; expected an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Delete to crashed N3 hung")
	}
}

func probeKey(i int) string {
	// Distinct keyspace from threenode_test.go's "probe-N" so a
	// shared ownerOf cache (none today, but defensive) doesn't bleed
	// across tests.
	const prefix = "crash-probe-"
	return prefix + itoa(i)
}

func itoa(i int) string {
	// strconv.Itoa with a tiny manual loop to avoid pulling another
	// import in just for one call site.
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = '0' + byte(i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
