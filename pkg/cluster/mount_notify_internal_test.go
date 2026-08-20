package cluster

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
)

// The hook fires for every position that becomes serving, carrying the same
// opaque token MountedUnits reports - nothing about shale's mount state
// crosses the boundary.
func TestOnUnitMounted_FiresPerServingPosition(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2")

	var mu sync.Mutex
	var got []string
	done := make(chan struct{}, 64)
	c.cfg.OnUnitMounted = func(unit string) {
		mu.Lock()
		got = append(got, unit)
		mu.Unlock()
		done <- struct{}{}
	}
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	want := len(c.mounts.mountedList())
	if want == 0 {
		t.Fatal("fixture mounted nothing")
	}
	for range want {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d callbacks fired", len(got), want)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != want {
		t.Fatalf("callback fired %d times, want %d (one per serving position)", len(got), want)
	}
	// Tokens must be the public shape, and must name units this node holds.
	mounted := map[string]bool{}
	for _, u := range c.MountedUnits() {
		mounted[u] = true
	}
	for _, u := range got {
		if !mounted[u] {
			t.Fatalf("callback got token %q, which MountedUnits does not report", u)
		}
	}
}

// NO GOROUTINE LEAK: Close must drain in-flight callbacks. A hook that
// outlived Close would keep touching a closed cluster, and the whole package
// runs under goleak (main_test.go), so a leak fails there too - this pins the
// ORDERING that makes that true rather than relying on the callback being fast.
func TestOnUnitMounted_CloseWaitsForInFlightCallback(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2")

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var finished atomic.Bool
	var once sync.Once
	c.cfg.OnUnitMounted = func(string) {
		once.Do(func() { entered <- struct{}{} })
		<-release
		finished.Store(true)
	}
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("callback never ran")
	}

	closeReturned := make(chan struct{})
	go func() {
		c.closed.Store(true)
		c.closeOnce.Do(func() { close(c.closeCh) })
		c.loopWG.Wait() // what Close does before declaring the cluster stopped
		close(closeReturned)
	}()

	select {
	case <-closeReturned:
		t.Fatal("shutdown completed while a callback was still running; the goroutine is not tracked")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	select {
	case <-closeReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not complete after the callback returned")
	}
	if !finished.Load() {
		t.Fatal("callback did not finish")
	}
}

// A panicking callback must NOT fell the process: shale spawned the goroutine,
// so it contains the panic, logs it loudly with the unit token, and abandons
// that pass. The cluster keeps serving.
func TestOnUnitMounted_PanicIsRecoveredAndLogged(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2")

	var logMu sync.Mutex
	var log strings.Builder
	c.cfg.LogOutput = &syncWriter{mu: &logMu, w: &log}

	fired := make(chan struct{}, 64)
	c.cfg.OnUnitMounted = func(string) {
		defer func() { fired <- struct{}{} }()
		panic("consumer bug")
	}
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("callback never ran")
	}
	c.loopWG.Wait() // the recover must let the goroutine finish normally

	logMu.Lock()
	defer logMu.Unlock()
	if !strings.Contains(log.String(), "PANICKED") {
		t.Fatalf("panic was not logged loudly; log: %q", log.String())
	}
	if !strings.Contains(log.String(), "consumer bug") {
		t.Fatalf("log does not carry the panic value; log: %q", log.String())
	}
}

type syncWriter struct {
	mu *sync.Mutex
	w  *strings.Builder
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}
