package cluster

// White-box unit tests for the fanout primitive. The end-to-end
// replication behavior is covered by replicate_test.go (public-API
// black-box); these pin the fan-out accounting rules directly so a
// regression in the helper doesn't have to be diagnosed through the
// integration layer.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/ring"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFanout_AllSucceed(t *testing.T) {
	reps := []ring.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	acks, errs, ch := fanout(context.Background(), reps, 2,
		func(ctx context.Context, _ ring.Member) ([]byte, error) {
			return []byte("ok"), nil
		})
	if acks < 2 {
		t.Fatalf("acks: got %d want >=2", acks)
	}
	if len(errs) != 0 {
		t.Fatalf("errs: got %v want []", errs)
	}
	drain(ch)
}

// TestFanout_FailureBudgetExhausted: with R=3 + W=2, two failures
// exhaust the budget + fanout returns with whatever acks are in hand.
func TestFanout_FailureBudgetExhausted(t *testing.T) {
	reps := []ring.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	var nonAck int32
	acks, errs, ch := fanout(context.Background(), reps, 2,
		func(ctx context.Context, _ ring.Member) ([]byte, error) {
			if atomic.AddInt32(&nonAck, 1) <= 2 {
				return nil, errors.New("boom")
			}
			return nil, nil
		})
	if acks >= 2 {
		t.Fatalf("acks: got %d want <2 (budget exhausted)", acks)
	}
	if len(errs) < 2 {
		t.Fatalf("errs: got %v want >=2", errs)
	}
	drain(ch)
}

// TestFanout_TransientDoesNotCount: a transient (codes.Unavailable)
// reply must not count as an ack OR as a failure. With R=3 + W=2,
// (1 success, 1 transient, 1 success) must yield 2 acks success even
// though the failure budget would mark "1 down" as a real failure.
func TestFanout_TransientDoesNotCount(t *testing.T) {
	reps := []ring.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	var seq int32
	acks, errs, ch := fanout(context.Background(), reps, 2,
		func(ctx context.Context, _ ring.Member) ([]byte, error) {
			n := atomic.AddInt32(&seq, 1)
			if n == 1 {
				return nil, status.Error(codes.Unavailable, "transient")
			}
			return nil, nil
		})
	if acks < 2 {
		t.Fatalf("acks: got %d want 2", acks)
	}
	if len(errs) != 0 {
		t.Fatalf("transient should not be in errs: %v", errs)
	}
	drain(ch)
}

// TestFanout_SuccessAcksClampToRequired: more acks than required
// land; the call returns at the moment the requiredth ack arrives.
// Surplus replicas keep running + flow onto the result channel.
func TestFanout_SuccessAcksClampToRequired(t *testing.T) {
	reps := []ring.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	// Slow one replica so the other two ack first.
	acks, _, ch := fanout(context.Background(), reps, 2,
		func(ctx context.Context, m ring.Member) ([]byte, error) {
			if m.ID == "c" {
				time.Sleep(50 * time.Millisecond)
			}
			return nil, nil
		})
	if acks < 2 {
		t.Fatalf("acks: got %d want >=2", acks)
	}
	// Drain the channel; the surplus replica must eventually finish.
	totalSeen := 0
	for range ch {
		totalSeen++
	}
	if totalSeen != 3 {
		t.Fatalf("result channel surplus drain: got %d want 3", totalSeen)
	}
}

// TestFanout_EmptyReplicas returns zero acks + an immediately-closed
// channel without panicking.
func TestFanout_EmptyReplicas(t *testing.T) {
	acks, errs, ch := fanout(context.Background(), nil, 1,
		func(ctx context.Context, _ ring.Member) ([]byte, error) {
			return nil, nil
		})
	if acks != 0 || len(errs) != 0 {
		t.Fatalf("empty: got acks=%d errs=%v", acks, errs)
	}
	drain(ch)
}

// TestRequiredWriteAcks_Quorum: floor(R/2) + 1 across R=1..5.
func TestRequiredWriteAcks_Quorum(t *testing.T) {
	cases := []struct {
		r, want int
	}{{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}}
	for _, c := range cases {
		if got := requiredWriteAcks(WriteQuorum, c.r); got != c.want {
			t.Errorf("WriteQuorum R=%d: got %d want %d", c.r, got, c.want)
		}
	}
}

func TestRequiredWriteAcks_One(t *testing.T) {
	if got := requiredWriteAcks(WriteOne, 3); got != 1 {
		t.Errorf("WriteOne R=3: got %d want 1", got)
	}
}

func TestRequiredWriteAcks_All(t *testing.T) {
	if got := requiredWriteAcks(WriteAll, 3); got != 3 {
		t.Errorf("WriteAll R=3: got %d want 3", got)
	}
}

func drain(ch <-chan replicaResult) {
	for range ch {
	}
}
