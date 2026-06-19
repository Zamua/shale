//go:build slatedb

// Unit tests for the v0.8 Phase 2g transient-open retry (buildDbRetry): a
// RETURNED "empty SSTable" (store read-truncation under a concurrent-open burst)
// is retried; a timeout is NOT (the stalled open is still live); a non-transient
// error is not retried; exhausting the budget surfaces the underlying error so
// degraded boot is unchanged. White-box: drives the unexported retry directly
// with an injected open-attempt func, no real slatedb open.

package slate

import (
	"errors"
	"testing"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func TestIsTransientOpenError(t *testing.T) {
	if !isTransientOpenError(errors.New("Data error: empty SSTable")) {
		t.Fatal("empty SSTable must be transient (retryable)")
	}
	if isTransientOpenError(errors.New("some other open error")) {
		t.Fatal("a non-empty-SSTable error must NOT be transient")
	}
	if isTransientOpenError(nil) {
		t.Fatal("nil must not be transient")
	}
}

func TestBuildDbRetry(t *testing.T) {
	// Zero the backoff for test speed; restore after.
	orig := openRetryBackoff
	openRetryBackoff = func(int) time.Duration { return 0 }
	defer func() { openRetryBackoff = orig }()
	t.Setenv("SLATE_DB_OPEN_RETRIES", "3") // 1 initial + 3 retries = up to 4 attempts

	empty := errors.New("Data error: empty SSTable")

	t.Run("retries_transient_then_succeeds", func(t *testing.T) {
		calls := 0
		attempt := func(string, *slatedb.ObjectStore, *slatedb.Settings, *slatedb.DbCache) (*slatedb.Db, error, bool) {
			calls++
			if calls <= 2 {
				return nil, empty, false
			}
			return nil, nil, false // succeed on the 3rd attempt
		}
		if _, err := buildDbRetry(attempt, "u", nil, nil, nil); err != nil {
			t.Fatalf("want success after transient retries, got %v", err)
		}
		if calls != 3 {
			t.Fatalf("want 3 attempts (2 transient + success), got %d", calls)
		}
	})

	t.Run("non_transient_no_retry", func(t *testing.T) {
		calls := 0
		attempt := func(string, *slatedb.ObjectStore, *slatedb.Settings, *slatedb.DbCache) (*slatedb.Db, error, bool) {
			calls++
			return nil, errors.New("boom (not transient)"), false
		}
		_, err := buildDbRetry(attempt, "u", nil, nil, nil)
		if err == nil || calls != 1 {
			t.Fatalf("a non-transient error must return immediately (1 attempt); got err=%v calls=%d", err, calls)
		}
	})

	t.Run("timeout_no_retry", func(t *testing.T) {
		calls := 0
		attempt := func(string, *slatedb.ObjectStore, *slatedb.Settings, *slatedb.DbCache) (*slatedb.Db, error, bool) {
			calls++
			return nil, nil, true // timedOut: stalled open still live, MUST NOT retry
		}
		_, err := buildDbRetry(attempt, "u", nil, nil, nil)
		if err == nil || calls != 1 {
			t.Fatalf("a timeout must return immediately with NO retry (1 attempt); got err=%v calls=%d", err, calls)
		}
	})

	t.Run("exhausts_returns_underlying_error", func(t *testing.T) {
		calls := 0
		attempt := func(string, *slatedb.ObjectStore, *slatedb.Settings, *slatedb.DbCache) (*slatedb.Db, error, bool) {
			calls++
			return nil, empty, false // always transient -> exhaust the budget
		}
		_, err := buildDbRetry(attempt, "u", nil, nil, nil)
		if err == nil || !isTransientOpenError(err) {
			t.Fatalf("exhaustion must surface the underlying empty-SSTable error (degraded boot unchanged); got %v", err)
		}
		if calls != 4 {
			t.Fatalf("want 4 attempts (1 + 3 retries), got %d", calls)
		}
	})
}
