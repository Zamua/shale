//go:build slatedb

package slate_test

import (
	"bytes"
	"testing"

	slate "github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
	slatedb "slatedb.io/slatedb-go/uniffi"
)

// TestNewWithStoreCache_RoundTripWithCache pins that wiring a slatedb
// DbCache via WithDbCache (the fix for the no-block-cache SST re-read
// storm) does not break reads or writes: a value put through a
// cache-backed Db reads back byte-for-byte. The cache itself is an
// in-memory Moka cache shared the same way a node would share one across
// its backends.
func TestNewWithStoreCache_RoundTripWithCache(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	cache, err := slatedb.DbCacheNewMokaCache(slatedb.MokaCacheOptions{MaxCapacity: 64 << 20})
	if err != nil {
		t.Fatalf("build moka cache: %v", err)
	}
	defer cache.Destroy()

	s, err := slate.NewWithStoreCache("shale-cache-roundtrip", store, nil, nil, cache)
	if err != nil {
		t.Fatalf("NewWithStoreCache: %v", err)
	}
	defer s.Close()

	key, val := []byte("k1"), []byte("hello-cache")
	if err := s.Put(key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("Get returned %q, want %q", got, val)
	}

	// A missing key still reports not-found through the cache path.
	if _, err := s.Get([]byte("absent")); err != backend.ErrNotFound {
		t.Fatalf("Get(absent) = %v, want ErrNotFound", err)
	}
}

// TestNewWithStoreCache_NilCacheStillWorks is the inverse: nil cache
// (slatedb default, no block cache) round-trips, proving the cache
// argument is genuinely optional and the default path is unchanged.
func TestNewWithStoreCache_NilCacheStillWorks(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	s, err := slate.NewWithStoreCache("shale-cache-nil", store, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewWithStoreCache(nil cache): %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get returned %q, want %q", got, "v")
	}
}
