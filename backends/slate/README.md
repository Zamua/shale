# slate

`backend.Backend` implementation on top of SlateDB, the embedded LSM-tree
KV store with object-storage durability. Use this backend when you want
shale's clustering to sit over S3-compatible storage (AWS S3, MinIO, R2,
GCS via S3-compat) rather than local disk.

## Build / run requirements

This package is gated behind the `slatedb` build tag because it depends
on the cgo-backed `slatedb.io/slatedb-go` binding. The default
`go build ./...` (with CGO_ENABLED=0) excludes it, so consumers who only
want the memory or pebble backend pay no cgo cost.

To build with this backend:

```
CGO_ENABLED=1 \
CGO_LDFLAGS="-L/path/to/slatedb/target/release" \
DYLD_LIBRARY_PATH=/path/to/slatedb/target/release \
go build -tags slatedb ./...
```

See the package doc comment in `slate.go` for the object-store env-var
conventions.

## Custom slatedb settings

`Config.Settings` is a pass-through into the slatedb `DbBuilder`. Nil
leaves shale out of the picture: slatedb uses its own defaults.
Non-nil is forwarded verbatim via `DbBuilder.WithSettings`. Shale
never reads, mutates, copies, or validates the value.

```go
import (
    slatedb "slatedb.io/slatedb-go/uniffi"

    "github.com/Zamua/shale/backends/slate"
)

settings := slatedb.SettingsDefault()
// flush_interval expects a duration string in JSON form (note the
// nested quotes - the second argument is a JSON literal).
if err := settings.Set("flush_interval", `"250ms"`); err != nil {
    log.Fatalf("invalid settings: %v", err)
}

be, err := slate.New(slate.Config{
    Bucket:   "my-bucket",
    DbName:   "my-db",
    Settings: settings,
})
```

The mutation API on the binding's `*slatedb.Settings` is
`Set(key, valueJson string)` using dotted paths. Examples:

  - `settings.Set("flush_interval", `"250ms"`)`
  - `settings.Set("default_ttl", "42")`
  - `settings.Set("compactor_options.max_sst_size", "33554432")`
  - `settings.Set("object_store_cache_options.root_folder", `"/tmp/slatedb-cache"`)`

slatedb rejects invalid type combinations at `Set` time, before shale
ever sees the value. Other constructors that return `*slatedb.Settings`
work the same way (`SettingsFromFile`, `SettingsFromEnv`,
`SettingsFromJsonString`, ...).

### Note: AwaitDurable lives on WriteOptions, not Settings (slatedb-go v0.13.1)

In the slatedb-go v0.13.1 binding `*slatedb.Settings` is an opaque
uniffi handle with no exported struct fields; the only mutation API
is `Set(key, valueJson string)`. `AwaitDurable` is NOT a Settings
field at all in this binding: it lives on `slatedb.WriteOptions` as
a per-write knob, consumed by `PutWithOptions` / `DeleteWithOptions`
/ `WriteWithOptions` and by `DbTransaction.CommitWithOptions`.

The shale spec example shows `settings.AwaitDurable = false`; that
shape matches the Rust crate but does not compile against the Go
binding. Use `Config.WriteOptions` (next section) for the same
effect via the binding's actual API.

## Relaxed durability mode

`Config.WriteOptions` is a per-write pass-through into slatedb's
`*WithOptions` APIs. Nil leaves shale out of the picture: every
`Put`/`Delete` calls plain `db.Put`/`db.Delete` (which slatedb
internally treats as `AwaitDurable=true`), and every transaction
`Commit` calls plain `tx.Commit`. Non-nil is applied verbatim,
per-call, to `PutWithOptions` / `DeleteWithOptions` /
`CommitWithOptions`. Shale never reads, mutates, copies, or
validates the value.

Setting `AwaitDurable=false` opts every write into "ack at memtable
insert, eventually durable" mode: the call returns when the row is
visible to readers in the same process, without waiting for the WAL
flush to object storage (which runs on a background loop, default
~100ms). Wall-clock latency drops from tens-to-hundreds of
milliseconds (S3 round trip) to microseconds.

Tradeoff: if the writer process crashes inside the flush interval,
every write that was ack'd but not yet flushed is lost. The window
is bounded by the configured `flush_interval` (default 100ms in
slatedb v0.13).

```go
import (
    slatedb "slatedb.io/slatedb-go/uniffi"

    "github.com/Zamua/shale/backends/slate"
)

be, err := slate.New(slate.Config{
    Bucket: "my-bucket",
    DbName: "my-db",
    WriteOptions: &slatedb.WriteOptions{
        AwaitDurable: false, // fast-ack, eventually-durable
    },
})
```

### Recommended pairing: shale ReplicationFactor >= 2

Relaxed mode at R=1 (single replica per key) is unsafe: a writer
crash inside the flush interval drops un-flushed writes with no
recovery path. With `cluster.Config.ReplicationFactor >= 2`, the
same write lands on at least one other node whose own flush
schedule is independent; loss requires every ack'd replica to crash
inside the same window, which is much rarer for uncorrelated
failures.

For correlated failures (whole-DC outage, same-software-bug crash
cascade) even R>=2 doesn't fully cover the loss window; spreading
replicas across failure domains helps. See the shale spec under
"Backend durability is a backend concern" for the cluster-level
framing: shale notes the recommendation but doesn't enforce it.

## Test layers

Two tiers, both opt-in:

  - **Unit (`slatedb` tag)**: drives the binding against an in-process
    `memory:///` object store. Fast, no Docker.
    `make test-slate`.
  - **End-to-end (`slatedb integration` tags)**: spins up a real MinIO
    container via testcontainers-go, creates a fresh bucket, and runs
    the full Slate surface against it. Covers 1k small keys, 10x 1 MiB
    blobs, durability across writer-process reopen, and writer-epoch
    fencing (two writers against the same DB → second fences the
    first). Requires Docker + the SlateDB shared library.
    `make test-slate-minio`.

## v0.6 readiness

The slate backend is end-to-end validated against MinIO via
testcontainers; `make test-slate-minio` exercises it. v0.6 (the
hostthis migration to shale-with-SlateDB) gates on this passing
against the target deployment's chosen object store.
