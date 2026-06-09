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

### Caveat: AwaitDurable is NOT a Settings field in slatedb-go v0.13.1

The shale spec gives `settings.AwaitDurable = false` as the motivating
example for the pass-through (a fast-ack mode that acks at memtable
insert instead of waiting for the WAL flush to object storage). That
shape matches the Rust crate, but in the **slatedb-go v0.13.1 binding**:

  - `*slatedb.Settings` is an opaque uniffi handle with no exported
    struct fields; the only mutation API is
    `Set(key, valueJson string)`.
  - `AwaitDurable` lives on `slatedb.WriteOptions` (a per-write knob,
    used by `DeleteWithOptions` / `PutWithOptions` /
    `WriteWithOptions`), NOT on `Settings`.

Shale's slate backend always uses default `WriteOptions` internally,
so `AwaitDurable=false` is not reachable through `slate.Config`
today. If you need it, the slatedb-go binding has to surface either
the per-field `Settings` accessors (matching Rust) or a way to set a
backend-wide default `WriteOptions`. Upstreaming that to slatedb-go
is the right fix; shale's pass-through is ready to forward whatever
the binding exposes.

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
