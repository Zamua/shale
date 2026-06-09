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
