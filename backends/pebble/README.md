# pebble

`backend.Backend` implementation on top of [Pebble](https://github.com/cockroachdb/pebble),
CockroachDB's pure-Go LSM-tree key-value engine. Use this backend when each
shale node has durable local storage and you want a fast, embedded,
cgo-free durability story.

## Build / run requirements

Pure Go, no build tags, no native libs. `go build ./...` always includes
this package.

## Usage

```go
import (
    pebblebe "github.com/Zamua/shale/backends/pebble"
    "github.com/Zamua/shale/pkg/cluster"
)

be, err := pebblebe.New(pebblebe.Config{
    Dir: "/var/lib/shale/node-1",
})
if err != nil { /* handle */ }

c, err := cluster.Open(cluster.Config{
    NodeID:  "n1",
    Backend: be,
    // ...
})
```

## Custom pebble Options

`Config.Options` is a pass-through into `pebble.Open`. Nil leaves shale
out of the picture: pebble uses its own defaults. Non-nil is forwarded
verbatim. Shale never reads, mutates, copies, or validates the value.

```go
import (
    pebbledb "github.com/cockroachdb/pebble"
    pebblebe "github.com/Zamua/shale/backends/pebble"
)

opts := &pebbledb.Options{
    Cache:        pebbledb.NewCache(256 << 20), // 256 MiB block cache
    MemTableSize: 64 << 20,                     // 64 MiB memtable
    // any other pebble.Options field; see
    // https://pkg.go.dev/github.com/cockroachdb/pebble#Options
}

be, err := pebblebe.New(pebblebe.Config{
    Dir:     "/var/lib/shale/node-1",
    Options: opts,
})
```

If pebble rejects an Options combination, the error surfaces from
`pebble.Open` before shale sees the backend; shale has nothing to add.

## Transaction semantics

`Begin(SnapshotIsolation)` composes a `pebble.Snapshot` (point-in-time
read view) with a `pebble.Batch` (atomic write buffer). Reads inside
the transaction consult the batch overlay first
(read-your-own-writes) and fall back to the snapshot. `Commit`
applies the batch atomically.

Concurrent transactions are independent (each holds its own snapshot +
batch). Commit is last-writer-wins per key (no conflict detection).
`SerializableSnapshot` is rejected up-front so callers don't get a
silent downgrade.
