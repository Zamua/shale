# shale - spec

A Go library that turns N processes each holding a local key-value backend into ONE logical KV cluster, with horizontal write scaling by consistent-hashed sharding.

```go
import "github.com/Zamua/shale/pkg/cluster"

c, _ := cluster.Open(cluster.Config{...})
c.Put([]byte("user:alice"), []byte("data"))
val, _ := c.Get([]byte("user:alice"))
```

App code uses `c` like a local KV store. Behind the interface, shale figures out which node owns the key + forwards the operation.

---

## What it is, what it isn't

- It IS: a clustering layer that gives any KV-shaped storage engine horizontal scale-out.
- It IS: an embedded Go library - apps import it, each instance is a cluster node.
- It IS: backend-agnostic. Default backend ships SlateDB-on-object-storage; any `Backend` impl works.
- It IS NOT: a database service. There's no `shaled` daemon to deploy separately from your app.
- It IS NOT: SQL-aware. No query parsing, no cross-shard JOINs, no transactions across multiple shards.
- It IS NOT: a Postgres / MySQL frontend. Use Vitess / Citus for those.
- It IS NOT: a Redis Cluster competitor. If your data fits in Redis's model, use Redis.

The sweet spot: you want sharded KV semantics on top of object storage (cheap durability), without running Cassandra / Scylla / TiKV as a separate service.

---

## API surface

```go
type Cluster struct{...}

func Open(cfg Config) (*Cluster, error)
func (c *Cluster) Close() error

// Same shape as the Backend interface - keeps app code unchanged whether you
// migrate from a single Backend to a Cluster wrapping multiple.
func (c *Cluster) Put(key, value []byte) error
func (c *Cluster) Get(key []byte) ([]byte, error)
func (c *Cluster) Delete(key []byte) error
func (c *Cluster) ScanPrefix(prefix []byte) Iterator
func (c *Cluster) Begin(IsolationLevel) Transaction

// Cross-shard operations are explicit so you can see the fan-out cost
// at the call site.
func (c *Cluster) Aggregate(fn func(backend.Backend) any) []AggregateResult

// AggregateResult separates "fn ran + here's its return value" from
// "shale couldn't even run fn here" (snapshot fetch failed, peer
// unreachable). Exactly one of Value / Err is meaningful per entry.
type AggregateResult struct {
    Value any
    Err   error
}
```

Single-key ops are O(1) routing → one local-or-remote backend call. Cross-shard ops fan out to all owners.

---

## Backend interface

```go
package backend

type Backend interface {
    Put(key, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    ScanPrefix(prefix []byte) Iterator
    Begin(isolationLevel IsolationLevel) (Transaction, error)
    Close() error
}

type Iterator interface {
    Next() (key, value []byte, err error)
    Close() error
}

type Transaction interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Commit() error
    Rollback() error
}
```

Minimum surface that lets the cluster layer do its job. No backend-specific options leak through.

Shipped impls:
- `pkg/backend/memory` - in-process map; tests + dev
- `pkg/backend/slate` - SlateDB-on-object-storage (planned for v0.1)

BYO: implement the interface, register the impl, done.

---

## Cluster model

### Membership

Each node runs a `memberlist` instance (HashiCorp's SWIM gossip protocol). Nodes discover each other via configured seed addresses. Memberlist handles:
- Heartbeats + failure detection
- Broadcast of node-join / node-leave events
- Node metadata (the node's gRPC listen address)

shale subscribes to membership events; when membership changes, the ring is recomputed + shard ownership shifts trigger rebalancing.

The membership layer's event delegate uses non-blocking sends to its event channel so a slow subscriber can't deadlock memberlist's gossip goroutines. When the channel is full, the event is dropped + `Membership.DropCount()` is incremented. To keep the ring consistent with reality despite drops, the cluster runs a periodic reconciler (~5s) that calls `Membership.Snapshot()` (the authoritative current member list, sourced directly from memberlist's state, not from our event stream) and applies any missed adds / removes.

### Routing

The hash ring is **ring-based consistent hashing with bounded loads** (Karger 1997 + Mirrokni-Thorup 2017), implemented via `buraksezer/consistent`. Each node has ~64 virtual replicas on a 64-bit ring; a key hashes onto the ring and goes to the next clockwise virtual node. Adding/removing any node moves ~K/N keys.

For a key K:
1. `node := ring.LocateKey(K)` - local in-memory lookup, sub-microsecond
2. If `node.ID == local.ID` → call local Backend directly
3. Else → gRPC call to that node's shale service

Latency: local ops are local-Backend latency. Remote ops are local-Backend latency + one network RTT.

**Forwarding loop-guard.** Every inter-node Put / Get / Delete / ScanPrefix RPC carries a `forwarded` bool. When a node receives a request whose key it thinks belongs to a different shard, it normally dials the owning node + re-issues the request with `forwarded=true`. If the receiving node ALSO thinks the key belongs somewhere else AND `forwarded` is already true, it returns `FailedPrecondition` instead of re-forwarding. This prevents ping-pong loops during the brief window when two nodes disagree about ownership (e.g. just after a member join / leave, when one node's ring has updated and the other's hasn't yet). Clients see a transient error on that key; the next attempt after both rings converge succeeds.

**Why ring-based and not Jump Consistent Hash:** Jump (Lamping-Veach 2014) is zero-state and faster but requires bucket numbers 0..N-1 - removing an arbitrary middle bucket requires re-numbering, which doesn't compose with our gossip-based membership where any node can leave anywhere. Ring-based fits dynamic topology naturally at the cost of a few KB of in-memory state per node + an O(log N) binary search per lookup. Both negligible at our scales.

### Shard keys + hash tags

By default, a key is hashed in its entirety. Apps that want to co-locate related keys on the same shard use the **hash tags convention** (borrowed from Redis Cluster):

```
key                              hashed-on
"pastes/abc12345"                "pastes/abc12345"   (whole key, default)
"{alice}/pastes/abc12345"        "alice"             (just the tag)
"{alice}/versions/abc12345/v2"   "alice"             (just the tag)
```

Anywhere a key contains `{...}`, ONLY the contents between the first matching pair of braces are hashed. Keys with the same tag value land on the same shard. Apps choose the convention based on access patterns:

  - **Hot path = single-key lookup, secondary = grouped list**: shard by the lookup key (no hash tag); accept fan-out for the grouped list.
  - **Hot path = per-group queries, secondary = random-key lookups**: shard by the group (hash tag); accept the extra lookup hop for random-key reads.

The trade-off is the same one DynamoDB forces at table-creation time when you pick the partition key - except shale lets you mix-and-match per key (different prefixes can use different tagging).

For apps that need more control than hash tags, `Config.ShardKeyFn` is an escape hatch:

```go
cluster.Open(cluster.Config{
    ShardKeyFn: func(key []byte) []byte { return extractFirstPathSegment(key) },
    ...
})
```

ShardKeyFn is called for every Put/Get/Delete before ring lookup. Default: identity (hash whole key, honoring hash tags).

### Cross-shard operations

Anything that touches more than one shard goes through `Cluster.Aggregate`:

```go
results := c.Aggregate(func(b backend.Backend) any {
    // runs LOCALLY on each node in parallel; sees only that node's keys
    var slugs []string
    it := b.ScanPrefix([]byte("pastes/"))
    defer it.Close()
    for {
        k, _, err := it.Next()
        if err != nil || k == nil { break }
        slugs = append(slugs, string(k[len("pastes/"):]))
    }
    return slugs
})
// results is []AggregateResult - one per node. App checks .Err
// (snapshot-transport failure for that peer) before reading .Value,
// then flattens / merges as it sees fit.
```

Cost: O(K / N) per node, run in parallel. Wall-clock ≈ slowest-node scan + one gRPC round-trip. Bounded + safe for admin/rare operations. NOT recommended for hot-path queries (use shard keys to make those single-shard).

Under the hood, `Aggregate` snapshots each peer's local keyspace via an internal `LocalScan` RPC that bypasses ring routing and reads the receiving node's Backend directly. `LocalScan` is also used by the `stats` handler's `keys_held` counter. It is admin-only + intentionally undocumented on the public surface: apps should use `ScanPrefix` (single-shard, routed) or `Aggregate` (cross-shard, fan-out) instead. It's described here for transparency, not as part of the stable API.

### Replication (v0.4+)

v0.1-v0.3 ship with R=1: each key has exactly one owner, no copies. v0.4 introduces optional replication. The defaults preserve v0.3 behavior (R=1, single-owner) so existing deployments are not silently re-shaped; HA is opt-in via `Config.ReplicationFactor`.

#### Configuration knobs

Three knobs on `Config`, all per-cluster (not per-key):

  - **`ReplicationFactor int`** (default `1`). The number of nodes that hold a copy of each key. With R=1 the cluster behaves exactly as in v0.3: one owner per key, no replicas, no LWW envelope cost paid on the read path. With R>1 each write fans out to R nodes (primary + R-1 successors on the ring).
  - **`WriteConsistency`** (`One | Quorum | All`, default `Quorum`). The number of replica acks required before `Put` / `Delete` returns success. `Quorum = floor(R_live/2) + 1` where `R_live` is the number of replicas actually returned by `ring.LocateKeyN(key, R)` after the live-membership clamp (when the ring has fewer than R distinct members, `LocateKeyN` returns the available subset). The quorum tracks live R, not configured R: a configured R=5 cluster running on 2 live members yields W=2 for Quorum (a majority of live), not W=3 (unreachable, would block forever). `All` requires every live replica to ack; any down replica fails the write. `One` returns as soon as the primary acks and is the loosest setting.
  - **`ReadConsistency`** (`Nearest | Quorum | All`, default `Nearest`). `Nearest` reads with consistency target 1 (the call returns on the first replica's reply); `Quorum` waits for floor(R_live/2)+1 successful responses and returns the LWW winner; `All` waits for every live replica. In every case the dispatch goes to all R replicas so a down primary falls back to the next ring successor (`Quorum` / `All` tolerate up to `R - N` such failures; `Nearest` tolerates `R - 1`). Surplus successful responses past the consistency target seed read-repair feedback on `Quorum` / `All`.
  - **`WriteTimeout duration`** (default `5s`). Per-Put / per-Delete fan-out budget. The originator wraps the fanout context with this timeout so a blackholed peer (hung gRPC, half-open TCP) cancels at deadline instead of leaking goroutines indefinitely. A write that exhausts the timeout without reaching W acks returns `Unavailable`.
  - **`ReadTimeout duration`** (default `5s`). Per-Get fan-out budget. Same shape as `WriteTimeout`: an unreachable peer doesn't block the read indefinitely.

The default pairing (`Quorum` writes, `Nearest` reads) is the v0.4 baseline: writes are durable across a quorum loss, reads are cheap. Operators who want stronger read freshness flip `ReadConsistency` to `Quorum`. Operators willing to tolerate write loss for lower write latency flip `WriteConsistency` to `One`. The two knobs are independent.

#### Value envelope

When R>1, every Backend value is wrapped in an LWW envelope before storage:

```
Envelope {
  Stamp { TimestampNanos int64; NodeID string }
  Payload []byte
}
```

The envelope is opaque to `Backend`: the cluster layer encodes on `Put` and decodes on `Get`, so backend implementations are unchanged. A value written in v0.3 (no envelope) decodes as `Stamp{0, ""}`: it loses every comparison against a stamped write and is re-stamped on the next `Put`. This is the migration path; no offline conversion step is required.

`Delete` writes a tombstone: an empty-payload envelope carrying the current `Stamp`. The tombstone participates in LWW like any other write, so a delete that races with a concurrent write resolves by timestamp rather than by op order. `Get` treats an empty payload as `NotFound`. Tombstone GC is deferred (see "Out of scope" below).

Because the empty-payload shape is reserved for tombstones, `Put(key, nil)` and `Put(key, []byte(""))` are rejected with `ErrEmptyValue` at the cluster surface. Otherwise the same call would silently store a tombstone at R>1 (surfacing as `NotFound` on subsequent reads) while at R=1 it would store an empty payload (surfacing as an empty successful read): the asymmetry is a foot-gun. Apps remove a key by calling `Delete` explicitly.

#### LWW comparator

For two envelopes A and B, A wins iff:

  1. `A.Stamp.TimestampNanos > B.Stamp.TimestampNanos`, OR
  2. timestamps equal AND `A.Stamp.NodeID > B.Stamp.NodeID` (lexicographic).

`TimestampNanos` is `time.Now().UnixNano()` taken by the **originating node** at the moment `Put` is called, before fan-out. Every replica stores the same Stamp for that write; replicas do NOT re-stamp on receipt. This keeps the clock single-sourced per write and avoids skew between the primary's stamp and a successor's stamp for the same `Put`. The NodeID tiebreak resolves the rare case where two originators race with identical nanosecond clocks.

Clock skew between nodes is the standard LWW caveat: a node with a forward-skewed clock can shadow honestly-timestamped writes from healthier peers. Operators run NTP. shale does not synthesize a hybrid logical clock in v0.4; that lands later if real workloads hit the skew limitation.

#### Fan-out + ack accounting

With R>1, `ring.LocateKeyN(key, R)` returns the primary plus R-1 successors on the ring. Put dials all R in parallel:

  - The local node, if it is one of the R, writes via its local Backend.
  - Remote replicas receive a forwarded Put RPC carrying the envelope as-is.

`Put` returns success once W acks have arrived (W per `WriteConsistency`). Acks above W are not waited on but are NOT cancelled: the surplus writes continue in the background so the eventually-consistent state matches the consistency setting's intent. Failures above (R - W) cause `Put` to return error.

#### Read path

`Get` dispatches read RPCs to all R live replicas (so a hung / down primary falls back to a successor without losing the call) and waits for N successful responses per `ReadConsistency` (1 / quorum / R). Each replica returns its local envelope (or `NotFound`). The cluster layer:

  1. Collects responses up to the consistency target.
  2. Picks the LWW winner across the collected envelopes.
  3. Returns the winner's payload (or `NotFound` if the winner is a tombstone or no replica had the key).

Surplus responses past the consistency target keep flowing onto the result channel until every dispatched op reports (or `ReadTimeout` fires), so read-repair below can classify replicas that disagreed with the winner.

On `Quorum` / `All`, if the collected envelopes disagree (any replica returned an older Stamp than the winner, or `NotFound` while a peer has a value), the cluster issues **read-repair**: an async forwarded Put of the winner envelope to every lagging replica. Read-repair is best-effort: errors are swallowed, no retry. Read-repair is **skipped on `Nearest`** because a single-replica read has nothing to compare against; lagging replicas catch up via the next quorum/all read or via future anti-entropy (v0.4.1+).

#### Failure modes

  - **Replica down at Put time**: tolerated up to `R - W` simultaneous failures. With the default R=3 W=2, one replica down still completes writes. With `WriteConsistency=All`, any replica down fails the write. A down replica returns `codes.Unavailable` (real peer-down), which counts against the failure budget so the fanout fails fast once `R - W + 1` such replies land instead of waiting on every remaining peer.
  - **Network partition**: both sides accept writes for keys they can reach a sufficient quorum on. Writes on either side carry the originator's Stamp. On heal, the LWW comparator reconciles deterministically: the higher-timestamp write wins, lower-timestamp writes are lost. This is the AP choice (see "Consistency model"); workloads that cannot tolerate write loss should use a CP system.
  - **All R replicas down**: `Put` and `Get` return `Unavailable`. No way to make progress without at least one replica.
  - **Replica down at Read time**: tolerated up to (R - N) where N is the consistency-target count. `Nearest` with primary down falls back to the next ring successor; `Quorum` / `All` collect from surviving replicas and fail if too few respond.
  - **Replica mid-handoff (rebalance)**: the receiving node returns `codes.ResourceExhausted` (the migration-guard sentinel), distinct from `Unavailable`. The fanout classifies it as transient (skip this replica, wait for another) and does NOT count it against the failure budget. Without that distinction, a single mid-handoff replica during normal v0.3 rebalance would force every otherwise-quorum write to wait for the migration to complete.

#### Out of scope for v0.4

  - **Hinted handoff** (deferred to v0.4.1): when a replica is down at Put time, the coordinator should durably hint the missed write and replay it when the replica returns. v0.4 ships without hints, so a write that lands on (R - W) acks succeeds but the down replicas miss it permanently until a Quorum / All read repairs them or anti-entropy reconciles. The hint protocol is a v0.4.1 follow-up.
  - **Anti-entropy / Merkle-tree repair** (v0.4.1+): a background process that walks ranges + reconciles cold replicas without waiting for a read. Not in v0.4.
  - **Per-key replication factor**: every key in the cluster uses the same R. Per-key overrides are out of scope; operators who need per-prefix replication policy should run separate clusters.
  - **Rebalance-with-replication**: a separate workflow (planned post-v0.4) that integrates the v0.3 rebalance protocol with the v0.4 replication overlay so live writes flow via replication during the bulk copy and the per-range write-rejection window collapses to the ownership swap.

### Rebalancing (v0.3+)

v0.2 ships the gossip ring but does not migrate data: when membership changes, the ring redirects lookups to a node that does not hold the key, so reads against the new owner return not-found and writes land on the wrong shard. v0.3 closes that gap by physically migrating keys when membership changes, so the data state catches up to the ring state.

The design optimizes for an implementation small enough to reason about end to end, on the assumption that shale is embedded in apps run by individual developers and small teams (not Google-scale operators). It accepts brief per-range unavailability (tens of seconds for the streaming copy, milliseconds at the ownership swap) in exchange for: no coordinator election, no per-Backend WAL-tailing requirement, no five-state-machine recovery protocol. v0.4 adds replication (R>1), which is the natural place to recover read availability during migration by serving reads off a replica that already holds the range; trying to engineer that into v0.3 at R=1 buys little and complicates a lot.

R=1 throughout this section. Notes on what changes for R>1 are flagged at the end.

#### Trigger

Migration is driven by membership change, with a settling delay.

When `Membership` reports a join or leave, the node records the event and schedules a `rebalance.Evaluate()` for `T_settle` seconds in the future (default 5s, configurable via `Config.RebalanceSettleDelay`). Any further membership event inside the window resets the timer. When the timer fires, the node snapshots the current ring and proceeds.

The settling delay collapses bursts (rolling restart, several nodes joining within a second, a flapping peer) into one rebalance pass instead of thrashing the cluster through intermediate ring shapes. It also gives the existing membership reconciler (~5s) time to absorb missed events, so by the time `Evaluate` runs, every node tends to have the same view of who is in the cluster.

There is no consensus and no coordinator election. Each node independently decides what to send and what to receive based on its local view of membership. Because memberlist converges in bounded time and each node uses the same ring algorithm against the same member list, peers reach the same migration decisions without negotiation. Disagreement during convergence is handled by the existing forwarding loop-guard plus the per-range cutover below.

An operator can also trigger evaluation explicitly:

  - `shale rebalance --dry-run` prints the plan the local node would execute against the current ring without doing the migration.
  - `shale rebalance --apply` runs `Evaluate` immediately, bypassing the settling delay. Useful for planned topology changes (node decommission) where the operator wants to watch the plan before pulling the trigger.

#### Plan

Each node computes its plan locally from two inputs:

  - the previous ring snapshot (cached from the last evaluation, or empty on first run)
  - the new ring snapshot

The unit of migration is the **key range**, defined as the contiguous arc of the hash ring between two adjacent virtual nodes. `buraksezer/consistent` exposes enough state to enumerate ranges. For each range, the node compares old owner and new owner:

  - old=self, new=peer: schedule **send** to peer
  - old=peer, new=self: schedule **receive** from peer (passive; the source is the one who initiates the stream)
  - old=self, new=self: no-op
  - old=peer1, new=peer2: no-op for this node; the two peers handle it between themselves

Both sides of any (send, receive) pair derive the plan from the same ring inputs, so they agree on who sends what without explicit negotiation. The plan is held in memory; there is no persisted plan object and no plan ID. A crash mid-migration is recovered by the next `Evaluate` run on restart: the node observes the current ring versus what it actually holds and re-issues whatever migrations are still needed. This recovery path is the same code path as the steady-state one.

#### Wire

A single server-streaming gRPC method:

```
rpc MigrateRange(RangeSpec) returns (stream MigrateChunk)
```

`RangeSpec` identifies the arc of the ring being transferred (start + end hash values, plus a `ring_generation` freshness field). `MigrateChunk` carries either a `(key, value)` pair or a terminal marker with the count + checksum of all keys sent.

`ring_generation` is the destination's per-node monotonic ring-change counter at the moment it opened the stream. v0.3 carries this on the wire for future use, but the counter is NOT cluster-wide (each node bumps its own on every NotifyJoin/NotifyLeave + a node that joined later naturally has a lower count than a longer-lived peer), so the source CANNOT meaningfully compare the destination's value against its own. A strict less-than rejection spuriously cancels legitimate streams during normal join/leave races. v0.3 therefore accepts every stream regardless of the destination's reported generation; wrong-owner protection comes from the forwarding loop-guard + the per-key Put/Get migration guards instead. A cluster-wide ring generation (and the strict freshness check the field was originally designed for) lands when the gossip layer carries one in v0.4 or later.

The destination initiates the call (it is the node whose ring says "I am the new owner"). The source iterates its local Backend over keys whose hashed shard key falls in the range, streams `(key, value)` pairs, then closes with the terminal marker. The destination writes each pair to its local Backend as it arrives. Backpressure is gRPC flow control; no per-key acknowledgement is needed.

One range per RPC. Multiple ranges between the same source and destination run sequentially to keep the resource footprint predictable and the per-peer protocol single-threaded.

If the source's Backend does not support a range-bounded scan, it falls back to a full `ScanPrefix("")` with a range filter applied per key. This is wasteful but correct; backend authors are encouraged to implement a faster path.

#### Cutover

Each range has a lifecycle:

1. **Pre-migration**: source owns the range. Source serves reads and writes. Destination's ring already lists destination as the new owner, but `MigrateRange` has not started or is queued behind earlier ranges.
2. **Migrating**: destination opens `MigrateRange`. Source marks the range as migrating-out, destination marks it as migrating-in. While in this state:
   - **Reads** for keys in the range are served by the **source**. The forwarding loop-guard ensures that a read landing on the destination via stale ring is forwarded back to the source.
   - **Writes** for keys in the range are **rejected by the source** with a transient error (`ResourceExhausted` with retry hint). The destination, if it receives a write via the new ring, also rejects with `ResourceExhausted`. Clients retry with backoff; the SDK's client wrapper handles this transparently with a bounded retry budget. `ResourceExhausted` (not `Unavailable`) is the chosen code so the v0.4 replication fanout's failure budget can distinguish "this replica is mid-handoff, try another" from "this replica is dead, count it as failure"; conflating the two would force the fanout to wait on every peer instead of failing fast on real peer-down.
   - This is the chosen cutover semantics. Rejecting writes during the streaming copy is what avoids the "did this write make it across the cutover" problem without a WAL-tailing catch-up phase. The write-unavailability window per range is bounded by streaming time for that range's data.
3. **Swap**: source completes the stream by sending the terminal `MigrationDone{total_keys, checksum}` chunk. The destination validates the checksum against its own running CRC32 and ack's implicitly by reading + draining the stream to a clean EOF (the gRPC stream return value IS the ack; there is no separate `MigrateAck` message). The source's `MigrateRange` handler returns successfully only after the destination has consumed `MigrationDone`. The coordinator observes that successful return and flips ownership state for the range from "owner" to "former owner, forwards reads". This flip is atomic per range.
4. **Post-migration**: destination is authoritative for reads and writes. Source forwards any straggler read it receives for the range to the destination via the standard forwarding path. Once the source observes that all peers' rings agree the destination owns the range (or after a grace period of `T_drain`, default 30s), source deletes its local copy of the range's keys.

The write-rejection window is the price paid for avoiding WAL-tailing. At hostthis-scale workloads (tens of thousands of keys per range, small values), the streaming copy completes in well under a minute on a local network; clients retrying with exponential backoff over that window do not see user-visible errors.

#### Failure handling

  - **Source crashes mid-stream**: destination's `MigrateRange` call returns an error. Destination discards what it received (the partial range is not authoritative). On the next `Evaluate` pass, if the source comes back, migration retries. If the source does not come back, the range is owned by no one at R=1; reads return not-found, writes are rejected. This is the unavoidable R=1 failure mode and is the central motivation for v0.4 replication.
  - **Destination crashes mid-stream**: source's stream returns an error. Source remains the owner. On next `Evaluate`, migration retries to whichever node the new ring assigns.
  - **Checksum mismatch on `MigrationDone`**: destination errors its stream (does not drain it cleanly), the source's handler returns that error rather than success, and the coordinator does NOT flip state. Destination rolls back its writes for the range; source remains owner; migration retries on the next `Evaluate`. Checksum is computed over the sorted `(key, value)` byte stream.
  - **Ring divergence during cutover**: handled by the existing loop-guard. A read or write landing on the wrong node gets one forward; if the forwarded-to node also disagrees, the request returns `FailedPrecondition` and the client retries after the rings converge.
  - **Operator cancellation**: `shale rebalance --cancel` aborts in-progress streams. Source remains owner of any range not yet swapped; destination discards any partial state.

#### Observability

  - `shale topology` shows each range's state (`stable | migrating-out | migrating-in | draining`) and, for in-flight migrations, the source, destination, bytes streamed so far, and elapsed time.
  - Per-node counters: `rebalance_ranges_migrated_total`, `rebalance_bytes_streamed_total`, `rebalance_writes_rejected_total`, `rebalance_failures_total`.
  - Structured log line per range transition with `range_id`, `from`, `to`, `key_count`, `bytes`, `duration_ms`.

#### What v0.4 changes

With R>1, the read-unavailability problem largely disappears: reads can be served from any replica, including ones not currently sending or receiving. The migration unit becomes a (range, replica-position) pair rather than just a range. The write-rejection window can shrink to the duration of the ownership swap rather than the entire streaming copy, because the destination can receive writes via the replication path while the bulk copy is in flight and the conflict resolver (LWW, v0.4) reconciles. The wire protocol stays the same shape; the cutover protocol gains a "live writes also flow via replication" overlay.

The v0.3 design does not preclude any of this. It deliberately leaves the per-range cutover as the only place where v0.4 needs to intervene.

#### Known limitations in v0.3

  - **Write-unavailability window per migrating range** is proportional to the size of that range. A range with millions of small keys can stall writes for tens of seconds. Operators with hot ranges should plan topology changes during low-traffic windows or wait for v0.4.
  - **No live progress streaming**: the source streams to one destination; if the operator wants finer-grained progress than the per-range chunk counter, they can watch the gRPC stream metrics directly, but there is no built-in real-time per-key progress feed.
  - **Backend scan efficiency varies**: backends without range-bounded scans pay an O(total keys) cost per migrating range. The memory backend is fine; SlateDB's range scan is efficient; future backend authors should implement bounded range scans for production workloads.
  - **No throttling of concurrent migrations**: if many ranges change owners at once (e.g. a 3-node cluster grows to 6 nodes), every node opens its `MigrateRange` calls in parallel against its peers. Object-store IO is the practical limiter today; an explicit per-node concurrency cap is a v0.3.x follow-up if hot-spotting shows up in practice.

---

## Consistency model

shale is **per-key linearizable**. Each key has exactly one owner at a time; all ops for that key serialize through that owner's local Backend. Cross-key operations are NOT atomic (no distributed transactions).

The Backend's own transaction interface (`Begin/Commit/Rollback`) operates on the LOCAL Backend only - so transactions are scoped to keys owned by ONE node. shale's transaction proxy routes the transaction to that owner.

If a transaction touches keys owned by multiple nodes, shale returns an error. App code is expected to use a single shard-key prefix for related keys (the standard sharded-KV pattern).

---

## Failure handling

- **Single node crashes**: keys it owned are temporarily unavailable at R=1. With R>1 (v0.4+), replicas take over reads + writes up to (R - W) / (R - N) tolerated failures per the configured consistency.
- **Network partition**: nodes on each side see the other as failed. Both sides accept writes. On heal, conflicts resolve via Last-Write-Wins (LWW) using the originator's wall-clock timestamp + nodeID tiebreak (see "Replication (v0.4+)" for the full envelope + comparator). R=1 has no replication conflicts; R>1 relies on LWW.
- **Backend failure on one node**: that node reports unhealthy to memberlist; gets removed from ring; data unavailable until restored (or served from replicas under R>1).

---

## Non-goals

- **SQL queries** - Vitess (MySQL) / Citus (Postgres) do this well; shale stays out of that domain.
- **Cross-shard transactions** - requires consensus (Paxos/Raft); out of scope. Apps that need this use FoundationDB / TiKV.
- **Strong consistency across partitions** - shale chooses AP over CP (eventual consistency on partition heal). Need CP? Use Raft-based stores.
- **Multi-region replication** - out of scope for v1; possibly v2.
- **Backend-specific features** (Redis pub/sub, Postgres extensions, etc.) - the abstraction stays minimal.

---

## CLI + standalone binary

shale ships two binaries from v0.1, separate from the library import path. Their purpose is dev-cycle ergonomics: poking at a running cluster from the shell, spinning up nodes for integration tests, inspecting topology + state during debugging.

### `shaled` (standalone node)

Runs a shale node as its own process, without an app embedding it. Backend chosen via flag. Useful for:
  - Integration tests (shell scripts spin up N nodes on ephemeral ports)
  - Local multi-node dev clusters
  - Operators who want a managed shale process per host (instead of embedding inside their app)

```
shaled \
  --node-id node-1 \
  --bind-addr :7946 \
  --grpc-addr :7947 \
  --seeds node-2:7946,node-3:7946 \
  --backend memory|slate \
  --slate-bucket my-bucket           # if --backend=slate
```

shaled exposes the same gRPC service the inter-node forwarding layer uses. The CLI talks to it via that gRPC. For v0.1 (single-node), shaled is functional standalone: one node, one Backend, gRPC up so the CLI can hit it.

### `shale` (CLI)

Connects to ONE node's gRPC endpoint (via `--addr` flag or `SHALE_NODE` env, defaults to `127.0.0.1:7947`). All subcommands are operator + developer ergonomics; they're NOT a replacement for the library API (apps embed shale, they don't shell out to it).

Subcommands shipped in v0.1:

  - `shale put <key> <value>` - one-shot write; the node routes per the ring (single-node in v0.1 = always local)
  - `shale get <key>` - one-shot read; prints value to stdout, exits non-zero on not-found
  - `shale delete <key>` - one-shot delete; idempotent
  - `shale scan <prefix>` - prints key=value pairs for everything under prefix
  - `shale topology` - prints the cluster membership + ring: which nodes exist, what range each owns. In single-node v0.1, prints "single-node cluster, node=<id>"
  - `shale stats` - per-node counters: key count, request rate, p50/p99 latency for Put/Get
  - `shale ping` - liveness check; exits 0 if the node responds

Subcommands shipped in later versions:

  - v0.3+: `shale rebalance` - trigger immediate rebalance (normally automatic)
  - v0.3+: `shale migrate-from <backend-spec>` - one-shot migrator from an existing backend (e.g. raw SlateDB) into the shale cluster
  - v0.5+: `shale bench` - load tester; reads + writes at configurable rates

### Output conventions

  - One value per line on stdout, parseable by shell tools
  - Status / errors on stderr
  - Exit codes: 0 success, 1 generic error, 2 not-found, 3 connection error, 4 timeout
  - `--json` flag on every read subcommand for structured output (topology, stats)

### Why ship the CLI in v0.1

The library-only argument is "developers will write Go to test it." That's right for the LIBRARY surface (Put/Get/Delete from app code), but wrong for the OPERATOR surface (is the cluster healthy? who owns this key? what's the per-node load?). Without the CLI:

  - Dev cycle is slow: every quick check requires `go run`-ing a one-off program
  - Integration tests need scaffolding to spin up nodes; shaled lets shell scripts do it
  - Debugging a misbehaving cluster means writing custom inspect-via-Go programs
  - Migration tools (v0.3+) don't have a natural home

The CLI + shaled aren't "nice-to-have"; they're the daily-driver tools for everyone touching shale. Build them with v0.1.

---

## System testing

Local multi-node testing on a single machine is a first-class workflow. It validates the distributed-systems correctness (routing, membership transitions, rebalancing) in a controllable environment without paying for a real cloud cluster.

### Available per version

  - **v0.1**: NOT available. v0.1 ships single-node only. Multiple shaled processes run side-by-side but are independent islands; no cross-node routing.
  - **v0.2+**: full local multi-node testing. N shaled processes join a single logical cluster via memberlist + gRPC forwarding. Throughput tests + topology-change tests become possible.
  - **v0.3+**: rebalancing tests (kill a node, watch keys migrate to survivors; bring the node back, watch keys rebalance home).
  - **v0.4+**: replication tests (with R>1, kill a node mid-test, observe reads + writes continue against the surviving replicas).

### Local 3-node cluster (v0.2+ example)

Each node uses a distinct bucket prefix in shared object storage so their SlateDB stores don't conflict on the single-writer epoch:

```
# shared object storage (any S3-compatible; local MinIO works)
# each node writes under its own prefix:
#   shale-test/node-1/
#   shale-test/node-2/
#   shale-test/node-3/

shaled --node-id n1 --grpc-addr :7947 --bind-addr :7946 \
       --backend slate --slate-bucket shale-test --slate-db-name node-1 \
       --slate-endpoint http://localhost:9000 ...

shaled --node-id n2 --grpc-addr :7949 --bind-addr :7948 --seeds 127.0.0.1:7946 \
       --backend slate --slate-bucket shale-test --slate-db-name node-2 ...

shaled --node-id n3 --grpc-addr :7951 --bind-addr :7950 --seeds 127.0.0.1:7946 \
       --backend slate --slate-bucket shale-test --slate-db-name node-3 ...

# inspect + load test
shale topology --addr 127.0.0.1:7947   # all 3 nodes + ring assignments
shale bench --addr 127.0.0.1:7947 --writes 100k --keys-prefix bench:
```

`shale bench` (v0.5+) reports aggregate throughput, per-node request distribution, p50/p99 latencies.

### Throughput scaling expectations

On a single local machine, throughput should grow as nodes are added until a shared bottleneck is hit:

  - **CPU**: linear up to about (cores - 1) nodes. Each shaled process is small but each one runs its own SlateDB compaction, gRPC handlers, etc.
  - **Object storage**: usually the dominant bottleneck. A single local MinIO process serializes I/O; aggregate write throughput plateaus once MinIO saturates regardless of how many shaled nodes are above it.
  - **Local loopback network**: not a real constraint at our scales.

To exercise more dramatic local scaling, run multiple MinIO instances (each on its own port) and point each shaled node at its own dedicated MinIO. Bottleneck then moves to NVMe + CPU; typical 4-6x linear scaling before hitting limits on a beefy dev machine.

True linear scaling requires distributing nodes across machines (or against a managed object store like R2/S3 with independent capacity per shard). The local single-machine test demonstrates the routing + load-distribution patterns; cloud testing validates the linear-scaling claim under real network + storage isolation.

### Topology-change tests

The more valuable local tests are the failure-injection ones:

  1. **Node kill**: start 3 nodes serving 100k keys; pull the plug on one mid-test.
     - v0.2 with R=1: requests for that node's keys fail. Cluster topology view shrinks.
     - v0.4+ with R>1: replicas take over reads + writes seamlessly.

  2. **Node return**: bring the killed node back.
     - v0.2: it rejoins as an empty node. New keys hashing to it will land; old keys it used to own stay on the temporary takeover node (which v0.2 doesn't have, so they're just gone).
     - v0.3+: rebalancer migrates keys back from successors; ownership returns to the rejoined node.

  3. **Network partition**: simulate one side losing sight of the other (block memberlist UDP between two groups of processes).
     - v0.2 with R=1: both sides accept writes to keys they think they own. On heal, writes diverge; conflicts resolve via LWW or whatever policy is configured.
     - v0.4+ with R>=2: quorum reads/writes prevent split-brain divergence.

  4. **Slow node**: tc/iptables-induce 500ms latency on one node's gRPC.
     - Validates timeouts + retries at the cluster layer.
     - Validates that the bounded-loads consistent hashing doesn't pile work onto a struggling node.

  5. **Backend failure**: kill the MinIO instance one node depends on.
     - Validates that the node detects the failure, marks itself unhealthy, propagates that to memberlist, peers route around it.

### Test framework

The `tests/integration/` directory (per CLAUDE.md layout) holds in-process tests that spin up N nodes via goroutines + ephemeral ports. No external services required for the basic correctness tests. The MinIO-backed scaling tests are separate (`tests/scaling/`) and require an operator to bring up MinIO first.

The CI matrix runs the integration tests on every PR; the scaling tests run on demand.

---

## Roadmap

- [ ] **v0.1** - single-node Cluster wrapping one Backend; memory backend impl; SlateDB backend impl; gRPC service (used by CLI + ready for v0.2 inter-node); `shaled` standalone binary; `shale` CLI with put/get/delete/scan/topology/stats/ping. API lockup.
- [ ] **v0.2** - multi-node + memberlist + hash ring + gRPC forwarding. Static topology (no rebalance). `shale topology` now shows real membership + ring.
- [ ] **v0.3** - rebalancing on join/leave. Atomic ownership swap. `shale rebalance` + `shale migrate-from` subcommands.
- [~] **v0.4** (in progress) - replication factor R + tunable consistency. LWW conflict resolution. Sub-tasks:
  - [ ] LWW value envelope (Stamp + Payload), encoded on Put / decoded on Get, transparent to Backend
  - [ ] v0.3-value compatibility: bare values decode as `Stamp{0, ""}`, re-stamped on next Put
  - [ ] `Config.ReplicationFactor` (default 1) + `ring.LocateKeyN(key, R)`
  - [ ] `Config.WriteConsistency` (One / Quorum / All, default Quorum) with parallel fan-out + ack accounting
  - [ ] `Config.ReadConsistency` (Nearest / Quorum / All, default Nearest)
  - [ ] LWW comparator (higher TimestampNanos wins, NodeID lex tiebreak; stamped by originator pre-fan-out)
  - [ ] Async read-repair on Quorum / All when replicas disagree (skipped on Nearest; best-effort, errors swallowed)
  - [ ] Delete as empty-payload tombstone envelope; Get treats empty payload as NotFound
  - [ ] Failure handling: tolerate down replicas up to (R - W) / (R - N); Unavailable when all R down
- [ ] **v0.4.1** - hinted handoff for replicas down at Put time; foundations for anti-entropy.
- [ ] **v0.5** - Prometheus metrics, tracing hooks, real benchmarks vs single-node SlateDB. `shale bench` subcommand.
- [ ] **v0.6** - hostthis migration: swap raw SlateDB for shale-with-SlateDB-backend. Validate on production-like data.

Each version ships independently; users can adopt v0.1 today (functionally equivalent to using their Backend directly, plus the CLI for daily ergonomics) and grow into v0.2+ when their workload demands it.

---

## Inspirations

- **Olric** - same memberlist + consistent-hash pattern. In-memory only; we generalize to durable backends.
- **Vasto** - sharded RocksDB with jump consistent hash + single-master topology. Dormant; architecture is sound and we borrow heavily. We diverge by using ring-based hashing + gossip membership instead.
- **Cassandra / ScyllaDB** - shard-per-core + gossip + ring topology. Heavyweight services; we want library shape with the same model.
- **DynamoDB** - the access-pattern discipline. DDB forces you to commit to a partition key at table-creation time, which drives the rest of your data model. shale relaxes this to per-key hash tags, but the underlying principle (think about access patterns before key design) is the same.
- **Redis Cluster** - the hash-tag convention (`{tag}`-based key co-location) is borrowed directly.
- **SlateDB** - the default backend. Provides KV semantics on cheap object storage; we provide horizontal scale-out around it.
