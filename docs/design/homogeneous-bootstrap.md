# Homogeneous bootstrap: runtime try-join-else-form

Status: DESIGN (spec-first; no code yet)

## Problem

A shale deployment today is HETEROGENEOUS at the deployment layer: a
dedicated "seed" pod (its own StatefulSet, `HOSTTHIS_SHALE_SEEDS` empty) plus
N "joiner" pods (a second StatefulSet, `SEEDS` pointing at the seed's DNS).
The split exists for ONE reason: the form-vs-join decision is **config-driven**.

In `cluster.Open` (`pkg/cluster/cluster.go`), the rule is structural:

- `BindAddr == ""` -> single-node, local-only.
- `BindAddr != ""` -> multi-node: `membership.Open(Seeds)` joins the seeds.
- `len(Seeds) == 0` -> founder: forms the 1-node ring, keeps `initGenState`'s
  gen-0 (`multibackend.go`: the `len(c.cfg.Seeds) > 0` branch is the joiner
  generation-learning; no seeds skips it).
- `len(Seeds) > 0` -> joiner: learns the live `{generation, unit-count}` from a
  seed before serving.

Because "empty seeds" is a static config flag, the founder needs a DIFFERENT
config than the joiners, which forces two StatefulSets. The runtime peers are
already homogeneous (same ownership math, same gossip, no leader); only
bootstrap config + the deployment packaging differ.

This also causes bug #453: a RESTARTING founder still has empty seeds, so it
re-forms a fresh gen-0 1-node ring (desiring every position) instead of
rejoining the live ring. The config flag says "you're the founder" every boot,
even when there is already a ring to join.

## Goal

One StatefulSet of `0..N` IDENTICAL pods. Every pod has the same config;
whoever boots first bootstraps the ring; a restart REJOINS the live ring.
This is how etcd / consul / cassandra run (homogeneous N-node, any node seeds).
It retires the seed pod and fixes #453 with the same change.

## Design

### 1. One config for every pod

`SEEDS` = a stable **headless Service** DNS that resolves to ALL the
StatefulSet's pods (`hostthis-headless` -> A records for `hostthis-0..N`).
Identical for every pod. No "empty seeds" special case.

### 2. Bootstrap = runtime try-join-else-form

On `Open` (multi-node), the node:

1. **Try to JOIN.** `membership.Open` Joins the seed list (the headless
   Service). memberlist resolves it to the live peer set.
   - If Join returns peers (a ring already exists) -> this node is a JOINER:
     learn the live `{generation, unit-count}` from a peer (the existing
     `len(Seeds)>0` path), then serve. NO re-form.
   - If Join returns NO peers after a bounded discovery window -> this node may
     need to FORM. Go to step 3.
2. The discovery window is bounded (re-resolve the headless Service + retry Join
   for `BootstrapJoinTimeout`, e.g. a few seconds). A fresh pod whose peers are
   still starting keeps retrying; it does not prematurely decide it is alone.
3. **Contend to FORM via a CAS-lease.** Forming is NOT unconditional - see the
   concurrent-first-boot race below. The node attempts a conditional write
   (`If-None-Match: *`) of a `cluster-init` marker in shared object storage. The
   marker records the cluster's founding `{generation: 0, unit-count}`.
   - **CAS wins** -> this node is the founder: form the gen-0 ring, mount the
     whole keyspace, start serving. Other pods will discover it on their next
     Join and become joiners.
   - **CAS loses** (someone else wrote the marker first) -> re-Join: the winner
     is now a live member; this node joins it as a normal joiner.

### 3. The concurrent-first-boot race (the hard part)

A fresh cluster (or a full-cluster restart) brings up all N pods at once. They
ALL Join, find no existing ring (nobody is up yet), and ALL reach step 3. Without
a tiebreak they ALL form -> N split-brain gen-0 rings -> data divergence.

The `cluster-init` CAS-lease is the tiebreak: exactly ONE conditional-write of
the marker succeeds; the other N-1 get a precondition failure, fall back to
re-Join, and find the winner (directly, or transitively once gossip converges).
This reuses shale's existing CAS / conditional-write primitive (the same
`If-None-Match` semantics the serving markers and slatedb manifest fencing rely
on - see #435 CAS-lease serving markers).

memberlist gossip alone is NOT a sufficient tiebreak: two pods that haven't yet
gossip-connected can both believe they are alone. The CAS-lease is the
authoritative single-winner; gossip then converges everyone onto the winner.

### 4. Generation persistence (so a full restart resumes, not re-forms)

CONFIRMED by the code map (2026-06-23): `{generation, unit-count}` is **NOT
persisted anywhere durable today** - it lives only in `Cluster.genState`
(in-memory), seeded to gen-0 by `initGenState` (`multibackend_reshard.go`), and a
joiner learns the live value via the `GenState` RPC from a seed
(`learnGenerationFromSeed`, `multibackend_join_gen.go`), never from disk. So a
joiner that finds peers is fine, but a FULL-cluster restart (no peer to ask)
would reset everyone to gen-0 while the data sits at gen-N -> generation loss.
(Latent: prod is still gen-0, never resharded - but the homogeneous design must
not bake the bug in.)

So this design ADDS the missing persistence, folded into the same marker: the
`cluster-init` CAS marker carries `{generation, unit-count}` as its VALUE. It is
the durable source of truth. Write/update rules:
- FORM (marker absent) -> `PutIfAbsent(initKey, {gen:0, count:N})`.
- Reshard -> `CompareAndSet(initKey, {gen:bumped, count:2N}, expectedVersion)`
  so the durable record advances in lockstep with the in-memory `genState`.
- RESUME / any boot -> read the marker to get the current `{gen, count}` instead
  of trusting `initGenState`'s gen-0.

On boot, BEFORE deciding form-vs-join, every node reads the marker from shared
storage:

- Marker present -> the cluster exists. Join the live ring (peers up) or, if
  every peer is down (full restart), the first node to win a "resume" lease
  forms the ring AT THE PERSISTED GENERATION (not gen-0); the rest join it.
- Marker absent -> truly fresh cluster -> the CAS-lease founding above writes
  gen-0.

So "form" splits into FOUND (marker absent, gen-0) and RESUME (marker present,
all peers down, adopt the persisted generation). Both are CAS-lease-gated to a
single winner. A normal restart (some peers up) never reaches either - it Joins.

### 5. Deployment shape

- ONE StatefulSet `hostthis`, `replicas: N`, identical pod template.
- A headless Service (`clusterIP: None`) selecting the StatefulSet's pods;
  `SEEDS` = that Service's DNS.
- Stable per-pod identity still comes from the StatefulSet (pod name / ordinal /
  the optional PVC) - that is unchanged and still required (it is what makes a
  restarted pod re-own and re-mount ITS OWN units zero-copy, vs a cluster-wide
  re-mount reshuffle). The homogeneity is in the CONFIG + the bootstrap, not in
  dropping stable identity.

## Cutover (prod, gated on staging)

Data is zero-copy in object storage; nothing is copied. But the new pods have new
names (`hostthis-0..2` vs `hostthis-shard-seed-0` + `hostthis-shard-0/1`), so
consistent-hash ownership REMAPS to the new identities: a one-time re-mount
reshuffle (zero bytes moved, just slatedb re-opens, ~9s each now that the
version-bloat cold-start fix is in). With R=2 covering each unit:

1. Stand up the new single StatefulSet + headless Service alongside (scaled 0).
2. Scale the old two StatefulSets to 0 (brief quiesce).
3. Scale the new StatefulSet to N; it founds/resumes via the CAS-lease, mounts
   the units from object storage, serves.
4. Verify ring membership, writes, a kill-a-pod rejoin test, then delete the old
   StatefulSets.

A short cutover window (seconds-to-low-minutes, R=2 + ~9s opens). This MUST be
validated on the staging cluster first (kill-pod -> rejoins-not-reforms,
full-restart -> resumes-generation, concurrent-boot -> single founder).

## Test plan

- **Concurrent first boot:** start N nodes simultaneously against a fresh shared
  store -> exactly one founder (CAS-lease single-winner), one gen-0 ring, no
  split-brain. (In-process integration test, the existing 3-node harness.)
- **Restart rejoins (#453):** with the cluster up, restart one node -> it Joins
  the live ring, does NOT re-form a 1-node ring, resumes its owned units.
- **Full-cluster restart resumes generation:** bump the generation (reshard),
  stop all nodes, restart all -> the cluster resumes at the bumped generation,
  not gen-0; ownership matches pre-restart.
- **Single-winner under partition:** two nodes that cannot gossip-connect still
  produce one founder (the CAS-lease, not gossip, is the tiebreak).

## Open questions

- Where exactly the generation record lives relative to the `cluster-init`
  marker (one object vs marker + separate gen record), and whether the existing
  joiner generation-learning (`multibackend.go`) reads it directly.
- The `BootstrapJoinTimeout` value: long enough that a slow-starting peer is
  discovered before a node decides to contend-to-form, short enough that a
  genuine fresh cluster founds promptly.
- Whether the "resume" lease and the "found" lease are the same CAS marker with
  a generation field, or two markers. Likely one marker keyed by cluster-id with
  a generation field; the CAS is on its existence (found) and the resume is a
  short TTL lease on top.
