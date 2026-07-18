// Package cluster is shale's distribution layer and its public API surface:
// apps call cluster.Open(cfg) to get a Cluster handle that turns one or more
// storage backends on one or more nodes into a single keyspace. It owns
// routing, replication, membership-driven handoff and resharding. See
// docs/SPEC.md for the full model.
//
// # Two independent axes
//
// Do not confuse them; almost every predicate in the package tests one or the
// other, and a few (noted below) test both.
//
//   - NODE COUNT is set by Config.BindAddr. Empty means single-node: no
//     membership, and c.ring stays NIL. Non-empty means multi-node: Open brings
//     up a membership.Membership on BindAddr (joining via Config.Seeds) and a
//     ring.Ring fed from membership events.
//   - BACKEND MODE is set by Config.Backend vs Config.BackendFactory +
//     Config.UnitCount, plus Config.ReplicationFactor. That is the three-mode
//     matrix below.
//
// # Reading this package
//
// The package has THREE operating modes and THREE reshard mechanisms, and they
// are selected by different predicates in different files. This doc is the map:
// every table below is derived from the CODE (the predicates as written), so it
// stays honest even where the prose in docs/SPEC.md has drifted. The
// disagreements found while deriving it are listed at the end.
//
// # Mode decision matrix
//
// A mode is fixed at Open by validateBackendMode (cluster.go) plus
// Config.ReplicationFactor. Nothing re-selects a mode at runtime.
//
//	MODE                       CONFIG PREDICATE                        SELECTED BY / SET IN
//	-------------------------- --------------------------------------- ---------------------------------
//	legacy per-node            Config.Backend != nil                   validateBackendMode -> multi=false
//	                           (BackendFactory + UnitCount BOTH unset)  cluster.go
//	multi-backend R=1          BackendFactory != nil && !UnitCount      validateBackendMode -> multi=true
//	                           .IsZero() && ReplicationFactor <= 1      cluster.go
//	multi-backend R>1          the same, with ReplicationFactor > 1;    validateBackendMode (mode) +
//	                           the factory MUST also implement          initReplicatedFactory
//	                           storageunit.ReplicaBackendFactory        multibackend_replicated.go
//	                           (validated at Open, not at first use)
//
// Setting Backend AND (BackendFactory or UnitCount) is an Open error, as is
// setting only one of BackendFactory / UnitCount. ReplicationFactor 0 is
// normalized to 1 (normalizeConfig), so R=1 is the default.
//
// # The two mode predicates are NOT the same test
//
// This is the single most confusing thing in the package, so it is stated
// plainly. Two different predicates select "the R>1 path", and they disagree on
// a single-node cluster:
//
//	PREDICATE                 DEFINED IN                    TRUE WHEN
//	------------------------- ----------------------------- -----------------------------------------
//	c.replicaFactory != nil   initReplicatedFactory         multi && ReplicationFactor > 1
//	                          multibackend_replicated.go    (says nothing about the ring)
//	c.multiReplicated()       multibackend_replicated.go    multi && ReplicationFactor > 1
//	                                                        && ring != nil && !ring.Empty()
//
// c.ring is constructed ONLY on the multi-node branch of Open (Config.BindAddr
// set, cluster.go), so on a SINGLE-NODE cluster at R>1 the ring is nil and the
// two predicates diverge:
//
//   - the MOUNT + RECONCILE side keys off replicaFactory, so it takes the R>1
//     replica-position path (mountReplicaUnits, reconcileReplicaUnitsOverlap);
//   - the READ/WRITE side keys off multiReplicated(), so it takes the R=1
//     single-mount path (one node is one replica, which is the correct answer).
//
// So a single-node R>1 cluster mounts by ReplicaUnit but serves by single
// ownership. This is intentional, not a bug, but it means "is this cluster R>1?"
// has two answers and you must pick the one for the side you are on.
//
// A THIRD spelling of the same idea exists for the LEGACY path: "R>1 on the
// legacy per-node backend" is casReplicated() (cas.go), whose body is
// replicationFactor() > 1 && ring != nil && !ring.Empty() - and that expression
// is ALSO written out inline in Put, Get and Delete (cluster.go). It is
// multiReplicated() minus the c.multi conjunct. When adding a call site, reach
// for the named predicate rather than re-spelling the expression.
//
// # Where each mode dispatches
//
//	CONCERN            LEGACY                 MULTI R=1                    MULTI R>1
//	------------------ ---------------------- ---------------------------- ------------------------------
//	predicate          !c.multi               c.multi, replicaFactory==nil c.multiReplicated() (r/w) or
//	                                                                       replicaFactory!=nil (mount)
//	Put / Delete       c.backend directly     ownerOf + forward, freeze    putReplicatedUnit
//	                   (+ legacy R>1 replicate)  gate applies              multibackend_replicated.go
//	                   cluster.go, replicate.go  cluster.go                cluster.go entry
//	Get                c.backend / replicated getUnit via ownerOf          getReplicatedUnit
//	                   read path (replicate.go)  cluster.go                multibackend_replicated.go
//	boot mount         none (one backend)     initMultiBackend unit loop   mountReplicaUnits
//	                                          multibackend.go              multibackend_replicated.go
//	reconcile          rebalance.go           reconcileUnits unit loop     reconcileReplicaUnitsOverlap
//	                   (per-node key ranges)  multibackend_rebalance.go    multibackend_overlap.go
//	handoff shape      key-range migration    clean-cut lease handoff      overlap (acquire-then-release)
//	                                          (release then acquire)       gated by a serving marker
//
// The reconcile dispatch lives in one place, reconcileUnits
// (multibackend_rebalance.go): it branches on replicaFactory != nil into the
// R>1 half (declarative target, decentralized reshard step, overlap reconcile,
// drain checks, joining-state maintenance) and otherwise falls through to the
// R=1 GenUnit loop. Config.TestingForceCleanCut is a break-demo escape hatch in
// the R>1 half that swaps the overlap reconcile for the pre-overlap clean-cut
// one; it is never set in production.
//
// # Reshard decision matrix
//
// There are THREE reshard mechanisms. Which one runs is NOT a single switch:
// two of them are reached by an imperative call and one only by the reconcile
// tick, so an R>1 cluster can have two of them wired at once.
//
//	MECHANISM                  ENTRY                       PREDICATE                        FILE
//	-------------------------- --------------------------- -------------------------------- -----------------------------
//	single-node online bisect  Cluster.Reshard()           multi && (ring == nil ||         multibackend_reshard.go
//	                                                       len(ring.Members()) <= 1)
//	coordinated freeze barrier Cluster.Reshard()           multi && ring != nil &&          multibackend_reshard_barrier.go
//	                           -> reshardCoordinated()     len(ring.Members()) > 1
//	                                                       (NO R check, NO arbiter check)
//	decentralized arbiter      reconcileUnits() tick       replicaFactory != nil (R>1) &&   multibackend_reshard_driver.go
//	                           -> observeReshard()         c.arbiter != nil, which needs    (+ _arbiter, _declared,
//	                                                       Config.ConditionalStore != nil   _copy, _marker, _route)
//
// Cluster.Reshard() itself refuses in legacy mode and is serialized by
// reshardMu; the arbiter driver takes reshardMu too, so the imperative and
// decentralized paths cannot interleave, only alternate.
//
// The arbiter's TARGET (what the decentralized driver converges toward) is set
// one of two ways:
//
//	TARGET SOURCE      PREDICATE                            ENTRY / FILE
//	------------------ ------------------------------------ ---------------------------------------
//	external           Config.DeclarativeReshard == false    reshard.Arbiter.Retarget, called by a
//	                   (the default)                         test or an external driver
//	declared config    Config.DeclarativeReshard == true     observeDeclaredReshardTarget
//	                   && arbiter != nil && membership != nil multibackend_reshard_declared.go
//
// The declared path additionally requires the cluster to be STEADY (no split in
// flight), the arbiter to have converged (State.Count == State.Target), and
// UNANIMITY: every live member must advertise the same non-zero declared unit
// count in its membership metadata. A single member advertising 0 (an older
// image, or a pod mid-roll) is a VETO, which is what keeps a rolling deploy from
// flapping the target back and forth.
//
// So, to answer "which reshard runs" for a given deployment, ask in this order:
//
//  1. Is it legacy mode? Reshard() errors; there is no reshard.
//  2. Did something call Reshard() (or the operator-facing Reshard RPC)? Then
//     ring member count alone decides: 1 member -> inline bisect, >1 -> freeze
//     barrier. R and the arbiter are NOT consulted.
//  3. Otherwise, is an arbiter wired (R>1 + ConditionalStore)? Then every
//     reconcile tick steps any in-flight split/merge through observeReshard,
//     with the target set externally or from the declared count.
//
// # Where the SPEC disagrees with the code
//
// Derived 2026-07 by reading the predicates; recorded here rather than silently
// "fixing" either side.
//
//   - docs/SPEC.md "v0.9" says the decentralized reshard "replaces" the
//     imperative barrier "on the R>1 path", scoping the freeze barrier to R=1.
//     The code does not scope it: Cluster.Reshard() branches on ring member
//     count ONLY, so calling Reshard() (or the operator Reshard RPC, which the
//     SPEC documents as calling c.Reshard()) on a MULTI-NODE R>1 cluster runs
//     reshardCoordinated(), arbiter or not. The decentralized driver is an
//     ADDITIONAL path reached only from the reconcile tick, not a replacement.
//     Either Reshard() should refuse (or delegate) when an arbiter is wired, or
//     the SPEC should describe the two as coexisting. Not changed here: picking
//     between those is a behavior decision, not a documentation one.
//
//   - The same SPEC sentence says "the R=1 multi-node path keeps the coordinated
//     freeze barrier". True, but incomplete in the other direction: R=1 is not
//     what selects the barrier, ring size is.
//
// # File map
//
// Roughly in dependency order, so a first read can follow it top to bottom:
//
//	cluster.go                      Config, Open, mode validation, the public
//	                                Put/Get/Delete/Scan/Aggregate surface and
//	                                its dispatch
//	kv.go, replicate.go             legacy per-node key path + R>1 replication
//	cas.go, castx.go                CAS: owner-side validate-and-apply (cas.go)
//	                                and the cluster-side OCC tx buffer (castx.go)
//	peerclient.go                   the cluster-internal gRPC client
//	aggregate_snapshot.go           the iterator / snapshot adapters that let
//	                                Aggregate's cross-node fan-out present a
//	                                remote peer as one backend.Backend
//	multibackend_union_scan.go      the union scan across overlapping owners
//	rebalance.go                    legacy per-node key-range migration
//	multibackend.go                 multi-backend Open, unit mount, R=1 routing
//	multibackend_rebalance.go       reconcileUnits: THE reconcile dispatch
//	multibackend_replicated.go      R>1 mount, replicated read/write paths
//	multibackend_overlap*.go        R>1 overlap handoff (acquire-then-release)
//	multibackend_reshard.go         Reshard() entry + single-node online bisect
//	multibackend_reshard_barrier.go the coordinated cluster-wide freeze barrier
//	multibackend_reshard_arbiter.go arbiter construction/seeding + the pure
//	                                genState step function
//	multibackend_reshard_driver.go  the per-tick decentralized split/merge driver
//	multibackend_reshard_declared.go  declared-count -> arbiter target reconcile
//	mount_readiness.go              MountReadiness / Ready probe surface
//	debug_state.go                  the /debug/shale/state dump
package cluster
