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
//   - NODE COUNT is set by Config.Coordinator (the coordination port,
//     pkg/coord). Nil means single-node: no coordination, and c.coord stays
//     NIL, so every placement question answers "me". Non-nil means multi-node:
//     Open STARTS the coordinator (handing it this node's identity, declared
//     unit count and initial role set) and asks it who should hold each unit.
//     HOW it answers - SWIM gossip plus a consistent hash ring today
//     (pkg/coord/gossip), a lease/CAS table later - is invisible here.
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
//	multi-backend R>1          the same, with ReplicationFactor > 1     validateBackendMode (mode) +
//	                                                                    replicaLayout()
//	                                                                    multibackend_replicated.go
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
//	c.replicaLayout()         multibackend_replicated.go    multi && ReplicationFactor > 1
//	                                                        (says nothing about coordination)
//	c.multiReplicated()       multibackend_replicated.go    multi && ReplicationFactor > 1
//	                                                        && coord != nil && view not empty
//
// c.coord is set ONLY on the multi-node branch of Open (Config.Coordinator
// non-nil, cluster.go), so on a SINGLE-NODE cluster at R>1 there is no
// coordination view and the two predicates diverge:
//
//   - the MOUNT + RECONCILE side keys off replicaLayout(), so it takes the R>1
//     replica-position path (mountReplicaUnits, reconcileReplicaUnitsOverlap);
//   - the READ/WRITE side keys off multiReplicated(), so it takes the R=1
//     single-mount path (one node is one replica, which is the correct answer).
//
// So a single-node R>1 cluster mounts by ReplicaUnit but serves by single
// ownership. This is intentional, not a bug, but it means "is this cluster R>1?"
// has two answers and you must pick the one for the side you are on.
//
// replicaLayout() is additionally the ON-DISK LAYOUT SELECTOR: it is what
// mountRefFor turns each ReplicaUnit into, and therefore what decides whether
// this cluster's bytes live at the unit's own location or at a per-replica
// child of it. Substituting multiReplicated() there would make a single-node
// R>1 cluster address mounts its own earlier boots wrote somewhere else.
//
// A THIRD spelling of the same idea exists for the LEGACY path: "R>1 on the
// legacy per-node backend" is casReplicated() (cas.go), whose body is
// replicationFactor() > 1 && coord != nil && the view is non-empty - and that
// expression
// is ALSO written out inline in Put, Get and Delete (cluster.go). It is
// multiReplicated() minus the c.multi conjunct. When adding a call site, reach
// for the named predicate rather than re-spelling the expression.
//
// # Where each mode dispatches
//
//	CONCERN            LEGACY                 MULTI R=1                    MULTI R>1
//	------------------ ---------------------- ---------------------------- ------------------------------
//	predicate          !c.multi               c.multi, !replicaLayout()    c.multiReplicated() (r/w) or
//	                                                                       replicaLayout() (mount)
//	Put / Delete       c.backend directly     ownerOf + forward, under     putReplicatedUnit
//	                   (+ legacy R>1 replicate)  the per-unit write-pause  multibackend_replicated.go
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
// (multibackend_rebalance.go): the arbiter-driven reshard step (declarative
// target + observeReshard) runs FIRST on every tick regardless of R, then the
// pass branches on replicaLayout() into the R>1 half (overlap reconcile, drain
// checks, joining-state maintenance) or the R=1 GenUnit acquire/release loop
// plus the orphaned-hold sweep. Config.TestingForceCleanCut is a break-demo
// escape hatch in the R>1 half that swaps the overlap reconcile for the
// pre-overlap clean-cut one; it is never set in production.
//
// # Reshard decision matrix
//
// There are TWO reshard mechanisms: the single-node inline bisect (an
// imperative call, nobody to agree with) and the decentralized CAS arbiter
// (driven from the reconcile tick, for ANY R). The imperative multi-node entry
// DELEGATES to the arbiter.
//
//	MECHANISM                  ENTRY                       PREDICATE                        FILE
//	-------------------------- --------------------------- -------------------------------- -----------------------------
//	single-node online bisect  Cluster.Reshard()           multi && len(Members()) <= 1     multibackend_reshard.go
//	decentralized arbiter      reconcileUnits() tick       c.arbiter != nil, which needs    multibackend_reshard_driver.go
//	                           -> observeReshard();        Config.ConditionalStore != nil;  (R>1 drive; + _arbiter,
//	                           Cluster.Reshard() on a      the drive branches on            _declared, _copy, _marker,
//	                           multi-node cluster          replicaLayout(): R>1 envelope    _route) and
//	                           -> reshardDelegated()       dual-write vs R=1 parent-        multibackend_reshard_r1.go
//	                           (Retarget + converge)       anchored raw-bytes               (R=1 drive + delegation)
//
// Cluster.Reshard() refuses in legacy mode, and on a multi-node cluster
// refuses with ErrReshardNeedsConditionalStore when no arbiter is wired. The
// single-node bisect is serialized by reshardMu; the arbiter driver runs under
// reshardMu + reconcileMu on the reconcile tick, so the two cannot interleave.
// At R=1 the arbiter drive supports SPLIT only: a merge target is refused
// (never entered, never advanced toward) because R=1 stores raw bytes and the
// cross-node merge copy needs the R>1 envelope ordering.
//
// The arbiter's TARGET (what the decentralized driver converges toward) is set
// one of two ways:
//
//	TARGET SOURCE      PREDICATE                            ENTRY / FILE
//	------------------ ------------------------------------ ---------------------------------------
//	external           Config.DeclarativeReshard == false    reshard.Arbiter.Retarget, called by a
//	                   (the default)                         test or an external driver
//	declared config    Config.DeclarativeReshard == true     observeDeclaredReshardTarget
//	                   && arbiter != nil && coord != nil      multibackend_reshard_declared.go
//
// The declared path additionally requires the cluster to be STEADY (no split in
// flight), the arbiter to have converged (State.Count == State.Target), and
// UNANIMITY: every live member must advertise the same non-zero declared unit
// count through the coordination view. A single member advertising 0 (an older
// image, or a pod mid-roll) is a VETO, which is what keeps a rolling deploy from
// flapping the target back and forth.
//
// So, to answer "which reshard runs" for a given deployment, ask in this order:
//
//  1. Is it legacy mode? Reshard() errors; there is no reshard.
//  2. Did something call Reshard()? On a single-node cluster the inline bisect
//     runs. On a multi-node cluster it DELEGATES to the arbiter (Retarget the
//     agreed count + wait for local convergence), refusing with a typed error
//     when no ConditionalStore is configured.
//  3. Otherwise, is an arbiter wired (ConditionalStore, any R)? Then every
//     reconcile tick steps any in-flight split/merge through observeReshard,
//     with the target set externally (Retarget / a delegated Reshard) or from
//     the declared count.
//
// # File map
//
// Roughly in dependency order, so a first read can follow it top to bottom:
//
//	cluster.go                      Config, Open, mode validation, the coordination
//	                                loops, and the public
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
//	multibackend_reshard_r1.go      the R=1 parent-anchored drive + the
//	                                delegated multi-node Reshard entry
//	multibackend_reshard_arbiter.go arbiter construction/seeding + the pure
//	                                genState step function
//	multibackend_reshard_driver.go  the per-tick decentralized split/merge driver
//	multibackend_reshard_declared.go  declared-count -> arbiter target reconcile
//	mount_readiness.go              MountReadiness / Ready probe surface
//	debug_state.go                  the /debug/shale/state dump
package cluster
