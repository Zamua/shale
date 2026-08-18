# Switching coordination adapters on a live cluster (gossip -> CAS)

How to move a running shale cluster from one coordination adapter to another.
Written for the gossip -> CAS direction; the choreography is symmetric.

## Why there is no rolling switch

The two adapters discover members through disjoint mechanisms: gossip nodes
see each other via SWIM over the bind port; CAS nodes see each other via the
membership document in the conditional store. A node on one mechanism is
INVISIBLE to a node on the other. A rolling deploy that flips the adapter
pod-by-pod therefore partitions the cluster into two half-clusters, each with
a complete-looking view that omits the other half - both halves compute
placement over their own view and believe they own everything.

The blast radius is bounded by the storage layer, not the coordinator: the
durable epoch fence still guarantees at most one serving owner per unit (the
halves fence each other's mounts back and forth), so this mis-deploy costs
availability chaos - fence ping-pong, mass acquiring-window refusals - rather
than data loss. It is still an outage. Do not roll the switch.

**Invariant: no gossip-configured node may run concurrently with a
CAS-configured node against the same storage.** The switch is a full-stop.

## What survives the switch untouched

- **All data and all durable coordination state.** Unit databases, durable
  epochs, and serving markers live in the storage layer and are
  adapter-independent.
- **Placement.** Both adapters feed the same consistent-hash ring with the
  same member IDs, so a cluster whose NodeIDs are unchanged computes IDENTICAL
  key->unit->node placement before and after the switch. Units reopen exactly
  where they were; nothing relocates. (Corollary: keep NodeIDs stable across
  the switch, or placement moves and the boot becomes a mass handoff.)
- **Epoch discipline.** Every reopen bumps the fence at open start and
  republishes the serving marker at Ready, exactly as any restart does.

## The choreography

1. **Prepare the CAS configuration** ahead of time: a
   `storageunit.ConditionalStore` (the same store class the slate backend
   already uses) plus a document `Key` naming this cluster. Validate the
   config on a staging cluster first - including one full stop/start cycle.
2. **Disable the leave drain for the stopping deploy**: set
   `GracefulLeaveDrainTimeout` to 0. Drain-for-leave hands positions to
   successors; in a full stop there are none, so every node would otherwise
   stall its shutdown for the full drain budget waiting on hand-offs that
   cannot happen. With the timeout at 0, Close skips the drain and proceeds
   straight to flush-and-close.
3. **Scale to zero, gracefully.** SIGTERM -> Close flushes and closes every
   mount. Under relaxed backend durability a clean Close is what makes the
   acked-but-unflushed window empty; do not hard-kill the pods.
4. **Verify quiescence**: zero pods running.
5. **Flip the adapter config** (remove the gossip BindAddr/Seeds wiring;
   construct the CAS coordinator with the prepared store + key) and restore
   `GracefulLeaveDrainTimeout` to its normal value.
6. **Scale back up.** The first node to claim the membership document
   bootstraps as the founder; the rest observe it and join. Nodes mount their
   (unchanged) placements, epochs bump, markers publish.
7. **Verify**: MountReadiness pending=0 on every node; PlacementMembers
   agrees with View; no fence-recode churn in the logs after settling.

## Rollback

Symmetric: full stop, flip the config back to gossip, scale up. The CAS
membership document is additive state; a gossip cluster simply ignores it.
Delete the document key once the migration is final.

## Retiring the gossip adapter afterwards

Once no deployment runs the gossip adapter, it can leave the tree entirely:
`pkg/coord/gossip`, `pkg/membership`, and the memberlist dependency (plus its
transitive tree). The coordination PORT (`pkg/coord`) and the contract harness
(`internal/coordcontract`) stay - an out-of-tree adapter implements the same
interface and can pin itself with the same contract tests.
