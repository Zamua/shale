package cluster

// blob_units.go adds the unit-resolution accessors the blob byte plane needs
// (design section 11.5 / 11.7). The blob object key is UNIT-keyed
// (blob/<unit>/<blobid>), so StageBlob must derive the routed unit token for a
// route key, and the orphan sweep must enumerate the units this node has
// MOUNTED. RoutedUnitToken is a pure function of the cluster's
// generation-aware routing state (genUnitForKey); MountedUnits reads the live
// mount map. This file only renders them as the stable string tokens the
// object key uses and exposes them on *Cluster.
//
// The sweep enumerates MOUNTED units (not desired ownership) so its
// object-listing loop and its referenced-pointer scan read the SAME ownership
// view (the pointers a mounted unit's local scan can see). A desired-but-
// unmounted unit (cold boot / mid rebalance-acquire) is skipped this pass: a
// missed sweep is a storage leak, never data loss. The reshard-era reclamation
// of objects sitting under an OLD-generation parent token prefix (after a
// doubling re-keys a unit) is a documented leak-only follow-up and is NOT
// shipped here; the old reshard-ancestor union machinery is removed.
//
// The unit TOKEN is rendered "<gen>-<unitID>" in DECIMAL (e.g. "0-13"), NOT the
// GenUnit.String() form "g<gen>/u<id>": the token is a path segment in the
// object key blob/<unit>/<blobid>, so it must not contain a '/', and it always
// contains a '-' so it can never collide with the legacy sentinel "legacy"
// (design section 11.11 item 4). In LEGACY (non-multi) mode there are no units;
// the single node owns the whole keyspace, so the token is the fixed sentinel
// "legacy" and the sweep enumerates the one prefix blob/legacy/.

import (
	"strconv"

	"github.com/Zamua/shale/pkg/storageunit"
)

// legacyUnitToken is the unit token used in non-multi (legacy) mode, where the
// single backend owns the whole keyspace and there are no storage units. It can
// never collide with a real token because real tokens are "<gen>-<unitID>" and
// always contain a '-' (design section 11.11 item 4).
const legacyUnitToken = "legacy"

// unitToken renders a GenUnit as the stable "<gen>-<unitID>" decimal string the
// blob object key uses. It is the canonical rendering for the blob plane; every
// place that derives or enumerates a unit token MUST go through here so the
// object key (StageBlob), the pointer key (BindBlob via BlobRef.Unit), and the
// sweep prefix (SweepOrphans) agree by construction.
func unitToken(gu storageunit.GenUnit) string {
	return strconv.FormatUint(uint64(gu.Gen), 10) + "-" + strconv.FormatUint(uint64(gu.ID), 10)
}

// RoutedUnitToken returns the stable unit token the bytes for ANY routeKey are
// keyed under: blob/<token>/<blobid>. It is a PURE function of routeKey and the
// current routing state (no network, no lease): it uses the SAME ShardKeyFn +
// hash + generation resolution the routed write uses (genUnitForKey), so a
// blob's object key lands under the same unit the pointer (and the app's
// metadata) route to.
//
// It is NAMED "routed" deliberately: it answers "which token does this key route
// to", NOT "does this node own that unit" - it performs no ownership check, so a
// caller must not read it as an ownership assertion. In multi-backend mode the
// token is "<gen>-<unitID>" for the key's resolved GenUnit; in legacy mode there
// is one keyspace and the token is the "legacy" sentinel. Used by StageBlob (to
// key the object) and GetBlob (to build the route shard for the bref read; see
// BlobKV.GetBlob).
func (c *Cluster) RoutedUnitToken(routeKey []byte) string {
	if !c.multi {
		return legacyUnitToken
	}
	return unitToken(c.genUnitForKey(routeKey))
}

// MountedUnits returns the unit tokens whose blob objects the orphan sweep may
// reclaim this pass: exactly the units this node currently has MOUNTED, one
// finite prefix blob/<token>/ per token (design section 11.7). This is the set
// whose pointers LocalScanPrefix("bref/") can see, so the sweep's object-listing
// loop and its referenced-pointer scan read the SAME ownership view and agree by
// construction. A desired-but-unmounted unit (a cold boot or a mid-flight
// rebalance-acquire window, where the unit is DESIRED but not yet MOUNTED) is
// SKIPPED this pass - a missed sweep is a storage leak, never data loss (GetBlob
// still resolves via the stored pointer's ObjKey). Sweeping a desired-but-
// unmounted unit would list its objects while its brefs are absent from the
// local scan, mis-classifying every bound blob as unreferenced (the P0 the
// MOUNTED view fixes).
//
// In legacy mode this node owns the whole keyspace, so the single sentinel
// ["legacy"] (the one prefix blob/legacy/).
//
// In multi-backend mode it is the set of GenUnits currently in the mount table
// (each ReplicaUnit's Unit field is the GenUnit), rendered as tokens and
// deduplicated across replica positions. The reshard-era reclamation of objects
// under an OLD-generation parent token prefix (after a doubling re-keys a unit)
// is a documented leak-only follow-up, NOT shipped here.
func (c *Cluster) MountedUnits() []string {
	if !c.multi {
		return []string{legacyUnitToken}
	}
	mounted := c.mounts.mountedList()
	seen := make(map[string]struct{}, len(mounted))
	out := make([]string, 0, len(mounted))
	for _, ru := range mounted {
		tok := unitToken(ru.Unit)
		if _, ok := seen[tok]; ok {
			continue
		}
		seen[tok] = struct{}{}
		out = append(out, tok)
	}
	return out
}
