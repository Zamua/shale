// Package chaos holds the multi-node chaos/soak harness for shale. The oracle
// in this file is the heart of it: a pure, I/O-free model of every ACKED write
// the workload made, against which reads are continuously checked under the one
// invariant the whole v0.8 multi-node story rests on:
//
//	NO ACKED WRITE MAY BE LOST.
//
// This file (and its sibling oracle_test.go) carry NO build tag, so the model is
// compiled + unit-tested by the normal `go test ./...`. Only the live soak that
// drives a real in-process cluster is gated behind `//go:build chaos`.
//
// DDD note: the oracle is the domain layer. It knows about keys, values,
// versions, and tombstones - nothing about rings, nodes, gRPC, or generations.
// The harness (orchestration) feeds it acked writes and observed reads; the
// cluster (infrastructure) is reached only through an adapter interface. That
// separation is what lets the SAME oracle judge an in-process soak today and a
// real N-node deploy later.
package chaos

import (
	"fmt"
	"sort"
	"sync"
)

// Verdict is the result of checking one observed read against the model.
type Verdict int

const (
	// VerdictPass means the read matches the latest acked state, OR it is a benign
	// race (the reader saw a slightly older value than a write the writer has
	// just acked but the read raced - only tolerated when the observed version
	// is the latest-acked-or-newer; an OLDER acked version is never a pass).
	VerdictPass Verdict = iota
	// VerdictLost means a key that has an acked live value read back not-found (or
	// a read error that is not a tolerated transient). THE failure the gate exists
	// to catch.
	VerdictLost
	// VerdictResurrected means a key whose latest acked op was a Delete read back a
	// value (a tombstone that lost to a resurrected older write, or a delete that
	// did not stick). A delete is an acked write; losing it is a loss.
	VerdictResurrected
	// VerdictStale means the read returned an acked value for the key but at a
	// version strictly OLDER than the latest acked version. The newer acked write
	// was lost / not visible where an older one is - a real consistency violation
	// under durable-before-ack + single-writer-per-key.
	VerdictStale
	// VerdictCorrupt means the read returned a payload that was NEVER acked for
	// this key (not the current value, not any prior version's value). Indicates a
	// cross-key bleed or a torn write.
	VerdictCorrupt
)

func (v Verdict) String() string {
	switch v {
	case VerdictPass:
		return "PASS"
	case VerdictLost:
		return "LOST"
	case VerdictResurrected:
		return "RESURRECTED"
	case VerdictStale:
		return "STALE"
	case VerdictCorrupt:
		return "CORRUPT"
	default:
		return fmt.Sprintf("Verdict(%d)", int(v))
	}
}

// everValuesCap bounds how many recent acked payloads per key the oracle retains
// in everValues. Without a cap, everValues grows by one entry per OVERWRITE of a
// key, unbounded across a long soak (the writer overwrites the same bounded key
// set indefinitely once it hits maxKeysPerWriter), so memory climbs with run
// length. The cap keeps the model's footprint flat. Correctness of the PASS/FAIL
// verdict is preserved: everValues only distinguishes a STALE read (an OLDER
// acked value re-surfacing) from a CORRUPT read (a value never acked for the
// key). Both are VIOLATIONS, so evicting a value older than the cap can only
// reclassify a (genuinely-lost) very-old-value read from STALE to CORRUPT - never
// turn a violation into a pass. The cap is generous (a re-surfacing value is
// almost always recent), so a real stale read is still labeled STALE in practice.
const everValuesCap = 64

// entry is the per-key model state: the latest acked value (or tombstone) and a
// bounded set of recent payloads acked for the key (so a read of an out-of-date
// payload is told apart from a payload that was never written at all).
type entry struct {
	version    uint64 // latest acked version for this key (monotonic per key)
	value      string // latest acked value; empty + deleted=true means tombstone
	deleted    bool   // true if the latest acked op was a Delete
	everValues map[string]uint64
	// everMinVer is the lowest version still retained in everValues after eviction.
	// A read whose decoded version is BELOW everMinVer but is a well-formed value
	// addressed to this key is still treated as a (stale) violation rather than
	// corruption - see Verify - so trimming everValues never excuses a real loss.
	everMinVer uint64
	// inflight > 0 while a write/delete for this key has been ISSUED to the
	// cluster but has not yet returned (acked or failed). During that window the
	// key's true state is ambiguous from the model's point of view: the durable
	// change may have already landed on the owner before Put/Delete returns to the
	// writer. A read in this window is therefore tolerated (it may legitimately
	// see EITHER the prior acked state OR the not-yet-recorded new state), so the
	// reader never flags a benign read-vs-in-flight-write race as a loss. The
	// single-writer-per-key rule keeps inflight 0-or-1 in practice; it is a counter
	// only for defensiveness.
	inflight int
}

// recordEver adds value@version to everValues and evicts the oldest entries past
// everValuesCap, keeping the model's per-key footprint flat across a long soak.
func (e *entry) recordEver(value string, version uint64) {
	e.everValues[value] = version
	if len(e.everValues) <= everValuesCap {
		return
	}
	// Evict the lowest-version entries until we are back at the cap. A re-surfacing
	// value is overwhelmingly a recent one, so this keeps the useful window.
	type vv struct {
		val string
		ver uint64
	}
	all := make([]vv, 0, len(e.everValues))
	for v, ver := range e.everValues {
		all = append(all, vv{v, ver})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ver < all[j].ver })
	drop := len(all) - everValuesCap
	for i := range drop {
		delete(e.everValues, all[i].val)
	}
	e.everMinVer = all[drop].ver
}

// Oracle records every acked write/delete and judges observed reads. It is safe
// for concurrent use: many writer goroutines record acks and many reader
// goroutines query Expected concurrently. The lock is held only for the
// in-memory map ops - never across any I/O (the oracle does no I/O).
type Oracle struct {
	mu    sync.RWMutex
	model map[string]*entry
}

// NewOracle returns an empty model.
func NewOracle() *Oracle {
	return &Oracle{model: make(map[string]*entry)}
}

// BeginOp marks that the writer is about to ISSUE a write/delete for key (before
// calling the cluster). It is paired with EndOp, called after the op returns
// (whether it acked or failed). Between the two, Verify tolerates a read that
// reflects EITHER the prior acked state or the pending new state: the durable
// change may land on the owner before the client call returns, so a not-found or
// a different value mid-flight is a benign race, not a loss. Crucially, the
// tolerance is scoped to the single key under mutation - a loss on ANY other key
// is still caught with full strictness.
func (o *Oracle) BeginOp(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.model[key]
	if e == nil {
		e = &entry{everValues: make(map[string]uint64)}
		o.model[key] = e
	}
	e.inflight++
}

// EndOp clears the in-flight mark for key. Called after the cluster op returns,
// AFTER the matching Record* (so by the time the window closes, the model already
// reflects the ack). Safe to call even if BeginOp created the entry for a key
// that then failed to ack (the entry simply has no acked version yet).
func (o *Oracle) EndOp(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if e := o.model[key]; e != nil && e.inflight > 0 {
		e.inflight--
	}
}

// RecordPut records that the cluster ACKED a Put(key, value). The caller MUST
// only call this after the cluster returned success (a retryable error is NOT an
// ack). version is the writer's monotonic counter for this key; the oracle
// asserts it strictly increases, which catches a caller bug where two writers
// share a key (the model's single-writer-per-key assumption). Returns the value
// to use so the caller and the model agree on exact bytes.
func (o *Oracle) RecordPut(key, value string, version uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.model[key]
	if e == nil {
		e = &entry{everValues: make(map[string]uint64)}
		o.model[key] = e
	}
	if version <= e.version && e.version != 0 {
		// A caller that records out of order is a harness bug, not a cluster bug;
		// fail loudly so it cannot silently mask a real loss. Panicking here is
		// correct: it can only fire from misuse of the model, never from the SUT.
		panic(fmt.Sprintf("oracle: RecordPut(%q) version %d not greater than recorded %d (two writers on one key?)", key, version, e.version))
	}
	e.version = version
	e.value = value
	e.deleted = false
	e.recordEver(value, version)
}

// RecordDelete records that the cluster ACKED a Delete(key). Same contract as
// RecordPut: only call after success. The latest acked state becomes a tombstone;
// a subsequent read MUST be not-found.
func (o *Oracle) RecordDelete(key string, version uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	e := o.model[key]
	if e == nil {
		e = &entry{everValues: make(map[string]uint64)}
		o.model[key] = e
	}
	if version <= e.version && e.version != 0 {
		panic(fmt.Sprintf("oracle: RecordDelete(%q) version %d not greater than recorded %d", key, version, e.version))
	}
	e.version = version
	e.value = ""
	e.deleted = true
}

// Observed is what a reader actually saw for a key: the value bytes, and whether
// the read came back not-found. (A transient retryable error is retried by the
// reader before it ever builds an Observed; an Observed is a settled, non-error
// read result.)
type Observed struct {
	Value    string
	NotFound bool
}

// Verify judges an observed read of key against the model and returns a verdict
// plus a human-readable detail string for the violation report. The observed
// value is expected to be self-describing (the writer encodes the version into
// it via EncodeValue); Verify decodes it to compare versions. A read of the
// LATEST acked version - or of a not-yet-recorded NEWER version (the reader
// raced a writer that has not yet returned from Put) - is a PASS; an OLDER acked
// version, a not-found for a live key, a value for a deleted key, or a payload
// never acked for the key, is a violation.
//
// "Newer than latest acked" cannot actually be observed under durable-before-ack
// + single-writer-per-key (a value is durable before its Put returns, and the
// model is updated the instant Put returns, so by the time a reader could read
// the value the model already has it). We still tolerate equal-or-newer rather
// than requiring exact-latest, because a writer records the ack a hair AFTER the
// cluster made it durable+visible: a reader can legitimately see version N while
// the writer is between "Put returned" and "oracle.RecordPut(N)". Treating
// equal-or-newer as PASS closes that benign window without masking a real loss
// (a LOST/STALE read is strictly an OLDER or absent value, which this never
// excuses).
func (o *Oracle) Verify(key string, obs Observed) (Verdict, string) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	e := o.model[key]
	if e == nil {
		// The reader only reads keys the model knows; an unknown key here is a
		// harness bug. Treat as pass (nothing to assert) but note it.
		return VerdictPass, "key not in model (no acked write yet)"
	}

	if e.inflight > 0 {
		// A write/delete for this key is in flight: its true state is ambiguous
		// (the durable change may have landed before the client call returned). We
		// still reject a CORRUPT read (a payload that does not belong to this key at
		// all), which no in-flight ambiguity can excuse, but tolerate not-found / an
		// older acked value / AND the value of the very write now in flight (which
		// the cluster may have made durable+visible a hair before Put returned and
		// RecordPut ran, so it is not yet in everValues). We recognize the pending
		// write by decoding the observed value: a well-formed value addressed to
		// THIS key at a version >= the current model version is the in-flight write
		// landing early, not corruption.
		if !obs.NotFound && obs.Value != "" {
			if _, ok := e.everValues[obs.Value]; !ok {
				vKey, vVer, decodeOK := DecodeValue(obs.Value)
				if decodeOK && vKey == key && vVer >= e.version {
					return VerdictPass, "" // the in-flight write, observed before its ack was recorded
				}
				return VerdictCorrupt, fmt.Sprintf("key %q read returned value %q never acked for this key (observed during an in-flight write)", key, obs.Value)
			}
		}
		return VerdictPass, ""
	}

	if e.deleted {
		// Latest acked op was a Delete: a correct read is not-found.
		if obs.NotFound {
			return VerdictPass, ""
		}
		// A value came back for a deleted key. If it is the exact value of the
		// delete's PREDECESSOR write, this is a resurrected-older-write (the
		// delete was lost or a stale value out-raced the tombstone). Either way it
		// is a lost acked delete.
		return VerdictResurrected, fmt.Sprintf("key %q latest acked op is Delete (version %d) but read returned value %q", key, e.version, obs.Value)
	}

	// Latest acked op was a Put: a correct read is the latest value.
	if obs.NotFound {
		return VerdictLost, fmt.Sprintf("key %q has acked value (version %d, %q) but read returned not-found", key, e.version, e.value)
	}
	if obs.Value == e.value {
		return VerdictPass, ""
	}
	// The observed value differs from the latest. Decode its version to tell a
	// STALE acked value apart from a CORRUPT never-acked payload.
	if ver, ok := e.everValues[obs.Value]; ok {
		// It is a value we DID ack for this key, but not the latest -> stale.
		if ver < e.version {
			return VerdictStale, fmt.Sprintf("key %q read returned acked-but-stale value %q (version %d); latest acked is version %d (%q)", key, obs.Value, ver, e.version, e.value)
		}
		// ver >= e.version with a non-equal value cannot happen (everValues is
		// keyed by value, and version is monotonic), but be defensive.
		return VerdictPass, ""
	}
	// Not in the (bounded) everValues. Before calling it CORRUPT, check whether it
	// is a well-formed value ADDRESSED TO THIS KEY at a version we may have EVICTED
	// past the everValuesCap (a very old acked value re-surfacing). That is still a
	// lost/stale violation - just one whose exact predecessor we no longer retain -
	// so we label it STALE rather than CORRUPT. This keeps the cap from ever
	// excusing a real loss: an evicted-old value re-surfacing stays a violation.
	if vKey, vVer, ok := DecodeValue(obs.Value); ok && vKey == key && vVer < e.version && vVer <= e.everMinVer {
		return VerdictStale, fmt.Sprintf("key %q read returned an acked-but-stale value %q (version %d, evicted from the retained window <= %d); latest acked is version %d (%q)", key, obs.Value, vVer, e.everMinVer, e.version, e.value)
	}
	return VerdictCorrupt, fmt.Sprintf("key %q read returned value %q never acked for this key (latest acked version %d = %q)", key, obs.Value, e.version, e.value)
}

// Keys returns a snapshot of every key the model knows about. Readers sample
// from this set so they only ever read keys that have an acked write to check.
func (o *Oracle) Keys() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]string, 0, len(o.model))
	for k := range o.model {
		out = append(out, k)
	}
	return out
}

// Snapshot returns a copy of the full latest-acked state, used by the final
// full-sweep verification (read every acked key from every node). The returned
// map is independent of the model (safe to iterate while writers continue, though
// the final sweep runs after writers have stopped).
func (o *Oracle) Snapshot() map[string]Observed {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make(map[string]Observed, len(o.model))
	for k, e := range o.model {
		out[k] = Observed{Value: e.value, NotFound: e.deleted}
	}
	return out
}

// EncodeValue builds the self-describing value the writer stores: the version is
// embedded so a reader (via Verify) can tell a stale acked value from a corrupt
// never-acked one, and so a value is globally unique per (key, version). Format:
// "v<version>|<key>|<payload>". Including the key guards against a cross-key bleed
// (the same version on a different key yields different bytes).
func EncodeValue(key string, version uint64, payload string) string {
	return fmt.Sprintf("v%d|%s|%s", version, key, payload)
}

// DecodeValue parses a value built by EncodeValue back into its key + version.
// ok is false if the value is not in the expected "v<version>|<key>|<payload>"
// shape. Used by Verify to recognize an in-flight write that landed durably a
// hair before its ack was recorded (the value addresses this key at a version
// >= the model's current version), distinguishing it from genuine corruption.
func DecodeValue(s string) (key string, version uint64, ok bool) {
	if len(s) < 2 || s[0] != 'v' {
		return "", 0, false
	}
	firstBar := indexByte(s, '|')
	if firstBar < 0 {
		return "", 0, false
	}
	verStr := s[1:firstBar]
	rest := s[firstBar+1:]
	secondBar := indexByte(rest, '|')
	if secondBar < 0 {
		return "", 0, false
	}
	key = rest[:secondBar]
	var v uint64
	if _, err := fmt.Sscanf(verStr, "%d", &v); err != nil {
		return "", 0, false
	}
	return key, v, true
}

// indexByte returns the index of the first occurrence of b in s, or -1. Local
// helper to keep oracle.go free of the bytes/strings import for one call.
func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
