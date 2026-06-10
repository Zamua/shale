package backend

import "errors"

// ErrNotFound is returned by Backend.Get when the key doesn't exist.
// Backends MUST return this exact sentinel (errors.Is-comparable) so
// the cluster layer can treat absence uniformly.
var ErrNotFound = errors.New("backend: key not found")

// ErrCrossShard is returned by the cluster layer when a transaction
// touches keys owned by different shards. Backends never see this
// directly; the cluster's transaction proxy enforces the constraint.
var ErrCrossShard = errors.New("backend: cross-shard transaction not supported")

// ErrClosed is returned by any Backend method called after Close.
var ErrClosed = errors.New("backend: closed")

// ErrCASConflict is returned by a CAS-buffered transaction's Commit (and
// surfaced by Cluster.Transact when its retry budget is exhausted) when the
// owner's validate-and-apply step found that a key in the read-set changed
// between the client's read and the commit. It is the optimistic-concurrency
// retry signal, NOT a hard failure: the caller re-reads + recomputes against
// fresh state and tries again. Distinct from a backend / ownership error so
// retry helpers can branch on it (errors.Is-comparable).
var ErrCASConflict = errors.New("backend: CAS conflict; read-set changed, retry")
