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
