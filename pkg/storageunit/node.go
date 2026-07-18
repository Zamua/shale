package storageunit

// NodeID identifies a node (compute) in the cluster. It mirrors
// ring.Member.ID: the cluster-unique node identity the ring places units
// on. Named here as its own type so the ownership derivation has no compile
// dependency on pkg/ring (the ring depends on this domain, not the other
// way around; the cluster layer wires them together).
type NodeID string
