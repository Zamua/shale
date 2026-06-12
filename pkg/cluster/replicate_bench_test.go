package cluster_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/cluster"
)

// BenchmarkPutReplicated isolates the replicated-write hot line in
// putReplicated (replicate.go): building the envelope bytes from the
// caller's value. The fix dropped a redundant defensive copy of the
// payload (append([]byte(nil), value...)) because Encode already
// allocates its own buffer and copies env.Payload into it without
// retaining or mutating the input. The "Redundant copy" sub-benchmark
// reproduces the OLD form; "Direct" reproduces the shipped form. With
// a multi-MB value the difference is one large alloc + one O(len) copy
// per op (visible as B/op and allocs/op).
func BenchmarkPutReplicated(b *testing.B) {
	value := make([]byte, 10<<20) // 10 MiB, the cap-sized hot case
	for i := range value {
		value[i] = byte(i)
	}
	stamp := cluster.Stamp{TimestampNanos: 1_717_000_000_000_000_000, NodeID: "node-1"}

	b.Run("RedundantCopy", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		var sink []byte
		for i := 0; i < b.N; i++ {
			// OLD form: inner defensive copy of the whole payload,
			// then Encode copies it AGAIN into its own buffer.
			sink = cluster.Encode(cluster.Envelope{
				Stamp:   stamp,
				Payload: append([]byte(nil), value...),
			})
		}
		_ = sink
	})

	b.Run("Direct", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		var sink []byte
		for i := 0; i < b.N; i++ {
			// SHIPPED form: pass value straight through; Encode owns
			// the single copy into its returned buffer.
			sink = cluster.Encode(cluster.Envelope{
				Stamp:   stamp,
				Payload: value,
			})
		}
		_ = sink
	})
}
