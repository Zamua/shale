package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
)

// runTopology handles `shale topology`. With --json, emits a
// structured object compatible with `jq`. Without --json, prints a
// short human summary: "single-node cluster, node=<id>" when there is
// exactly one node, otherwise a header + one line per node with the
// local node tagged "(this)".
func runTopology(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("topology", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, "Usage: shale topology\n")
	}
	if err := fs.Parse(args); err != nil {
		return exitGeneric
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return exitGeneric
	}

	cli, cleanup, code := dial(opts.addr, stderr)
	if code != exitOK {
		return code
	}
	defer cleanup()

	ctx, cancel := withTimeout(context.Background(), opts.timeout)
	defer cancel()

	resp, err := cli.Topology(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "shale topology: %v\n", err)
		return exitCodeForRPCError(err)
	}

	if opts.json {
		// Hand-rolled shape so the JSON contract is stable across
		// proto regenerations + doesn't leak protobuf field-name
		// conventions (proto would emit nodeId / grpcAddr).
		type nodeOut struct {
			NodeID   string `json:"node_id"`
			GRPCAddr string `json:"grpc_addr,omitempty"`
			IsSelf   bool   `json:"is_self"`
		}
		out := struct {
			NodeID     string    `json:"node_id"`
			SingleNode bool      `json:"single_node"`
			Nodes      []nodeOut `json:"nodes"`
		}{
			NodeID:     resp.GetNodeId(),
			SingleNode: resp.GetSingleNode(),
			Nodes:      make([]nodeOut, 0, len(resp.GetNodes())),
		}
		for _, n := range resp.GetNodes() {
			out.Nodes = append(out.Nodes, nodeOut{
				NodeID:   n.GetNodeId(),
				GRPCAddr: n.GetGrpcAddr(),
				IsSelf:   n.GetNodeId() == resp.GetNodeId(),
			})
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return exitGeneric
		}
		return exitOK
	}

	if resp.GetSingleNode() {
		_, _ = fmt.Fprintf(stdout, "single-node cluster, node=%s\n", resp.GetNodeId())
		return exitOK
	}
	// Multi-node: header + one line per node. The local node is
	// tagged "(this)" so an operator scanning the output sees which
	// node served the RPC at a glance.
	_, _ = fmt.Fprintf(stdout, "cluster: %d nodes\n", len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		suffix := ""
		if n.GetNodeId() == resp.GetNodeId() {
			suffix = "  (this)"
		}
		_, _ = fmt.Fprintf(stdout, "  %s  %s%s\n", n.GetNodeId(), n.GetGrpcAddr(), suffix)
	}
	return exitOK
}
