package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	pb "github.com/Zamua/shale/pkg/rpc/proto"
)

// rebalanceClient is the narrow surface cmd_rebalance.go needs from
// *rpc.Client. Defined locally (not in pkg/rpc) so tests can pass a
// fake without dialing a real server, and so the dependency from the
// CLI on the proto types stays explicit at this seam.
type rebalanceClient interface {
	Topology(ctx context.Context) (*pb.TopologyResponse, error)
	ProposeRebalance(ctx context.Context, dryRun, apply, cancel bool) (*pb.ProposeRebalanceResponse, error)
}

// runRebalance handles `shale rebalance --dry-run | --apply | --cancel`.
//
// Exactly one mode flag is required. --dry-run prints the plan the
// local node would execute against the current ring (human or JSON);
// --apply runs Evaluate immediately, bypassing the 5s settle delay;
// --cancel aborts any in-flight migration. --apply / --cancel print
// "ok" on success. All three exit 0 on success, non-zero on RPC
// failure (mapped through exitCodeForRPCError).
func runRebalance(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("rebalance", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, `Usage: shale rebalance (--dry-run | --apply | --cancel)

Modes (exactly one required):
  --dry-run   show the plan that WOULD run; no execution
  --apply     execute the plan immediately, bypassing the 5s settle delay
  --cancel    abort any in-flight migration

Output:
  --dry-run prints a human plan, or a JSON array under --json.
  --apply / --cancel print "ok" on success.
`)
	}
	var (
		dryRun = fs.Bool("dry-run", false, "show the plan without executing")
		apply  = fs.Bool("apply", false, "execute the plan now, skipping the settle delay")
		cancel = fs.Bool("cancel", false, "abort any in-flight migration")
	)
	if err := fs.Parse(args); err != nil {
		return exitGeneric
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return exitGeneric
	}
	if code := validateRebalanceMode(*dryRun, *apply, *cancel, stderr); code != exitOK {
		fs.Usage()
		return code
	}

	cli, cleanup, code := dial(opts.addr, stderr)
	if code != exitOK {
		return code
	}
	defer cleanup()

	return runRebalanceWithClient(cli, opts, *dryRun, *apply, *cancel, stdout, stderr)
}

// runRebalanceWithClient is the testable core: takes an already-built
// rebalanceClient so tests can inject a fake. Splitting at this seam
// avoids needing a real grpc server in unit tests of output formatting.
func runRebalanceWithClient(cli rebalanceClient, opts globalOpts, dryRun, apply, cancel bool, stdout, stderr io.Writer) int {
	ctx, cancelCtx := withTimeout(context.Background(), opts.timeout)
	defer cancelCtx()

	resp, err := cli.ProposeRebalance(ctx, dryRun, apply, cancel)
	if err != nil {
		fmt.Fprintf(stderr, "shale rebalance: %v\n", err)
		return exitCodeForRPCError(err)
	}

	switch {
	case apply, cancel:
		fmt.Fprintln(stdout, "ok")
		return exitOK
	case dryRun:
		return emitDryRun(ctx, cli, resp, opts.json, stdout, stderr)
	default:
		// Unreachable: validateRebalanceMode guarantees one branch.
		return exitGeneric
	}
}

// emitDryRun prints the plan as either a human summary or a JSON
// array. The human form includes a "cluster: N nodes" header sourced
// from a follow-up Topology call so the operator sees plan-vs-cluster
// context at a glance. Topology failure degrades gracefully: the
// header is omitted, the plan is still printed.
func emitDryRun(ctx context.Context, cli rebalanceClient, resp *pb.ProposeRebalanceResponse, asJSON bool, stdout, stderr io.Writer) int {
	ranges := resp.GetRanges()
	if asJSON {
		return emitDryRunJSON(ranges, stdout)
	}

	clusterSize, topoOK := dryRunClusterSize(ctx, cli)
	if topoOK {
		fmt.Fprintf(stdout, "cluster: %d nodes\n", clusterSize)
	}
	fmt.Fprintf(stdout, "proposed plan: %d partition moves\n", len(ranges))
	var totalKeys uint64
	for _, r := range ranges {
		fmt.Fprintf(stdout, "  partition %d  %s → %s  (est ~%d keys)\n",
			r.GetPartitionId(), r.GetCurrentOwner(), r.GetProposedOwner(), r.GetEstimatedKeyCount())
		totalKeys += r.GetEstimatedKeyCount()
	}
	fmt.Fprintf(stdout, "total estimated keys to migrate: %d\n", totalKeys)
	return exitOK
}

// emitDryRunJSON marshals the plan as a JSON array. One object per
// range, field names lowercased + snake_cased so they're stable across
// proto regenerations (the proto would emit partitionId / proposedOwner).
func emitDryRunJSON(ranges []*pb.RangePlanItem, stdout io.Writer) int {
	type item struct {
		PartitionID       uint64 `json:"partition_id"`
		CurrentOwner      string `json:"current_owner"`
		ProposedOwner     string `json:"proposed_owner"`
		EstimatedKeyCount uint64 `json:"estimated_key_count"`
	}
	out := make([]item, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, item{
			PartitionID:       r.GetPartitionId(),
			CurrentOwner:      r.GetCurrentOwner(),
			ProposedOwner:     r.GetProposedOwner(),
			EstimatedKeyCount: r.GetEstimatedKeyCount(),
		})
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return exitGeneric
	}
	return exitOK
}

// dryRunClusterSize fetches the node count from Topology. Returns
// (size, true) on success, (0, false) on any error: the dry-run is
// still useful without the header, so we don't abort the command for
// a missing topology readout.
func dryRunClusterSize(ctx context.Context, cli rebalanceClient) (int, bool) {
	topo, err := cli.Topology(ctx)
	if err != nil {
		return 0, false
	}
	nodes := topo.GetNodes()
	if len(nodes) == 0 {
		// A single-node response from a stub may omit the per-node
		// list; treat the cluster as 1 in that case so the header is
		// still meaningful.
		if topo.GetNodeId() != "" {
			return 1, true
		}
		return 0, false
	}
	return len(nodes), true
}

// validateRebalanceMode enforces "exactly one of dry-run / apply /
// cancel". Writes a one-line diagnostic to stderr on failure so the
// operator sees what went wrong before the usage block is reprinted.
func validateRebalanceMode(dryRun, apply, cancel bool, stderr io.Writer) int {
	n := 0
	for _, b := range []bool{dryRun, apply, cancel} {
		if b {
			n++
		}
	}
	switch n {
	case 0:
		fmt.Fprintln(stderr, "shale rebalance: one of --dry-run, --apply, --cancel is required")
		return exitGeneric
	case 1:
		return exitOK
	default:
		fmt.Fprintln(stderr, "shale rebalance: --dry-run, --apply, and --cancel are mutually exclusive")
		return exitGeneric
	}
}
