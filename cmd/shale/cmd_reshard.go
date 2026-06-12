package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	pb "github.com/Zamua/shale/pkg/rpc/proto"
)

// reshardClient is the narrow surface cmd_reshard.go needs from
// *rpc.Client. Defined locally (mirroring rebalanceClient) so tests can
// inject a fake without dialing a real server, and so the CLI's
// dependency on the proto types stays explicit at this seam.
type reshardClient interface {
	Reshard(ctx context.Context) (*pb.ReshardResponse, error)
}

// runReshard handles `shale reshard`: it triggers a doubling reshard
// (N -> 2N) on the dialed node, which becomes the coordinator for the
// cluster-wide barrier. There are no mode flags - a doubling is the only
// supported reshard.
//
// Two failure shapes, mapped to distinct exit codes:
//
//   - TRANSPORT failure (node unreachable, timeout): the returned gRPC
//     error, mapped via exitCodeForRPCError (3 connection / 4 timeout / 1).
//   - GUARD rejection (legacy mode, a reshard already in flight, membership
//     unstable mid-reshard): a non-empty resp.Error string carried in the
//     response, NOT a gRPC status. Reported to stderr; exit 1 (generic). A
//     refused reshard left the generation untouched, so retrying is safe.
//
// On success, prints the {from -> to} unit-count transition (or a JSON
// object under --json) and exits 0.
func runReshard(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("reshard", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `Usage: shale reshard

Triggers a doubling reshard (unit count N -> 2N) on the target node, which
coordinates a cluster-wide FREEZE -> BISECT -> FLIP -> RESUME barrier. No acked
write is lost. Safe to retry after a clean failure (the generation is unchanged
on any refused reshard).

A reshard is REFUSED (exit 1, generation unchanged) when:
  - the node runs legacy single-backend mode (not started with --unit-count > 1)
  - another reshard is already in flight
  - cluster membership is unstable across the barrier

Output:
  prints "reshard: N -> 2N" on success, or a JSON object under --json.
`)
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

	return runReshardWithClient(cli, opts, stdout, stderr)
}

// runReshardWithClient is the testable core: it takes an already-built
// reshardClient so tests can inject a fake, avoiding a real grpc server
// for output-formatting + error-mapping coverage.
func runReshardWithClient(cli reshardClient, opts globalOpts, stdout, stderr io.Writer) int {
	ctx, cancel := withTimeout(context.Background(), opts.timeout)
	defer cancel()

	resp, err := cli.Reshard(ctx)
	if err != nil {
		// Transport / status failure (unreachable, timeout): map to the
		// connection / timeout / generic exit code.
		_, _ = fmt.Fprintf(stderr, "shale reshard: %v\n", err)
		return exitCodeForRPCError(err)
	}
	if e := resp.GetError(); e != "" {
		// Guard rejection carried in the response field (not a gRPC status):
		// the reshard was REFUSED and the generation is unchanged. Generic
		// exit; retry is safe once the cause (in-flight / membership) clears.
		_, _ = fmt.Fprintf(stderr, "shale reshard: refused: %s\n", e)
		return exitGeneric
	}

	from, to := resp.GetFromUnitCount(), resp.GetToUnitCount()
	if opts.json {
		out := struct {
			FromUnitCount uint32 `json:"from_unit_count"`
			ToUnitCount   uint32 `json:"to_unit_count"`
		}{FromUnitCount: from, ToUnitCount: to}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return exitGeneric
		}
		return exitOK
	}

	_, _ = fmt.Fprintf(stdout, "reshard: %d -> %d\n", from, to)
	return exitOK
}
