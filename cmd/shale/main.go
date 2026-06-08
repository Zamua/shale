// Package main is the shale operator + developer CLI.
//
// shale connects to ONE running node's gRPC endpoint (a shaled
// process, or an app that has embedded shale and exposed
// pkg/rpc.Server) and invokes the same ShaleNode RPCs that the
// inter-node forwarding layer uses in v0.2+. The CLI is therefore
// a thin shell over pkg/rpc.Client: parse flags, dial, call,
// format output, exit with the appropriate code.
//
// Subcommand dispatch is a simple switch on os.Args[1]; each
// subcommand owns its own flag.FlagSet so per-command flags don't
// leak across boundaries. Global flags (--addr, --json) are pulled
// out of os.Args[1:] before the subcommand is picked, so they may
// appear in any position relative to the subcommand + its args.
//
// Exit codes (matching docs/SPEC.md):
//
//	0 success
//	1 generic error
//	2 not-found
//	3 connection error
//	4 timeout
//
// Stdout is reserved for parseable output (values, key=value lines,
// structured topology/stats). Everything else - status, errors,
// usage - goes to stderr.
package main

import (
	"fmt"
	"io"
	"os"
)

// usage prints the top-level help text to w.
func usage(w io.Writer) {
	fmt.Fprint(w, `shale - operator CLI for a shale cluster node

Usage:
  shale [global flags] <subcommand> [args...]

Subcommands:
  put <key> <value>   write a value (no output on success)
  get <key>           read a value to stdout (exit 2 if not found)
  delete <key>        delete a key (idempotent)
  scan <prefix>       stream key=value lines under prefix
  topology            describe the cluster membership + ring
  stats               per-node counters
  ping                liveness check (exit 0 if up, 3 if not)

Global flags:
  --addr host:port    target node's gRPC endpoint (default 127.0.0.1:7947;
                      may also be set via SHALE_NODE)
  --json              structured output for topology + stats

Exit codes:
  0 success   1 generic error   2 not-found   3 connection error   4 timeout
`)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is the testable entrypoint. It returns the process exit code
// instead of calling os.Exit, so tests can drive it without forking.
func run(args []string, stdout, stderr io.Writer) int {
	opts, sub, subArgs, err := parseGlobal(args)
	if err != nil {
		fmt.Fprintf(stderr, "shale: %v\n", err)
		usage(stderr)
		return exitGeneric
	}
	if sub == "" {
		usage(stderr)
		return exitGeneric
	}
	if sub == "-h" || sub == "--help" || sub == "help" {
		usage(stderr)
		return exitOK
	}

	switch sub {
	case "put":
		return runPut(opts, subArgs, stdout, stderr)
	case "get":
		return runGet(opts, subArgs, stdout, stderr)
	case "delete", "del", "rm":
		return runDelete(opts, subArgs, stdout, stderr)
	case "scan":
		return runScan(opts, subArgs, stdout, stderr)
	case "topology":
		return runTopology(opts, subArgs, stdout, stderr)
	case "stats":
		return runStats(opts, subArgs, stdout, stderr)
	case "ping":
		return runPing(opts, subArgs, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "shale: unknown subcommand %q\n", sub)
		usage(stderr)
		return exitGeneric
	}
}
