package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

// runPing handles `shale ping`. Exits 0 if the node answers Ping
// within the per-RPC timeout, 4 on timeout, 3 on any other transport
// failure. Spec calls out: "exits 0 if node responds, 3 if not".
// Timeouts get their own code (4) because the user often cares to
// distinguish "the node is reachable but slow" from "the node is
// not reachable at all".
func runPing(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ping", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, "Usage: shale ping\n")
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

	if err := cli.Ping(ctx); err != nil {
		_, _ = fmt.Fprintf(stderr, "shale ping: %v\n", err)
		return exitCodeForRPCError(err)
	}
	_, _ = fmt.Fprintln(stdout, "ok")
	return exitOK
}
