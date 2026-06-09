package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

// runPut handles `shale put <key> <value>`. On success, nothing is
// written to stdout (matches the spec: "no output on success"). A
// connection / timeout / generic error goes to stderr + the matching
// exit code.
func runPut(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, "Usage: shale put <key> <value>\n")
	}
	if err := fs.Parse(args); err != nil {
		return exitGeneric
	}
	if fs.NArg() != 2 {
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

	if err := cli.Put(ctx, []byte(fs.Arg(0)), []byte(fs.Arg(1))); err != nil {
		_, _ = fmt.Fprintf(stderr, "shale put: %v\n", err)
		return exitCodeForRPCError(err)
	}
	_ = stdout // intentionally silent on success
	return exitOK
}
