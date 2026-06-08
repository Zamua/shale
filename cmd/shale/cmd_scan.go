package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

// runScan handles `shale scan <prefix>`. Streams every key=value
// pair under prefix to stdout, one per line, in key-ascending order
// (the order the underlying Backend.ScanPrefix emits). Empty prefix
// is allowed - "scan everything".
//
// The scan uses no per-RPC timeout: a large prefix can stream for a
// while + opts.timeout is meant for single round-trip calls. If you
// need to cap a long scan, send SIGINT.
func runScan(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, "Usage: shale scan <prefix>\n")
	}
	if err := fs.Parse(args); err != nil {
		return exitGeneric
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return exitGeneric
	}

	cli, cleanup, code := dial(opts.addr, stderr)
	if code != exitOK {
		return code
	}
	defer cleanup()

	// Streaming RPC: parent context lives for the whole scan, not
	// just one round trip. Cancel only on early exit.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := cli.ScanPrefix(ctx, []byte(fs.Arg(0)))
	if err != nil {
		fmt.Fprintf(stderr, "shale scan: %v\n", err)
		return exitCodeForRPCError(err)
	}

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return exitOK
		}
		if err != nil {
			fmt.Fprintf(stderr, "shale scan: %v\n", err)
			return exitCodeForRPCError(err)
		}
		// key=value\n - matches the spec ("key=value pairs"). Keys
		// + values are raw bytes; we don't escape, so binary data
		// can produce ugly output. That's the same trade-off a
		// raw `cat` makes; appropriate for v0.1.
		if _, err := fmt.Fprintf(stdout, "%s=%s\n", msg.GetKey(), msg.GetValue()); err != nil {
			return exitGeneric
		}
	}
}
