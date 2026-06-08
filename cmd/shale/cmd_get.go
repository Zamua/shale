package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

// runGet handles `shale get <key>`. On a hit, writes the raw value
// bytes to stdout followed by a newline. On absence, exits 2 with no
// stdout output (stderr gets a short note). Connection / timeout
// errors map per exitCodeForRPCError.
func runGet(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, "Usage: shale get <key>\n")
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

	ctx, cancel := withTimeout(context.Background(), opts.timeout)
	defer cancel()

	val, found, err := cli.Get(ctx, []byte(fs.Arg(0)))
	if err != nil {
		fmt.Fprintf(stderr, "shale get: %v\n", err)
		return exitCodeForRPCError(err)
	}
	if !found {
		fmt.Fprintf(stderr, "shale get: key not found\n")
		return exitNotFound
	}
	// Trailing newline so the value is comfortable for shell tools
	// to consume. Values that already end in \n get a doubled one;
	// that's the deliberate trade for the much more common case
	// where the value is a single line of text or a token.
	if _, err := stdout.Write(val); err != nil {
		return exitGeneric
	}
	if _, err := io.WriteString(stdout, "\n"); err != nil {
		return exitGeneric
	}
	return exitOK
}
