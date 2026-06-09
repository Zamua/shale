package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

// runDelete handles `shale delete <key>`. The operation is idempotent
// at the Backend layer (deleting an absent key is OK), so this never
// returns exitNotFound for "the key wasn't there" - it returns
// exitOK. Connection / timeout / generic errors map per
// exitCodeForRPCError.
func runDelete(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, "Usage: shale delete <key>\n")
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

	if err := cli.Delete(ctx, []byte(fs.Arg(0))); err != nil {
		_, _ = fmt.Fprintf(stderr, "shale delete: %v\n", err)
		return exitCodeForRPCError(err)
	}
	_ = stdout // intentionally silent on success
	return exitOK
}
