package main

import (
	"context"
	"fmt"
	"io"
)

// runDelete handles `shale delete <key>`. The operation is idempotent
// at the Backend layer (deleting an absent key is OK), so this never
// returns exitNotFound for "the key wasn't there" - it returns
// exitOK. Connection / timeout / generic errors map per
// exitCodeForRPCError.
func runDelete(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs, cli, cleanup, code := setupCmd(opts, "delete", "Usage: shale delete <key>\n", 1, args, stderr)
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
