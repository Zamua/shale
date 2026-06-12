package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/Zamua/shale/pkg/rpc"
)

// setupCmd runs the byte-identical preamble shared by the fixed-arg
// subcommands (put/delete/get/ping/stats): build a ContinueOnError
// flag set bound to stderr with a one-line usage, parse args, enforce
// an exact positional-arg count, then dial the node.
//
// It returns the parsed flag set (for fs.Arg access), the connected
// client, a cleanup func, and an exit code. The caller MUST inspect
// code first: when code != exitOK the fs/cli/cleanup are not usable
// (cleanup is a safe no-op, but there's nothing to clean up) and the
// caller should return code immediately. On success the caller is
// responsible for `defer cleanup()` and for deriving its own timeout
// context, so the deferred teardown runs in the command's own scope.
func setupCmd(opts globalOpts, name, usage string, wantArgs int, args []string, stderr io.Writer) (*flag.FlagSet, *rpc.Client, func(), int) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, usage)
	}
	if err := fs.Parse(args); err != nil {
		return fs, nil, func() {}, exitGeneric
	}
	if fs.NArg() != wantArgs {
		fs.Usage()
		return fs, nil, func() {}, exitGeneric
	}

	cli, cleanup, code := dial(opts.addr, stderr)
	if code != exitOK {
		return fs, nil, cleanup, code
	}
	return fs, cli, cleanup, exitOK
}
