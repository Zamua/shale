package main

import (
	"fmt"
	"os"
	"time"
)

// defaultAddr is the canonical local gRPC endpoint for a single-node
// shaled. Documented in docs/SPEC.md.
const defaultAddr = "127.0.0.1:7947"

// defaultTimeout is the per-RPC + dial deadline. Picked to be long
// enough to absorb a cold-start network hiccup, short enough that an
// unreachable node fails the user's command quickly.
const defaultTimeout = 5 * time.Second

// envAddr is the env var consulted when --addr is not given.
const envAddr = "SHALE_NODE"

// globalOpts collects the flags that may appear anywhere on the
// command line, before or after the subcommand.
type globalOpts struct {
	addr    string
	json    bool
	timeout time.Duration
}

// parseGlobal scans args for the known global flags + returns them
// plus the subcommand name + its positional arguments. Global flags
// are accepted in --flag value, --flag=value, or single-dash form.
// Unknown --flags are NOT consumed here; they're forwarded to the
// subcommand's own FlagSet (which will reject them with its own
// usage text).
//
// The subcommand is the first non-flag token; everything after it is
// returned verbatim as subArgs (so the subcommand can parse its own
// flags + positionals without re-doing the global-flag dance).
func parseGlobal(args []string) (opts globalOpts, sub string, subArgs []string, err error) {
	opts.addr = envOrDefault(envAddr, defaultAddr)
	opts.timeout = defaultTimeout

	i := 0
	for i < len(args) {
		a := args[i]
		switch {
		case a == "--addr" || a == "-addr":
			if i+1 >= len(args) {
				return opts, "", nil, fmt.Errorf("flag --addr requires a value")
			}
			opts.addr = args[i+1]
			i += 2
		case len(a) > 7 && (a[:7] == "--addr=" || (len(a) > 6 && a[:6] == "-addr=")):
			// --addr=host:port or -addr=host:port
			if a[1] == '-' {
				opts.addr = a[7:]
			} else {
				opts.addr = a[6:]
			}
			i++
		case a == "--json" || a == "-json":
			opts.json = true
			i++
		case a == "-h" || a == "--help" || a == "help":
			// Treat help as the subcommand so main can render usage
			// + exit 0. Anything after is ignored.
			sub = "help"
			subArgs = args[i+1:]
			return opts, sub, subArgs, nil
		case a == "--":
			// Conventional end-of-flags marker. Everything after
			// is positional; the first becomes the subcommand.
			i++
			if i < len(args) {
				sub = args[i]
				subArgs = args[i+1:]
			}
			return opts, sub, subArgs, nil
		case len(a) > 0 && a[0] != '-':
			// First non-flag token = subcommand.
			sub = a
			subArgs = args[i+1:]
			return opts, sub, subArgs, nil
		default:
			// Unknown flag - hand it off to the subcommand layer.
			// We don't know whether it takes an argument here, so
			// the safe move is to stop global parsing and let the
			// subcommand decide. The first time the parser sees an
			// unknown flag with no subcommand identified yet, that's
			// a usage error.
			return opts, "", nil, fmt.Errorf("unknown global flag: %s", a)
		}
	}
	return opts, "", nil, nil
}

// envOrDefault returns os.Getenv(key) if non-empty, else def.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
