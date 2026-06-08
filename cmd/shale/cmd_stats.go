package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
)

// runStats handles `shale stats`. With --json, emits a structured
// object. Without, prints aligned key/value lines on stdout.
//
// Latency percentiles are placeholders in v0.1 (the server reports
// 0); we emit them anyway so the column shape is stable for v0.5
// when real histograms land.
func runStats(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, "Usage: shale stats\n")
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

	resp, err := cli.Stats(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "shale stats: %v\n", err)
		return exitCodeForRPCError(err)
	}

	if opts.json {
		out := struct {
			KeysHeld     uint64  `json:"keys_held"`
			Puts         uint64  `json:"puts"`
			Gets         uint64  `json:"gets"`
			Deletes      uint64  `json:"deletes"`
			Scans        uint64  `json:"scans"`
			LatencyMsP50 float64 `json:"latency_ms_p50"`
			LatencyMsP99 float64 `json:"latency_ms_p99"`
		}{
			KeysHeld:     resp.GetKeysHeld(),
			Puts:         resp.GetPuts(),
			Gets:         resp.GetGets(),
			Deletes:      resp.GetDeletes(),
			Scans:        resp.GetScans(),
			LatencyMsP50: resp.GetLatencyMsP50(),
			LatencyMsP99: resp.GetLatencyMsP99(),
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return exitGeneric
		}
		return exitOK
	}

	// Aligned columns so eyeball-scanning works on a terminal.
	fmt.Fprintf(stdout, "keys_held       %d\n", resp.GetKeysHeld())
	fmt.Fprintf(stdout, "puts            %d\n", resp.GetPuts())
	fmt.Fprintf(stdout, "gets            %d\n", resp.GetGets())
	fmt.Fprintf(stdout, "deletes         %d\n", resp.GetDeletes())
	fmt.Fprintf(stdout, "scans           %d\n", resp.GetScans())
	fmt.Fprintf(stdout, "latency_ms_p50  %.3f\n", resp.GetLatencyMsP50())
	fmt.Fprintf(stdout, "latency_ms_p99  %.3f\n", resp.GetLatencyMsP99())
	return exitOK
}
