package main

import (
	"context"
	"encoding/json"
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
	_, cli, cleanup, code := setupCmd(opts, "stats", "Usage: shale stats\n", 0, args, stderr)
	if code != exitOK {
		return code
	}
	defer cleanup()

	ctx, cancel := withTimeout(context.Background(), opts.timeout)
	defer cancel()

	resp, err := cli.Stats(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "shale stats: %v\n", err)
		return exitCodeForRPCError(err)
	}

	if opts.json {
		out := struct {
			KeysHeld         uint64  `json:"keys_held"`
			Puts             uint64  `json:"puts"`
			Gets             uint64  `json:"gets"`
			Deletes          uint64  `json:"deletes"`
			Scans            uint64  `json:"scans"`
			LatencyMsP50     float64 `json:"latency_ms_p50"`
			LatencyMsP99     float64 `json:"latency_ms_p99"`
			DesiredUnits     uint64  `json:"desired_units"`
			MountedUnits     uint64  `json:"mounted_units"`
			PendingUnits     uint64  `json:"pending_units"`
			FailedOpenUnits  uint64  `json:"failed_open_units"`
			LastAcquireError string  `json:"last_acquire_error"`
		}{
			KeysHeld:         resp.GetKeysHeld(),
			Puts:             resp.GetPuts(),
			Gets:             resp.GetGets(),
			Deletes:          resp.GetDeletes(),
			Scans:            resp.GetScans(),
			LatencyMsP50:     resp.GetLatencyMsP50(),
			LatencyMsP99:     resp.GetLatencyMsP99(),
			DesiredUnits:     resp.GetDesiredUnits(),
			MountedUnits:     resp.GetMountedUnits(),
			PendingUnits:     resp.GetPendingUnits(),
			FailedOpenUnits:  resp.GetFailedOpenUnits(),
			LastAcquireError: resp.GetLastAcquireError(),
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return exitGeneric
		}
		return exitOK
	}

	// Aligned columns so eyeball-scanning works on a terminal.
	_, _ = fmt.Fprintf(stdout, "keys_held          %d\n", resp.GetKeysHeld())
	_, _ = fmt.Fprintf(stdout, "puts               %d\n", resp.GetPuts())
	_, _ = fmt.Fprintf(stdout, "gets               %d\n", resp.GetGets())
	_, _ = fmt.Fprintf(stdout, "deletes            %d\n", resp.GetDeletes())
	_, _ = fmt.Fprintf(stdout, "scans              %d\n", resp.GetScans())
	_, _ = fmt.Fprintf(stdout, "latency_ms_p50     %.3f\n", resp.GetLatencyMsP50())
	_, _ = fmt.Fprintf(stdout, "latency_ms_p99     %.3f\n", resp.GetLatencyMsP99())
	// Mount readiness (multi-backend; all zero on a legacy node).
	_, _ = fmt.Fprintf(stdout, "desired_units      %d\n", resp.GetDesiredUnits())
	_, _ = fmt.Fprintf(stdout, "mounted_units      %d\n", resp.GetMountedUnits())
	_, _ = fmt.Fprintf(stdout, "pending_units      %d\n", resp.GetPendingUnits())
	_, _ = fmt.Fprintf(stdout, "failed_open_units  %d\n", resp.GetFailedOpenUnits())
	if e := resp.GetLastAcquireError(); e != "" {
		_, _ = fmt.Fprintf(stdout, "last_acquire_error %s\n", e)
	}
	return exitOK
}
