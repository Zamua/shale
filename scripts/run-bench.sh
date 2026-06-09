#!/usr/bin/env bash
# scripts/run-bench.sh - drive the v0.5 comparative benchmark suite.
#
# Builds cmd/shale-bench, runs each scenario, captures JSON results,
# and renders one combined markdown table on stdout. Operators redirect
# this into docs/BENCH-v0.5.md to refresh the canonical numbers; CI
# can diff successive runs against an earlier checkpoint to spot
# regressions.
#
# Tuning knobs (env vars; all optional):
#   SHALE_BENCH_OPS         per-scenario op count (default 10000)
#   SHALE_BENCH_VALUE_SIZE  payload bytes per put (default 1024)
#   SHALE_BENCH_CONC        client goroutines (default 4)
#   SHALE_BENCH_DIR         workspace for the JSON output + binary
#                           (default: a fresh mktemp dir, deleted on
#                           exit; set this to a stable path if you
#                           want to inspect per-scenario JSON after
#                           the run)
#
# Exit: 0 on success. Any scenario failure marks that row "FAILED"
# in the table but does NOT abort the suite, so partial results are
# always emitted. The script exits non-zero only if it can't even
# build the harness.

set -uo pipefail

OPS=${SHALE_BENCH_OPS:-10000}
VALUE_SIZE=${SHALE_BENCH_VALUE_SIZE:-1024}
CONC=${SHALE_BENCH_CONC:-4}

if [[ -n ${SHALE_BENCH_DIR:-} ]]; then
  WORK_DIR="$SHALE_BENCH_DIR"
  mkdir -p "$WORK_DIR"
else
  WORK_DIR=$(mktemp -d -t shale-bench-XXXXXX)
  trap 'rm -rf "$WORK_DIR"' EXIT
fi

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$WORK_DIR/shale-bench"

# Build the harness once. CGO disabled so the binary stays portable
# (this suite intentionally exercises only pure-Go backends; the
# slate-backend matrix uses a separate target).
echo "Building shale-bench..." >&2
( cd "$REPO_ROOT" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/shale-bench/ )
if [[ ! -x "$BIN" ]]; then
  echo "scripts/run-bench.sh: build failed" >&2
  exit 1
fi

# Each scenario: label,mode,backend,nodes,rf
SCENARIOS=(
  "raw-pebble,raw,pebble,1,1"
  "cluster-pebble-n1-r1,cluster,pebble,1,1"
  "cluster-pebble-n3-r1,cluster,pebble,3,1"
  "cluster-pebble-n3-r3,cluster,pebble,3,3"
  "raw-memory,raw,memory,1,1"
  "cluster-memory-n1-r1,cluster,memory,1,1"
  "cluster-memory-n3-r1,cluster,memory,3,1"
  "cluster-memory-n3-r3,cluster,memory,3,3"
)

# Run scenarios sequentially and aggregate.
RESULTS_FILE="$WORK_DIR/results.jsonl"
: > "$RESULTS_FILE"

for entry in "${SCENARIOS[@]}"; do
  IFS=',' read -r label mode backend nodes rf <<< "$entry"
  echo "Running $label (mode=$mode backend=$backend nodes=$nodes rf=$rf ops=$OPS)..." >&2
  set +e
  out=$("$BIN" \
    --mode "$mode" \
    --backend "$backend" \
    --nodes "$nodes" \
    --rf "$rf" \
    --ops "$OPS" \
    --value-size "$VALUE_SIZE" \
    --concurrency "$CONC" \
    --scenario "$label" \
    --json 2>"$WORK_DIR/$label.stderr")
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "$label FAILED (rc=$rc). stderr:" >&2
    cat "$WORK_DIR/$label.stderr" >&2
    # Synthesize a failure marker the renderer will pick up.
    printf '{"scenario":"%s","mode":"%s","backend":"%s","nodes":%d,"replication_factor":%d,"failed":true}\n' \
      "$label" "$mode" "$backend" "$nodes" "$rf" >> "$RESULTS_FILE"
    continue
  fi
  # Append as one line (jq -c) so the renderer can iterate.
  printf '%s\n' "$out" | tr -d '\n' >> "$RESULTS_FILE"
  printf '\n' >> "$RESULTS_FILE"
done

# Render the markdown table on stdout. Uses awk + sed (no jq dep) so
# the script runs anywhere Go does.
python3 - "$RESULTS_FILE" "$OPS" "$VALUE_SIZE" "$CONC" <<'PY'
import json
import sys
from datetime import datetime, timezone

path, ops, value_size, conc = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

rows = []
with open(path) as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))

def fmt(v, suffix=""):
    if v is None:
        return "FAILED"
    if isinstance(v, float):
        if v >= 100:
            return f"{v:.0f}{suffix}"
        if v >= 10:
            return f"{v:.1f}{suffix}"
        return f"{v:.2f}{suffix}"
    return f"{v}{suffix}"

def cell(row, phase, field):
    if row.get("failed"):
        return "FAILED"
    for p in row.get("phases", []):
        if p.get("phase") == phase:
            return fmt(p.get(field))
    return "-"

print(f"# shale v0.5 benchmark results")
print()
print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}  ")
print(f"Workload: {ops} ops per phase (write then read), {value_size}-byte values, {conc} concurrent clients.  ")
print()
print("| Scenario | Backend | Nodes | R | Put p50 (ms) | Put p99 (ms) | Get p50 (ms) | Get p99 (ms) | Put ops/s | Get ops/s |")
print("|---|---|---|---|---:|---:|---:|---:|---:|---:|")
for row in rows:
    print("| {scenario} | {backend} | {nodes} | {r} | {wp50} | {wp99} | {rp50} | {rp99} | {wtput} | {rtput} |".format(
        scenario=row.get("scenario", "-"),
        backend=row.get("backend", "-"),
        nodes=row.get("nodes", "-"),
        r=row.get("replication_factor", "-"),
        wp50=cell(row, "write", "p50_ms"),
        wp99=cell(row, "write", "p99_ms"),
        rp50=cell(row, "read", "p50_ms"),
        rp99=cell(row, "read", "p99_ms"),
        wtput=cell(row, "write", "throughput_ops_per_sec"),
        rtput=cell(row, "read", "throughput_ops_per_sec"),
    ))
PY
