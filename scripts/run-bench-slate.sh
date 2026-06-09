#!/usr/bin/env bash
# scripts/run-bench-slate.sh - drive the v0.5 slate-backend bench suite.
#
# Builds cmd/shale-bench with -tags slatedb (CGO + the slatedb cdylib),
# brings up a local MinIO container, runs each slate scenario against
# it, and renders one markdown table on stdout. Sibling of
# scripts/run-bench.sh; kept separate because slate has external
# dependencies (Docker, SlateDB shared lib) that the pure pebble/memory
# suite should never require.
#
# MinIO lifecycle: this script starts and tears down its own MinIO
# container (`shale-bench-minio`, host port 19000). If an external
# MinIO is already running and you want to reuse it, set
# SHALE_BENCH_S3_ENDPOINT (+ bucket/credentials) before invoking and
# the script will skip the container dance.
#
# Required tunables (only when reusing an external MinIO):
#   SHALE_BENCH_S3_ENDPOINT     full URL (e.g. http://host:9000)
#   SHALE_BENCH_S3_BUCKET       must already exist
#   SHALE_BENCH_S3_ACCESS_KEY
#   SHALE_BENCH_S3_SECRET_KEY
#
# Optional tunables (apply either way):
#   SHALE_BENCH_OPS         per-scenario op count (default 500)
#   SHALE_BENCH_VALUE_SIZE  payload bytes per put (default 1024)
#   SHALE_BENCH_CONC        client goroutines (default 8)
#   SHALE_BENCH_DIR         workspace for binary + JSON (default mktemp)
#   SLATEDB_LIB_DIR         dir holding libslatedb_uniffi.{dylib,so}
#                           (default /private/tmp/slatedb-src/target/release;
#                           the Makefile's bench-v0.5-slate target inherits
#                           it from the same variable test-slate uses)
#
# Op counts default smaller than scripts/run-bench.sh because every
# slate Put round-trips to object storage; the harness is calibrated
# so each scenario finishes in < 60s on local MinIO at conc=8.

set -uo pipefail

OPS=${SHALE_BENCH_OPS:-500}
VALUE_SIZE=${SHALE_BENCH_VALUE_SIZE:-1024}
CONC=${SHALE_BENCH_CONC:-8}
SLATEDB_LIB_DIR=${SLATEDB_LIB_DIR:-/private/tmp/slatedb-src/target/release}

if [[ -n ${SHALE_BENCH_DIR:-} ]]; then
  WORK_DIR="$SHALE_BENCH_DIR"
  mkdir -p "$WORK_DIR"
else
  WORK_DIR=$(mktemp -d -t shale-bench-slate-XXXXXX)
  trap 'rm -rf "$WORK_DIR"' EXIT
fi

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$WORK_DIR/shale-bench"
MINIO_CONTAINER="shale-bench-minio"
MINIO_IMAGE="minio/minio:RELEASE.2024-01-16T16-07-38Z"
MC_IMAGE="minio/mc:RELEASE.2024-01-16T16-06-34Z"
MINIO_HOST_PORT=19000

# Track whether we started MinIO ourselves; only tear down what we
# brought up. Reusing an external MinIO is a feature (CI, shared dev
# box), but stomping on it would be a surprise.
STARTED_MINIO=0

cleanup_minio() {
  if [[ $STARTED_MINIO -eq 1 ]]; then
    echo "Tearing down MinIO container..." >&2
    docker rm -f "$MINIO_CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap 'cleanup_minio' EXIT

# Bring up MinIO if the operator didn't point us at one already. The
# bucket name is fixed when we own the container (no point in churning
# UUIDs; the container is wiped on teardown).
if [[ -z ${SHALE_BENCH_S3_ENDPOINT:-} ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "scripts/run-bench-slate.sh: docker not on PATH and no SHALE_BENCH_S3_ENDPOINT set" >&2
    exit 1
  fi

  # Clean up any stale container from a prior failed run before binding
  # the host port.
  docker rm -f "$MINIO_CONTAINER" >/dev/null 2>&1 || true

  export SHALE_BENCH_S3_ENDPOINT="http://127.0.0.1:${MINIO_HOST_PORT}"
  export SHALE_BENCH_S3_BUCKET="shale-bench"
  export SHALE_BENCH_S3_ACCESS_KEY="minioadmin"
  export SHALE_BENCH_S3_SECRET_KEY="minioadmin"

  echo "Starting MinIO ($MINIO_IMAGE) on :$MINIO_HOST_PORT..." >&2
  docker run -d --rm \
    --name "$MINIO_CONTAINER" \
    -p "${MINIO_HOST_PORT}:9000" \
    -e "MINIO_ROOT_USER=${SHALE_BENCH_S3_ACCESS_KEY}" \
    -e "MINIO_ROOT_PASSWORD=${SHALE_BENCH_S3_SECRET_KEY}" \
    "$MINIO_IMAGE" server /data >/dev/null
  STARTED_MINIO=1

  # Wait for /minio/health/live. ~30s upper bound; MinIO is usually
  # ready in <3s on this hardware.
  echo "Waiting for MinIO to come up..." >&2
  for _ in $(seq 1 30); do
    if curl -fs "${SHALE_BENCH_S3_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  if ! curl -fs "${SHALE_BENCH_S3_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
    echo "MinIO did not come up within 30s" >&2
    exit 1
  fi

  # Pre-create the bucket: SlateDB's object_store crate does not
  # auto-create.
  docker run --rm --network host \
    -e "MC_HOST_local=http://${SHALE_BENCH_S3_ACCESS_KEY}:${SHALE_BENCH_S3_SECRET_KEY}@127.0.0.1:${MINIO_HOST_PORT}" \
    "$MC_IMAGE" mb -p "local/${SHALE_BENCH_S3_BUCKET}" >/dev/null
else
  echo "Reusing existing MinIO at $SHALE_BENCH_S3_ENDPOINT (bucket=$SHALE_BENCH_S3_BUCKET)" >&2
fi

# Build the harness with -tags slatedb. CGO required.
echo "Building shale-bench (slatedb tag)..." >&2
( cd "$REPO_ROOT" && \
  CGO_ENABLED=1 \
  CGO_LDFLAGS="-L${SLATEDB_LIB_DIR}" \
  DYLD_LIBRARY_PATH="${SLATEDB_LIB_DIR}:${DYLD_LIBRARY_PATH:-}" \
  LD_LIBRARY_PATH="${SLATEDB_LIB_DIR}:${LD_LIBRARY_PATH:-}" \
  go build -tags slatedb -o "$BIN" ./cmd/shale-bench/ )
if [[ ! -x "$BIN" ]]; then
  echo "scripts/run-bench-slate.sh: build failed" >&2
  exit 1
fi

# Each slate node uses its own DbName (= node id) so the per-node
# writer epochs don't fence each other. Scenarios cover the same
# shape as the pebble/memory suite: raw baseline, 1-node cluster,
# 3-node cluster at R=1 and R=3. The "-relaxed" variants open slate
# with WriteOptions{AwaitDurable=false} via --slate-await-durable=false
# so we can measure the throughput delta of fast-ack mode.
#
# Per spec: relaxed + R=1 is unsafe (single replica crash loses
# un-flushed writes). Included here for measurement / parity with the
# strict suite; the markdown the docs render labels those rows
# "informational only, do not run in production without replication".
SCENARIOS=(
  "raw-slate,raw,slate,1,1,true"
  "cluster-slate-n1-r1,cluster,slate,1,1,true"
  "cluster-slate-n3-r1,cluster,slate,3,1,true"
  "cluster-slate-n3-r3,cluster,slate,3,3,true"
  "raw-slate-relaxed,raw,slate,1,1,false"
  "cluster-slate-n1-r1-relaxed,cluster,slate,1,1,false"
  "cluster-slate-n3-r1-relaxed,cluster,slate,3,1,false"
  "cluster-slate-n3-r3-relaxed,cluster,slate,3,3,false"
)

RESULTS_FILE="$WORK_DIR/results.jsonl"
: > "$RESULTS_FILE"

# Each scenario gets a unique key prefix in the bucket (the DbName is
# id-based on the bench side). To keep one scenario's stale writers
# from fencing the next scenario's opens, we rotate the BUCKET key
# namespace per scenario via a unique per-run suffix on DbName. But
# the binary's DbName scheme is fixed ("shale-bench-<id>"), so to
# avoid cross-scenario interference we re-create the bucket between
# scenarios when we own MinIO. (When reusing an external MinIO we
# leave it alone; the operator is responsible for cleanup.)
reset_bucket() {
  if [[ $STARTED_MINIO -ne 1 ]]; then
    return
  fi
  docker run --rm --network host \
    -e "MC_HOST_local=http://${SHALE_BENCH_S3_ACCESS_KEY}:${SHALE_BENCH_S3_SECRET_KEY}@127.0.0.1:${MINIO_HOST_PORT}" \
    "$MC_IMAGE" rb --force "local/${SHALE_BENCH_S3_BUCKET}" >/dev/null 2>&1 || true
  docker run --rm --network host \
    -e "MC_HOST_local=http://${SHALE_BENCH_S3_ACCESS_KEY}:${SHALE_BENCH_S3_SECRET_KEY}@127.0.0.1:${MINIO_HOST_PORT}" \
    "$MC_IMAGE" mb -p "local/${SHALE_BENCH_S3_BUCKET}" >/dev/null
}

export DYLD_LIBRARY_PATH="${SLATEDB_LIB_DIR}:${DYLD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="${SLATEDB_LIB_DIR}:${LD_LIBRARY_PATH:-}"

for entry in "${SCENARIOS[@]}"; do
  IFS=',' read -r label mode backend nodes rf await_durable <<< "$entry"
  reset_bucket
  echo "Running $label (mode=$mode backend=$backend nodes=$nodes rf=$rf await_durable=$await_durable ops=$OPS conc=$CONC)..." >&2
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
    --slate-await-durable="$await_durable" \
    --json 2>"$WORK_DIR/$label.stderr")
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "$label FAILED (rc=$rc). stderr:" >&2
    cat "$WORK_DIR/$label.stderr" >&2
    printf '{"scenario":"%s","mode":"%s","backend":"%s","nodes":%d,"replication_factor":%d,"await_durable":%s,"failed":true}\n' \
      "$label" "$mode" "$backend" "$nodes" "$rf" "$await_durable" >> "$RESULTS_FILE"
    continue
  fi
  # Tag the JSON row with await_durable before persisting so the table
  # renderer can split strict vs relaxed without re-parsing the label.
  printf '%s\n' "$out" \
    | python3 -c 'import json, sys; row=json.loads(sys.stdin.read()); row["await_durable"]=(sys.argv[1]=="true"); print(json.dumps(row))' "$await_durable" \
    >> "$RESULTS_FILE"
done

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

def put_tput(row):
    if row.get("failed"):
        return None
    for p in row.get("phases", []):
        if p.get("phase") == "write":
            return p.get("throughput_ops_per_sec")
    return None

print("# shale v0.5 slate benchmark results")
print()
print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}  ")
print(f"Workload: {ops} ops per phase (write then read), {value_size}-byte values, {conc} concurrent clients.  ")
print()

strict  = [r for r in rows if r.get("await_durable") is True]
relaxed = [r for r in rows if r.get("await_durable") is False]

def render(title, subset):
    print(f"## {title}")
    print()
    print("| Scenario | Backend | Nodes | R | Put p50 (ms) | Put p99 (ms) | Get p50 (ms) | Get p99 (ms) | Put ops/s | Get ops/s |")
    print("|---|---|---|---|---:|---:|---:|---:|---:|---:|")
    for row in subset:
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
    print()

render("AwaitDurable=true (strict; current shale default)", strict)
render("AwaitDurable=false (relaxed; pair with R>=2 in production)", relaxed)

# Side-by-side put-throughput comparison, strict vs relaxed, same shape.
def shape_key(r):
    return (r.get("mode"), r.get("nodes"), r.get("replication_factor"))
strict_by_shape  = {shape_key(r): r for r in strict}
relaxed_by_shape = {shape_key(r): r for r in relaxed}
print("## Strict vs relaxed put throughput")
print()
print("| Shape | Strict ops/s | Relaxed ops/s | Speedup |")
print("|---|---:|---:|---:|")
for shape in strict_by_shape:
    s_row = strict_by_shape[shape]
    r_row = relaxed_by_shape.get(shape)
    s_t = put_tput(s_row)
    r_t = put_tput(r_row) if r_row else None
    label = s_row.get("scenario", "-").replace("-relaxed", "")
    if s_t and r_t:
        speedup = f"{r_t / s_t:.1f}x"
    else:
        speedup = "-"
    print(f"| {label} | {fmt(s_t)} | {fmt(r_t)} | {speedup} |")
PY
