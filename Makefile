# shale - developer Makefile
#
# Most day-to-day work is just `go test ./...` and `go build ./...`.
# The targets below cover the cases where build tags + cgo + a
# locally-built SlateDB shared library need to line up just so.
#
# SlateDB lib location: the slatedb-go binding links against the Rust
# crate's compiled cdylib (libslatedb_uniffi.{dylib,so}). Point
# SLATEDB_LIB_DIR at the directory containing it. Defaults to a
# `slatedb-src/target/release` sibling of this repo - override with
# `make SLATEDB_LIB_DIR=/path test-slate` if your checkout lives
# elsewhere.

SLATEDB_LIB_DIR ?= /private/tmp/slatedb-src/target/release

# Default test target - no cgo, no Docker, no build tags. Fast.
.PHONY: test
test:
	go test ./...

# Build target - same shape; the slate package is tag-gated so this
# does not require cgo or the SlateDB lib.
.PHONY: build
build:
	go build ./...

# Run the slate-backend unit tests (memory-only, no MinIO). Requires
# CGO + the SlateDB shared library.
.PHONY: test-slate
test-slate:
	CGO_ENABLED=1 \
	CGO_LDFLAGS="-L$(SLATEDB_LIB_DIR)" \
	DYLD_LIBRARY_PATH="$(SLATEDB_LIB_DIR):$$DYLD_LIBRARY_PATH" \
	LD_LIBRARY_PATH="$(SLATEDB_LIB_DIR):$$LD_LIBRARY_PATH" \
	go test -tags slatedb -count=1 -timeout 120s ./backends/slate/...

# End-to-end validation of the SlateDB backend against a real MinIO
# instance, spun up via testcontainers-go. Requires Docker (colima,
# Docker Desktop, or any Docker daemon reachable at the default
# socket) AND the SlateDB shared library + CGO toolchain that
# `test-slate` already needs. Long-running (~minutes); kept off the
# default `go test ./...` path via build tags so the regular dev
# loop stays fast.
#
# Docker socket discovery: testcontainers-go probes the standard
# locations (/var/run/docker.sock, Docker Desktop, rootless). On
# colima you need TWO env vars set:
#   DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
#     - tells the Docker client (and testcontainers) which socket to
#       talk to from the host.
#   TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
#     - tells the Ryuk reaper sidecar what path to bind-mount inside
#       its container. Ryuk needs the IN-VM socket path, which on
#       colima is /var/run/docker.sock (not the host path).
# Without the override, Ryuk fails to start ("operation not
# supported: could not start container") and every test fails before
# MinIO even comes up. Docker Desktop users don't need either var.
.PHONY: test-slate-minio
test-slate-minio:
	CGO_ENABLED=1 \
	CGO_LDFLAGS="-L$(SLATEDB_LIB_DIR)" \
	DYLD_LIBRARY_PATH="$(SLATEDB_LIB_DIR):$$DYLD_LIBRARY_PATH" \
	LD_LIBRARY_PATH="$(SLATEDB_LIB_DIR):$$LD_LIBRARY_PATH" \
	go test -tags 'slatedb integration' -count=1 -timeout 600s -v ./backends/slate/...

# v0.5 comparative benchmark suite: drives cmd/shale-bench across every
# scenario (raw pebble + memory baselines vs shale cluster at R=1 and
# R=3, single-node and 3-node) and prints one markdown table. Operators
# refresh docs/BENCH-v0.5.md via:
#
#   make bench-v0.5 > docs/BENCH-v0.5.md
#
# Tuning knobs (env vars):
#   SHALE_BENCH_OPS         per-scenario op count (default 10000)
#   SHALE_BENCH_VALUE_SIZE  payload bytes per put (default 1024)
#   SHALE_BENCH_CONC        client goroutines (default 4)
#   SHALE_BENCH_DIR         keep per-scenario JSON in a stable dir
#
# Numbers are machine-specific; do NOT pin a checked-in expected-output
# fixture against the result. The whole point of the harness is letting
# operators run it on THEIR target hardware.
.PHONY: bench-v0.5
bench-v0.5:
	bash scripts/run-bench.sh

# v0.5 slate-backend bench suite: runs the 4 slate scenarios (raw +
# cluster at R=1/R=3, single-node + 3-node) against a local MinIO
# spun up by the script via Docker. Requires:
#   - Docker reachable (colima on dev box; any daemon in CI)
#   - SlateDB cdylib at SLATEDB_LIB_DIR (inherited from this Makefile;
#     same path test-slate already uses)
#   - cgo toolchain (matches test-slate's expectations)
#
# Smaller op count than bench-v0.5 because each Put round-trips to
# object storage (~80-100ms on local MinIO); scenarios are sized to
# stay under 60s wall-clock each.
#
# Reuse an external MinIO instead of letting the script bring one
# up by exporting SHALE_BENCH_S3_ENDPOINT (+ bucket/credentials)
# before running. Refresh the docs via:
#
#   make bench-v0.5-slate > /tmp/bench-slate.md
#
# then paste the rendered table into the "Slate scenarios" section
# of docs/BENCH-v0.5.md.
.PHONY: bench-v0.5-slate
bench-v0.5-slate:
	SLATEDB_LIB_DIR="$(SLATEDB_LIB_DIR)" bash scripts/run-bench-slate.sh
