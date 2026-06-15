// Package shaled is the shared run-loop for every `shaled-*` binary.
//
// Pre-v0.5, cmd/shaled (in the core module) hardcoded the backend
// dispatch with a `--backend memory|pebble|slate` flag; that shape
// forced the core to import every backend module and defeated the
// dep-weight isolation the multi-module split is supposed to deliver.
// v0.5 splits the binary three ways:
//
//   - cmd/shaled in the core module is memory-only. It is the
//     reference + the integration-test driver.
//   - backends/slate/cmd/shaled-slate wires the slate backend.
//   - backends/pebble/cmd/shaled-pebble wires the pebble backend.
//
// All three (and any third-party shaled-*) share the same run-loop
// behavior: parse the standard cluster flags, open the gRPC listener,
// open the Cluster wrapping the supplied Backend, serve until
// SIGINT/SIGTERM, and shut down cleanly. That shared work lives here.
//
// Usage from a per-backend main:
//
//	fs := flag.NewFlagSet("shaled-foo", flag.ContinueOnError)
//	std := shaled.BindStdFlags(fs)
//	// register backend-specific flags here, e.g.
//	dir := fs.String("foo-dir", os.Getenv("SHALE_FOO_DIR"), "...")
//	if err := fs.Parse(argv); err != nil { ... }
//	if err := std.Validate(); err != nil { ... }
//
//	be, closeBackend, err := openFooBackend(*dir, logger)
//	if err != nil { ... }
//
//	return shaled.Run(shaled.RunConfig{
//	    Std:           *std,
//	    BackendLabel:  "foo",
//	    Backend:       be,
//	    CloseBackend:  closeBackend,
//	    Logger:        logger,
//	})
//
// The helper is intentionally narrow: it doesn't know what flags a
// particular backend wants, doesn't open any storage, and doesn't
// participate in error formatting beyond wrapping listen / cluster /
// grpc errors. Each shaled-* main owns its own flag set + backend
// constructor.
package shaled

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// StdConfig holds the standard cluster/node flags every shaled-*
// binary accepts. Populated by BindStdFlags after fs.Parse returns.
type StdConfig struct {
	NodeID   string
	GRPCAddr string
	BindAddr string
	Seeds    []string

	// ReplicationFactor is the number of nodes that hold a copy of each
	// key. Threaded into cluster.Config.ReplicationFactor in BOTH single-
	// backend and multi-backend modes. Default 1 (single owner per key, no
	// replicas); cluster.normalizeConfig normalizes a 0 to 1, so a default
	// and an explicit 1 are identical. Populated from --replication-factor /
	// SHALE_REPLICATION_FACTOR by Validate.
	ReplicationFactor int

	// UnitCount is the validated power-of-two number of storage units N for
	// MULTI-BACKEND mode. Consumed ONLY when the binary's main supplies a
	// BackendFactory (RunConfig.BackendFactory); in single-backend mode it is
	// ignored. Validate parses --unit-count / SHALE_UNIT_COUNT through
	// storageunit.NewUnitCount (rejecting non-power-of-two values before any
	// backend opens) and stores the resulting value here so Run never
	// re-parses it. Default 1.
	UnitCount storageunit.UnitCount

	// seedsRaw holds the comma-separated form pre-split; tests that
	// poke at the flag set directly can inspect it. Populated by
	// BindStdFlags after Parse; Seeds is derived from it inside
	// Validate.
	seedsRaw string

	// replicationFactorRaw + unitCountRaw hold the parsed int flag values
	// pre-validation. Populated by BindStdFlags; ReplicationFactor +
	// UnitCount are derived from them inside Validate.
	replicationFactorRaw int
	unitCountRaw         int
}

// BindStdFlags registers the standard cluster/node flags into fs and
// returns a *StdConfig whose fields are populated after fs.Parse
// runs. Backend-specific flags should be registered on the SAME
// fs (so the resulting binary has one merged --help surface).
//
// Flags + env-var fallbacks registered:
//
//	--node-id            SHALE_NODE_ID             (required)
//	--grpc-addr          SHALE_GRPC_ADDR           (default ":7947")
//	--bind-addr          SHALE_BIND_ADDR           (default ":7946")
//	--seeds              SHALE_SEEDS               (comma-separated; optional)
//	--replication-factor SHALE_REPLICATION_FACTOR  (int; default 1)
//	--unit-count         SHALE_UNIT_COUNT          (power-of-two int; default 1)
//
// Validation (required-ness, parsing of --seeds into a slice, the
// --unit-count power-of-two check) happens in StdConfig.Validate, which
// the caller runs after fs.Parse.
func BindStdFlags(fs *flag.FlagSet) *StdConfig {
	std := &StdConfig{}
	fs.StringVar(&std.NodeID, "node-id", envOr("SHALE_NODE_ID", ""),
		"unique node identifier (required)")
	fs.StringVar(&std.GRPCAddr, "grpc-addr", envOr("SHALE_GRPC_ADDR", ":7947"),
		"address the gRPC service binds to (host:port; host may be empty)")
	fs.StringVar(&std.BindAddr, "bind-addr", envOr("SHALE_BIND_ADDR", ":7946"),
		"memberlist bind address")
	fs.StringVar(&std.seedsRaw, "seeds", envOr("SHALE_SEEDS", ""),
		"comma-separated peer addresses for cluster join")
	fs.IntVar(&std.replicationFactorRaw, "replication-factor",
		envOrInt("SHALE_REPLICATION_FACTOR", 1),
		"number of nodes that hold a copy of each key (default 1)")
	fs.IntVar(&std.unitCountRaw, "unit-count",
		envOrInt("SHALE_UNIT_COUNT", 1),
		"power-of-two number of storage units N (multi-backend mode only; default 1)")
	return std
}

// Validate enforces required fields and finalizes derived values
// (parsing --seeds into a slice, validating --unit-count as a power of
// two, recording --replication-factor). Call after fs.Parse.
func (s *StdConfig) Validate() error {
	if strings.TrimSpace(s.NodeID) == "" {
		return errors.New("--node-id is required (or set SHALE_NODE_ID)")
	}
	s.Seeds = SplitSeeds(s.seedsRaw)
	s.ReplicationFactor = s.replicationFactorRaw
	uc, err := storageunit.NewUnitCount(s.unitCountRaw)
	if err != nil {
		return fmt.Errorf("--unit-count: %w (must be a power of two)", err)
	}
	s.UnitCount = uc
	return nil
}

// RunConfig bundles everything Run needs to drive a single shaled-*
// process. Constructed by per-backend mains after they've parsed
// flags + opened their backend.
type RunConfig struct {
	// Std is the validated standard cluster/node config.
	Std StdConfig

	// BackendLabel is the short name printed in the startup log line
	// (e.g. "memory", "pebble", "slate"). Cosmetic.
	BackendLabel string

	// Backend is the opened backend.Backend for SINGLE-BACKEND (legacy
	// per-node) mode. Run takes ownership for the lifetime of the process;
	// the Cluster's Close releases it. Mutually exclusive with
	// BackendFactory: exactly one of Backend / BackendFactory must be set
	// (Run validates the XOR before binding the listener).
	Backend backend.Backend

	// CloseBackend, if non-nil, is invoked after the Cluster has
	// been closed and is the place to release any backend-owned
	// resources that don't fit inside Backend.Close (file locks,
	// background goroutines the constructor owns, etc.). Single-backend
	// mode only.
	CloseBackend func() error

	// BackendFactory selects MULTI-BACKEND mode (v0.8): the per-node
	// factory that opens / closes one backend per owned storage unit. The
	// cluster mounts owned units and routes each key to its unit's owner
	// (key -> shardKey -> unit -> owner) instead of a single Backend. The
	// unit count is taken from Std.UnitCount. Mutually exclusive with
	// Backend; Run validates the XOR.
	BackendFactory storageunit.BackendFactory

	// CloseFactory, if non-nil, is the multi-backend analogue of
	// CloseBackend: invoked after Cluster.Close to release any backing-level
	// resources the factory owns (the slate Backing's shared connection
	// state, an operator-owned block cache, etc.). Cluster.Close already
	// closes every MOUNTED unit via the factory; CloseFactory is for
	// backing-level teardown the cluster does not own.
	CloseFactory func() error

	// Logger is where startup + shutdown lines are written. Required.
	Logger *log.Logger
}

// Run wires the supplied Backend into a Cluster, serves gRPC on
// Std.GRPCAddr, blocks until SIGINT/SIGTERM, and shuts down cleanly.
// Returns nil on a clean shutdown; non-nil if listener bind, cluster
// open, or gRPC serve fails.
func Run(cfg RunConfig) error {
	if cfg.Logger == nil {
		return errors.New("shaled.Run: Logger required")
	}
	// Backend-vs-BackendFactory XOR, validated BEFORE binding the listener
	// so a misconfigured node fails fast with a clear per-binary error and
	// never reserves a port. The cluster re-validates downstream
	// (validateBackendMode), but this is the friendlier first line.
	hasBackend := cfg.Backend != nil
	hasFactory := cfg.BackendFactory != nil
	switch {
	case hasBackend && hasFactory:
		return errors.New("shaled.Run: set EITHER Backend OR BackendFactory, not both")
	case !hasBackend && !hasFactory:
		return errors.New("shaled.Run: Backend or BackendFactory required")
	}
	logger := cfg.Logger

	// closeOwned releases whichever backing resource this run owns (the
	// single backend's CloseBackend, or the factory's CloseFactory). Used on
	// every early-return path below and after a clean shutdown.
	closeOwned := cfg.CloseBackend
	if hasFactory {
		closeOwned = cfg.CloseFactory
	}

	// We must reserve the gRPC listener BEFORE opening the cluster so
	// that the GRPCAddr we broadcast to peers is the resolved bound
	// address (matters when --grpc-addr uses port 0 for an ephemeral
	// listener; tests do this).
	lis, err := net.Listen("tcp", cfg.Std.GRPCAddr)
	if err != nil {
		_ = closeBackendQuiet(closeOwned)
		return fmt.Errorf("listen %s: %w", cfg.Std.GRPCAddr, err)
	}

	// Build ONE cluster.Config from Std plus whichever backend mode is set.
	// ReplicationFactor is threaded in BOTH modes (the fix: single-backend
	// Run previously omitted it, pinning the legacy path to the cluster
	// default). UnitCount is carried only in multi-backend mode; legacy mode
	// leaves it zero (validateBackendMode requires that).
	clusterCfg := cluster.Config{
		NodeID:            cfg.Std.NodeID,
		BindAddr:          cfg.Std.BindAddr,
		GRPCAddr:          lis.Addr().String(),
		Seeds:             cfg.Std.Seeds,
		ReplicationFactor: cfg.Std.ReplicationFactor,
	}
	if hasFactory {
		clusterCfg.BackendFactory = cfg.BackendFactory
		clusterCfg.UnitCount = cfg.Std.UnitCount
	} else {
		clusterCfg.Backend = cfg.Backend
	}

	c, err := cluster.Open(clusterCfg)
	if err != nil {
		_ = lis.Close()
		_ = closeBackendQuiet(closeOwned)
		return fmt.Errorf("open cluster: %w", err)
	}

	grpcServer := grpc.NewServer()
	rpc.NewServer(c).Register(grpcServer)

	logger.Printf("shaled: node=%s grpc=%s backend=%s",
		cfg.Std.NodeID, lis.Addr().String(), cfg.BackendLabel)
	if cfg.Std.BindAddr != "" {
		logger.Printf("shaled: bind-addr=%s seeds=%v", cfg.Std.BindAddr, cfg.Std.Seeds)
		logger.Printf("shaled: joined cluster, members=%d", len(c.Members()))
	}

	// Serve in the background; surface listener errors back on
	// serveErr.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(lis)
	}()

	// Wait for SIGINT/SIGTERM or a fatal serve error.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		logger.Printf("shaled: shutdown signal received, draining gRPC...")
	case err := <-serveErr:
		// Serve returned before any signal: the listener died or grpc
		// hit a fatal error. Treat it as a startup/runtime failure.
		_ = c.Close()
		_ = closeBackendQuiet(closeOwned)
		if err != nil {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return errors.New("grpc serve returned unexpectedly")
	}

	// Graceful shutdown: stop accepting new RPCs, drain in-flight,
	// then release backend resources.
	grpcServer.GracefulStop()
	if err := <-serveErr; err != nil {
		logger.Printf("shaled: grpc serve returned: %v", err)
	}

	if err := c.Close(); err != nil {
		logger.Printf("shaled: cluster close: %v", err)
	}
	if closeOwned != nil {
		if err := closeOwned(); err != nil {
			logger.Printf("shaled: backend close: %v", err)
		}
	}

	logger.Printf("shaled: shutdown complete")
	return nil
}

// SplitSeeds turns "a:7946,b:7946" into ["a:7946", "b:7946"], trimming
// whitespace and dropping empty entries. Exported so per-backend tests
// can share the same parsing as the runtime.
func SplitSeeds(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// EnvOr returns the env-var value if set + non-empty, else fallback.
// Exported so per-backend mains can use the same convention for their
// backend-specific flags without re-implementing the helper.
func EnvOr(key, fallback string) string {
	return envOr(key, fallback)
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// envOrInt returns the env-var value parsed as an int if set + non-empty
// + parseable, else fallback. A non-empty but unparseable value falls back
// silently to fallback; the flag layer re-surfaces an explicit bad
// --flag=value at parse time, and StdConfig.Validate re-validates the final
// value (e.g. the --unit-count power-of-two check), so a bad env value is
// caught downstream rather than panicking here.
func envOrInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func closeBackendQuiet(fn func() error) error {
	if fn == nil {
		return nil
	}
	return fn()
}
