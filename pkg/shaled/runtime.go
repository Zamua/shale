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
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on http.DefaultServeMux (debug server only)
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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

	// fs is the FlagSet BindStdFlags bound into, retained so Run can ask
	// whether a flag was EXPLICITLY supplied rather than defaulted. Only
	// --bind-addr needs this today: a single-backend binary must blank a
	// DEFAULTED bind address (so a bare `shaled` is a working single node)
	// while still honoring an explicit one (so the operator gets the clear
	// Open error instead of a silent downgrade). Nil when a caller built
	// StdConfig by hand rather than through BindStdFlags.
	fs *flag.FlagSet

	// bindAddrEnvSet records that SHALE_BIND_ADDR carried a non-empty value.
	// That is an explicit operator choice too, and flag.Visit cannot see it
	// (the env value arrives as the flag's DEFAULT). An empty value does not
	// count: envOr falls back to the flag default for it.
	bindAddrEnvSet bool

	// GracefulLeaveDrainTimeout, when > 0, makes a multi-backend overlap node
	// drain its owned units to successors on shutdown (broadcast leave, keep
	// serving until the successors are Ready) before closing, so a graceful
	// scale-down is available. 0 (default) disables it = the prior immediate
	// teardown. Threaded into cluster.Config.GracefulLeaveDrainTimeout.
	// Populated from --graceful-leave-drain / SHALE_GRACEFUL_LEAVE_DRAIN_TIMEOUT
	// by Validate (a Go duration string, e.g. "90s").
	GracefulLeaveDrainTimeout time.Duration

	// WriteTimeout, when > 0, overrides cluster.Config.WriteTimeout (the
	// wall-clock budget for a Put/Delete, INCLUDING the Option-A
	// transient-retry loop). 0 (default) leaves the cluster default (5s).
	// Raising it above the successor MOUNT window lets a write that lands
	// on a transiently-fenced/acquiring leg during a graceful scale-down
	// RETRY in-call until the successor is serving, instead of returning a
	// client-retryable error - the during-leave write-availability knob.
	// Populated from --write-timeout / SHALE_WRITE_TIMEOUT by Validate.
	WriteTimeout time.Duration

	// replicationFactorRaw + unitCountRaw hold the parsed int flag values
	// pre-validation. Populated by BindStdFlags; ReplicationFactor +
	// UnitCount are derived from them inside Validate.
	replicationFactorRaw  int
	unitCountRaw          int
	gracefulLeaveDrainRaw string
	writeTimeoutRaw       string
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
	// An EMPTY SHALE_BIND_ADDR is not an explicit choice: envOr falls back
	// to the flag default for it, so the resulting BindAddr is the default.
	std := &StdConfig{fs: fs, bindAddrEnvSet: os.Getenv("SHALE_BIND_ADDR") != ""}
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
	fs.StringVar(&std.gracefulLeaveDrainRaw, "graceful-leave-drain",
		envOr("SHALE_GRACEFUL_LEAVE_DRAIN_TIMEOUT", ""),
		"on shutdown, drain owned units to successors for up to this Go duration before closing (e.g. 90s); empty/0 disables")
	fs.StringVar(&std.writeTimeoutRaw, "write-timeout",
		envOr("SHALE_WRITE_TIMEOUT", ""),
		"override the per-write wall-clock budget incl. transient-retry (e.g. 12s); empty/0 keeps the 5s default")
	return std
}

// BindAddrExplicit reports whether the operator actually asked for this bind
// address, as opposed to inheriting the flag default. True when --bind-addr
// was passed on the command line or SHALE_BIND_ADDR was set in the
// environment.
//
// It exists because the flag's DEFAULT and an explicit value must be treated
// differently by a single-backend binary. A defaulted bind address is an
// artifact of a shared flag constructor (shaled-slate needs the default,
// because its multi-backend mode is a legitimate multi-node configuration);
// blanking it lets a bare `shaled` come up as the single node it now is. An
// EXPLICIT bind address is a real request for a cluster, and must reach
// cluster.Open so the operator gets the error naming the multi-backend
// requirement rather than a silently non-clustered node.
//
// Returns true when the FlagSet is unknown (a hand-built StdConfig), which
// fails toward honoring the value the caller set.
func (s *StdConfig) BindAddrExplicit() bool {
	if s.bindAddrEnvSet {
		return true
	}
	if s.fs == nil {
		return true
	}
	explicit := false
	s.fs.Visit(func(f *flag.Flag) {
		if f.Name == "bind-addr" {
			explicit = true
		}
	})
	return explicit
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
	if raw := strings.TrimSpace(s.gracefulLeaveDrainRaw); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("--graceful-leave-drain: %w (want a Go duration like 90s)", err)
		}
		if d < 0 {
			return errors.New("--graceful-leave-drain must not be negative")
		}
		s.GracefulLeaveDrainTimeout = d
	}
	if raw := strings.TrimSpace(s.writeTimeoutRaw); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("--write-timeout: %w (want a Go duration like 12s)", err)
		}
		if d < 0 {
			return errors.New("--write-timeout must not be negative")
		}
		s.WriteTimeout = d
	}
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

	// ConditionalStore, when set (multi-backend mode), enables the homogeneous
	// runtime try-join-else-form bootstrap: the cluster's form-vs-join decision
	// + durable generation live in the shared CAS store's __cluster/init marker
	// instead of the empty-seeds-means-founder config split. It also backs the
	// decentralized reshard arbiter. nil keeps the legacy seed-RPC bootstrap.
	ConditionalStore storageunit.ConditionalStore

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
	hasFactory, err := validateBackendXOR(cfg)
	if err != nil {
		return err
	}
	if !hasFactory {
		// Single-backend binaries are single-node. Drop a DEFAULTED bind
		// address so a bare invocation works, keep an explicit one so Open
		// can refuse it loudly, and reject seeds-without-bind-addr outright.
		bindAddr, err := resolveSingleBackendBindAddr(&cfg.Std)
		if err != nil {
			_ = closeBackendQuiet(cfg.CloseBackend)
			return err
		}
		cfg.Std.BindAddr = bindAddr
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

	// Build ONE cluster.Config from Std plus whichever backend mode is set,
	// using the resolved (post-bind) listener address as the broadcast
	// GRPCAddr. clusterConfig is the pure, testable seam: it does the whole
	// Backend-vs-BackendFactory -> cluster.Config mapping with no I/O, so a
	// test can assert the mapping (R + UnitCount + which mode) without binding
	// a port or serving gRPC.
	c, err := cluster.Open(clusterConfig(cfg, lis.Addr().String()))
	if err != nil {
		_ = lis.Close()
		_ = closeBackendQuiet(closeOwned)
		return fmt.Errorf("open cluster: %w", err)
	}

	// Optional debug/observability server (net/http/pprof). Off unless
	// SHALE_DEBUG_ADDR is set, so production is unaffected; when set (e.g. on a
	// staging node under investigation) it exposes /debug/pprof/goroutine so a
	// wedged mount / acquire can be inspected live via `kubectl port-forward`
	// without crashing the process. Bound to a SEPARATE listener from gRPC.
	if dbgAddr := os.Getenv("SHALE_DEBUG_ADDR"); dbgAddr != "" {
		// Per-node handoff-state dump alongside pprof (same DefaultServeMux).
		http.HandleFunc("/debug/shale/state", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(c.DebugState()))
		})
		dbgSrv := &http.Server{Addr: dbgAddr}
		go func() {
			logger.Printf("shaled: debug/pprof listening on %s", dbgAddr)
			if err := dbgSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Printf("shaled: debug server: %v", err)
			}
		}()
	}

	// Keepalive enforcement MUST permit the peer client's keepalive (set in
	// cluster.peerDialOptions: ClientParameters{Time: 30s, PermitWithoutStream:
	// true}). gRPC's DEFAULT server enforcement is MinTime 5m + no pings without
	// an active stream, so it answers a client that pings a (legitimately) idle
	// cross-node connection every 30s with GoAway "too_many_pings"
	// (ENHANCE_YOUR_CALM), tearing the connection down. That churned the
	// cross-node gen-learning + cross-shard scan on a slow cold-start (a peer
	// whose mount takes minutes keeps the connection idle long enough for the
	// keepalive to fire). MinTime 10s (< the client's 30s) + PermitWithoutStream
	// accepts the client's pings so the connection stays up.
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
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

	// Graceful shutdown ORDER MATTERS for the overlap scale-down drain. Run
	// c.Close() FIRST: it runs DrainForLeave at its top (while c.closed is still
	// false), and the leaving node must keep RECEIVING UNION WRITES over its gRPC
	// transport throughout that drain - the whole point of the overlap handoff
	// (it serves W=quorum for its moving units alongside the stable replica, so
	// the writes ride through instead of waiting on the successor mount). ONLY
	// AFTER the drain + cluster teardown do we GracefulStop the gRPC server.
	// DrainForLeave's own contract pins this: "the REAL departure (the
	// coordinator's Close ...) ... run AFTER this drain returns." The previous order
	// (GracefulStop first) tore the transport down BEFORE the drain, so every
	// union write a peer dual-wrote to this draining node got connection-refused
	// for the entire drain window (W=quorum unmet on its moving units = the
	// observed during-leave write blip).
	if err := c.Close(); err != nil {
		logger.Printf("shaled: cluster close: %v", err)
	}
	grpcServer.GracefulStop()
	if err := <-serveErr; err != nil {
		logger.Printf("shaled: grpc serve returned: %v", err)
	}
	if closeOwned != nil {
		if err := closeOwned(); err != nil {
			logger.Printf("shaled: backend close: %v", err)
		}
	}

	logger.Printf("shaled: shutdown complete")
	return nil
}

// validateBackendXOR enforces the Backend-vs-BackendFactory contract:
// exactly one of cfg.Backend / cfg.BackendFactory must be set. It is the
// fail-fast guard Run runs BEFORE binding the listener (and buildCluster runs
// before Open), mirroring cluster.validateBackendMode with a friendlier
// per-binary message. Returns hasFactory so callers can pick the right teardown
// hook without re-deriving it.
func validateBackendXOR(cfg RunConfig) (hasFactory bool, err error) {
	hasBackend := cfg.Backend != nil
	hasFactory = cfg.BackendFactory != nil
	switch {
	case hasBackend && hasFactory:
		return false, errors.New("shaled.Run: set EITHER Backend OR BackendFactory, not both")
	case !hasBackend && !hasFactory:
		return false, errors.New("shaled.Run: Backend or BackendFactory required")
	}
	return hasFactory, nil
}

// resolveSingleBackendBindAddr decides what bind address a SINGLE-BACKEND
// binary should actually run with, and refuses the one configuration that
// would otherwise fail silently.
//
// A single Backend is single-node only: it cannot open a storage unit, so it
// cannot fence a prior writer, so it cannot take part in a lease handoff.
// cluster.Open rejects Backend + BindAddr for exactly that reason. But the
// shared flag constructor defaults --bind-addr to ":7946" (shaled-slate needs
// that default, since its multi-backend mode IS a real multi-node
// configuration), so a bare `shaled` would inherit a bind address it never
// asked for and fail to start.
//
// Three cases, and only the middle one is a judgement call:
//
//   - seeds set, no explicit bind address: ERROR. Blanking here would come up
//     as an isolated single node that silently ignored the seeds. Across a
//     fleet that is N independent divergent stores, which is the same
//     silent-failure class the Open rejection exists to close.
//   - bind address DEFAULTED: blank it. The operator did not ask for a
//     cluster; give them the working single node a bare invocation should be.
//   - bind address EXPLICIT: keep it, and let cluster.Open refuse with the
//     message naming the multi-backend requirement. An operator who typed
//     --bind-addr wants a cluster and deserves to hear why they cannot have
//     one with this backend, rather than getting a quiet downgrade.
func resolveSingleBackendBindAddr(std *StdConfig) (string, error) {
	if std.BindAddrExplicit() {
		return std.BindAddr, nil
	}
	if len(std.Seeds) > 0 {
		return "", errors.New("shaled: --seeds requires --bind-addr; a single Backend is single-node only, " +
			"so seeds without a bind address would silently start an isolated node that joins nothing " +
			"(multi-node requires BackendFactory + --unit-count)")
	}
	return "", nil
}

// clusterConfig maps a RunConfig (plus the resolved gRPC broadcast address)
// onto ONE cluster.Config. It is pure (no I/O) so it is the testable seam for
// "Run threads ReplicationFactor + UnitCount + the right backend mode into the
// cluster.Config it builds":
//
//   - common (both modes): NodeID, the resolved GRPCAddr, the Coordinator (a
//     gossip adapter carrying BindAddr + Seeds, or nil for single-node), and
//     ReplicationFactor (the fix: single-backend Run previously omitted R,
//     pinning the legacy path to the cluster default).
//   - single-backend (Backend set): Backend only; UnitCount stays zero
//     (cluster.validateBackendMode requires it zero in legacy mode).
//   - multi-backend (BackendFactory set): BackendFactory + UnitCount; Backend
//     stays nil.
//
// It does NOT validate the XOR (callers do that first via validateBackendXOR);
// if BOTH are somehow set it prefers the factory, but that state never reaches
// here on the Run / buildCluster paths.
func clusterConfig(cfg RunConfig, grpcAddr string) cluster.Config {
	clusterCfg := cluster.Config{
		NodeID:                    cfg.Std.NodeID,
		GRPCAddr:                  grpcAddr,
		Coordinator:               coordinatorFor(cfg.Std),
		ReplicationFactor:         cfg.Std.ReplicationFactor,
		GracefulLeaveDrainTimeout: cfg.Std.GracefulLeaveDrainTimeout,
		WriteTimeout:              cfg.Std.WriteTimeout, // 0 -> cluster default (5s)
	}
	// GenLearnBudget: a joiner's total seed-generation re-sweep budget. 0 ->
	// cluster default (defaultGenLearnBudget, 180s). Overridable for a backend
	// whose cold-start mount runs longer than the default (and, with a small
	// value, to reproduce the pre-fix single-shot crash-loop on a real cluster).
	if d := envDur("SHALE_GEN_LEARN_BUDGET"); d > 0 {
		clusterCfg.GenLearnBudget = d
	}
	// SHALE_DEBUG_MOUNT_DELAY: a debug/repro lever that sleeps before this node's
	// unit mount (delaying when it begins serving gRPC), to reproduce the
	// cold-start window a joiner's gen query must ride out. No-op unless set.
	if d := envDur("SHALE_DEBUG_MOUNT_DELAY"); d > 0 {
		clusterCfg.TestingMountDelay = d
	}
	if cfg.BackendFactory != nil {
		clusterCfg.BackendFactory = cfg.BackendFactory
		clusterCfg.UnitCount = cfg.Std.UnitCount
	} else {
		clusterCfg.Backend = cfg.Backend
	}
	clusterCfg.ConditionalStore = cfg.ConditionalStore
	// The homogeneous deployment (a ConditionalStore-backed multi-backend
	// node) drives its shard count declaratively: the operator changes
	// SHALE_UNIT_COUNT in the deployment and applies it, and the cluster
	// reshards to match once every node agrees. Enable it whenever the arbiter
	// is wired in multi-backend mode; without an arbiter or in single-backend
	// mode it is inert.
	clusterCfg.DeclarativeReshard = cfg.ConditionalStore != nil && cfg.BackendFactory != nil
	return clusterCfg
}

// coordinatorFor builds this node's coordination adapter from the operator
// flags, or returns nil for single-node (no bind address = nobody to
// coordinate with, which is exactly what a nil Coordinator means to
// cluster.Open).
//
// The gossip knobs stop here: BindAddr, Seeds and the memberlist log sink are
// the ADAPTER's config, not the cluster's. Swapping in a lease/CAS coordinator
// later is a change to this one function.
func coordinatorFor(std StdConfig) coord.Coordinator {
	if std.BindAddr == "" {
		return nil
	}
	return gossip.New(gossip.Config{
		BindAddr: std.BindAddr,
		Seeds:    std.Seeds,
	})
}

// buildCluster validates the Backend-vs-BackendFactory XOR and opens a Cluster
// from the RunConfig, using grpcAddr as the broadcast gRPC address. It is the
// cluster-construction core extracted out of Run so the wiring (R + UnitCount +
// single-vs-multi mode -> a live cluster that accepts writes) is testable
// WITHOUT binding a listener, serving gRPC, or sending a shutdown signal: a
// test calls buildCluster with an in-process BackendFactory + ReplicationFactor
// and exercises Put/Get on the returned cluster directly. Run uses it after it
// has reserved the listener (so grpcAddr is the resolved bound address); a test
// can pass any addr (single-node clusters never dial it). The caller owns the
// returned cluster's Close.
func buildCluster(cfg RunConfig, grpcAddr string) (*cluster.Cluster, error) {
	if _, err := validateBackendXOR(cfg); err != nil {
		return nil, err
	}
	return cluster.Open(clusterConfig(cfg, grpcAddr))
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

// envDur parses a Go duration (e.g. "180s", "40s") from env; returns 0 when the
// key is unset, empty, or unparseable, so callers fall back to their default.
func envDur(key string) time.Duration {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return 0
}

func closeBackendQuiet(fn func() error) error {
	if fn == nil {
		return nil
	}
	return fn()
}
