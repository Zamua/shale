// Command shaled runs a single shale node as its own process. It opens
// the configured Backend, wraps it in a cluster.Cluster, and serves the
// gRPC ShaleNode service (pkg/rpc) on --grpc-addr. The CLI (cmd/shale)
// and, in v0.2+, peer nodes talk to this service.
//
// In v0.1 the node is single-node; --bind-addr and --seeds are accepted
// (so operators can pin a future-stable invocation today) but unused.
// In v0.2 they activate the memberlist + inter-node forwarding paths.
//
// Backend selection happens at process start via --backend. The slate
// backend requires a binary built with -tags slatedb; without the tag,
// --backend=slate fails fast with an instructive error (see
// backend_default.go vs backend_slatedb.go).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "shaled: %v\n", err)
		os.Exit(1)
	}
}

// config is the resolved set of options after flag + env parsing.
type config struct {
	nodeID   string
	grpcAddr string
	bindAddr string
	seeds    []string
	backend  string

	// slate-only; ignored when backend != "slate".
	slate slateConfig
}

// slateConfig mirrors the fields the slate backend will need. Held in
// its own struct so the slate build-tag stub and real impl can share
// the shape without main.go importing backend/slate directly.
type slateConfig struct {
	Bucket    string
	DbName    string
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

// run parses argv, wires up the node, serves until SIGINT/SIGTERM, then
// shuts down. Split from main so tests can drive it without exiting the
// process.
func run(argv []string, stderr *os.File) error {
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds)

	cfg, err := parseFlags(argv)
	if err != nil {
		return err
	}

	be, closeBackend, err := openBackend(cfg, logger)
	if err != nil {
		return err
	}

	c, err := cluster.Open(cluster.Config{
		NodeID:   cfg.nodeID,
		Backend:  be,
		BindAddr: cfg.bindAddr,
		Seeds:    cfg.seeds,
	})
	if err != nil {
		_ = closeBackend()
		return fmt.Errorf("open cluster: %w", err)
	}

	lis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		_ = c.Close()
		_ = closeBackend()
		return fmt.Errorf("listen %s: %w", cfg.grpcAddr, err)
	}

	grpcServer := grpc.NewServer()
	rpc.NewServer(c).Register(grpcServer)

	logger.Printf("shaled: node=%s grpc=%s backend=%s",
		cfg.nodeID, lis.Addr().String(), cfg.backend)
	if cfg.bindAddr != "" || len(cfg.seeds) > 0 {
		logger.Printf("shaled: bind-addr=%s seeds=%v (reserved for v0.2, unused in v0.1)",
			cfg.bindAddr, cfg.seeds)
	}

	// Serve in the background; surface listener errors back on serveErr.
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
		_ = closeBackend()
		if err != nil {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return errors.New("grpc serve returned unexpectedly")
	}

	// Graceful shutdown: stop accepting new RPCs, drain in-flight, then
	// release backend resources.
	grpcServer.GracefulStop()
	if err := <-serveErr; err != nil {
		logger.Printf("shaled: grpc serve returned: %v", err)
	}

	if err := c.Close(); err != nil {
		logger.Printf("shaled: cluster close: %v", err)
	}
	if err := closeBackend(); err != nil {
		logger.Printf("shaled: backend close: %v", err)
	}

	logger.Printf("shaled: shutdown complete")
	return nil
}

// parseFlags resolves config from argv. Every flag has an env-var
// fallback so the binary can be driven entirely from the environment
// (which is friendlier to compose / systemd / launchd).
func parseFlags(argv []string) (config, error) {
	fs := flag.NewFlagSet("shaled", flag.ContinueOnError)

	var (
		nodeID   = fs.String("node-id", envOr("SHALE_NODE_ID", ""), "unique node identifier (required)")
		grpcAddr = fs.String("grpc-addr", envOr("SHALE_GRPC_ADDR", ":7947"), "address the gRPC service binds to (host:port; host may be empty)")
		bindAddr = fs.String("bind-addr", envOr("SHALE_BIND_ADDR", ":7946"), "memberlist bind address (placeholder for v0.2; unused in v0.1)")
		seedsRaw = fs.String("seeds", envOr("SHALE_SEEDS", ""), "comma-separated peer addresses (placeholder for v0.2; unused in v0.1)")
		backend  = fs.String("backend", envOr("SHALE_BACKEND", ""), "backend engine: memory|slate (required)")

		slateBucket    = fs.String("slate-bucket", envOr("SHALE_SLATE_BUCKET", ""), "slate: object-storage bucket")
		slateDbName    = fs.String("slate-db-name", envOr("SHALE_SLATE_DB_NAME", ""), "slate: logical database name")
		slateEndpoint  = fs.String("slate-endpoint", envOr("SHALE_SLATE_ENDPOINT", ""), "slate: S3-compatible endpoint URL")
		slateRegion    = fs.String("slate-region", envOr("SHALE_SLATE_REGION", "us-east-1"), "slate: object-storage region")
		slateAccessKey = fs.String("slate-access-key", envOr("SHALE_SLATE_ACCESS_KEY", ""), "slate: access key ID")
		slateSecretKey = fs.String("slate-secret-key", envOr("SHALE_SLATE_SECRET_KEY", ""), "slate: secret access key")
		slateUseSSL    = fs.String("slate-use-ssl", envOr("SHALE_SLATE_USE_SSL", "true"), "slate: use TLS to reach the endpoint (true|false)")
	)

	if err := fs.Parse(argv); err != nil {
		return config{}, err
	}

	if strings.TrimSpace(*nodeID) == "" {
		return config{}, errors.New("--node-id is required (or set SHALE_NODE_ID)")
	}
	if strings.TrimSpace(*backend) == "" {
		return config{}, errors.New("--backend is required (memory|slate, or set SHALE_BACKEND)")
	}
	switch *backend {
	case "memory", "slate":
	default:
		return config{}, fmt.Errorf("--backend %q: must be one of memory|slate", *backend)
	}

	cfg := config{
		nodeID:   *nodeID,
		grpcAddr: *grpcAddr,
		bindAddr: *bindAddr,
		seeds:    splitSeeds(*seedsRaw),
		backend:  *backend,
		slate: slateConfig{
			Bucket:    *slateBucket,
			DbName:    *slateDbName,
			Endpoint:  *slateEndpoint,
			Region:    *slateRegion,
			AccessKey: *slateAccessKey,
			SecretKey: *slateSecretKey,
			UseSSL:    strings.EqualFold(*slateUseSSL, "true"),
		},
	}

	if cfg.backend == "slate" {
		// Required slate fields surface here so the error message lands
		// at parse time, not deep inside the backend constructor.
		if cfg.slate.Bucket == "" {
			return config{}, errors.New("--slate-bucket is required when --backend=slate")
		}
		if cfg.slate.DbName == "" {
			return config{}, errors.New("--slate-db-name is required when --backend=slate")
		}
		if cfg.slate.AccessKey == "" || cfg.slate.SecretKey == "" {
			return config{}, errors.New("--slate-access-key and --slate-secret-key are required when --backend=slate")
		}
	}

	return cfg, nil
}

// openBackend dispatches on cfg.backend. Returns the Backend, a close
// function (for releasing any backend-owned resources beyond what
// Backend.Close handles), and any setup error.
//
// The slate branch lives in backend_slatedb.go (built only with
// -tags slatedb); backend_default.go provides the fail-fast stub when
// the tag is missing.
func openBackend(cfg config, logger *log.Logger) (backend.Backend, func() error, error) {
	switch cfg.backend {
	case "memory":
		return openMemoryBackend(logger)
	case "slate":
		return openSlateBackend(cfg.slate, logger)
	default:
		// Defensive: parseFlags already rejected this.
		return nil, nil, fmt.Errorf("unknown backend %q", cfg.backend)
	}
}

// splitSeeds turns "a:7946,b:7946" into ["a:7946", "b:7946"], trimming
// whitespace and dropping empty entries.
func splitSeeds(raw string) []string {
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

// envOr returns the env-var value if set + non-empty, else fallback.
func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
