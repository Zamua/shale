// backend_slatedb.go: the real openSlateBackend, built only with
// -tags slatedb. Wires shaled-slate's parsed flags into
// slate.Config (including Settings, which stays at nil here so
// slatedb's own defaults apply; operators who want non-default
// Settings can fork this main and pass cfg.Settings explicitly).

//go:build slatedb

package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

func openSlateBackend(cfg slateConfig, logger *log.Logger) (backend.Backend, func() error, error) {
	be, err := slate.New(slate.Config{
		Bucket:    cfg.Bucket,
		DbName:    cfg.DbName,
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey,
		UseSSL:    cfg.UseSSL,
		// Settings is intentionally nil: shaled-slate exposes the
		// "operator runs slate with defaults" surface. Operators who
		// need non-default slatedb Settings build their own shaled-*
		// from this directory as a template.
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open slate: %w", err)
	}
	logger.Printf("shaled-slate: slate backend bucket=%s db=%s endpoint=%s region=%s",
		cfg.Bucket, cfg.DbName, cfg.Endpoint, cfg.Region)
	// Close is a no-op: backend.Backend.Close (invoked by
	// shaled.Run on shutdown) is enough for the slate backend.
	return be, func() error { return nil }, nil
}

// openSlateFactory builds the multi-backend slate Backing over the shared
// bucket and returns a per-node Handle (a storageunit.BackendFactory) plus a
// CloseFactory that releases any unit still mounted on the Handle at
// shutdown. The Handle implements OpenUnit/CloseUnit so cluster.Open mounts
// the node's owned units and a lease handoff is copy-free (every node's
// Handle points at the SAME bucket). --slate-db-name is ignored here: the
// per-unit DbName is derived from each GenUnit by the factory.
func openSlateFactory(cfg slateConfig, logger *log.Logger) (storageunit.BackendFactory, func() error, error) {
	backing, err := slate.NewBacking(slate.BackingConfig{
		Bucket:    cfg.Bucket,
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey,
		UseSSL:    cfg.UseSSL,
		KeyPrefix: cfg.KeyPrefix,
		// RelaxedReplicaDurability opts the R>=2 replica path into ack-at-
		// memtable writes (durability via replication + background WAL flush)
		// when the operator set --slate-relaxed-durability / SHALE_SLATE_
		// RELAXED_DURABILITY. No-op on the R=1 path. See BackingConfig.
		RelaxedReplicaDurability: cfg.RelaxedDurability,
		// Settings + Cache intentionally nil: shaled-slate exposes the
		// "operator runs slate with defaults" surface, same as the single
		// backend. Operators who need a shared block cache or non-default
		// slatedb Settings build their own shaled-* from this directory.
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open slate backing: %w", err)
	}
	handle := backing.Handle()
	logger.Printf("shaled-slate: multi-backend slate bucket=%s key-prefix=%q endpoint=%s region=%s units=%d relaxed-durability=%v",
		cfg.Bucket, cfg.KeyPrefix, cfg.Endpoint, cfg.Region, cfg.UnitCount.N(), cfg.RelaxedDurability)
	// CloseFactory releases any unit the Handle still has mounted after
	// Cluster.Close. Cluster.Close already closes the mounted units it
	// tracks; Handle.Close is the backing-level safety net (best-effort
	// flush + shutdown of anything left).
	return handle, handle.Close, nil
}

// openSlateCondStore builds the shared CAS arbiter over the same bucket the
// multi-backend factory uses. It backs the homogeneous try-join-else-form
// bootstrap (the __cluster/init form-lock + durable generation marker) and the
// decentralized reshard arbiter. NewBacking takes the endpoint WITH its scheme;
// the minio client behind the conditional store wants a bare host:port, so the
// scheme is stripped here (UseSSL already carries the transport choice).
func openSlateCondStore(cfg slateConfig) (storageunit.ConditionalStore, error) {
	host := strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "https://"), "http://")
	cs, err := slate.NewMinioConditionalStore(slate.MinioConditionalStoreConfig{
		EndpointHost: host,
		AccessKey:    cfg.AccessKey,
		SecretKey:    cfg.SecretKey,
		UseSSL:       cfg.UseSSL,
		Bucket:       cfg.Bucket,
		KeyPrefix:    cfg.KeyPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("open slate conditional store: %w", err)
	}
	return cs, nil
}
