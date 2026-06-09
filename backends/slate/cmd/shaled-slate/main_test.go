package main

import (
	"os"
	"strings"
	"testing"
)

func TestRun_RequiresNodeID(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "d")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--node-id") {
		t.Fatalf("want --node-id required error, got %v", err)
	}
}

func TestRun_RequiresBucket(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-bucket") {
		t.Fatalf("want --slate-bucket required error, got %v", err)
	}
}

func TestRun_RequiresDbName(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("want --slate-db-name required error, got %v", err)
	}
}

func TestRun_RequiresCreds(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "d")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-access-key") {
		t.Fatalf("want --slate-access-key required error, got %v", err)
	}
}
