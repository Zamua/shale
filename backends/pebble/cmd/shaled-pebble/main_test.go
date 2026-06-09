package main

import (
	"os"
	"strings"
	"testing"
)

func TestRun_RequiresNodeID(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "")
	t.Setenv("SHALE_PEBBLE_DIR", "/tmp/shaled-pebble-test")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--node-id") {
		t.Fatalf("want --node-id required error, got %v", err)
	}
}

func TestRun_RequiresPebbleDir(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_PEBBLE_DIR", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--pebble-dir") {
		t.Fatalf("want --pebble-dir required error, got %v", err)
	}
}
