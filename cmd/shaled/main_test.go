package main

import (
	"os"
	"strings"
	"testing"
)

func TestRun_RequiresNodeID(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--node-id") {
		t.Fatalf("want --node-id required error, got %v", err)
	}
}
