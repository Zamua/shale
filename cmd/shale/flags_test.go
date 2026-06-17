package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseGlobalDefaults(t *testing.T) {
	t.Setenv(envAddr, "")
	opts, sub, subArgs, err := parseGlobal([]string{"get", "alpha"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if opts.addr != defaultAddr {
		t.Fatalf("addr: want %q, got %q", defaultAddr, opts.addr)
	}
	if opts.json {
		t.Fatalf("json: want false")
	}
	if sub != "get" {
		t.Fatalf("sub: want get, got %q", sub)
	}
	if !reflect.DeepEqual(subArgs, []string{"alpha"}) {
		t.Fatalf("subArgs: want [alpha], got %v", subArgs)
	}
}

func TestParseGlobalEnvFallback(t *testing.T) {
	t.Setenv(envAddr, "10.0.0.5:7999")
	opts, _, _, err := parseGlobal([]string{"ping"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if opts.addr != "10.0.0.5:7999" {
		t.Fatalf("addr: want env value, got %q", opts.addr)
	}
}

func TestParseGlobalFlagBeatsEnv(t *testing.T) {
	t.Setenv(envAddr, "10.0.0.5:7999")
	opts, _, _, err := parseGlobal([]string{"--addr", "127.0.0.1:1234", "ping"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if opts.addr != "127.0.0.1:1234" {
		t.Fatalf("addr: want flag value, got %q", opts.addr)
	}
}

func TestParseGlobalEqualsForm(t *testing.T) {
	t.Setenv(envAddr, "")
	opts, sub, _, err := parseGlobal([]string{"--addr=127.0.0.1:1234", "stats"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if opts.addr != "127.0.0.1:1234" {
		t.Fatalf("addr: want 127.0.0.1:1234, got %q", opts.addr)
	}
	if sub != "stats" {
		t.Fatalf("sub: want stats, got %q", sub)
	}
}

func TestParseGlobalTimeoutFlag(t *testing.T) {
	t.Setenv(envAddr, "")
	// space form
	opts, sub, _, err := parseGlobal([]string{"--timeout", "15s", "put", "k", "v"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if opts.timeout != 15*time.Second {
		t.Fatalf("timeout: want 15s, got %v", opts.timeout)
	}
	if sub != "put" {
		t.Fatalf("sub: want put, got %q", sub)
	}
	// equals form
	opts, _, _, err = parseGlobal([]string{"--timeout=20s", "put", "k", "v"})
	if err != nil {
		t.Fatalf("parseGlobal equals: %v", err)
	}
	if opts.timeout != 20*time.Second {
		t.Fatalf("timeout equals: want 20s, got %v", opts.timeout)
	}
	// default when absent
	opts, _, _, err = parseGlobal([]string{"put", "k", "v"})
	if err != nil {
		t.Fatalf("parseGlobal default: %v", err)
	}
	if opts.timeout != defaultTimeout {
		t.Fatalf("timeout default: want %v, got %v", defaultTimeout, opts.timeout)
	}
	// invalid duration is a usage error
	if _, _, _, err := parseGlobal([]string{"--timeout", "notaduration", "put"}); err == nil {
		t.Fatalf("invalid --timeout must be a usage error")
	}
}

func TestParseGlobalJSONFlag(t *testing.T) {
	t.Setenv(envAddr, "")
	opts, sub, _, err := parseGlobal([]string{"--json", "topology"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if !opts.json {
		t.Fatalf("json: want true")
	}
	if sub != "topology" {
		t.Fatalf("sub: want topology, got %q", sub)
	}
}

func TestParseGlobalRejectsAddrWithoutValue(t *testing.T) {
	t.Setenv(envAddr, "")
	_, _, _, err := parseGlobal([]string{"--addr"})
	if err == nil {
		t.Fatalf("want error for --addr with no value")
	}
}

func TestParseGlobalSubcommandArgsPassThrough(t *testing.T) {
	t.Setenv(envAddr, "")
	_, sub, subArgs, err := parseGlobal([]string{"put", "k", "v"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if sub != "put" {
		t.Fatalf("sub: want put, got %q", sub)
	}
	if !reflect.DeepEqual(subArgs, []string{"k", "v"}) {
		t.Fatalf("subArgs: want [k v], got %v", subArgs)
	}
}

func TestParseGlobalDoubleDashEndsFlags(t *testing.T) {
	t.Setenv(envAddr, "")
	_, sub, subArgs, err := parseGlobal([]string{"--", "get", "--addr"})
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if sub != "get" {
		t.Fatalf("sub: want get, got %q", sub)
	}
	if !reflect.DeepEqual(subArgs, []string{"--addr"}) {
		t.Fatalf("subArgs: want [--addr], got %v", subArgs)
	}
}
