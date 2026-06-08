package main

import (
	"reflect"
	"testing"
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
