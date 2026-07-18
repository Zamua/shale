// envguard.go: the PURE per-process object-store env-config guard for the
// slate backend. Config.applyEnv (config.go, slatedb-tagged) configures the
// underlying object_store crate by writing PROCESS-GLOBAL AWS_* env vars, so
// a second construction with DIFFERENT object-store config in one process
// would silently clobber the first's env and authenticate requests with the
// wrong credentials. registerEnvConfig makes that mistake fail fast at
// construction time instead. It is INTENTIONALLY tagless (no slatedb/cgo
// dependency) so the guard's behavior is unit-testable WITHOUT the heavy
// slatedb Rust/cgo build (same precedent as dbname.go).

package slate

import (
	"fmt"
	"strings"
	"sync"
)

// envConfigTuple is the exact set of values Config.applyEnv writes to the
// process-global AWS_* env vars: Endpoint -> AWS_ENDPOINT_URL, Region ->
// AWS_REGION, AccessKey -> AWS_ACCESS_KEY_ID, SecretKey ->
// AWS_SECRET_ACCESS_KEY, UseSSL -> AWS_ALLOW_HTTP. (The bucket is NOT part
// of the tuple: it flows through the s3:// URL, not the env, so it cannot
// collide across constructions.)
type envConfigTuple struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	useSSL    bool
}

// envConfigRegistry records the FIRST tuple applyEnv applied in this
// process. Package state, guarded by mu; cleared only by the test-only
// resetEnvConfigForTest.
var envConfigRegistry struct {
	mu    sync.Mutex
	set   bool
	tuple envConfigTuple
}

// registerEnvConfig records t as this process's object-store env config.
// The first call records it and returns nil; an IDENTICAL repeat is a
// no-op returning nil (many constructions legitimately share one config,
// e.g. every Handle over one Backing). A CONFLICTING call returns an error
// naming ONLY the differing field names, never the values (AccessKey and
// SecretKey are secrets), so a second Backing/Slate with different
// object-store config fails fast at construction instead of silently
// clobbering the first's process env.
func registerEnvConfig(t envConfigTuple) error {
	envConfigRegistry.mu.Lock()
	defer envConfigRegistry.mu.Unlock()
	if !envConfigRegistry.set {
		envConfigRegistry.set = true
		envConfigRegistry.tuple = t
		return nil
	}
	prev := envConfigRegistry.tuple
	if t == prev {
		return nil
	}
	var fields []string
	if t.endpoint != prev.endpoint {
		fields = append(fields, "Endpoint")
	}
	if t.region != prev.region {
		fields = append(fields, "Region")
	}
	if t.accessKey != prev.accessKey {
		fields = append(fields, "AccessKey")
	}
	if t.secretKey != prev.secretKey {
		fields = append(fields, "SecretKey")
	}
	if t.useSSL != prev.useSSL {
		fields = append(fields, "UseSSL")
	}
	return fmt.Errorf("slate: conflicting object-store env config: %s differ from the config this process already applied (the AWS_* env vars are process-global; one process supports one object-store config)", strings.Join(fields, ", "))
}

// resetEnvConfigForTest clears the process registry so each guard test can
// exercise the first-registration path. Test-only: the registry is package
// state that is otherwise write-once per process.
func resetEnvConfigForTest() {
	envConfigRegistry.mu.Lock()
	defer envConfigRegistry.mu.Unlock()
	envConfigRegistry.set = false
	envConfigRegistry.tuple = envConfigTuple{}
}
