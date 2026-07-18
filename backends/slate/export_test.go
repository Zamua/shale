// export_test.go: test-only exports for the EXTERNAL slate_test package.
// Compiled only into the test binary, so nothing here is part of the
// shipped API surface.

package slate

// ResetEnvGuardForTest clears the per-process object-store env-config
// registry (envguard.go). The TAGGED integration fixtures (startMinIO /
// startFactoryMinIO and the openbench proxy) call it because each test
// stands up its OWN object-store endpoint (an ephemeral testcontainers
// port, or a per-test proxy), so the process sees a DIFFERENT env tuple
// per test: without the reset the first fixture's registration would make
// every later construction fail with the config-conflict error and the
// tagged suite could never run green in one process. Production keeps the
// write-once-per-process contract; only a fixture that OWNS the process
// env for its test may reset.
var ResetEnvGuardForTest = resetEnvConfigForTest
