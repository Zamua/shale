// envguard_test.go: PURE (tagless, no cgo/slatedb) tests of the per-process
// object-store env-config guard (envguard.go). These pin the fail-fast
// contract Config.applyEnv relies on: the first registration wins, an
// identical repeat is a no-op, and a CONFLICTING registration errors with
// the differing field names and NEVER leaks a credential value into the
// error message.

package slate

import (
	"strings"
	"sync"
	"testing"
)

func guardBaseTuple() envConfigTuple {
	return envConfigTuple{
		endpoint:  "http://127.0.0.1:9000",
		region:    "us-east-1",
		accessKey: "first-access-key-value",
		secretKey: "first-secret-key-value",
		useSSL:    false,
	}
}

func TestRegisterEnvConfig_FirstRegistrationSucceeds(t *testing.T) {
	resetEnvConfigForTest()
	if err := registerEnvConfig(guardBaseTuple()); err != nil {
		t.Fatalf("first registerEnvConfig: unexpected error: %v", err)
	}
}

func TestRegisterEnvConfig_IdenticalRepeatSucceeds(t *testing.T) {
	resetEnvConfigForTest()
	if err := registerEnvConfig(guardBaseTuple()); err != nil {
		t.Fatalf("first registerEnvConfig: %v", err)
	}
	if err := registerEnvConfig(guardBaseTuple()); err != nil {
		t.Fatalf("identical re-register: unexpected error: %v", err)
	}
}

// TestRegisterEnvConfig_ConflictNamesFieldWithoutSecretValues drives one
// conflict per tuple field and asserts the error (a) names the differing
// field and (b) contains NEITHER side's credential values.
func TestRegisterEnvConfig_ConflictNamesFieldWithoutSecretValues(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(*envConfigTuple)
		wantField string
	}{
		{"endpoint", func(c *envConfigTuple) { c.endpoint = "http://10.9.8.7:9000" }, "Endpoint"},
		{"region", func(c *envConfigTuple) { c.region = "eu-west-1" }, "Region"},
		{"accessKey", func(c *envConfigTuple) { c.accessKey = "second-access-key-value" }, "AccessKey"},
		{"secretKey", func(c *envConfigTuple) { c.secretKey = "second-secret-key-value" }, "SecretKey"},
		{"useSSL", func(c *envConfigTuple) { c.useSSL = true }, "UseSSL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetEnvConfigForTest()
			first := guardBaseTuple()
			if err := registerEnvConfig(first); err != nil {
				t.Fatalf("first registerEnvConfig: %v", err)
			}
			second := guardBaseTuple()
			tc.mutate(&second)
			err := registerEnvConfig(second)
			if err == nil {
				t.Fatalf("conflicting registerEnvConfig (%s): want error, got nil", tc.name)
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.wantField) {
				t.Errorf("conflict error does not name field %q: %q", tc.wantField, msg)
			}
			for _, secret := range []string{
				first.accessKey, first.secretKey,
				second.accessKey, second.secretKey,
			} {
				if strings.Contains(msg, secret) {
					t.Errorf("conflict error leaks credential value %q: %q", secret, msg)
				}
			}
		})
	}
}

// TestRegisterEnvConfig_ConcurrentRegistration races identical and
// conflicting registrations from many goroutines. Exactly one tuple wins
// (whichever registered first); every registration of the winner returns
// nil and every registration of the loser errors. Run under -race this
// also proves the registry itself is data-race free.
func TestRegisterEnvConfig_ConcurrentRegistration(t *testing.T) {
	resetEnvConfigForTest()
	tupleA := guardBaseTuple()
	tupleB := guardBaseTuple()
	tupleB.secretKey = "other-secret-key-value"

	const perSide = 16
	errsA := make([]error, perSide)
	errsB := make([]error, perSide)
	var wg sync.WaitGroup
	for i := 0; i < perSide; i++ {
		wg.Add(2)
		go func(i int) { defer wg.Done(); errsA[i] = registerEnvConfig(tupleA) }(i)
		go func(i int) { defer wg.Done(); errsB[i] = registerEnvConfig(tupleB) }(i)
	}
	wg.Wait()

	nilCount := func(errs []error) int {
		n := 0
		for _, err := range errs {
			if err == nil {
				n++
			}
		}
		return n
	}
	nilA, nilB := nilCount(errsA), nilCount(errsB)
	aWon := nilA == perSide && nilB == 0
	bWon := nilB == perSide && nilA == 0
	if !aWon && !bWon {
		t.Fatalf("concurrent registration: want one tuple to win every call, got %d/%d nil for A and %d/%d nil for B", nilA, perSide, nilB, perSide)
	}

	// The winner stays registered: re-registering it succeeds, the loser
	// still conflicts.
	winner, loser := tupleA, tupleB
	if bWon {
		winner, loser = tupleB, tupleA
	}
	if err := registerEnvConfig(winner); err != nil {
		t.Errorf("re-register of winning tuple after race: unexpected error: %v", err)
	}
	if err := registerEnvConfig(loser); err == nil {
		t.Errorf("register of losing tuple after race: want error, got nil")
	}
}
