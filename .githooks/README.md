# .githooks

Repo-owned git hooks for shale. Plain shell scripts; no third-party hook
framework. Activate them once per clone with:

```
git config core.hooksPath .githooks
```

After that, every commit you make from this clone runs the hooks below.

## pre-commit

Runs against the staged Go files (no-op when none are staged):

1. `gofmt -l` on each staged `.go` file. Fails if any are not formatted.
2. `go vet ./...` across the whole module.
3. `golangci-lint run --new-from-rev HEAD --fix=false`, if the tool is
   on `PATH`. The hook prints a warning and skips this step when
   `golangci-lint` is not installed, so a fresh contributor isn't blocked.

The CI `lint` workflow runs the same checks (golangci-lint + gofmt) so
anything that bypasses the local hook is still caught upstream.

## Installing golangci-lint

The CI workflow pins **`v2.12.2`**. Install the same version locally:

```
brew install golangci-lint
# or pin explicitly:
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
```

Verify:

```
golangci-lint --version
```

## Bypassing the hook for one commit

`git commit --no-verify` skips the hook for a single commit. Use it
sparingly: the CI lint job will still flag any issue you skip.
