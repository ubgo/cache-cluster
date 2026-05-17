# Contributing to cache-cluster

Thanks for contributing. `cache-cluster` layers peer-aware distribution on top of any [`github.com/ubgo/cache`](https://github.com/ubgo/cache) backend. Read the contract section before changing routing or fill behaviour.

## Build, test, lint gate

This module has zero third-party dependencies. The full local gate, run from the module root, must be clean before any PR:

```sh
gofmt -w .                       # format (zero files reported by gofmt -l .)
go build ./...                   # must compile
go test -race -count=1 ./...     # race detector on, no flakes
golangci-lint run ./...          # must report 0 issues
```

Or via [Task](https://taskfile.dev/):

```sh
task fmt          # gofmt -w .
task test:race    # go test -race -count=2 ./...
task lint         # golangci-lint run ./...
task check        # fmt:check + vet + race tests (the pre-PR gate)
```

CI runs the same commands. A PR must be `gofmt`-clean, build, pass `go test -race ./...`, and produce **0** `golangci-lint` issues (`errcheck`, `govet`, `staticcheck`, `revive`, `gocritic`, `misspell`, `unused`, `ineffassign`, `unconvert`). The only configured exclusion is the unused `ctx` parameter, which interface compliance forces handlers/adapters to keep.

## The conformance-suite contract

The local backend a `Node` wraps must itself pass `github.com/ubgo/cache/cachetest`.`Run`. `cache-cluster` does not redefine the cache contract — it composes nodes that each delegate to a conformant `cache.Cache`. Tests must keep the single-node and multi-node routing/single-flight behaviour green; concurrent tests run under `-race`.

## Local dev setup (the `replace` directive)

`github.com/ubgo/cache` is not yet published. `go.mod` carries:

```
replace github.com/ubgo/cache => ../cache
```

A sibling checkout of the core repo at `../cache` is required to build and test. **Do not edit `go.mod`** (including the `replace`) in a feature PR; the replace is dropped and a real version pinned only when the core module is tagged, as a deliberate release step.

## Doc-comment style

- Every exported symbol has a doc comment that starts with its name (`revive`'s exported-comment rule is enabled — a non-conforming comment fails the lint gate).
- Document **why** and **invariants**, not just what. The consistent-hashing algorithm, owner routing, single-flight de-dup, and the peer HTTP protocol get inline comments explaining the algorithm and edge cases (empty ring, unknown owner, truncated peer responses).
- Lock-ordering and concurrency assumptions are documented at the method, not in a sidecar doc.

## Scope rules

Do not modify `go.mod`, `LICENSE`, `NOTICE`, or `.gitignore` in a behaviour PR. `CLAUDE.md` and `TECHNICAL.md` are gitignored local context files — keep them out of commits.
