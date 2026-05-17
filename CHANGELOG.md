# Changelog

All notable changes to `github.com/ubgo/cache-cluster` are documented here.
Format follows Keep a Changelog; the project follows SemVer (pre-GA in `v0.x`).

## [Unreleased]

### Added

- Consistent-hash `Ring` (virtual nodes; minimal reshuffle on membership
  change; `Add`/`Remove`/`Owner`/`Peers`).
- `Node`: owner-routed `Get`/`Set`/`Del`/`Has` with HTTP peer proxying,
  single-flight load-fill at the owner, and `Handler()` for peer-to-peer
  traffic. Backed by any `cache.Cache` for local storage.
- Zero third-party dependencies.
- Tested in-process (httptest peers): once-per-key cluster-wide fill,
  consistent ownership, non-owner Set routing, delete-forces-reload, ring
  distribution + no-rebalance-on-remove.

[Unreleased]: https://github.com/ubgo/cache-cluster/commits/main
