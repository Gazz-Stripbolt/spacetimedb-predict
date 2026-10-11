# Changelog

All notable changes to `@pogly/spacetimedb-predict` are listed here. It follows [semantic versioning](https://semver.org/).
While it's 0.x and experimental, minor versions may change the API.

## 0.1.0 (2026-10-11)

First release.

- `predict()`: a prediction layer over the SpacetimeDB client cache. `call()` shows a prediction at once, re-runs it
  on every server change (rebase), drops it when the call returns, and rolls it back on errors and timeouts.
- Temporary ids for auto-inc inserts (`isTempId`), `isPending` / `isPredicted` per row, `stats`, `resync()`.
- React: `usePredicted`, `usePredictedCall`.
- `shimDb`: `ctx.db` over a prediction draft, so a TypeScript module's reducer logic can run as the prediction.
