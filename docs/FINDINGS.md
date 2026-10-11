# Findings: client-side prediction for SpacetimeDB (experiment)

What we tried, what we measured and what we'd recommend. SpacetimeDB 2.11, TypeScript SDK.

## The question

Upstream issues [#2453](https://github.com/clockworklabs/SpacetimeDB/issues/2453) (client-side prediction) and
[#4442](https://github.com/clockworklabs/SpacetimeDB/issues/4442) (optimistic updates for reducers) ask for the UI to
change the moment a user acts, instead of after the round trip. Can a client library do that well today, without
changes to the SDK?

## Answer: yes

A prediction layer over the SDK's client cache works, and it needed nothing from the SDK beyond its public cache
and callbacks.

| | Measured |
|---|---|
| Click to visible, predicted | **0.8 ms** (node), **1 ms** (browser) |
| Click to visible, waiting for the server | 304–316 ms (through a proxy adding 150 ms each way) |
| Frames with the predicted and the real row both showing | 0 (sampled every animation frame) |
| Rollback when the server refuses | immediate on the reply; the toast shows the server's message |

Behaviour tests ([`tests/predict.test.mts`](../tests/predict.test.mts), 8/8) and browser tests
([`tests/browser.test.mts`](../tests/browser.test.mts), 4/4) cover: inserts with temporary ids, updates, ordered
pending inserts, a refusal the client couldn't foresee (a private rate-limit table), someone else's change landing
under a pending prediction (rebased), a prediction that stops applying (dropped, then refused by the server too),
and timeouts.

## How it works

- **Predictions are functions, re-run on every server change.** Not recorded diffs: a "toggle" re-run on top of
  someone else's toggle still means "flip whatever is there now". This is the Replicache/Linear model: server state,
  plus pending mutations replayed in call order.
- **The SDK cache stays the source of truth.** The layer keeps its own copy of the server rows in step with the
  cache's insert/update/delete callbacks, and stacks the predictions on top, each in its own copy-on-write layer.
  Nothing is written into the SDK's cache.
- **No flicker on confirmation, thanks to the SDK's ordering.** In 2.11 a `ReducerResult` carries the caller's
  transaction. The SDK applies it to the cache and fires callbacks first, and settles the reducer's promise a few
  microtasks later. Dropping the prediction when the promise settles is therefore seamless, as long as listeners
  aren't told about the server change in between. The first version notified on a microtask, and the test caught
  the real row and its prediction in the same notified state. Server-driven notifications now go out on the next
  task, after the call has settled; local ones (a new prediction) stay on a microtask, before anything can paint.
- **Temporary ids** for auto-inc inserts count down from the top of the u64 range, so they sort after real rows and
  never collide. A prediction gets the same temp ids on every re-run, so keys are stable while it's pending.
- **Write it once (TS modules).** The demo's reducer bodies live in `logic.ts`. The module runs them with the real
  `ctx`; the page runs the *same functions* as predictions, with `shimDb` providing `ctx.db` (insert, iter, unique
  `find`/`update`/`delete`, index `filter`) over the prediction's draft. Rules the client can't see (private tables,
  rate limits) stay in the module's wrapper; the server's refusal rolls the prediction back.

## Cost

[`tests/bench.mts`](../tests/bench.mts): rebuilding the predicted view after a server change (median, ms):

| Rows | 0 pending | 1 pending | 10 pending |
|---|---|---|---|
| 100 | 0.00 | 0.05 | 0.07 |
| 1,000 | 0.00 | 0.27 | 0.28 |
| 10,000 | 0.03 | 0.99 | 0.99 |
| 50,000 | 0.25 | 5.85 | 5.98 |

The first version copied every table once per pending prediction (66 ms at 50,000 rows with 10 pending). With a
maintained base and per-prediction layers, the cost is reading the merged rows once per change, plus the
predictions' own work.

## Limits

- **Shared logic needs a TypeScript module.** Rust and C# modules (Pogly's is C#) can still use the layer, with
  hand-written predictions, usually a few lines each.
- **The React key of a predicted insert changes on confirmation** (temp id → real id), so that one row remounts.
  Reducers don't return the ids they assigned, so the layer can't map them. A client-generated id column (or a
  UUID primary key) avoids it.
- **Non-deterministic logic predicts approximately.** `ctx.timestamp`, random numbers and anything from private
  tables differ on the client; the confirmed row simply replaces the guess.
- **Reconnects:** pending calls whose replies never come time out (10 s default) and roll back. `resync()` re-reads
  the cache after a resubscribe.
- **Reducers called from one connection are applied in call order**, which the rebase relies on. Calls from several
  connections at once would need a shared predictor.

## For Pogly

Pogly already previews continuous edits locally (drags, sliders) and commits at the end, so those gain nothing.
Discrete actions wait a round trip today: show/hide (`UpdateElementTransparency`, 15 call sites), delete, z-order,
group/ungroup, text edits. Each waits one round trip to Maincloud: tens of milliseconds nearby, more from far
away (not measured here). Hand-written predictions for 3–4 of those would make them instant. It's worth a small trial, not a
rewrite, and Pogly's own element store would need to read from the layer.

## Recommendation

**Go**, as an experimental 0.x package, `@pogly/spacetimedb-predict` (core + React hooks + `shimDb`). It fills a
real gap that upstream has on its roadmap but not in the SDK. If Clockwork ships native prediction, this becomes a
reference for the behaviour (ordering, rebase, the confirmation-flicker fix).
