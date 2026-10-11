# @pogly/spacetimedb-predict

Optimistic updates and client-side prediction for SpacetimeDB clients (experimental). Call a reducer and the UI
changes at once; the prediction is re-run on every server change until the reply comes back, then the real rows take
over without a flicker. Refused or timed-out calls roll back.

```bash
npm install @pogly/spacetimedb-predict
```

```ts
import { predict } from '@pogly/spacetimedb-predict';

const p = predict({ task: { table: conn.db.task, key: (t) => t.id, autoInc: 'id' } });

await p.call(
  () => conn.reducers.toggleTask({ id }),
  (d) => {
    const t = d.task.find(id);
    if (t) d.task.update({ ...t, done: !t.done });
  }
);
p.subscribe(() => render(p.table('task').rows()));
```

- React: `usePredicted(p, 'task')` and `usePredictedCall(p, send, prediction)` from `@pogly/spacetimedb-predict/react`.
- Shared reducer logic (TypeScript modules): `shimDb(draft, { task: { unique: ['id'] } })` from
  `@pogly/spacetimedb-predict/shim` gives the logic a `ctx.db` over the prediction.

The full README, a demo and the measurements are in the
[repository](https://github.com/Gazz-Stripbolt/spacetimedb-predict).
