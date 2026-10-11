/**
 * Demo module: a shared task list. The reducer bodies live in ./logic.ts, which the web client
 * also runs as its prediction. Serves the web client at /route/.
 */
import { schema, table, t, Router, SyncResponse, SenderError } from 'spacetimedb/server';
import * as logic from './logic';
import PAGE from './page.gen';

const task = table(
  { name: 'task', public: true },
  {
    id: t.u64().primaryKey().autoInc(),
    owner: t.identity(),
    title: t.string(),
    done: t.bool(),
    pos: t.f64(),
  }
);

/**
 * Server-only state the client can't see: how many tasks each person added recently. Its rule
 * (at most 5 adds per 10 seconds) is what a prediction can't know, so the server can still say no.
 */
const rate = table(
  { name: 'rate' },
  { owner: t.identity().primaryKey(), windowStart: t.timestamp(), count: t.u32() }
);

const spacetimedb = schema({ task, rate });
export default spacetimedb;

const RATE_WINDOW_MICROS = 10_000_000n;
const RATE_MAX = 5;

type Ctx = Parameters<Parameters<typeof spacetimedb.reducer>[1]>[0];

/** Run shared logic with the real ctx; its refusals become SenderErrors. */
function run<A>(fn: (ctx: logic.Ctx, args: A) => void, ctx: Ctx, args: A) {
  try {
    fn(ctx as unknown as logic.Ctx, args);
  } catch (e) {
    if (e instanceof logic.Refused) throw new SenderError(e.message);
    throw e;
  }
}

export const addTask = spacetimedb.reducer({ title: t.string() }, (ctx, args) => {
  const now = ctx.timestamp.microsSinceUnixEpoch;
  const r = ctx.db.rate.owner.find(ctx.sender);
  if (!r || now - r.windowStart.microsSinceUnixEpoch > RATE_WINDOW_MICROS) {
    if (r) ctx.db.rate.owner.update({ ...r, windowStart: ctx.timestamp, count: 1 });
    else ctx.db.rate.insert({ owner: ctx.sender, windowStart: ctx.timestamp, count: 1 });
  } else if (r.count >= RATE_MAX) {
    throw new SenderError('slow down: at most 5 new tasks per 10 seconds');
  } else {
    ctx.db.rate.owner.update({ ...r, count: r.count + 1 });
  }
  run(logic.addTask, ctx, args);
});

export const toggleTask = spacetimedb.reducer({ id: t.u64() }, (ctx, args) => run(logic.toggleTask, ctx, args));
export const renameTask = spacetimedb.reducer({ id: t.u64(), title: t.string() }, (ctx, args) => run(logic.renameTask, ctx, args));
export const deleteTask = spacetimedb.reducer({ id: t.u64() }, (ctx, args) => run(logic.deleteTask, ctx, args));
export const moveTask = spacetimedb.reducer({ id: t.u64(), before: t.u64() }, (ctx, args) => run(logic.moveTask, ctx, args));

/** Test helper: forget everyone's rate limits. */
export const resetRate = spacetimedb.reducer((ctx) => {
  for (const r of [...ctx.db.rate.iter()]) ctx.db.rate.owner.delete(r.owner);
});

export const page = spacetimedb.httpHandler(
  () => new SyncResponse(PAGE, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' } })
);
export const router = spacetimedb.httpRouter(new Router().get('/', page));
