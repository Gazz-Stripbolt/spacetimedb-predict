// Prediction behaviour against a real module, through a latency proxy.
//
//   DB=predict npx tsx tests/predict.test.mts      (server on :3000; the test starts the proxy)
//
// "a" talks through the proxy (+DELAY ms each way) and predicts; "b" talks to the server directly
// and plays someone else changing things underneath a's predictions.
import assert from 'node:assert/strict';
import type { Identity } from 'spacetimedb';
import { predict, isTempId, PredictionTimeout, type Draft } from '../client/src/index.ts';
import { shimDb } from '../client/src/shim.ts';
import * as logic from '../demo/module/src/logic.ts';
import { DbConnection } from '../demo/web/src/bindings/index.ts';
import { startProxy } from './proxy.mts';

const DB = process.env.DB ?? 'predict';
const DELAY = Number(process.env.DELAY ?? 150);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
type Tables = { task: logic.Task };

const proxy = await startProxy(3100, 3000, DELAY);

function connect(port: number): Promise<{ conn: DbConnection; identity: Identity }> {
  return new Promise((resolve, reject) =>
    DbConnection.builder()
      .withUri(`ws://127.0.0.1:${port}`)
      .withDatabaseName(DB)
      .onConnect((conn, identity) => {
        conn.subscriptionBuilder().onApplied(() => resolve({ conn, identity })).subscribe(['SELECT * FROM task']);
      })
      .onConnectError((_c, e) => reject(e))
      .build()
  );
}

const a = await connect(3100);
const b = await connect(3000);
const p = predict<Tables>({ task: { table: a.conn.db.task, key: (t) => t.id, autoInc: 'id' } });
const view = p.table('task');
const shared =
  <A,>(id: Identity, fn: (ctx: logic.Ctx, args: A) => void, args: A) =>
  (d: Draft<Tables>) =>
    fn({ sender: id, db: shimDb(d, { task: { unique: ['id'] } }) } as logic.Ctx, args);
const A = {
  add: (title: string) => p.call(() => a.conn.reducers.addTask({ title }), shared(a.identity, logic.addTask, { title })),
  toggle: (id: bigint) => p.call(() => a.conn.reducers.toggleTask({ id }), shared(a.identity, logic.toggleTask, { id })),
  rename: (id: bigint, title: string) => p.call(() => a.conn.reducers.renameTask({ id, title }), shared(a.identity, logic.renameTask, { id, title })),
};
const rows = () => [...view.rows()].sort((x, y) => x.pos - y.pos);
const byTitle = (t: string) => rows().find((r) => r.title === t);
const server = (t: string) => [...a.conn.db.task.iter()].find((r) => r.title === t);
const tick = () => new Promise<void>((r) => queueMicrotask(r));

// Every state the UI would have been told about.
const seen: logic.Task[][] = [];
p.subscribe(() => seen.push([...view.rows()]));

let passed = 0;
async function test(name: string, fn: () => Promise<void>) {
  await fn();
  passed++;
  console.log(`  ok  ${name}`);
}
await b.conn.reducers.resetRate({});

await test('an insert shows at once with a temporary id, then becomes the real row without a duplicate', async () => {
  seen.length = 0;
  const t0 = performance.now();
  const done = A.add('alpha');
  await tick();
  const predicted = byTitle('alpha');
  const visibleAfter = performance.now() - t0;
  assert.ok(predicted && isTempId(predicted.id), 'visible immediately, with a temp id');
  assert.ok(view.isPredicted(predicted.id) && view.isPending(predicted.id));
  assert.equal(server('alpha'), undefined, 'not on the server yet');
  await done;
  const confirmedAfter = performance.now() - t0;
  await tick();
  const real = byTitle('alpha')!;
  assert.ok(!isTempId(real.id) && !view.isPending(real.id));
  assert.equal(rows().filter((r) => r.title === 'alpha').length, 1);
  const dupes = seen.filter((s) => s.filter((r) => r.title === 'alpha').length > 1).length;
  assert.equal(dupes, 0, 'no notified state ever had both the temp and the real row');
  console.log(`      predicted visible after ${visibleAfter.toFixed(1)} ms; server confirmed after ${confirmedAfter.toFixed(0)} ms`);
});

await test('updates show at once and stay put when the server agrees', async () => {
  const id = byTitle('alpha')!.id;
  const done = A.toggle(id);
  await tick();
  assert.equal(view.find(id)!.done, true);
  assert.equal(server('alpha')!.done, false, 'server copy unchanged so far');
  seen.length = 0;
  await done;
  await tick();
  assert.equal(view.find(id)!.done, true);
  assert.ok(seen.every((s) => s.find((r) => r.id === id)?.done === true), 'never flipped back on the way');
});

await test('several pending inserts keep their order and land in it', async () => {
  const calls = ['one', 'two', 'three'].map((t) => A.add(t));
  await tick();
  assert.deepEqual(rows().slice(-3).map((r) => r.title), ['one', 'two', 'three']);
  assert.equal(p.pendingCount, 3);
  await Promise.all(calls);
  await tick();
  assert.deepEqual(rows().slice(-3).map((r) => r.title), ['one', 'two', 'three']);
  assert.ok(rows().every((r) => !isTempId(r.id)));
});

await test("the server says no (a rule the client can't see): the prediction rolls back", async () => {
  await b.conn.reducers.resetRate({});
  const calls = ['r1', 'r2', 'r3', 'r4', 'r5', 'r6'].map((t) => A.add(t).then(() => null, (e) => String(e?.message ?? e)));
  await tick();
  assert.ok(byTitle('r6'), 'predicted like the others');
  const results = await Promise.all(calls);
  assert.deepEqual(results.slice(0, 5), [null, null, null, null, null]);
  assert.match(results[5]!, /slow down/);
  await tick();
  assert.equal(byTitle('r6'), undefined, 'rolled back');
  assert.ok(byTitle('r5'));
  assert.equal(p.stats.rolledBack, 1);
  await b.conn.reducers.resetRate({});
});

await test("someone else's change lands under a pending prediction: it's rebased, both show", async () => {
  const id = byTitle('one')!.id;
  const rebases = p.stats.rebases;
  const done = A.rename(id, 'one (renamed by a)');
  await tick();
  await b.conn.reducers.toggleTask({ id }); // reaches the server before a's rename does
  await sleep(DELAY + 50); // and reaches a, while the rename is still pending
  const mid = view.find(id)!;
  assert.equal(mid.title, 'one (renamed by a)', 'still predicted');
  assert.equal(mid.done, true, "b's toggle shows through");
  assert.ok(p.stats.rebases > rebases);
  await done;
  await tick();
  assert.deepEqual([view.find(id)!.title, view.find(id)!.done], ['one (renamed by a)', true]);
});

await test('a prediction that no longer applies is dropped, and the server refuses it too', async () => {
  const id = byTitle('two')!.id;
  const skipped = p.stats.skipped;
  const done = A.rename(id, 'two (too late)').then(() => null, (e) => String(e?.message ?? e));
  await tick();
  assert.equal(view.find(id)!.title, 'two (too late)');
  await b.conn.reducers.deleteTask({ id });
  await sleep(DELAY + 50);
  assert.equal(view.find(id), undefined, 'gone: the rename has nothing to apply to');
  assert.ok(p.stats.skipped > skipped);
  assert.match((await done)!, /no such task/);
});

await test('no answer in time: the prediction rolls back with PredictionTimeout', async () => {
  const before = rows().length;
  const call = p.call(() => new Promise(() => {}), (d) => void d.task.insert({ id: 0n, owner: a.identity, title: 'lost', done: false, pos: 999 }), { timeoutMs: 200 });
  await tick();
  assert.equal(rows().length, before + 1);
  await assert.rejects(call, PredictionTimeout);
  await tick();
  assert.equal(rows().length, before);
});

await test('click to visible: predicted vs waiting for the server', async () => {
  const samples = { predicted: [] as number[], server: [] as number[] };
  for (let i = 0; i < 5; i++) {
    const id = byTitle('three')!.id;
    let t0 = performance.now();
    const call = A.toggle(id);
    await tick();
    samples.predicted.push(performance.now() - t0);
    await call;
    const want = !view.find(id)!.done;
    t0 = performance.now();
    await a.conn.reducers.toggleTask({ id });
    while (a.conn.db.task.id.find(id)!.done !== want) await sleep(1);
    samples.server.push(performance.now() - t0);
  }
  const med = (xs: number[]) => xs.sort((x, y) => x - y)[2];
  console.log(`      median: predicted ${med(samples.predicted).toFixed(2)} ms, server round trip ${med(samples.server).toFixed(0)} ms (+${DELAY} ms each way)`);
  assert.ok(med(samples.predicted) < 5 && med(samples.server) >= 2 * DELAY);
});

console.log(`\n${passed} passed · stats ${JSON.stringify(p.stats)}`);
a.conn.disconnect();
b.conn.disconnect();
proxy.close();
process.exit(0);
