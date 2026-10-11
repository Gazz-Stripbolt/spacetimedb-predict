// Cost of the overlay: rebuilding the predicted view after a server change, by table size and
// number of pending predictions. No server needed: a fake cached table stands in for the SDK's.
//
//   npx tsx tests/bench.mts
import { predict, type CachedTable } from '../client/src/index.ts';

type Row = { id: bigint; title: string; done: boolean; pos: number };

function fakeTable(n: number) {
  const rows = new Map<bigint, Row>();
  for (let i = 1n; i <= BigInt(n); i++) rows.set(i, { id: i, title: `task ${i}`, done: false, pos: Number(i) });
  const cbs: ((ctx: unknown, oldRow: Row, newRow: Row) => void)[] = [];
  const table: CachedTable<Row> = {
    iter: () => rows.values(),
    onInsert: () => {},
    onDelete: () => {},
    onUpdate: (cb) => void cbs.push(cb),
  };
  // A server update to the last row, as the SDK would report it.
  const change = () => {
    const old = rows.get(BigInt(n))!;
    const row = { ...old, pos: old.pos + 0.5 };
    rows.set(row.id, row);
    for (const cb of cbs) cb(undefined, old, row);
  };
  return { table, change };
}

const sizes = [100, 1_000, 10_000, 50_000];
const pendings = [0, 1, 10];
console.log('rebuild after a server change (median of 21), ms:');
console.log(['rows'.padStart(8), ...pendings.map((p) => `${p} pending`.padStart(12))].join(''));
for (const n of sizes) {
  const cells: string[] = [];
  for (const k of pendings) {
    const f = fakeTable(n);
    const p = predict<{ task: Row }>({ task: { table: f.table, key: (r) => r.id, autoInc: 'id' } });
    // Pending calls that never return (within the bench), each a small update.
    for (let i = 0; i < k; i++) {
      void p.call(() => new Promise(() => {}), (d) => {
        const t = d.task.find(BigInt(i + 1));
        if (t) d.task.update({ ...t, done: !t.done });
      }, { timeoutMs: 1e9 }).catch(() => {});
    }
    const times: number[] = [];
    for (let r = 0; r < 21; r++) {
      f.change();
      const t0 = performance.now();
      p.table('task').rows();
      times.push(performance.now() - t0);
    }
    times.sort((a, b) => a - b);
    cells.push(times[10].toFixed(2).padStart(12));
  }
  console.log([String(n).padStart(8), ...cells].join(''));
}
process.exit(0);
