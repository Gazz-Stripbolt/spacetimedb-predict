// Demo: a shared task list, with prediction on or off. With it on, every action shows at once
// (the module's own logic runs on the client as the prediction) and the server's answer replaces
// it; with it off, the list waits for the round trip. Open with ?host=ws://127.0.0.1:3100 to go
// through the latency proxy (tests/proxy.mts).
import { StrictMode, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import type { Identity } from 'spacetimedb';
import { predict, isTempId, type Draft, type Predictor } from '@pogly/spacetimedb-predict';
import { usePredicted } from '@pogly/spacetimedb-predict/react';
import { shimDb } from '@pogly/spacetimedb-predict/shim';
import * as logic from '../../module/src/logic';
import { DbConnection } from './bindings/index';

const params = new URLSearchParams(location.search);
const routed = location.pathname.match(/\/v1\/database\/([^/]+)\/route/);
const HOST = params.get('host') ?? (routed ? location.origin.replace(/^http/, 'ws') : 'ws://127.0.0.1:3000');
const DB = params.get('db') ?? (routed ? decodeURIComponent(routed[1]) : 'predict');

type Tables = { task: logic.Task };

function connect(): Promise<{ conn: DbConnection; identity: Identity }> {
  return new Promise((resolve, reject) =>
    DbConnection.builder()
      .withUri(HOST)
      .withDatabaseName(DB)
      .onConnect((conn, identity) => resolve({ conn, identity }))
      .onConnectError((_c, e) => reject(e))
      .build()
  );
}

/** Run a shared logic function against the prediction draft, as `identity`. */
function shared<A>(identity: Identity, fn: (ctx: logic.Ctx, args: A) => void, args: A) {
  return (d: Draft<Tables>) => fn({ sender: identity, db: shimDb(d, { task: { unique: ['id'] } }) } as logic.Ctx, args);
}

function App({ conn, identity, p }: { conn: DbConnection; identity: Identity; p: Predictor<Tables> }) {
  const [predictOn, setPredictOn] = useState(params.get('predict') !== 'off');
  const [toast, setToast] = useState('');
  const [lastMs, setLastMs] = useState<number | null>(null);
  const [, setTick] = useState(0);
  const tasks = [...usePredicted(p, 'task')].sort((a, b) => a.pos - b.pos);
  const table = p.table('task');
  const clickAt = useRef<{ t: number; sig: string } | null>(null);
  const sig = tasks.map((t) => `${t.id}:${t.done}:${t.title}:${t.pos}`).join('|');

  // Click-to-visible: the first render whose rows differ from the ones at the click.
  useLayoutEffect(() => {
    const c = clickAt.current;
    if (c && c.sig !== sig) {
      setLastMs(performance.now() - c.t);
      clickAt.current = null;
    }
  }, [sig]);
  useEffect(() => p.subscribe(() => setTick((n) => n + 1)), [p]);

  async function act<A>(send: () => Promise<unknown>, fn: (ctx: logic.Ctx, args: A) => void, args: A) {
    clickAt.current = { t: performance.now(), sig };
    try {
      if (predictOn) await p.call(send, shared(identity, fn, args));
      else await send();
    } catch (e) {
      setToast(String((e as Error)?.message ?? e));
      setTimeout(() => setToast(''), 3500);
    }
  }
  const r = conn.reducers;
  const [title, setTitle] = useState('');

  return (
    <>
      <header>
        <h1>⚡ Spacetime Predict</h1>
        <label className="switch">
          <input type="checkbox" id="predict" checked={predictOn} onChange={(e) => setPredictOn(e.target.checked)} /> prediction
        </label>
        <span id="last" className="muted">{lastMs === null ? 'click something' : `last action visible after ${lastMs.toFixed(0)} ms`}</span>
        <span id="pending" className="muted">{p.pendingCount} pending</span>
      </header>
      <main>
        <form
          id="add"
          onSubmit={(e) => {
            e.preventDefault();
            const t = title;
            setTitle('');
            void act(() => r.addTask({ title: t }), logic.addTask, { title: t });
          }}
        >
          <input value={title} onChange={(e) => setTitle(e.target.value)} placeholder="New task" maxLength={logic.MAX_TITLE} />
          <button className="btn primary">Add</button>
        </form>
        <ul id="tasks">
          {tasks.map((t, i) => (
            <li
              key={String(t.id)}
              data-id={String(t.id)}
              className={[table.isPending(t.id) ? 'pending' : '', t.done ? 'done' : '', isTempId(t.id) ? 'temp' : ''].join(' ')}
            >
              <input type="checkbox" checked={t.done} onChange={() => void act(() => r.toggleTask({ id: t.id }), logic.toggleTask, { id: t.id })} />
              <span
                className="title"
                onDoubleClick={() => {
                  const title = prompt('Rename', t.title);
                  if (title !== null) void act(() => r.renameTask({ id: t.id, title }), logic.renameTask, { id: t.id, title });
                }}
              >
                {t.title}
              </span>
              <span className="who">{t.owner.isEqual(identity) ? 'you' : t.owner.toHexString().slice(0, 6)}</span>
              <button className="x" title="Move up" disabled={i === 0} onClick={() => void act(() => r.moveTask({ id: t.id, before: tasks[i - 1].id }), logic.moveTask, { id: t.id, before: tasks[i - 1].id })}>↑</button>
              <button className="x" title="Delete" onClick={() => void act(() => r.deleteTask({ id: t.id }), logic.deleteTask, { id: t.id })}>×</button>
            </li>
          ))}
        </ul>
        {tasks.length === 0 && <p className="muted">No tasks yet.</p>}
      </main>
      <footer className="muted">
        <span id="stats">
          {p.stats.calls} calls · {p.stats.confirmed} confirmed · {p.stats.rolledBack} rolled back · {p.stats.rebases} rebases
        </span>{' '}
        · Faded rows are predictions waiting for the server. The module allows 5 new tasks per 10 s: add a sixth quickly to see a rollback.
      </footer>
      <div id="toast" className={toast ? 'show' : ''}>{toast}</div>
    </>
  );
}

const status = document.getElementById('status')!;
connect()
  .then(({ conn, identity }) => {
    const p = predict<Tables>({ task: { table: conn.db.task, key: (t) => t.id, autoInc: 'id' } });
    conn
      .subscriptionBuilder()
      .onApplied(() => {
        status.remove();
        createRoot(document.getElementById('root')!).render(
          <StrictMode>
            <App conn={conn} identity={identity} p={p} />
          </StrictMode>
        );
      })
      .subscribe(['SELECT * FROM task']);
  })
  .catch((e) => (status.textContent = `can't connect: ${e}`));
