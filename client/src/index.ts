/**
 * # spacetimedb-predict: optimistic updates / client-side prediction for SpacetimeDB clients
 *
 * The rows a UI shows are the SDK's client cache (what the server has confirmed) plus the
 * predicted effects of this client's reducer calls that haven't come back yet. Predictions are
 * functions, not recorded diffs: whenever the server's rows change, every pending prediction is
 * re-run, in call order, on top of the new server state ("rebase", as Replicache and Linear do).
 * A prediction is dropped when its reducer call returns. On success the server's own update is
 * already in the cache by then (the SDK applies the caller's transaction before resolving the
 * call), so the swap is seamless. On failure the prediction simply disappears: that's the rollback.
 *
 * Nothing here touches the SDK's cache. It reads it, listens to it, and layers predictions on top.
 */

/** Anything with `iter()` and row callbacks: a table handle from the generated bindings. */
export interface CachedTable<Row> {
  iter(): Iterable<Row>;
  onInsert(cb: (ctx: unknown, row: Row) => void): void;
  onDelete(cb: (ctx: unknown, row: Row) => void): void;
  onUpdate?(cb: (ctx: unknown, oldRow: Row, newRow: Row) => void): void;
  removeOnInsert?(cb: (ctx: unknown, row: Row) => void): void;
  removeOnDelete?(cb: (ctx: unknown, row: Row) => void): void;
  removeOnUpdate?(cb: (ctx: unknown, oldRow: Row, newRow: Row) => void): void;
}

/** How to treat one table: its cache handle, its primary key, and an optional auto-inc column. */
export interface TableSpec<Row> {
  table: CachedTable<Row>;
  /** The row's primary key. */
  key: (row: Row) => unknown;
  /**
   * The auto-increment primary key column, if any. A predicted insert with `0` there gets a
   * temporary id (counting down from the top of the u64 range, so it sorts after real rows).
   */
  autoInc?: keyof Row & string;
}

/** A predicted table, as prediction functions see it: like the cache, but writable. */
export interface DraftTable<Row> {
  rows(): Row[];
  /** The primary key of a row. */
  keyOf(row: Row): unknown;
  find(key: unknown): Row | undefined;
  /** Insert a row. With an auto-inc column at 0, a temporary id is assigned; the row is returned. */
  insert(row: Row): Row;
  /** Replace the row with the same key. Returns false (and does nothing) if it isn't there. */
  update(row: Row): boolean;
  /** Insert or replace by key. */
  upsert(row: Row): Row;
  /** Delete by key. Returns whether a row was there. */
  delete(key: unknown): boolean;
}

export type Draft<T extends Record<string, unknown>> = { [K in keyof T]: DraftTable<T[K]> };

/** What a UI reads: the predicted rows of one table. */
export interface PredictedTable<Row> {
  rows(): readonly Row[];
  find(key: unknown): Row | undefined;
  /** Whether a pending prediction inserted, changed or deleted this row's key. */
  isPending(key: unknown): boolean;
  /** Whether this row exists only as a prediction (a predicted insert). */
  isPredicted(key: unknown): boolean;
}

export interface CallOptions {
  /** Give up on the server after this long and roll the prediction back (ms). Default 10 000. */
  timeoutMs?: number;
}

export interface Stats {
  calls: number;
  confirmed: number;
  rolledBack: number;
  timedOut: number;
  /** Re-runs of pending predictions because the server's rows changed underneath them. */
  rebases: number;
  /** Predictions that threw while being re-run on newer server data (and were skipped). */
  skipped: number;
}

type Prediction<T extends Record<string, unknown>> = {
  id: number;
  run: (draft: Draft<T>) => void;
  /** Temporary ids handed out by this prediction, by table, reused on every re-run. */
  temps: Map<string, bigint[]>;
};

const TEMP_TOP = (1n << 64n) - 1n;

/** Stable string form of a key (bigints, strings, numbers, Identity-like objects with toHexString). */
export function keyString(k: unknown): string {
  if (typeof k === 'bigint') return `n${k}`;
  if (typeof k === 'string') return `s${k}`;
  if (typeof k === 'number' || typeof k === 'boolean') return `${typeof k}${k}`;
  if (k && typeof k === 'object') {
    const o = k as { toHexString?: () => string; __identity__?: bigint; __connection_id__?: bigint };
    if (typeof o.toHexString === 'function') return `h${o.toHexString()}`;
    return `j${JSON.stringify(k, (_k, v) => (typeof v === 'bigint' ? `${v}n` : v))}`;
  }
  return `u${String(k)}`;
}

/**
 * Layer predictions over a connection's client cache.
 *
 * ```ts
 * const p = predict({ task: { table: conn.db.task, key: (t) => t.id, autoInc: 'id' } });
 * await p.call(() => conn.reducers.toggleTask({ id }), (d) => {
 *   const t = d.task.find(id);
 *   if (t) d.task.update({ ...t, done: !t.done });
 * });
 * p.subscribe(() => render(p.table('task').rows()));
 * ```
 */
export function predict<T extends Record<string, unknown>>(specs: { [K in keyof T]: TableSpec<T[K]> }) {
  const names = Object.keys(specs) as (keyof T & string)[];
  const pending: Prediction<T>[] = [];
  const listeners = new Set<() => void>();
  let nextId = 1;
  let nextTemp = TEMP_TOP;
  const stats: Stats = { calls: 0, confirmed: 0, rolledBack: 0, timedOut: 0, rebases: 0, skipped: 0 };

  // The server's rows, kept in step with the cache's callbacks once first read. Predictions are a
  // layer on top (`over`: key -> row, or GONE for a delete), rebuilt by re-running them. Nothing
  // copies a whole table except materializing `rows()` for a reader, once per change.
  const GONE = Symbol('gone');
  type Layer = Map<string, Map<string, unknown>>;
  let base: Layer | null = null;
  let over: Layer = new Map();
  let dirty = true;
  let touched = new Map<string, Set<string>>();
  let inserted = new Map<string, Set<string>>();
  const materialized = new Map<string, unknown[]>();

  function ensureBase(): Layer {
    if (!base) {
      base = new Map();
      for (const name of names) {
        const spec = specs[name];
        const m = new Map<string, unknown>();
        for (const row of spec.table.iter()) m.set(keyString(spec.key(row)), row);
        base.set(name, m);
      }
    }
    return base;
  }

  /** Look a key up through layers (newest first), then the server's rows. */
  function lookup(layers: Map<string, unknown>[], k: string): unknown {
    for (const l of layers) {
      if (l.has(k)) {
        const v = l.get(k);
        return v === GONE ? undefined : v;
      }
    }
    return undefined;
  }

  function merge(name: string, layers: Map<string, unknown>[]): unknown[] {
    const b = ensureBase().get(name)!;
    if (layers.every((l) => l.size === 0)) return [...b.values()];
    const out: unknown[] = [];
    const seen = new Set<string>();
    for (const [k, row] of b) {
      seen.add(k);
      const hit = layers.find((l) => l.has(k));
      if (!hit) out.push(row);
      else if (hit.get(k) !== GONE) out.push(hit.get(k));
    }
    // Rows only the layers have (predicted inserts), oldest layer first.
    for (const l of [...layers].reverse()) {
      for (const [k, v] of l) {
        if (seen.has(k)) continue;
        seen.add(k);
        const top = lookup(layers, k);
        if (top !== undefined) out.push(top);
      }
    }
    return out;
  }

  function rebuild() {
    if (!dirty) return;
    dirty = false;
    const b = ensureBase();
    over = new Map(names.map((n) => [n, new Map()]));
    touched = new Map(names.map((n) => [n, new Set()]));
    inserted = new Map(names.map((n) => [n, new Set()]));
    materialized.clear();
    for (const p of pending) {
      // Each prediction writes to its own layer, merged in only if it finishes: one that throws
      // halfway changes nothing.
      const mine: Layer = new Map(names.map((n) => [n, new Map()]));
      const marks: [string, string, boolean][] = [];
      const tempCursor = new Map<string, number>();
      const draft = {} as Draft<T>;
      for (const name of names) {
        const spec = specs[name] as TableSpec<any>;
        const layers = [mine.get(name)!, over.get(name)!];
        const has = (k: string) => {
          for (const l of layers) if (l.has(k)) return l.get(k) !== GONE;
          return b.get(name)!.has(k);
        };
        const get = (k: string) => (layers.some((l) => l.has(k)) ? lookup(layers, k) : b.get(name)!.get(k));
        const tbl: DraftTable<any> = {
          rows: () => merge(name, layers),
          keyOf: (row) => spec.key(row),
          find: (k) => get(keyString(k)) as any,
          insert(row) {
            if (spec.autoInc && (row[spec.autoInc] === 0n || row[spec.autoInc] === 0)) {
              // Reuse this prediction's temp ids on re-runs, so keys stay stable for the UI.
              const list = p.temps.get(name) ?? (p.temps.set(name, []), p.temps.get(name)!);
              const i = tempCursor.get(name) ?? 0;
              tempCursor.set(name, i + 1);
              if (list.length <= i) list.push(nextTemp--);
              row = { ...row, [spec.autoInc]: list[i] };
            }
            const k = keyString(spec.key(row));
            mine.get(name)!.set(k, row);
            marks.push([name, k, true]);
            return row;
          },
          update(row) {
            const k = keyString(spec.key(row));
            if (!has(k)) return false;
            mine.get(name)!.set(k, row);
            marks.push([name, k, false]);
            return true;
          },
          upsert(row) {
            return tbl.update(row) ? row : tbl.insert(row);
          },
          delete(key) {
            const k = keyString(key);
            if (!has(k)) return false;
            mine.get(name)!.set(k, GONE);
            marks.push([name, k, false]);
            return true;
          },
        };
        (draft as Record<string, DraftTable<any>>)[name] = tbl;
      }
      try {
        p.run(draft);
      } catch {
        stats.skipped++;
        continue;
      }
      for (const name of names) for (const [k, v] of mine.get(name)!) over.get(name)!.set(k, v);
      for (const [name, k, isInsert] of marks) {
        touched.get(name)!.add(k);
        if (isInsert) inserted.get(name)!.add(k);
      }
    }
  }

  // Local changes (a new prediction, a dropped one) notify on the next microtask, so a prediction
  // shows before anything can paint. Server changes notify on the next task: when the server
  // confirms a call, the SDK applies the update and only then settles the call, a few microtasks
  // later, and notifying in between would show the real row next to its prediction.
  let microQueued = false;
  let taskQueued = false;
  const notify = () => {
    for (const l of [...listeners]) l();
  };
  function changed(fromServer = false) {
    dirty = true;
    if (fromServer) {
      if (taskQueued) return;
      taskQueued = true;
      setTimeout(() => {
        taskQueued = false;
        notify();
      }, 0);
    } else {
      if (microQueued) return;
      microQueued = true;
      queueMicrotask(() => {
        microQueued = false;
        notify();
      });
    }
  }

  const serverChanged = () => {
    if (pending.length) stats.rebases++;
    changed(true);
  };
  for (const name of names) {
    const spec = specs[name] as TableSpec<any>;
    const rows = () => base?.get(name);
    spec.table.onInsert((_ctx, row) => {
      rows()?.set(keyString(spec.key(row)), row);
      serverChanged();
    });
    spec.table.onDelete((_ctx, row) => {
      rows()?.delete(keyString(spec.key(row)));
      serverChanged();
    });
    spec.table.onUpdate?.((_ctx, oldRow, newRow) => {
      rows()?.delete(keyString(spec.key(oldRow)));
      rows()?.set(keyString(spec.key(newRow)), newRow);
      serverChanged();
    });
  }

  const tables = new Map<string, PredictedTable<unknown>>();

  return {
    /**
     * Call a reducer with a prediction of its effect. The prediction shows immediately, is re-run
     * on top of any server change until the call returns, and disappears when it does (the real
     * rows are in the cache by then). Rejections roll back and rethrow.
     */
    async call(send: () => Promise<unknown>, prediction: (draft: Draft<T>) => void, opts: CallOptions = {}): Promise<void> {
      const p: Prediction<T> = { id: nextId++, run: prediction, temps: new Map() };
      stats.calls++;
      pending.push(p);
      changed();
      const drop = () => {
        const i = pending.indexOf(p);
        if (i >= 0) pending.splice(i, 1);
        changed();
      };
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new PredictionTimeout()), opts.timeoutMs ?? 10_000);
      });
      try {
        await Promise.race([send(), timeout]);
        stats.confirmed++;
      } catch (e) {
        if (e instanceof PredictionTimeout) stats.timedOut++;
        else stats.rolledBack++;
        throw e;
      } finally {
        clearTimeout(timer);
        drop();
      }
    },

    /** The predicted rows of one table. */
    table<K extends keyof T & string>(name: K): PredictedTable<T[K]> {
      let t = tables.get(name);
      if (!t) {
        t = {
          rows() {
            rebuild();
            let r = materialized.get(name);
            if (!r) materialized.set(name, (r = merge(name, [over.get(name)!])));
            return r;
          },
          find(key) {
            rebuild();
            const k = keyString(key);
            const o = over.get(name)!;
            return (o.has(k) ? lookup([o], k) : ensureBase().get(name)!.get(k)) as any;
          },
          isPending(key) {
            rebuild();
            return touched.get(name)!.has(keyString(key));
          },
          isPredicted(key) {
            rebuild();
            return inserted.get(name)!.has(keyString(key));
          },
        };
        tables.set(name, t);
      }
      return t as PredictedTable<T[K]>;
    },

    /** Called (once per microtask, coalesced) whenever the predicted rows may have changed. */
    subscribe(listener: () => void): () => void {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },

    /** Re-read the server's rows from the cache (after a reconnect or a resubscribe, say). */
    resync() {
      base = null;
      changed(true);
    },

    /** How many calls are still waiting for the server. */
    get pendingCount() {
      return pending.length;
    },

    stats,
  };
}

export type Predictor<T extends Record<string, unknown>> = ReturnType<typeof predict<T>>;

export class PredictionTimeout extends Error {
  constructor() {
    super('no reply from the server in time; the prediction was rolled back');
  }
}

/** Whether an auto-inc key is a temporary one handed out by a prediction. */
export function isTempId(id: bigint): boolean {
  return id > TEMP_TOP - (1n << 32n);
}
