/**
 * Run a module's own reducer logic as the prediction ("write it once").
 *
 * A TypeScript module's reducer body only needs a small part of `ctx.db`: `insert`, `iter`,
 * `count`, unique-column accessors (`find` / `update` / `delete`) and index accessors (`filter`).
 * `shimDb` builds that surface over a prediction's draft, so the same function can run on the
 * server (with the real ctx) and on the client (with this one). Put the logic in a file both
 * import, and keep it free of server-only calls.
 *
 * What the client can't know, it can't predict: private tables, other people's unsent writes,
 * `ctx.random`. Write the shared logic so those checks happen in the module's wrapper, or accept
 * that the prediction is optimistic and the server may still refuse (which rolls it back).
 */
import type { Draft, DraftTable } from './index.js';

/** Accessors for one table: unique columns (by field name) and btree indexes (by accessor name). */
export interface ShimTableSpec<Row> {
  /** Unique (and primary key) columns, e.g. `['id']`. The accessor is the field name. */
  unique?: (keyof Row & string)[];
  /** Btree indexes: accessor name → the columns it covers, in order. */
  indexes?: Record<string, (keyof Row & string)[]>;
}

const same = (a: unknown, b: unknown): boolean => {
  if (a === b) return true;
  const ah = (a as { toHexString?: () => string })?.toHexString;
  const bh = (b as { toHexString?: () => string })?.toHexString;
  return typeof ah === 'function' && typeof bh === 'function' && ah.call(a) === bh.call(b);
};

function shimTable<Row extends Record<string, unknown>>(t: DraftTable<Row>, spec: ShimTableSpec<Row>) {
  const out: Record<string, unknown> = {
    insert: (row: Row) => t.insert(row),
    iter: () => t.rows()[Symbol.iterator](),
    count: () => BigInt(t.rows().length),
    [Symbol.iterator]: () => t.rows()[Symbol.iterator](),
  };
  for (const col of spec.unique ?? []) {
    const find = (v: unknown) => t.rows().find((r) => same(r[col], v)) ?? null;
    out[col] = {
      find,
      update(row: Row) {
        const old = find(row[col]);
        if (!old) throw new Error(`no row with ${col} = ${String(row[col])}`);
        if (same(t.keyOf(old), t.keyOf(row))) {
          t.update(row);
        } else {
          // Updating through a non-primary unique column may change the primary key.
          t.delete(t.keyOf(old));
          t.insert(row);
        }
        return row;
      },
      delete(v: unknown) {
        const old = find(v);
        return old ? t.delete(t.keyOf(old)) : false;
      },
    };
  }
  for (const [name, cols] of Object.entries(spec.indexes ?? {})) {
    out[name] = {
      filter(v: unknown) {
        const prefix = cols.length > 1 && Array.isArray(v) ? v : [v];
        return t.rows().filter((r) => prefix.every((p, i) => same(r[cols[i]], p)))[Symbol.iterator]();
      },
    };
  }
  return out;
}

/**
 * A `ctx.db` look-alike over a prediction's draft. `specs` lists, per table, the unique columns
 * and indexes your shared logic uses.
 */
export function shimDb<T extends Record<string, unknown>>(
  draft: Draft<T>,
  specs: { [K in keyof T]?: ShimTableSpec<T[K]> }
): Record<keyof T, any> {
  const db = {} as Record<keyof T, any>;
  for (const name of Object.keys(draft) as (keyof T & string)[]) {
    db[name] = shimTable(draft[name] as DraftTable<any>, (specs[name] ?? {}) as ShimTableSpec<any>);
  }
  return db;
}
