// The demo's reducer logic, shared by the module (with the real ctx) and the web client (with
// spacetimedb-predict's shim, as the prediction). Keep it free of server-only imports.
import type { Identity } from 'spacetimedb';

export interface Task {
  id: bigint;
  owner: Identity;
  title: string;
  done: boolean;
  /** Sort position: new tasks go last; moves pick a value between neighbours. */
  pos: number;
}

/** The part of `ctx` the logic uses. The module's ctx and the client shim both fit it. */
export interface Ctx {
  sender: Identity;
  db: {
    task: {
      insert(row: Task): Task;
      iter(): Iterable<Task>;
      id: {
        find(id: bigint): Task | null | undefined;
        update(row: Task): Task;
        delete(id: bigint): boolean;
      };
    };
  };
}

/** A refusal the client can show. The module turns it into a SenderError. */
export class Refused extends Error {}

export const MAX_TITLE = 80;

function title(t: string): string {
  const s = t.trim();
  if (!s) throw new Refused('title is empty');
  if (s.length > MAX_TITLE) throw new Refused('title too long');
  return s;
}

function task(ctx: Ctx, id: bigint): Task {
  const t = ctx.db.task.id.find(id);
  if (!t) throw new Refused('no such task');
  return t;
}

export function addTask(ctx: Ctx, args: { title: string }): void {
  let last = 0;
  for (const t of ctx.db.task.iter()) last = Math.max(last, t.pos);
  ctx.db.task.insert({ id: 0n, owner: ctx.sender, title: title(args.title), done: false, pos: last + 1 });
}

export function toggleTask(ctx: Ctx, args: { id: bigint }): void {
  const t = task(ctx, args.id);
  ctx.db.task.id.update({ ...t, done: !t.done });
}

export function renameTask(ctx: Ctx, args: { id: bigint; title: string }): void {
  const t = task(ctx, args.id);
  ctx.db.task.id.update({ ...t, title: title(args.title) });
}

export function deleteTask(ctx: Ctx, args: { id: bigint }): void {
  task(ctx, args.id);
  ctx.db.task.id.delete(args.id);
}

/** Move a task to just before `before` (or to the end, with 0). */
export function moveTask(ctx: Ctx, args: { id: bigint; before: bigint }): void {
  const t = task(ctx, args.id);
  const sorted = [...ctx.db.task.iter()].filter((x) => x.id !== t.id).sort((a, b) => a.pos - b.pos);
  let pos: number;
  if (args.before === 0n) {
    pos = (sorted[sorted.length - 1]?.pos ?? 0) + 1;
  } else {
    const i = sorted.findIndex((x) => x.id === args.before);
    if (i < 0) throw new Refused('no such task');
    pos = i === 0 ? sorted[0].pos - 1 : (sorted[i - 1].pos + sorted[i].pos) / 2;
  }
  ctx.db.task.id.update({ ...t, pos });
}
