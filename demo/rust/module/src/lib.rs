//! The demo module in Rust: the same task list as `demo/module` (TypeScript), with the reducer
//! logic in the shared `task_logic` crate, which Rust clients also run as their prediction.

use spacetimedb::{Identity, ReducerContext, Table, Timestamp};
use task_logic::Tasks;

#[spacetimedb::table(accessor = task, public)]
#[derive(Clone)]
pub struct Task {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub title: String,
    pub done: bool,
    pub pos: f64,
}

/// Server-only state the client can't see: how many tasks each person added recently. At most 5
/// adds per 10 seconds, a rule a prediction can't know about.
#[spacetimedb::table(accessor = rate)]
pub struct Rate {
    #[primary_key]
    pub owner: Identity,
    pub window_start: Timestamp,
    pub count: u32,
}

const RATE_WINDOW_MICROS: i64 = 10_000_000;
const RATE_MAX: u32 = 5;

/// The module's side of [`Tasks`]: the real tables.
struct Db<'a>(&'a ReducerContext);

fn to_logic(t: Task) -> task_logic::Task {
    task_logic::Task {
        id: t.id,
        owner: t.owner,
        title: t.title,
        done: t.done,
        pos: t.pos,
    }
}

fn from_logic(t: task_logic::Task) -> Task {
    Task {
        id: t.id,
        owner: t.owner,
        title: t.title,
        done: t.done,
        pos: t.pos,
    }
}

impl Tasks for Db<'_> {
    fn sender(&self) -> Identity {
        self.0.sender()
    }
    fn all(&self) -> Vec<task_logic::Task> {
        self.0.db.task().iter().map(to_logic).collect()
    }
    fn find(&self, id: u64) -> Option<task_logic::Task> {
        self.0.db.task().id().find(id).map(to_logic)
    }
    fn insert(&mut self, task: task_logic::Task) {
        self.0.db.task().insert(from_logic(task));
    }
    fn update(&mut self, task: task_logic::Task) {
        self.0.db.task().id().update(from_logic(task));
    }
    fn delete(&mut self, id: u64) {
        self.0.db.task().id().delete(id);
    }
}

#[spacetimedb::reducer]
pub fn add_task(ctx: &ReducerContext, title: String) -> Result<(), String> {
    let now = ctx.timestamp.to_micros_since_unix_epoch();
    match ctx.db.rate().owner().find(ctx.sender()) {
        Some(r) if now - r.window_start.to_micros_since_unix_epoch() <= RATE_WINDOW_MICROS => {
            if r.count >= RATE_MAX {
                return Err("slow down: at most 5 new tasks per 10 seconds".into());
            }
            ctx.db.rate().owner().update(Rate {
                count: r.count + 1,
                ..r
            });
        }
        Some(r) => {
            ctx.db.rate().owner().update(Rate {
                window_start: ctx.timestamp,
                count: 1,
                ..r
            });
        }
        None => {
            ctx.db.rate().insert(Rate {
                owner: ctx.sender(),
                window_start: ctx.timestamp,
                count: 1,
            });
        }
    }
    task_logic::add_task(&mut Db(ctx), &title)
}

#[spacetimedb::reducer]
pub fn toggle_task(ctx: &ReducerContext, id: u64) -> Result<(), String> {
    task_logic::toggle_task(&mut Db(ctx), id)
}

#[spacetimedb::reducer]
pub fn rename_task(ctx: &ReducerContext, id: u64, title: String) -> Result<(), String> {
    task_logic::rename_task(&mut Db(ctx), id, &title)
}

#[spacetimedb::reducer]
pub fn delete_task(ctx: &ReducerContext, id: u64) -> Result<(), String> {
    task_logic::delete_task(&mut Db(ctx), id)
}

#[spacetimedb::reducer]
pub fn move_task(ctx: &ReducerContext, id: u64, before: u64) -> Result<(), String> {
    task_logic::move_task(&mut Db(ctx), id, before)
}

/// Test helper: forget everyone's rate limits.
#[spacetimedb::reducer]
pub fn reset_rate(ctx: &ReducerContext) {
    for r in ctx.db.rate().iter().collect::<Vec<_>>() {
        ctx.db.rate().owner().delete(r.owner);
    }
}
