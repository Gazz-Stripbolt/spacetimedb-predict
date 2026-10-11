//! The demo's reducer logic, written once. The Rust module runs it against its tables; a Rust
//! client (a Bevy game, say) runs the same functions against a prediction draft. Both sides
//! implement [`Tasks`] over their own row type, converting to and from [`Task`].
//!
//! This mirrors `demo/module/src/logic.ts`: same rules, same refusal messages.

use spacetimedb_lib::Identity;

/// A task as the logic sees it. Both sides convert their own row type to and from this.
#[derive(Clone, Debug, PartialEq)]
pub struct Task {
    pub id: u64,
    pub owner: Identity,
    pub title: String,
    pub done: bool,
    /// Sort position: new tasks go last; moves pick a value between neighbours.
    pub pos: f64,
}

/// The part of the database the logic touches.
pub trait Tasks {
    fn sender(&self) -> Identity;
    fn all(&self) -> Vec<Task>;
    fn find(&self, id: u64) -> Option<Task>;
    /// Insert; an `id` of 0 means "assign one" (auto-inc on the server, a temp id in a prediction).
    fn insert(&mut self, task: Task);
    fn update(&mut self, task: Task);
    fn delete(&mut self, id: u64);
}

pub const MAX_TITLE: usize = 80;

fn title(t: &str) -> Result<String, String> {
    let s = t.trim();
    if s.is_empty() {
        return Err("title is empty".into());
    }
    if s.chars().count() > MAX_TITLE {
        return Err("title too long".into());
    }
    Ok(s.to_string())
}

fn task(db: &impl Tasks, id: u64) -> Result<Task, String> {
    db.find(id).ok_or_else(|| "no such task".to_string())
}

pub fn add_task(db: &mut impl Tasks, title_: &str) -> Result<(), String> {
    let last = db.all().iter().map(|t| t.pos).fold(0.0, f64::max);
    let owner = db.sender();
    db.insert(Task {
        id: 0,
        owner,
        title: title(title_)?,
        done: false,
        pos: last + 1.0,
    });
    Ok(())
}

pub fn toggle_task(db: &mut impl Tasks, id: u64) -> Result<(), String> {
    let t = task(db, id)?;
    db.update(Task { done: !t.done, ..t });
    Ok(())
}

pub fn rename_task(db: &mut impl Tasks, id: u64, title_: &str) -> Result<(), String> {
    let t = task(db, id)?;
    db.update(Task {
        title: title(title_)?,
        ..t
    });
    Ok(())
}

pub fn delete_task(db: &mut impl Tasks, id: u64) -> Result<(), String> {
    task(db, id)?;
    db.delete(id);
    Ok(())
}

/// Move a task to just before `before` (or to the end, with 0).
pub fn move_task(db: &mut impl Tasks, id: u64, before: u64) -> Result<(), String> {
    let t = task(db, id)?;
    let mut sorted: Vec<Task> = db.all().into_iter().filter(|x| x.id != t.id).collect();
    sorted.sort_by(|a, b| a.pos.total_cmp(&b.pos));
    let pos = if before == 0 {
        sorted.last().map_or(0.0, |x| x.pos) + 1.0
    } else {
        let i = sorted
            .iter()
            .position(|x| x.id == before)
            .ok_or("no such task")?;
        if i == 0 {
            sorted[0].pos - 1.0
        } else {
            (sorted[i - 1].pos + sorted[i].pos) / 2.0
        }
    };
    db.update(Task { pos, ..t });
    Ok(())
}
