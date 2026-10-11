//! # spacetimedb-predict: optimistic updates / client-side prediction for SpacetimeDB Rust clients
//!
//! The rows a game shows are the SDK's client cache (what the server has confirmed) plus the
//! predicted effects of this client's reducer calls that haven't come back yet. Predictions are
//! closures, not recorded diffs: whenever the server's rows change, every pending prediction is
//! re-run, in call order, on top of the new server state ("rebase", the classic replay of
//! unacknowledged inputs). A prediction is dropped when its call settles. On success the server's
//! own rows are already in the cache by then: the SDK applies the caller's transaction before it
//! runs the call's `_then` callback. On failure the prediction simply disappears: that's the
//! rollback.
//!
//! Nothing here writes to the SDK's cache. It listens to it and layers predictions on top.
//!
//! There's no async and no background work: call [`Predictor::flush`] once per frame, right
//! after `conn.frame_tick()`. It rebuilds the predicted view if anything changed and returns what
//! changed in it ([`Change`]: inserted, updated, deleted, or a predicted row replaced by its real
//! row), plus the calls that settled. Because it runs after `frame_tick` has finished, a confirmed
//! insert never shows next to its own prediction.
//!
//! ```ignore
//! impl PredictRow for Task {
//!     type Key = u64;
//!     fn key(&self) -> u64 { self.id }
//!     fn auto_inc(&mut self) -> Option<&mut u64> { Some(&mut self.id) }
//! }
//!
//! let p = Predictor::new();
//! p.table(&conn.db.task());
//!
//! p.call(
//!     |settle| conn.reducers.toggle_task_then(id, move |ctx, res| settle.done(ctx, res)),
//!     move |d| {
//!         let mut t = d.table::<Task>().find(&id).ok_or("no such task")?;
//!         t.done = !t.done;
//!         d.table::<Task>().update(t);
//!         Ok(())
//!     },
//! );
//!
//! // Every frame:
//! conn.frame_tick()?;
//! let f = p.flush();
//! for change in f.changes::<Task>() { /* spawn / update / despawn / re-key */ }
//! ```

use spacetimedb_sdk::__codegen::AbstractEventContext;
use spacetimedb_sdk::{Event, ReducerEvent, Table, TableWithPrimaryKey, Timestamp};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// A row type the predictor can track. Implement it for the row types your bindings generate.
pub trait PredictRow: Clone + PartialEq + Send + 'static {
    /// The primary key.
    type Key: Clone + Eq + Hash + Debug + Send + 'static;

    /// The row's primary key.
    fn key(&self) -> Self::Key;

    /// The auto-increment primary key column, if the table has one. A predicted insert with 0
    /// there gets a temporary id (counting down from `u64::MAX`, so it sorts after real rows).
    fn auto_inc(&mut self) -> Option<&mut u64> {
        None
    }
}

/// Temporary ids handed out to predicted inserts are in the top 2³² of the u64 range.
pub fn is_temp_id(id: u64) -> bool {
    id > u64::MAX - (1 << 32)
}

/// A change to the predicted view, as reported by [`Predictor::flush`].
#[derive(Clone, Debug, PartialEq)]
pub enum Change<R> {
    Inserted(R),
    Updated {
        old: R,
        new: R,
    },
    Deleted(R),
    /// A predicted insert was confirmed: `temp` (with its temporary id) is now `real`. Re-key the
    /// entity instead of despawning and respawning it.
    Replaced {
        temp: R,
        real: R,
    },
}

/// How a call ended.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Confirmed,
    /// The server refused (the reducer's error), or the request couldn't be sent.
    RolledBack(String),
    /// No answer within the call's timeout.
    TimedOut,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Settled {
    pub call: CallId,
    pub outcome: Outcome,
}

pub type CallId = u64;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Stats {
    pub calls: u64,
    pub confirmed: u64,
    pub rolled_back: u64,
    pub timed_out: u64,
    /// Server changes that arrived while predictions were pending (each re-runs them).
    pub rebases: u64,
    /// Predictions that failed while being re-run on newer server data (and were skipped).
    pub skipped: u64,
}

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The result of one [`Predictor::flush`]: what changed in the predicted view, per table, and
/// which calls settled since the last flush.
#[derive(Default)]
pub struct Flush {
    changes: HashMap<TypeId, Box<dyn Any + Send>>,
    pub settled: Vec<Settled>,
}

impl Flush {
    /// The changes to one table's predicted rows, in no particular order.
    pub fn changes<R: PredictRow>(&self) -> &[Change<R>] {
        self.changes
            .get(&TypeId::of::<R>())
            .and_then(|c| c.downcast_ref::<Vec<Change<R>>>())
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Nothing changed and nothing settled.
    pub fn is_empty(&self) -> bool {
        self.settled.is_empty() && self.changes.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Per-table state
// ---------------------------------------------------------------------------

struct TableState<R: PredictRow> {
    /// The server's rows, kept in step with the cache's callbacks.
    base: HashMap<R::Key, R>,
    /// All pending predictions' effects: a row, or None for a delete.
    over: HashMap<R::Key, Option<R>>,
    touched: HashSet<R::Key>,
    inserted: HashSet<R::Key>,
    /// Touched at the last flush (their rows may need reverting).
    prev_touched: HashSet<R::Key>,
    /// Keys the server changed since the last flush.
    server_changed: HashSet<R::Key>,
    /// The predicted view as last reported by flush.
    shown: HashMap<R::Key, R>,
    first_flush: bool,
    /// Rows this client's own reducer calls inserted, by the reducer's timestamp (for Replaced).
    own_inserts: Vec<(Timestamp, R::Key)>,
    /// (temporary id, real key) pairs from confirmed calls since the last flush.
    replacements: Vec<(u64, R::Key)>,
}

impl<R: PredictRow> TableState<R> {
    fn new(rows: impl Iterator<Item = R>) -> Self {
        Self {
            base: rows.map(|r| (r.key(), r)).collect(),
            over: HashMap::new(),
            touched: HashSet::new(),
            inserted: HashSet::new(),
            prev_touched: HashSet::new(),
            server_changed: HashSet::new(),
            shown: HashMap::new(),
            first_flush: true,
            own_inserts: Vec::new(),
            replacements: Vec::new(),
        }
    }

    fn get(&self, k: &R::Key) -> Option<R> {
        match self.over.get(k) {
            Some(v) => v.clone(),
            None => self.base.get(k).cloned(),
        }
    }

    fn rows_with(&self, layers: &[&HashMap<R::Key, Option<R>>]) -> Vec<R> {
        let lookup = |k: &R::Key| layers.iter().find_map(|l| l.get(k));
        let mut out = Vec::with_capacity(self.base.len());
        for (k, row) in &self.base {
            match lookup(k) {
                None => out.push(row.clone()),
                Some(Some(v)) => out.push(v.clone()),
                Some(None) => {}
            }
        }
        // Rows only the layers have (predicted inserts), oldest layer first.
        let mut seen: HashSet<&R::Key> = HashSet::new();
        for l in layers.iter().rev() {
            for k in l.keys() {
                if self.base.contains_key(k) || !seen.insert(k) {
                    continue;
                }
                if let Some(Some(v)) = lookup(k) {
                    out.push(v.clone());
                }
            }
        }
        out
    }
}

fn auto_inc_of<R: PredictRow>(row: &R) -> Option<u64> {
    let mut r = row.clone();
    r.auto_inc().map(|v| *v)
}

trait AnyTable: Send {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn clear_over(&mut self);
    fn merge(&mut self, layer: Box<dyn Any + Send>);
    fn record_replacements(&mut self, ts: &Timestamp, temps: &[u64]);
    fn clear_own_inserts(&mut self);
    fn diff(&mut self) -> Option<Box<dyn Any + Send>>;
}

impl<R: PredictRow> AnyTable for TableState<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn clear_over(&mut self) {
        self.over.clear();
        self.touched.clear();
        self.inserted.clear();
    }
    fn merge(&mut self, layer: Box<dyn Any + Send>) {
        let layer = *layer.downcast::<DraftLayer<R>>().expect("layer type");
        self.over.extend(layer.mine);
        for (k, inserted) in layer.marks {
            if inserted {
                self.inserted.insert(k.clone());
            }
            self.touched.insert(k);
        }
    }
    fn record_replacements(&mut self, ts: &Timestamp, temps: &[u64]) {
        let real: Vec<R::Key> = self
            .own_inserts
            .iter()
            .filter(|(t, _)| t == ts)
            .map(|(_, k)| k.clone())
            .collect();
        // Pair by order only when the counts agree; otherwise report plain deletes and inserts.
        if real.len() == temps.len() {
            self.replacements.extend(temps.iter().copied().zip(real));
        }
    }
    fn clear_own_inserts(&mut self) {
        self.own_inserts.clear();
    }
    fn diff(&mut self) -> Option<Box<dyn Any + Send>> {
        let mut changes: Vec<Change<R>> = Vec::new();
        let mut handled: HashSet<R::Key> = HashSet::new();
        for (temp_id, real_k) in std::mem::take(&mut self.replacements) {
            let temp_k = self
                .shown
                .iter()
                .find(|(_, r)| auto_inc_of(*r) == Some(temp_id))
                .map(|(k, _)| k.clone());
            let Some(temp_k) = temp_k else { continue };
            let (Some(real), true) = (self.get(&real_k), self.get(&temp_k).is_none()) else {
                continue;
            };
            if self.shown.contains_key(&real_k) {
                continue;
            }
            let temp = self.shown.remove(&temp_k).expect("found above");
            self.shown.insert(real_k.clone(), real.clone());
            handled.insert(temp_k);
            handled.insert(real_k);
            changes.push(Change::Replaced { temp, real });
        }
        let candidates: HashSet<R::Key> = if self.first_flush {
            self.first_flush = false;
            let mut all: HashSet<R::Key> = self.base.keys().cloned().collect();
            all.extend(self.over.keys().cloned());
            all
        } else {
            let mut c: HashSet<R::Key> = std::mem::take(&mut self.server_changed);
            c.extend(self.prev_touched.iter().cloned());
            c.extend(self.touched.iter().cloned());
            c
        };
        self.server_changed.clear();
        for k in candidates {
            if handled.contains(&k) {
                continue;
            }
            match (self.shown.get(&k).cloned(), self.get(&k)) {
                (None, Some(new)) => {
                    self.shown.insert(k, new.clone());
                    changes.push(Change::Inserted(new));
                }
                (Some(old), Some(new)) if old != new => {
                    self.shown.insert(k, new.clone());
                    changes.push(Change::Updated { old, new });
                }
                (Some(old), None) => {
                    self.shown.remove(&k);
                    changes.push(Change::Deleted(old));
                }
                _ => {}
            }
        }
        self.prev_touched = self.touched.clone();
        if changes.is_empty() {
            None
        } else {
            Some(Box::new(changes))
        }
    }
}

// ---------------------------------------------------------------------------
// Drafts: what predictions write to
// ---------------------------------------------------------------------------

struct DraftLayer<R: PredictRow> {
    mine: HashMap<R::Key, Option<R>>,
    marks: Vec<(R::Key, bool)>,
}

/// What a prediction sees: every registered table, writable. Writes go to the prediction's own
/// layer, which is kept only if the prediction returns `Ok`.
pub struct Draft<'a> {
    tables: &'a HashMap<TypeId, Box<dyn AnyTable>>,
    layers: HashMap<TypeId, Box<dyn Any + Send>>,
    temps: &'a mut HashMap<TypeId, Vec<u64>>,
    cursors: HashMap<TypeId, usize>,
    next_temp: &'a mut u64,
}

impl<'a> Draft<'a> {
    /// One table of the draft. Panics if `R` wasn't registered with [`Predictor::table`].
    pub fn table<R: PredictRow>(&mut self) -> DraftTable<'_, R> {
        let tid = TypeId::of::<R>();
        let state = self
            .tables
            .get(&tid)
            .and_then(|t| t.as_any().downcast_ref::<TableState<R>>())
            .unwrap_or_else(|| {
                panic!(
                    "{} isn't registered with Predictor::table",
                    std::any::type_name::<R>()
                )
            });
        let layer = self
            .layers
            .entry(tid)
            .or_insert_with(|| {
                Box::new(DraftLayer::<R> {
                    mine: HashMap::new(),
                    marks: Vec::new(),
                })
            })
            .downcast_mut::<DraftLayer<R>>()
            .expect("layer type");
        DraftTable {
            state,
            layer,
            temps: self.temps.entry(tid).or_default(),
            cursor: self.cursors.entry(tid).or_default(),
            next_temp: self.next_temp,
        }
    }
}

/// One table of a [`Draft`]: like the cache, but writable.
pub struct DraftTable<'d, R: PredictRow> {
    state: &'d TableState<R>,
    layer: &'d mut DraftLayer<R>,
    temps: &'d mut Vec<u64>,
    cursor: &'d mut usize,
    next_temp: &'d mut u64,
}

impl<R: PredictRow> DraftTable<'_, R> {
    fn has(&self, k: &R::Key) -> bool {
        self.find(k).is_some()
    }

    pub fn find(&self, k: &R::Key) -> Option<R> {
        match self.layer.mine.get(k) {
            Some(v) => v.clone(),
            None => self.state.get(k),
        }
    }

    pub fn rows(&self) -> Vec<R> {
        self.state.rows_with(&[&self.layer.mine, &self.state.over])
    }

    /// Insert a row. With an auto-inc column at 0, a temporary id is assigned. Returns the row.
    pub fn insert(&mut self, mut row: R) -> R {
        if let Some(id) = row.auto_inc() {
            if *id == 0 {
                // Reuse this prediction's temp ids on re-runs, so keys stay stable.
                let i = *self.cursor;
                *self.cursor += 1;
                if self.temps.len() <= i {
                    self.temps.push(*self.next_temp);
                    *self.next_temp -= 1;
                }
                *id = self.temps[i];
            }
        }
        let k = row.key();
        self.layer.mine.insert(k.clone(), Some(row.clone()));
        self.layer.marks.push((k, true));
        row
    }

    /// Replace the row with the same key. Returns false (and does nothing) if it isn't there.
    pub fn update(&mut self, row: R) -> bool {
        let k = row.key();
        if !self.has(&k) {
            return false;
        }
        self.layer.mine.insert(k.clone(), Some(row));
        self.layer.marks.push((k, false));
        true
    }

    /// Insert or replace by key.
    pub fn upsert(&mut self, row: R) -> R {
        if self.update(row.clone()) {
            row
        } else {
            self.insert(row)
        }
    }

    /// Delete by key. Returns whether a row was there.
    pub fn delete(&mut self, k: &R::Key) -> bool {
        if !self.has(k) {
            return false;
        }
        self.layer.mine.insert(k.clone(), None);
        self.layer.marks.push((k.clone(), false));
        true
    }
}

// ---------------------------------------------------------------------------
// The predictor
// ---------------------------------------------------------------------------

type PredictionFn = Box<dyn Fn(&mut Draft) -> Result<(), String> + Send>;

struct Pending {
    id: CallId,
    run: PredictionFn,
    temps: HashMap<TypeId, Vec<u64>>,
    deadline: Instant,
}

struct Inner {
    tables: HashMap<TypeId, Box<dyn AnyTable>>,
    pending: Vec<Pending>,
    settled: Vec<Settled>,
    dirty: bool,
    next_id: CallId,
    next_temp: u64,
    stats: Stats,
}

impl Inner {
    fn rebuild(&mut self) {
        if !self.dirty {
            return;
        }
        self.dirty = false;
        for t in self.tables.values_mut() {
            t.clear_over();
        }
        let Inner {
            tables,
            pending,
            next_temp,
            stats,
            ..
        } = self;
        for p in pending.iter_mut() {
            let mut d = Draft {
                tables: &*tables,
                layers: HashMap::new(),
                temps: &mut p.temps,
                cursors: HashMap::new(),
                next_temp: &mut *next_temp,
            };
            let ok = matches!(
                catch_unwind(AssertUnwindSafe(|| (p.run)(&mut d))),
                Ok(Ok(()))
            );
            let Draft { layers, .. } = d;
            if !ok {
                stats.skipped += 1;
                continue;
            }
            for (tid, layer) in layers {
                if let Some(t) = tables.get_mut(&tid) {
                    t.merge(layer);
                }
            }
        }
    }

    fn settle(&mut self, id: CallId, outcome: Outcome, ts: Option<&Timestamp>) {
        let Some(i) = self.pending.iter().position(|p| p.id == id) else {
            return; // already timed out
        };
        let p = self.pending.remove(i);
        match &outcome {
            Outcome::Confirmed => {
                self.stats.confirmed += 1;
                if let Some(ts) = ts {
                    for (tid, temps) in &p.temps {
                        if let Some(t) = self.tables.get_mut(tid) {
                            t.record_replacements(ts, temps);
                        }
                    }
                }
            }
            Outcome::RolledBack(_) => self.stats.rolled_back += 1,
            Outcome::TimedOut => self.stats.timed_out += 1,
        }
        self.settled.push(Settled { call: id, outcome });
        self.dirty = true;
    }

    fn state<R: PredictRow>(&mut self) -> &mut TableState<R> {
        self.tables
            .get_mut(&TypeId::of::<R>())
            .and_then(|t| t.as_any_mut().downcast_mut::<TableState<R>>())
            .unwrap_or_else(|| {
                panic!(
                    "{} isn't registered with Predictor::table",
                    std::any::type_name::<R>()
                )
            })
    }
}

/// Predictions layered over a connection's client cache. Cheap to clone (it's a handle).
#[derive(Clone)]
pub struct Predictor {
    inner: Arc<Mutex<Inner>>,
}

impl Default for Predictor {
    fn default() -> Self {
        Self::new()
    }
}

impl Predictor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                tables: HashMap::new(),
                pending: Vec::new(),
                settled: Vec::new(),
                dirty: true,
                next_id: 1,
                next_temp: u64::MAX,
                stats: Stats::default(),
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Track a table: reads its current rows and follows its insert/update/delete callbacks.
    /// Register before subscribing, or after: both work. The first [`flush`](Self::flush)
    /// reports every row as inserted.
    pub fn table<H, E>(&self, handle: &H)
    where
        H: TableWithPrimaryKey,
        H::Row: PredictRow,
        H::EventContext: AbstractEventContext<Event = Event<E>>,
        E: 'static,
    {
        type R<H> = <H as Table>::Row;
        self.lock().tables.insert(
            TypeId::of::<R<H>>(),
            Box::new(TableState::<R<H>>::new(handle.iter())),
        );

        let me = self.clone();
        handle.on_insert(move |ctx, row| {
            let mut g = me.lock();
            let k = row.key();
            let reducer_ts = match ctx.event() {
                Event::Reducer(ReducerEvent { timestamp, .. }) => Some(*timestamp),
                _ => None,
            };
            let s = g.state::<R<H>>();
            s.base.insert(k.clone(), row.clone());
            s.server_changed.insert(k.clone());
            if let Some(ts) = reducer_ts {
                s.own_inserts.push((ts, k));
            }
            server_changed(&mut g);
        });
        let me = self.clone();
        handle.on_delete(move |_ctx, row| {
            let mut g = me.lock();
            let k = row.key();
            let s = g.state::<R<H>>();
            s.base.remove(&k);
            s.server_changed.insert(k);
            server_changed(&mut g);
        });
        let me = self.clone();
        handle.on_update(move |_ctx, old, new| {
            let mut g = me.lock();
            let s = g.state::<R<H>>();
            let (ko, kn) = (old.key(), new.key());
            s.base.remove(&ko);
            s.base.insert(kn.clone(), new.clone());
            s.server_changed.insert(ko);
            s.server_changed.insert(kn);
            server_changed(&mut g);
        });
    }

    /// Re-read a table's server rows from the cache (after a reconnect or a resubscribe, say).
    pub fn resync<H>(&self, handle: &H)
    where
        H: Table,
        H::Row: PredictRow,
    {
        let mut g = self.lock();
        let s = g.state::<H::Row>();
        let old: HashSet<_> = s.base.keys().cloned().collect();
        s.base = handle.iter().map(|r| (r.key(), r)).collect();
        s.server_changed.extend(old);
        s.server_changed
            .extend(s.base.keys().cloned().collect::<Vec<_>>());
        g.dirty = true;
    }

    /// Call a reducer with a prediction of its effect, with the default 10 s timeout.
    ///
    /// `send` gets a [`Settle`] to call from the reducer's `_then` callback:
    /// `|settle| conn.reducers.toggle_task_then(id, move |ctx, res| settle.done(ctx, res))`.
    /// If `send` returns an error, the prediction is rolled back at once.
    ///
    /// `prediction` is re-run on top of every server change until the call settles. Return `Err`
    /// (don't panic) when it no longer applies; it's then skipped.
    pub fn call<S, P>(&self, send: S, prediction: P) -> CallId
    where
        S: FnOnce(Settle) -> spacetimedb_sdk::Result<()>,
        P: Fn(&mut Draft) -> Result<(), String> + Send + 'static,
    {
        self.call_with_timeout(DEFAULT_TIMEOUT, send, prediction)
    }

    pub fn call_with_timeout<S, P>(&self, timeout: Duration, send: S, prediction: P) -> CallId
    where
        S: FnOnce(Settle) -> spacetimedb_sdk::Result<()>,
        P: Fn(&mut Draft) -> Result<(), String> + Send + 'static,
    {
        let id = {
            let mut g = self.lock();
            let id = g.next_id;
            g.next_id += 1;
            g.stats.calls += 1;
            g.pending.push(Pending {
                id,
                run: Box::new(prediction),
                temps: HashMap::new(),
                deadline: Instant::now() + timeout,
            });
            g.dirty = true;
            id
        };
        // Not holding the lock: the SDK runs nothing of ours while sending, but don't rely on it.
        if let Err(e) = send(Settle {
            inner: self.inner.clone(),
            id,
        }) {
            self.lock()
                .settle(id, Outcome::RolledBack(format!("couldn't send: {e}")), None);
        }
        id
    }

    /// Rebuild the predicted view if anything changed, roll back timed-out calls, and report what
    /// changed since the last flush. Call once per frame, right after `conn.frame_tick()`.
    pub fn flush(&self) -> Flush {
        let mut g = self.lock();
        let now = Instant::now();
        let late: Vec<CallId> = g
            .pending
            .iter()
            .filter(|p| p.deadline <= now)
            .map(|p| p.id)
            .collect();
        for id in late {
            g.settle(id, Outcome::TimedOut, None);
        }
        g.rebuild();
        let mut f = Flush {
            changes: HashMap::new(),
            settled: std::mem::take(&mut g.settled),
        };
        for (tid, t) in g.tables.iter_mut() {
            if let Some(c) = t.diff() {
                f.changes.insert(*tid, c);
            }
            t.clear_own_inserts();
        }
        f
    }

    /// The predicted rows of one table.
    pub fn rows<R: PredictRow>(&self) -> Vec<R> {
        let mut g = self.lock();
        g.rebuild();
        let s = g.state::<R>();
        s.rows_with(&[&s.over])
    }

    pub fn find<R: PredictRow>(&self, k: &R::Key) -> Option<R> {
        let mut g = self.lock();
        g.rebuild();
        g.state::<R>().get(k)
    }

    /// Whether a pending prediction inserted, changed or deleted this key.
    pub fn is_pending<R: PredictRow>(&self, k: &R::Key) -> bool {
        let mut g = self.lock();
        g.rebuild();
        g.state::<R>().touched.contains(k)
    }

    /// Whether this row exists only as a prediction (a predicted insert).
    pub fn is_predicted<R: PredictRow>(&self, k: &R::Key) -> bool {
        let mut g = self.lock();
        g.rebuild();
        g.state::<R>().inserted.contains(k)
    }

    /// How many calls are still waiting for the server.
    pub fn pending_count(&self) -> usize {
        self.lock().pending.len()
    }

    pub fn stats(&self) -> Stats {
        self.lock().stats.clone()
    }
}

fn server_changed(g: &mut Inner) {
    if !g.pending.is_empty() {
        g.stats.rebases += 1;
    }
    g.dirty = true;
}

/// Settles one call: pass it into the reducer's `_then` callback and call [`Settle::done`].
pub struct Settle {
    inner: Arc<Mutex<Inner>>,
    id: CallId,
}

impl Settle {
    /// Settle the call from its `_then` callback: `Ok(Ok(()))` confirms it, anything else rolls
    /// it back with the reason.
    pub fn done<C, E, I>(self, ctx: &C, result: Result<Result<(), String>, I>)
    where
        C: AbstractEventContext<Event = ReducerEvent<E>>,
        I: Display,
    {
        let outcome = match result {
            Ok(Ok(())) => Outcome::Confirmed,
            Ok(Err(reason)) => Outcome::RolledBack(reason),
            Err(e) => Outcome::RolledBack(e.to_string()),
        };
        let ts = ctx.event().timestamp;
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .settle(self.id, outcome, Some(&ts));
    }

    /// The call this settles.
    pub fn call(&self) -> CallId {
        self.id
    }
}
