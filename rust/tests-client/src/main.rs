//! Prediction behaviour against the demo module, through a latency proxy: the Rust twin of
//! tests/predict.test.mts.
//!
//!   DB=predict-rs PROXY=3120 DELAY=150 cargo run -p predict-tests-client   (scripts/test-rs.sh)
//!
//! "a" talks through the proxy and predicts; "b" talks to the server directly and plays someone
//! else changing things underneath a's predictions. Both are driven like a game loop: frame_tick
//! on each connection, then flush, every ~5 ms.

mod module_bindings;

use module_bindings::*;
use spacetimedb_predict::{
    is_temp_id, Change, Draft, DraftTable, Outcome, PredictRow, Predictor, Settled,
};
use spacetimedb_sdk::{DbContext, Identity, Table};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

impl PredictRow for Task {
    type Key = u64;
    fn key(&self) -> u64 {
        self.id
    }
    fn auto_inc(&mut self) -> Option<&mut u64> {
        Some(&mut self.id)
    }
}

// --- The demo's reducer logic, as predictions (mirrors demo/module/src/logic.ts) -----------

fn title(t: &str) -> Result<String, String> {
    let s = t.trim();
    if s.is_empty() {
        return Err("title is empty".into());
    }
    if s.chars().count() > 80 {
        return Err("title too long".into());
    }
    Ok(s.to_string())
}

fn predict_add(
    me: Identity,
    t: String,
) -> impl Fn(&mut Draft) -> Result<(), String> + Send + 'static {
    move |d| {
        let mut tasks = d.table::<Task>();
        let last = tasks.rows().iter().map(|t| t.pos).fold(0.0, f64::max);
        tasks.insert(Task {
            id: 0,
            owner: me,
            title: title(&t)?,
            done: false,
            pos: last + 1.0,
        });
        Ok(())
    }
}

fn predict_toggle(id: u64) -> impl Fn(&mut Draft) -> Result<(), String> + Send + 'static {
    move |d| {
        let mut tasks = d.table::<Task>();
        let t = tasks.find(&id).ok_or("no such task")?;
        tasks.update(Task { done: !t.done, ..t });
        Ok(())
    }
}

fn predict_rename(
    id: u64,
    t: String,
) -> impl Fn(&mut Draft) -> Result<(), String> + Send + 'static {
    move |d| {
        let mut tasks = d.table::<Task>();
        let row = tasks.find(&id).ok_or("no such task")?;
        tasks.update(Task {
            title: title(&t)?,
            ..row
        });
        Ok(())
    }
}

// --- Harness ---------------------------------------------------------------------------------

struct World {
    a: DbConnection,
    b: DbConnection,
    p: Predictor,
    me: Identity,
    /// Every change flush reported, and every settled call, in order.
    changes: Vec<Change<Task>>,
    settled: Vec<Settled>,
    /// The most copies of one title the predicted view ever held after a flush.
    max_dupes: HashMap<String, usize>,
}

impl World {
    /// One frame: tick both connections, then flush the predictor.
    fn frame(&mut self) {
        self.a.frame_tick().expect("a frame_tick");
        self.b.frame_tick().expect("b frame_tick");
        let f = self.p.flush();
        self.changes.extend(f.changes::<Task>().iter().cloned());
        self.settled.extend(f.settled);
        let mut counts: HashMap<String, usize> = HashMap::new();
        for t in self.p.rows::<Task>() {
            *counts.entry(t.title).or_default() += 1;
        }
        for (t, n) in counts {
            let m = self.max_dupes.entry(t).or_default();
            *m = (*m).max(n);
        }
    }

    fn pump(&mut self, ms: u64) {
        let end = Instant::now() + Duration::from_millis(ms);
        while Instant::now() < end {
            self.frame();
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn until(&mut self, what: &str, mut cond: impl FnMut(&World) -> bool) {
        let end = Instant::now() + Duration::from_secs(10);
        while !cond(self) {
            assert!(Instant::now() < end, "timed out waiting for {what}");
            self.frame();
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn outcome(&self, call: u64) -> Option<Outcome> {
        self.settled
            .iter()
            .find(|s| s.call == call)
            .map(|s| s.outcome.clone())
    }

    fn by_title(&self, t: &str) -> Option<Task> {
        self.p.rows::<Task>().into_iter().find(|r| r.title == t)
    }

    fn server(&self, t: &str) -> Option<Task> {
        self.a.db.task().iter().find(|r| r.title == t)
    }

    fn add(&self, t: &str) -> u64 {
        let t2 = t.to_string();
        self.p.call(
            |s| {
                self.a
                    .reducers
                    .add_task_then(t2, move |ctx, res| s.done(ctx, res))
            },
            predict_add(self.me, t.to_string()),
        )
    }

    fn toggle(&self, id: u64) -> u64 {
        self.p.call(
            |s| {
                self.a
                    .reducers
                    .toggle_task_then(id, move |ctx, res| s.done(ctx, res))
            },
            predict_toggle(id),
        )
    }

    fn rename(&self, id: u64, t: &str) -> u64 {
        let t2 = t.to_string();
        self.p.call(
            |s| {
                self.a
                    .reducers
                    .rename_task_then(id, t2, move |ctx, res| s.done(ctx, res))
            },
            predict_rename(id, t.to_string()),
        )
    }

    /// Forget everyone's rate limits (through b) and wait for it.
    fn reset_rate(&mut self) {
        self.on_b(|b, done| {
            b.reducers
                .reset_rate_then(move |_, _| done.store(true, Ordering::SeqCst))
                .unwrap();
        });
    }

    /// Run a reducer on b and wait for it to finish.
    fn on_b(&mut self, send: impl FnOnce(&DbConnection, Arc<AtomicBool>)) {
        let done = Arc::new(AtomicBool::new(false));
        send(&self.b, done.clone());
        self.until("b's reducer", |_| done.load(Ordering::SeqCst));
    }
}

fn connect(port: u16, db: &str) -> DbConnection {
    let conn = DbConnection::builder()
        .with_uri(format!("http://127.0.0.1:{port}").as_str())
        .with_database_name(db)
        .build()
        .expect("connect");
    let applied = Arc::new(AtomicBool::new(false));
    let flag = applied.clone();
    conn.subscription_builder()
        .on_applied(move |_| flag.store(true, Ordering::SeqCst))
        .subscribe(["SELECT * FROM task"]);
    let end = Instant::now() + Duration::from_secs(10);
    while !applied.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < end,
            "subscription on :{port} never applied"
        );
        conn.frame_tick().expect("frame_tick");
        std::thread::sleep(Duration::from_millis(5));
    }
    conn
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Two connections to `db` (a through the proxy, b direct), and a predictor on a.
fn world(db: &str, proxy: u16) -> World {
    let a = connect(proxy, db);
    let b = connect(3000, db);
    let me = a.identity();
    let p = Predictor::new();
    p.table(&a.db.task());
    let mut w = World {
        a,
        b,
        p,
        me,
        changes: vec![],
        settled: vec![],
        max_dupes: HashMap::new(),
    };
    w.frame(); // the initial rows
    w.reset_rate();
    w
}

/// Predictions written by hand in the client, against the TypeScript demo module.
fn hand_written(db: &str, proxy: u16, delay: u64) -> usize {
    let mut w = world(db, proxy);
    let mut passed = 0;
    let mut ok = |name: &str| {
        passed += 1;
        println!("  ok  {name}");
    };

    // 1
    {
        w.changes.clear();
        let t0 = Instant::now();
        let call = w.add("alpha");
        w.frame();
        let visible = t0.elapsed();
        let predicted = w.by_title("alpha").expect("visible after one frame");
        assert!(is_temp_id(predicted.id), "temp id");
        assert!(w.p.is_predicted::<Task>(&predicted.id) && w.p.is_pending::<Task>(&predicted.id));
        assert!(w.server("alpha").is_none(), "not on the server yet");
        assert!(matches!(&w.changes[..], [Change::Inserted(t)] if t.title == "alpha"));
        w.until("alpha settles", |w| w.outcome(call).is_some());
        let confirmed = t0.elapsed();
        assert_eq!(w.outcome(call), Some(Outcome::Confirmed));
        let real = w.by_title("alpha").unwrap();
        assert!(!is_temp_id(real.id) && !w.p.is_pending::<Task>(&real.id));
        assert_eq!(
            w.max_dupes.get("alpha"),
            Some(&1),
            "never two alphas after a flush"
        );
        let replaced: Vec<_> = w
            .changes
            .iter()
            .filter(|c| matches!(c, Change::Replaced { .. }))
            .collect();
        assert!(
            matches!(replaced[..], [Change::Replaced { temp, real: r }] if temp.id == predicted.id && r.id == real.id),
            "one Replaced {{temp → real}}: {:?}",
            w.changes
        );
        assert!(
            !w.changes.iter().any(|c| matches!(c, Change::Deleted(_))),
            "no delete+insert churn"
        );
        println!(
            "      predicted visible after {:.2} ms; confirmed after {} ms",
            visible.as_secs_f64() * 1e3,
            confirmed.as_millis()
        );
        ok("an insert shows at once with a temporary id, then is Replaced by the real row, never both");
    }

    // 2
    {
        let id = w.by_title("alpha").unwrap().id;
        let call = w.toggle(id);
        w.frame();
        assert!(w.p.find::<Task>(&id).unwrap().done);
        assert!(
            !w.server("alpha").unwrap().done,
            "server copy unchanged so far"
        );
        w.changes.clear();
        w.until("toggle settles", |w| w.outcome(call).is_some());
        assert!(w.p.find::<Task>(&id).unwrap().done);
        assert!(
            w.changes.is_empty(),
            "nothing flipped back on the way: {:?}",
            w.changes
        );
        ok("updates show at once and stay put when the server agrees");
    }

    // 3
    {
        w.changes.clear();
        let calls: Vec<u64> = ["one", "two", "three"].iter().map(|t| w.add(t)).collect();
        w.frame();
        let titles = |w: &World| {
            let mut r = w.p.rows::<Task>();
            r.sort_by(|a, b| a.pos.partial_cmp(&b.pos).unwrap());
            r.iter()
                .rev()
                .take(3)
                .rev()
                .map(|t| t.title.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(titles(&w), ["one", "two", "three"]);
        assert_eq!(w.p.pending_count(), 3);
        w.until("three settle", |w| {
            calls.iter().all(|c| w.outcome(*c).is_some())
        });
        assert_eq!(titles(&w), ["one", "two", "three"]);
        assert!(w.p.rows::<Task>().iter().all(|t| !is_temp_id(t.id)));
        let replaced = w
            .changes
            .iter()
            .filter(|c| matches!(c, Change::Replaced { .. }))
            .count();
        assert_eq!(replaced, 3, "each temp row re-keyed: {:?}", w.changes);
        ok("several pending inserts keep their order and land in it");
    }

    // 4
    {
        w.on_b(|b, done| {
            b.reducers
                .reset_rate_then(move |_, _| done.store(true, Ordering::SeqCst))
                .unwrap();
        });
        let rolled = w.p.stats().rolled_back;
        let calls: Vec<u64> = (1..=6).map(|i| w.add(&format!("r{i}"))).collect();
        w.frame();
        assert!(w.by_title("r6").is_some(), "predicted like the others");
        w.until("burst settles", |w| {
            calls.iter().all(|c| w.outcome(*c).is_some())
        });
        for c in &calls[..5] {
            assert_eq!(w.outcome(*c), Some(Outcome::Confirmed));
        }
        match w.outcome(calls[5]) {
            Some(Outcome::RolledBack(reason)) => assert!(reason.contains("slow down"), "{reason}"),
            o => panic!("expected a rollback, got {o:?}"),
        }
        w.frame();
        assert!(w.by_title("r6").is_none(), "rolled back");
        assert!(w.by_title("r5").is_some());
        assert_eq!(w.p.stats().rolled_back, rolled + 1);
        w.on_b(|b, done| {
            b.reducers
                .reset_rate_then(move |_, _| done.store(true, Ordering::SeqCst))
                .unwrap();
        });
        ok("the server says no (a rule the client can't see): the prediction rolls back with the reason");
    }

    // 5
    {
        let id = w.by_title("one").unwrap().id;
        let rebases = w.p.stats().rebases;
        let call = w.rename(id, "one (renamed by a)");
        w.frame();
        w.on_b(|b, done| {
            b.reducers
                .toggle_task_then(id, move |_, _| done.store(true, Ordering::SeqCst))
                .unwrap();
        });
        w.pump(delay + 50); // b's change reaches a while the rename is still pending
        assert_eq!(w.p.pending_count(), 1, "rename still pending");
        let mid = w.p.find::<Task>(&id).unwrap();
        assert_eq!(mid.title, "one (renamed by a)", "still predicted");
        assert!(mid.done, "b's toggle shows through");
        assert!(w.p.stats().rebases > rebases);
        w.until("rename settles", |w| w.outcome(call).is_some());
        let end = w.p.find::<Task>(&id).unwrap();
        assert_eq!((end.title.as_str(), end.done), ("one (renamed by a)", true));
        ok("someone else's change lands under a pending prediction: it's rebased, both show");
    }

    // 6
    {
        let id = w.by_title("two").unwrap().id;
        let skipped = w.p.stats().skipped;
        let call = w.rename(id, "two (too late)");
        w.frame();
        assert_eq!(w.p.find::<Task>(&id).unwrap().title, "two (too late)");
        w.on_b(|b, done| {
            b.reducers
                .delete_task_then(id, move |_, _| done.store(true, Ordering::SeqCst))
                .unwrap();
        });
        w.pump(delay + 50);
        assert!(
            w.p.find::<Task>(&id).is_none(),
            "gone: the rename has nothing to apply to"
        );
        assert!(w.p.stats().skipped > skipped);
        w.until("rename settles", |w| w.outcome(call).is_some());
        match w.outcome(call) {
            Some(Outcome::RolledBack(reason)) => {
                assert!(reason.contains("no such task"), "{reason}")
            }
            o => panic!("expected a refusal, got {o:?}"),
        }
        ok("a prediction that no longer applies is dropped, and the server refuses it too");
    }

    // 7
    {
        let before = w.p.rows::<Task>().len();
        let me = w.me;
        let call = w.p.call_with_timeout(
            Duration::from_millis(200),
            |_settle| Ok(()), // never settled
            move |d| {
                d.table::<Task>().insert(Task {
                    id: 0,
                    owner: me,
                    title: "lost".into(),
                    done: false,
                    pos: 999.0,
                });
                Ok(())
            },
        );
        w.frame();
        assert_eq!(w.p.rows::<Task>().len(), before + 1);
        w.pump(300);
        assert_eq!(w.outcome(call), Some(Outcome::TimedOut));
        assert_eq!(w.p.rows::<Task>().len(), before);
        ok("no answer in time: the prediction rolls back as TimedOut");
    }

    // 8
    {
        let (mut predicted, mut server) = (vec![], vec![]);
        for _ in 0..5 {
            let id = w.by_title("three").unwrap().id;
            let was = w.p.find::<Task>(&id).unwrap().done;
            let t0 = Instant::now();
            let call = w.toggle(id);
            w.frame();
            assert_eq!(w.p.find::<Task>(&id).unwrap().done, !was);
            predicted.push(t0.elapsed().as_secs_f64() * 1e3);
            w.until("toggle settles", |w| w.outcome(call).is_some());
            let want = !w.a.db.task().id().find(&id).unwrap().done;
            let t0 = Instant::now();
            w.a.reducers.toggle_task(id).unwrap();
            w.until("server toggle", |w| {
                w.a.db.task().id().find(&id).unwrap().done == want
            });
            server.push(t0.elapsed().as_secs_f64() * 1e3);
        }
        let (mp, ms) = (median(predicted), median(server));
        println!("      median: predicted {mp:.2} ms (one frame), server round trip {ms:.0} ms (+{delay} ms each way)");
        assert!(mp < 5.0 && ms >= (2 * delay) as f64);
        ok("click to visible: predicted vs waiting for the server");
    }

    println!("  {passed} passed · stats {:?}", w.p.stats());
    w.a.disconnect().ok();
    w.b.disconnect().ok();
    passed
}

// --- Write it once: the module's own logic as the prediction --------------------------------

/// The client's side of [`task_logic::Tasks`]: a prediction draft's task table.
struct PredictedTasks<'d> {
    t: DraftTable<'d, Task>,
    me: Identity,
}

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

impl task_logic::Tasks for PredictedTasks<'_> {
    fn sender(&self) -> Identity {
        self.me
    }
    fn all(&self) -> Vec<task_logic::Task> {
        self.t.rows().into_iter().map(to_logic).collect()
    }
    fn find(&self, id: u64) -> Option<task_logic::Task> {
        self.t.find(&id).map(to_logic)
    }
    fn insert(&mut self, task: task_logic::Task) {
        self.t.insert(from_logic(task)); // id 0: the draft hands out a temporary id
    }
    fn update(&mut self, task: task_logic::Task) {
        self.t.update(from_logic(task));
    }
    fn delete(&mut self, id: u64) {
        self.t.delete(&id);
    }
}

/// A prediction that runs one of the module's reducer bodies against the draft.
fn shared(
    me: Identity,
    f: impl Fn(&mut PredictedTasks) -> Result<(), String> + Send + 'static,
) -> impl Fn(&mut Draft) -> Result<(), String> + Send + 'static {
    move |d| {
        f(&mut PredictedTasks {
            t: d.table::<Task>(),
            me,
        })
    }
}

impl World {
    fn shared_add(&self, t: &str) -> u64 {
        let (t2, t3) = (t.to_string(), t.to_string());
        self.p.call(
            |s| {
                self.a
                    .reducers
                    .add_task_then(t2, move |ctx, res| s.done(ctx, res))
            },
            shared(self.me, move |db| task_logic::add_task(db, &t3)),
        )
    }

    fn shared_rename(&self, id: u64, t: &str) -> u64 {
        let (t2, t3) = (t.to_string(), t.to_string());
        self.p.call(
            |s| {
                self.a
                    .reducers
                    .rename_task_then(id, t2, move |ctx, res| s.done(ctx, res))
            },
            shared(self.me, move |db| task_logic::rename_task(db, id, &t3)),
        )
    }

    fn shared_move(&self, id: u64, before: u64) -> u64 {
        self.p.call(
            |s| {
                self.a
                    .reducers
                    .move_task_then(id, before, move |ctx, res| s.done(ctx, res))
            },
            shared(self.me, move |db| task_logic::move_task(db, id, before)),
        )
    }

    fn sorted(&self) -> Vec<Task> {
        let mut r = self.p.rows::<Task>();
        r.sort_by(|a, b| a.pos.total_cmp(&b.pos));
        r
    }
}

/// The Rust module's reducer logic (`task_logic`), run on the client as the prediction, against
/// the Rust demo module.
fn shared_logic(db: &str, proxy: u16, delay: u64) -> usize {
    let mut w = world(db, proxy);
    let mut passed = 0;
    let mut ok = |name: &str| {
        passed += 1;
        println!("  ok  {name}");
    };

    // S1
    {
        w.changes.clear();
        let call = w.shared_add("shared alpha");
        w.frame();
        let predicted = w.by_title("shared alpha").expect("visible after one frame");
        assert!(is_temp_id(predicted.id));
        assert_eq!(predicted.owner, w.me, "the logic's sender is the client");
        w.until("settles", |w| w.outcome(call).is_some());
        assert_eq!(w.outcome(call), Some(Outcome::Confirmed));
        let real = w.by_title("shared alpha").unwrap();
        assert_eq!(w.max_dupes.get("shared alpha"), Some(&1));
        assert!(
            w.changes.iter().any(|c| matches!(c, Change::Replaced { temp, real: r } if temp.id == predicted.id && r.id == real.id)),
            "{:?}",
            w.changes
        );
        assert_eq!(
            (predicted.pos, predicted.title.as_str()),
            (real.pos, real.title.as_str()),
            "predicted exactly"
        );
        ok("the module's add_task runs as the prediction: shows at once, then Replaced by the real row");
    }

    // S2
    {
        let before = w.p.rows::<Task>().len();
        let skipped = w.p.stats().skipped;
        w.changes.clear();
        let call = w.shared_add("   ");
        w.frame();
        assert_eq!(w.p.rows::<Task>().len(), before, "never shown");
        assert!(w.changes.is_empty());
        assert!(
            w.p.stats().skipped > skipped,
            "the logic refused it on the client"
        );
        w.until("settles", |w| w.outcome(call).is_some());
        assert_eq!(
            w.outcome(call),
            Some(Outcome::RolledBack("title is empty".into())),
            "same words on the server"
        );
        ok("the logic refuses on the client (empty title): never shown; the server refuses with the same text");
    }

    // S3
    {
        w.reset_rate();
        let calls: Vec<u64> = (1..=6).map(|i| w.shared_add(&format!("s{i}"))).collect();
        w.frame();
        assert!(
            w.by_title("s6").is_some(),
            "the client can't see the rate limit"
        );
        w.until("burst settles", |w| {
            calls.iter().all(|c| w.outcome(*c).is_some())
        });
        match w.outcome(calls[5]) {
            Some(Outcome::RolledBack(reason)) => assert!(reason.contains("slow down"), "{reason}"),
            o => panic!("expected a rollback, got {o:?}"),
        }
        w.frame();
        assert!(w.by_title("s6").is_none() && w.by_title("s5").is_some());
        w.reset_rate();
        ok("a server-only rule (rate limit) still refuses: the prediction rolls back");
    }

    // S4
    {
        let id = w.by_title("s1").unwrap().id;
        let call = w.shared_rename(id, "s1 (renamed)");
        w.frame();
        w.on_b(|b, done| {
            b.reducers
                .toggle_task_then(id, move |_, _| done.store(true, Ordering::SeqCst))
                .unwrap();
        });
        w.pump(delay + 50);
        let mid = w.p.find::<Task>(&id).unwrap();
        assert_eq!(
            (mid.title.as_str(), mid.done),
            ("s1 (renamed)", true),
            "rebased on b's toggle"
        );
        w.until("settles", |w| w.outcome(call).is_some());
        let end = w.p.find::<Task>(&id).unwrap();
        assert_eq!((end.title.as_str(), end.done), ("s1 (renamed)", true));
        ok("someone else's change under a shared-logic prediction: rebased");
    }

    // S5
    {
        let rows = w.sorted();
        let n = rows.len();
        let (a, b, c) = (
            rows[n - 3].clone(),
            rows[n - 2].clone(),
            rows[n - 1].clone(),
        );
        w.changes.clear();
        let call = w.shared_move(c.id, b.id);
        w.frame();
        let predicted = w.p.find::<Task>(&c.id).unwrap().pos;
        assert_eq!(predicted, (a.pos + b.pos) / 2.0);
        let order: Vec<u64> = w
            .sorted()
            .iter()
            .rev()
            .take(3)
            .rev()
            .map(|t| t.id)
            .collect();
        assert_eq!(
            order,
            [a.id, c.id, b.id],
            "moved between its neighbours at once"
        );
        let after_predict = w.changes.len();
        w.until("settles", |w| w.outcome(call).is_some());
        w.frame();
        let server = w.a.db.task().id().find(&c.id).unwrap().pos;
        assert_eq!(
            server.to_bits(),
            predicted.to_bits(),
            "the server computed the same f64"
        );
        assert_eq!(
            w.changes.len(),
            after_predict,
            "no correction when it was confirmed: {:?}",
            &w.changes[after_predict..]
        );
        ok("move between neighbours: the prediction is exactly the server's pos, so nothing moves on confirm");
    }

    println!("  {passed} passed · stats {:?}", w.p.stats());
    w.a.disconnect().ok();
    w.b.disconnect().ok();
    passed
}

fn main() {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let proxy: u16 = env("PROXY", "3120").parse().expect("PROXY");
    let delay: u64 = env("DELAY", "150").parse().expect("DELAY");
    let (db_ts, db_rs) = (
        env("DB", "predict-rs"),
        env("DB_RS_MODULE", "predict-rs-mod"),
    );

    println!("hand-written predictions, against the TypeScript module ({db_ts}):");
    let a = hand_written(&db_ts, proxy, delay);
    println!("\nwrite it once: task_logic as the prediction, against the Rust module ({db_rs}):");
    let b = shared_logic(&db_rs, proxy, delay);
    println!("\n{} passed", a + b);
}
