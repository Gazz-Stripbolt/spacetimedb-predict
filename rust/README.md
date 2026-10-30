# spacetimedb-predict for Rust (Bevy or any game loop)

Optimistic updates / client-side prediction for Spacetime Rust clients, with the same behaviour as the TypeScript
package: a call's predicted effect shows at once, is re-run on top of every server change until the reply comes back
(rebase), and is replaced by the server's real rows without a flicker. Refused or timed-out calls roll back. Source:
[`spacetimedb-predict/src/lib.rs`](spacetimedb-predict/src/lib.rs) · tests: [`tests-client`](tests-client/src/main.rs).

No async and no background work: call `flush()` once per frame, right after `conn.frame_tick()`. It returns what
changed in the **predicted** view (inserted / updated / deleted / replaced) and which calls settled, so a game can turn
it straight into spawn / update / despawn / re-key.

```toml
[dependencies]
spacetimedb-sdk = "2.14"
spacetimedb-predict = { git = "https://github.com/Gazz-Stripbolt/spacetimedb-predict" }
```

## Use it

```rust
use spacetimedb_predict::{is_temp_id, Change, Outcome, PredictRow, Predictor};

// 1. Tell it about your generated row types.
impl PredictRow for Task {
    type Key = u64;
    fn key(&self) -> u64 { self.id }
    // Only for auto-inc keys: predicted inserts with 0 get a temporary id until the real row arrives.
    fn auto_inc(&mut self) -> Option<&mut u64> { Some(&mut self.id) }
}

// 2. Register tables (before or after subscribing).
let p = Predictor::new();
p.table(&conn.db.task());

// 3. Call reducers through it, with a prediction. Settle from the reducer's `_then` callback.
p.call(
    |settle| conn.reducers.toggle_task_then(id, move |ctx, res| settle.done(ctx, res)),
    move |d| {
        let mut tasks = d.table::<Task>();
        let t = tasks.find(&id).ok_or("no such task")?;   // Err = no longer applies: skipped
        tasks.update(Task { done: !t.done, ..t });
        Ok(())
    },
);

// 4. Every frame.
conn.frame_tick()?;
let f = p.flush();
for change in f.changes::<Task>() { /* see the Bevy example */ }
for s in &f.settled {
    if let Outcome::RolledBack(reason) = &s.outcome { show_toast(reason); }
}
```

Also: `p.rows::<Task>()`, `p.find::<Task>(&id)`, `p.is_pending::<Task>(&id)`, `p.is_predicted::<Task>(&id)`,
`p.pending_count()`, `p.stats()`, `p.call_with_timeout(..)` (default 10 s), `p.resync(&conn.db.task())` after a
reconnect, and `is_temp_id(id)`.

Predictions run on every rebase, so keep them deterministic and cheap, and return `Err` rather than panicking (a
panic is caught and counted as skipped, but it's slower and noisy).

## Bevy

The connection is driven from a system; the predictor's changes become entity commands. `Replaced` means "this
predicted entity is now the real one": re-key it instead of despawning and respawning.

```rust
#[derive(Resource)]
struct Net { conn: DbConnection, p: Predictor }

#[derive(Resource, Default)]
struct TaskEntities(HashMap<u64, Entity>);

fn net_tick(net: Res<Net>, mut commands: Commands, mut ents: ResMut<TaskEntities>) {
    net.conn.frame_tick().expect("connection");
    let f = net.p.flush();
    for change in f.changes::<Task>() {
        match change {
            Change::Inserted(t) => {
                let e = commands.spawn((TaskView::from(t), Predicted(is_temp_id(t.id)))).id();
                ents.0.insert(t.id, e);
            }
            Change::Updated { new, .. } => {
                if let Some(&e) = ents.0.get(&new.id) { commands.entity(e).insert(TaskView::from(new)); }
            }
            Change::Deleted(t) => {
                if let Some(e) = ents.0.remove(&t.id) { commands.entity(e).despawn(); }
            }
            Change::Replaced { temp, real } => {
                if let Some(e) = ents.0.remove(&temp.id) {
                    commands.entity(e).insert((TaskView::from(real), Predicted(false)));
                    ents.0.insert(real.id, e);
                }
            }
        }
    }
}

// Input → a predicted call (the same Predictor handle is cheap to clone).
fn toggle_on_click(net: Res<Net>, clicked: Query<&TaskView, With<Clicked>>) {
    for t in &clicked {
        let id = t.id;
        net.p.call(
            |s| net.conn.reducers.toggle_task_then(id, move |ctx, res| s.done(ctx, res)),
            move |d| { /* as above */ Ok(()) },
        );
    }
}
```

> **Tested with a console client, not inside Bevy.** The test client drives two connections exactly like a game loop
> (`frame_tick` + `flush` every ~5 ms) through a proxy adding 150 ms each way. The Bevy snippet above isn't compiled
> in CI.

## Write it once

Put the reducer bodies in a crate both sides use, written against a small trait instead of `ReducerContext`. The
module implements the trait over its tables; the client implements it over a prediction draft, and runs the *same
function* as the prediction. See [`demo/rust/logic`](../demo/rust/logic/src/lib.rs) (`task_logic`) and
[`demo/rust/module`](../demo/rust/module/src/lib.rs).

```rust
// task_logic (shared)
pub trait Tasks {
    fn sender(&self) -> Identity;
    fn all(&self) -> Vec<Task>;
    fn find(&self, id: u64) -> Option<Task>;
    fn insert(&mut self, task: Task);   // id 0 = assign one
    fn update(&mut self, task: Task);
    fn delete(&mut self, id: u64);
}

pub fn toggle_task(db: &mut impl Tasks, id: u64) -> Result<(), String> {
    let t = db.find(id).ok_or("no such task")?;
    db.update(Task { done: !t.done, ..t });
    Ok(())
}
```

**Module side:** the real tables.

```rust
struct Db<'a>(&'a ReducerContext);

impl Tasks for Db<'_> {
    fn sender(&self) -> Identity { self.0.sender() }
    fn find(&self, id: u64) -> Option<task_logic::Task> { self.0.db.task().id().find(id).map(to_logic) }
    fn update(&mut self, t: task_logic::Task) { self.0.db.task().id().update(from_logic(t)); }
    // all / insert / delete likewise
}

#[spacetimedb::reducer]
pub fn toggle_task(ctx: &ReducerContext, id: u64) -> Result<(), String> {
    task_logic::toggle_task(&mut Db(ctx), id)
}
```

**Client side:** a draft table, and your own identity as the sender.

```rust
struct PredictedTasks<'d> { t: DraftTable<'d, Task>, me: Identity }

impl task_logic::Tasks for PredictedTasks<'_> {
    fn sender(&self) -> Identity { self.me }
    fn find(&self, id: u64) -> Option<task_logic::Task> { self.t.find(&id).map(to_logic) }
    fn insert(&mut self, task: task_logic::Task) { self.t.insert(from_logic(task)); }   // 0 → a temp id
    fn update(&mut self, task: task_logic::Task) { self.t.update(from_logic(task)); }
    // all / delete likewise
}

p.call(
    |s| conn.reducers.toggle_task_then(id, move |ctx, res| s.done(ctx, res)),
    move |d| task_logic::toggle_task(&mut PredictedTasks { t: d.table::<Task>(), me }, id),
);
```

- Rules the logic checks (an empty title, a missing task) refuse **on the client**: the prediction returns `Err`, is
  never shown, and the server refuses the call with the same text.
- Rules that need server-only state (the demo's rate limit lives in a private table) stay in the module's reducer
  around the shared call; the server refuses and the prediction rolls back.
- Deterministic logic predicts exactly: the demo's `move_task` computes the same `f64` position on both sides, so a
  confirmed move changes nothing on screen.
- `spacetimedb_lib::Identity` (what the logic crate uses) is the same type the SDK re-exports, so rows convert field
  for field.

## How it settles without flicker

The Rust SDK runs a reducer's `_then` callback right after it applies the caller's transaction to the cache, inside
the same `frame_tick` (`sdks/rust/src/db_connection.rs`: `apply_update`, then the call's callback). The predictor
drops the prediction in that callback, and `flush` only reports the view after `frame_tick` returns, so the real row
and its prediction are never visible together.

`Replaced` pairing: the insert callbacks see `Event::Reducer` with the reducer's timestamp for rows this client's own
call inserted. When the call is confirmed, those rows are paired, in order, with the temporary ids the prediction
handed out for that table. If the counts differ (the reducer inserted more or fewer rows than predicted), no pairing
is made and you get a plain delete + insert instead.

## Tests

```bash
spacetime start                     # another terminal
scripts/test-rs.sh                  # publishes both demo modules, starts the latency proxy, runs the client
```

```
hand-written predictions, against the TypeScript module (predict-rs):
      predicted visible after 0.41 ms; confirmed after 316 ms
  ok  an insert shows at once with a temporary id, then is Replaced by the real row, never both
  ok  updates show at once and stay put when the server agrees
  ok  several pending inserts keep their order and land in it
  ok  the server says no (a rule the client can't see): the prediction rolls back with the reason
  ok  someone else's change lands under a pending prediction: it's rebased, both show
  ok  a prediction that no longer applies is dropped, and the server refuses it too
  ok  no answer in time: the prediction rolls back as TimedOut
      median: predicted 0.48 ms (one frame), server round trip 314 ms (+150 ms each way)
  ok  click to visible: predicted vs waiting for the server

write it once: task_logic as the prediction, against the Rust module (predict-rs-mod):
  ok  the module's add_task runs as the prediction: shows at once, then Replaced by the real row
  ok  the logic refuses on the client (empty title): never shown; the server refuses with the same text
  ok  a server-only rule (rate limit) still refuses: the prediction rolls back
  ok  someone else's change under a shared-logic prediction: rebased
  ok  move between neighbours: the prediction is exactly the server's pos, so nothing moves on confirm

13 passed
```

The two demo modules have the same schema, so one set of client bindings (generated from either) works against both.

Building needs OpenSSL development headers (the SDK uses `native-tls`): `libssl-dev` on Debian/Ubuntu (GitHub's
ubuntu runners have it), or set `OPENSSL_INCLUDE_DIR` / `OPENSSL_LIB_DIR`.
