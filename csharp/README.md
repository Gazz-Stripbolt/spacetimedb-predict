# spacetimedb-predict for C# (Unity, Godot, .NET)

Optimistic updates / client-side prediction for SpacetimeDB's C# client SDK: the C# twin of
[`client/`](../client). Call a reducer and the predicted rows change at once. Pending predictions are re-run on top
of every server change (rebase), dropped when the reducer's result arrives, and rolled back if the server says no.
Source: [`SpacetimeDB.Predict/Predictor.cs`](SpacetimeDB.Predict/Predictor.cs), one file, `netstandard2.1`.

> [!NOTE]
> Tested with a .NET 8 console client (frame loop: `FrameTick`, then `Flush`) against a real module through a
> latency proxy: [`tests/`](tests), 14/14. **Not tested inside Unity or Godot** yet; it uses
> nothing beyond the SDK and the base class library, and no threads, async or reflection.

## Install

- **Unity:** copy `SpacetimeDB.Predict/Predictor.cs` into your project, next to the SpacetimeDB SDK package.
- **Godot / .NET:** reference `SpacetimeDB.Predict/SpacetimeDB.Predict.csproj`, or copy the file.

## Use it

```csharp
using SpacetimeDB.Predict;
using SpacetimeDB.Types;

var p = new Predictor<Reducer>();
var tasks = p.Track(conn.Db.Task, t => t.Id,
    new AutoInc<Task>(t => t.Id, (t, id) => t.Id = id),   // predicted inserts get temporary ids
    ctx => ctx.Event);                                     // optional: enables OnReplaced

// One line per predicted reducer: its result settles the oldest pending call of that reducer.
conn.Reducers.OnToggleTask += (ctx, _) => p.Settle(ctx.Event);

// Call with a prediction. Shows at once; Flush raises the events.
p.Call<Reducer.ToggleTask>(() => conn.Reducers.ToggleTask(id), d =>
{
    var t = d.Of(tasks);
    var row = t.Find(id) ?? throw new Exception("no such task");   // throwing = this prediction doesn't apply
    t.Update(new Task(row.Id, row.Owner, row.Title, !row.Done, row.Pos));
});
```

**Unity:** drive it from one `MonoBehaviour`, and spawn from the *predicted* rows:

```csharp
void Start()
{
    tasks.OnInsert += row => objects[row.Id] = Spawn(row);
    tasks.OnUpdate += (old, row) => Refresh(objects[row.Id], row);
    tasks.OnDelete += row => { Destroy(objects[row.Id]); objects.Remove(row.Id); };
    // A predicted insert was confirmed: keep the GameObject, give it the real id.
    tasks.OnReplaced += (temp, real) => { objects[real.Id] = objects[temp.Id]; objects.Remove(temp.Id); };
    p.OnRolledBack += (reducer, reason) => ShowToast(reason);
}

void Update()
{
    conn.FrameTick();   // the SDK applies server messages here
    p.Flush();          // then predictions are rebuilt and the events above are raised
}
```

**Godot:** the same, from `_Process(double delta)`.

## How it behaves

- **Settling is first in, first out per reducer.** The generated C# API has no per-call callback, but reducer events
  (`OnToggleTask`, ...) only fire for this client's own calls, in call order, so the oldest pending call of that
  reducer is the one that came back. Only wire `Settle` for reducers you call through `p.Call`. A reducer you also
  call directly (without a prediction) would settle a predicted call early.
- **No flicker on confirmation.** Inside `FrameTick` the SDK applies the caller's transaction to the cache, and only
  then raises the reducer event that settles the prediction. `Flush` runs after `FrameTick`, so the events never
  show a predicted row next to its real one.
- **`OnReplaced(temp, real)`** instead of `OnDelete(temp)` + `OnInsert(real)` when a predicted insert is confirmed.
  The real rows are the ones the confirming transaction inserted into that table, matched in insert order. Pass
  `eventOf: ctx => ctx.Event` to `Track` to enable it.
- **Rollback:** a `Failed` / `OutOfEnergy` result, or no result within `DefaultTimeout` (10 s; per call with
  `timeout:`), removes the prediction and raises `OnRolledBack(reducerType, reason)` on the next `Flush`. A timed-out
  call stays queued as a ghost for one more timeout, so its late result settles it and not the next call. After a
  disconnect, call `Clear()`, and `Resync()` once resubscribed.
- **Reading:** `tasks.Rows()`, `Find(key)`, `IsPending(key)`, `IsPredicted(key)`, `Prediction.IsTempId(id)`,
  `p.PendingCount`, `p.Stats`. Reads are always current (they rebuild if needed); events come from `Flush`.
- **Rows are compared by BSATN bytes** for `OnUpdate`. Predictions build new row objects on every re-run, and the
  generated `Equals(object)` on 2.11 types can recurse forever.
- **Not thread-safe:** use it on the thread that calls `FrameTick`.

## Write it once

A C# module and a C# client (Unity, Godot) can share the reducer logic: put it in a plain C# file both compile, have
it work against a small interface, and implement that interface twice, over `ctx.Db` on the server and over the
prediction draft on the client. The demo does exactly this:
[`demo/csharp/logic/TaskLogic.cs`](../demo/csharp/logic/TaskLogic.cs) is compiled into the module
([`demo/csharp/module`](../demo/csharp/module)) *and* into the test client.

```csharp
// TaskLogic.cs, shared: plain C# (netstandard2.1-friendly), no SpacetimeDB server or client types beyond Identity.
public interface ITasks
{
    Identity Sender { get; }
    IEnumerable<TaskData> All();
    TaskData? Find(ulong id);
    void Insert(TaskData task);   // Id 0 = assign one (auto-inc on the server, a temporary id in a prediction)
    void Update(TaskData task);
    void Delete(ulong id);
}

public static void ToggleTask(ITasks db, ulong id)
{
    var t = db.Find(id) ?? throw new RefusedException("no such task");
    t.Done = !t.Done;
    db.Update(t);
}
```

**Module side**: `ITasks` over the real tables, and each reducer is one line:

```csharp
sealed class Db : ITasks
{
    readonly ReducerContext ctx;
    public Db(ReducerContext ctx) => this.ctx = ctx;
    public Identity Sender => ctx.Sender;
    public IEnumerable<TaskData> All() => ctx.Db.Task.Iter().Select(ToLogic).ToList();
    public TaskData? Find(ulong id) => ctx.Db.Task.Id.Find(id) is Task t ? ToLogic(t) : null;
    public void Insert(TaskData task) => ctx.Db.Task.Insert(FromLogic(task));
    public void Update(TaskData task) => ctx.Db.Task.Id.Update(FromLogic(task));
    public void Delete(ulong id) => ctx.Db.Task.Id.Delete(id);
}

[SpacetimeDB.Reducer]
public static void ToggleTask(ReducerContext ctx, ulong id) => Logic.ToggleTask(new Db(ctx), id);
```

**Client side**: `ITasks` over the draft ([`tests/SharedSuite.cs`](tests/SharedSuite.cs), `PredictedTasks`), and
the prediction *is* the module's code:

```csharp
sealed class PredictedTasks : ITasks
{
    readonly DraftTable<Task, ulong> t;
    public PredictedTasks(DraftTable<Task, ulong> table, Identity sender) { t = table; Sender = sender; }
    public Identity Sender { get; }
    public IEnumerable<TaskData> All() => t.Rows().Select(ToLogic).ToList();
    public TaskData? Find(ulong id) => t.Find(id) is Task r ? ToLogic(r) : (TaskData?)null;
    public void Insert(TaskData task) => t.Insert(FromLogic(task));   // Id 0 → temporary id (AutoInc)
    public void Update(TaskData task) { if (!t.Update(FromLogic(task))) throw new RefusedException("no such task"); }
    public void Delete(ulong id) => t.Delete(id);
}

p.Call<Reducer.ToggleTask>(() => conn.Reducers.ToggleTask(id),
    d => Logic.ToggleTask(new PredictedTasks(d.Of(tasks), myIdentity), id));
```

What that buys you, all covered by the tests:

- The prediction does what the server will do, down to computed values (the moved task's new position matches the
  server's to the bit).
- A refusal the logic can see (an empty title) throws in the prediction, so nothing is shown, and the server refuses
  with the same message (`title is empty`).
- Rules only the module knows (here a private rate-limit table, checked in the module's `AddTask` before it calls
  the shared logic) still roll the prediction back.

Keep server-only checks (private tables, `ctx.Rng`, timestamps) in the module's wrapper rather than in the shared
file. The client can't know them, and the rollback covers them.

## Tests

```bash
spacetime start                 # another terminal
npm install && npm run build:web
scripts/test-cs.sh              # publishes demo/module (predict-cs) and demo/csharp/module (predict-cs-mod),
                                # starts a +150 ms proxy, runs csharp/tests; MODE=ts|shared|all
```

Two suites, 14 tests:

- **Hand-written predictions** against the TypeScript module (9): predicted inserts with temporary ids,
  `OnReplaced`, updates, ordered pending inserts, a refusal (a private rate limit), someone else's change under a
  pending prediction, a prediction that stops applying, timeouts, and click-to-visible: about 0.2 ms predicted
  against about 307 ms waiting for the server.
- **Write it once** against the C# module (5): the shared add with a temp id then `OnReplaced`, the logic refusing
  on the client with the server's identical message, a server-only refusal, a rebase, and a move whose predicted
  position matches the server's exactly.
