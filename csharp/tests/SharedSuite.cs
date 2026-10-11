// "Write it once": the C# module (demo/csharp/module) and this client both run
// demo/csharp/logic/TaskLogic.cs. The module runs it against its tables; the client runs the very
// same functions as its predictions, through PredictedTasks (ITasks over a prediction draft).
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using CsMod.Types;
using SpacetimeDB;
using SpacetimeDB.Predict;
using TaskLogic;
using TaskRow = CsMod.Types.Task;

/// <summary>
/// The client's side of ITasks: a prediction draft. The module's side is the same interface over
/// ctx.Db (see Db in demo/csharp/module/Lib.cs). Reusable as is for any client of that module.
/// </summary>
sealed class PredictedTasks : ITasks
{
    readonly DraftTable<TaskRow, ulong> t;

    public PredictedTasks(DraftTable<TaskRow, ulong> table, Identity sender)
    {
        t = table;
        Sender = sender;
    }

    static TaskData ToLogic(TaskRow r) => new TaskData { Id = r.Id, Owner = r.Owner, Title = r.Title, Done = r.Done, Pos = r.Pos };
    static TaskRow FromLogic(TaskData d) => new TaskRow(d.Id, d.Owner, d.Title, d.Done, d.Pos);

    public Identity Sender { get; }
    public IEnumerable<TaskData> All() => t.Rows().Select(ToLogic).ToList();
    public TaskData? Find(ulong id) => t.Find(id) is TaskRow r ? ToLogic(r) : (TaskData?)null;
    public void Insert(TaskData task) => t.Insert(FromLogic(task)); // Id 0 → a temporary id (AutoInc)
    public void Update(TaskData task)
    {
        if (!t.Update(FromLogic(task))) throw new RefusedException("no such task");
    }
    public void Delete(ulong id) => t.Delete(id);
}

static class SharedSuite
{
    static readonly string Db = Environment.GetEnvironmentVariable("SHARED_DB") ?? "predict-cs-mod";
    static readonly int ProxyPort = int.Parse(Environment.GetEnvironmentVariable("PROXY_PORT") ?? "3110");
    static readonly int Delay = int.Parse(Environment.GetEnvironmentVariable("DELAY") ?? "150");

    static DbConnection a = null!, b = null!;
    static Identity aId;
    static Predictor<Reducer> p = null!;
    static PredictedTable<TaskRow, ulong> tasks = null!;
    static readonly List<(Type, string)> rollbacks = new();

    // The client's reducer calls: send the reducer, predict with the shared logic.
    static ITasks Draft(Draft d) => new PredictedTasks(d.Of(tasks), aId);
    static void Add(string title) => p.Call<Reducer.AddTask>(() => a.Reducers.AddTask(title), d => Logic.AddTask(Draft(d), title));
    static void Toggle(ulong id) => p.Call<Reducer.ToggleTask>(() => a.Reducers.ToggleTask(id), d => Logic.ToggleTask(Draft(d), id));
    static void Rename(ulong id, string title) => p.Call<Reducer.RenameTask>(() => a.Reducers.RenameTask(id, title), d => Logic.RenameTask(Draft(d), id, title));
    static void Move(ulong id, ulong before) => p.Call<Reducer.MoveTask>(() => a.Reducers.MoveTask(id, before), d => Logic.MoveTask(Draft(d), id, before));

    public static int Run(Action<string, Action> test)
    {
        a = Connect(ProxyPort, out aId);
        b = Connect(3000, out _);
        p = new Predictor<Reducer>();
        tasks = p.Track(a.Db.Task, t => t.Id, new AutoInc<TaskRow>(t => t.Id, (t, id) => t.Id = id), ctx => ctx.Event);
        a.Reducers.OnAddTask += (ctx, _) => p.Settle(ctx.Event);
        a.Reducers.OnToggleTask += (ctx, _) => p.Settle(ctx.Event);
        a.Reducers.OnRenameTask += (ctx, _, _) => p.Settle(ctx.Event);
        a.Reducers.OnMoveTask += (ctx, _, _) => p.Settle(ctx.Event);
        p.OnRolledBack += (type, reason) => rollbacks.Add((type, reason));
        var replaced = 0;
        var inserts = 0;
        var deletes = 0;
        tasks.OnReplaced += (_, _) => replaced++;
        tasks.OnInsert += _ => inserts++;
        tasks.OnDelete += _ => deletes++;
        ResetRate();

        test("shared logic: an add shows at once with a temp id, then OnReplaced with the real row", () =>
        {
            Pump(20);
            (replaced, inserts, deletes) = (0, 0, 0);
            var sw = Stopwatch.StartNew();
            Add("  shared  ");
            var predicted = ByTitle("shared"); // the logic trimmed it, on the client too
            var visible = sw.Elapsed.TotalMilliseconds;
            Assert(predicted != null && Prediction.IsTempId(predicted.Id), "visible at once, trimmed, with a temp id");
            Assert(predicted!.Owner == aId && predicted.Pos >= 1, "owner and position from the shared logic");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            var real = ByTitle("shared")!;
            Assert(!Prediction.IsTempId(real.Id) && real.Pos == predicted.Pos, "the real row, at the predicted position");
            Assert(replaced == 1 && inserts == 1 && deletes == 0, $"replaced={replaced} inserts={inserts} deletes={deletes}");
            Console.WriteLine($"      predicted visible after {visible:F2} ms; server confirmed after {sw.Elapsed.TotalMilliseconds:F0} ms");
        });

        test("shared logic refuses on the client (empty title): never shown, and the server refuses with the same text", () =>
        {
            var skipped = p.Stats.Skipped;
            rollbacks.Clear();
            var before = tasks.Rows().Count;
            Add("   ");
            Assert(tasks.Rows().Count == before, "nothing predicted");
            Assert(p.Stats.Skipped > skipped, "the prediction threw (RefusedException) and was skipped");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(rollbacks.Count == 1 && rollbacks[0].Item2.Contains("title is empty"),
                $"the server's refusal, same message: {string.Join("; ", rollbacks.Select(r => r.Item2))}");
            Console.WriteLine($"      server said: {rollbacks[0].Item2.Split('\n')[0]}");
        });

        test("a rule only the server knows (rate limit): the shared prediction shows, then rolls back", () =>
        {
            ResetRate();
            rollbacks.Clear();
            foreach (var i in Enumerable.Range(1, 6)) Add($"burst {i}");
            Assert(ByTitle("burst 6") != null, "predicted like the others");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(ByTitle("burst 6") == null && ByTitle("burst 5") != null, "the sixth rolled back");
            Assert(rollbacks.Count == 1 && rollbacks[0].Item2.Contains("slow down"), "with the server's reason");
            ResetRate();
        });

        test("shared logic rebases: someone else's change under a pending prediction", () =>
        {
            var id = ByTitle("burst 1")!.Id;
            Rename(id, "renamed by a");
            b.Reducers.ToggleTask(id);
            Pump(Delay + 60);
            var mid = tasks.Find(id)!;
            Assert(mid.Title == "renamed by a" && mid.Done, "both show while pending");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(tasks.Find(id)!.Title == "renamed by a" && tasks.Find(id)!.Done, "both, confirmed");
        });

        test("shared logic with more to it (move between neighbours) predicts exactly what the server does", () =>
        {
            var order = () => tasks.Rows().OrderBy(t => t.Pos).Select(t => t.Id).ToList();
            var ids = order();
            var last = ids[^1];
            Move(last, ids[1]); // to just before the second task
            var predicted = order();
            var predictedPos = tasks.Find(last)!.Pos;
            Assert(predicted[1] == last, "moved at once");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(order().SequenceEqual(predicted), "the server's order is the predicted one");
            Assert(tasks.Find(last)!.Pos == predictedPos, $"same position: {tasks.Find(last)!.Pos} vs {predictedPos}");
        });

        a.Disconnect();
        b.Disconnect();
        return 0;
    }

    // --- helpers -------------------------------------------------------------------------------

    static TaskRow? ByTitle(string t) => tasks.Rows().FirstOrDefault(r => r.Title == t);

    static void ResetRate()
    {
        var done = false;
        void Handler(ReducerEventContext ctx) => done = true;
        b.Reducers.OnResetRate += Handler;
        b.Reducers.ResetRate();
        PumpUntil(() => done);
        b.Reducers.OnResetRate -= Handler;
    }

    static void Frame()
    {
        a.FrameTick();
        b.FrameTick();
        p?.Flush();
    }

    static void Pump(int ms)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < ms)
        {
            Frame();
            Thread.Sleep(5);
        }
    }

    static void PumpUntil(Func<bool> cond, int timeoutMs = 5000)
    {
        var sw = Stopwatch.StartNew();
        while (!cond())
        {
            if (sw.ElapsedMilliseconds > timeoutMs) throw new Exception("timed out waiting");
            Frame();
            Thread.Sleep(1);
        }
    }

    static DbConnection Connect(int port, out Identity identity)
    {
        Identity? id = null;
        var applied = false;
        Exception? error = null;
        var conn = DbConnection.Builder()
            .WithUri($"ws://127.0.0.1:{port}")
            .WithDatabaseName(Db)
            .OnConnect((c, i, _) =>
            {
                id = i;
                c.SubscriptionBuilder().OnApplied(_ => applied = true).Subscribe(new[] { "SELECT * FROM task" });
            })
            .OnConnectError(e => error = e)
            .Build();
        var sw = Stopwatch.StartNew();
        while (!applied)
        {
            if (error != null) throw error;
            if (sw.ElapsedMilliseconds > 10000) throw new Exception($"couldn't connect on :{port}");
            conn.FrameTick();
            Thread.Sleep(5);
        }
        identity = id!.Value;
        return conn;
    }

    static void Assert(bool cond, string what)
    {
        if (!cond) throw new Exception($"assertion failed: {what}");
    }
}
