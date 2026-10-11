// Prediction behaviour in C#, against the demo module, through the latency proxy.
//
//   scripts/test-cs.sh      (publishes demo/module as predict-cs and starts the proxy)
//
// "a" talks through the proxy (+DELAY ms each way) and predicts; "b" talks to the server directly
// and plays someone else changing things underneath a's predictions. A frame loop drives both:
// FrameTick, then Flush, like a game's Update.
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using SpacetimeDB;
using SpacetimeDB.Predict;
using SpacetimeDB.Types;
using TaskRow = SpacetimeDB.Types.Task;

static class Program
{
    static readonly string Db = Environment.GetEnvironmentVariable("DB") ?? "predict-cs";
    static readonly int ProxyPort = int.Parse(Environment.GetEnvironmentVariable("PROXY_PORT") ?? "3110");
    static readonly int Delay = int.Parse(Environment.GetEnvironmentVariable("DELAY") ?? "150");

    static DbConnection a = null!, b = null!;
    static Identity aId;
    static Predictor<Reducer> p = null!;
    static PredictedTable<TaskRow, ulong> tasks = null!;
    static readonly List<string[]> flushed = new();
    static readonly List<(Type, string)> rollbacks = new();
    static int passed;

    static int Main()
    {
        // MODE: ts (hand-written predictions against the TypeScript module), shared (the C# module's
        // own logic as the prediction, see SharedSuite.cs), or all.
        var mode = Environment.GetEnvironmentVariable("MODE") ?? "all";
        if (mode is "ts" or "all")
        {
            Console.WriteLine($"-- hand-written predictions, TypeScript module ({Db})");
            RunTsSuite();
        }
        if (mode is "shared" or "all")
        {
            Console.WriteLine($"-- write it once: TaskLogic.cs on both sides, C# module");
            SharedSuite.Run(Test);
        }
        Console.WriteLine($"\n{passed} passed");
        return 0;
    }

    static void RunTsSuite()
    {
        a = Connect(ProxyPort, out aId);
        b = Connect(3000, out _);

        p = new Predictor<Reducer>();
        tasks = p.Track(a.Db.Task, t => t.Id, new AutoInc<TaskRow>(t => t.Id, (t, id) => t.Id = id), ctx => ctx.Event);
        // One line per predicted reducer: its result settles the oldest pending call.
        a.Reducers.OnAddTask += (ctx, _) => p.Settle(ctx.Event);
        a.Reducers.OnToggleTask += (ctx, _) => p.Settle(ctx.Event);
        a.Reducers.OnRenameTask += (ctx, _, _) => p.Settle(ctx.Event);
        p.OnChanged += () => flushed.Add(tasks.Rows().Select(t => t.Title).ToArray());
        p.OnRolledBack += (type, reason) => rollbacks.Add((type, reason));
        var replaced = 0;
        var inserts = 0;
        var deletes = 0;
        tasks.OnReplaced += (_, _) => replaced++;
        tasks.OnInsert += _ => inserts++;
        tasks.OnDelete += _ => deletes++;

        ResetRate();

        Test("an insert shows at once with a temporary id, then becomes the real row; never both in a flushed view", () =>
        {
            flushed.Clear();
            var sw = Stopwatch.StartNew();
            Add("alpha");
            var predicted = ByTitle("alpha");
            var visible = sw.Elapsed.TotalMilliseconds;
            Assert(predicted != null && Prediction.IsTempId(predicted.Id), "visible at once, with a temp id");
            Assert(tasks.IsPredicted(predicted!.Id) && tasks.IsPending(predicted.Id), "marked as predicted and pending");
            Assert(a.Db.Task.Iter().All(t => t.Title != "alpha"), "not in the SDK cache yet");
            PumpUntil(() => p.PendingCount == 0);
            var confirmed = sw.Elapsed.TotalMilliseconds;
            Pump(30);
            var real = ByTitle("alpha")!;
            Assert(!Prediction.IsTempId(real.Id) && !tasks.IsPending(real.Id), "the real row, settled");
            Assert(tasks.Rows().Count(t => t.Title == "alpha") == 1, "exactly one");
            Assert(flushed.All(f => f.Count(t => t == "alpha") <= 1), "no flushed view had both rows");
            Console.WriteLine($"      predicted visible after {visible:F2} ms; server confirmed after {confirmed:F0} ms");
        });

        Test("a confirmed insert raises OnReplaced (temp → real), not a delete and an insert", () =>
        {
            Pump(20);
            (replaced, inserts, deletes) = (0, 0, 0);
            Add("beta");
            Pump(10);
            Assert(inserts == 1 && replaced == 0, $"the prediction appeared: inserts={inserts}");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(replaced == 1, $"replaced={replaced}");
            Assert(inserts == 1 && deletes == 0, $"no churn: inserts={inserts} deletes={deletes}");
        });

        Test("updates show at once and stay put when the server agrees", () =>
        {
            var id = ByTitle("alpha")!.Id;
            flushed.Clear();
            var doneStates = new List<bool>();
            Action watch = () => doneStates.Add(tasks.Find(id)!.Done);
            p.OnChanged += watch;
            Toggle(id);
            Assert(tasks.Find(id)!.Done, "predicted");
            Assert(!a.Db.Task.Id.Find(id)!.Done, "server copy unchanged so far");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            p.OnChanged -= watch;
            Assert(tasks.Find(id)!.Done, "still done after the confirm");
            Assert(doneStates.All(d => d), $"never flipped back on the way: {string.Join(",", doneStates)}");
        });

        Test("several pending inserts keep their order and land in it", () =>
        {
            foreach (var t in new[] { "one", "two", "three" }) Add(t);
            Assert(p.PendingCount == 3, "three pending");
            Assert(Titles().TakeLast(3).SequenceEqual(new[] { "one", "two", "three" }), "in order while predicted");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(Titles().TakeLast(3).SequenceEqual(new[] { "one", "two", "three" }), "in order once real");
            Assert(tasks.Rows().All(t => !Prediction.IsTempId(t.Id)), "no temp ids left");
        });

        Test("the server says no (a rule the client can't see): the prediction rolls back with the reason", () =>
        {
            ResetRate();
            rollbacks.Clear();
            var before = p.Stats.RolledBack;
            foreach (var i in Enumerable.Range(1, 6)) Add($"r{i}");
            Assert(ByTitle("r6") != null, "predicted like the others");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(ByTitle("r6") == null, "rolled back");
            Assert(ByTitle("r5") != null, "the first five stay");
            Assert(p.Stats.RolledBack == before + 1, "one rollback");
            Assert(rollbacks.Count == 1 && rollbacks[0].Item1 == typeof(Reducer.AddTask) && rollbacks[0].Item2.Contains("slow down"),
                $"OnRolledBack with the server's reason: {string.Join("; ", rollbacks.Select(r => r.Item2))}");
            ResetRate();
        });

        Test("someone else's change lands under a pending prediction: it's rebased, both show", () =>
        {
            var id = ByTitle("one")!.Id;
            var rebases = p.Stats.Rebases;
            Rename(id, "one (renamed by a)");
            b.Reducers.ToggleTask(id); // reaches the server before a's rename does
            Pump(Delay + 60);
            var mid = tasks.Find(id)!;
            Assert(mid.Title == "one (renamed by a)", "still predicted");
            Assert(mid.Done, "b's toggle shows through");
            Assert(p.Stats.Rebases > rebases, "rebased");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(tasks.Find(id)!.Title == "one (renamed by a)" && tasks.Find(id)!.Done, "both, confirmed");
        });

        Test("a prediction that no longer applies is dropped, and the server refuses it too", () =>
        {
            var id = ByTitle("two")!.Id;
            var skipped = p.Stats.Skipped;
            rollbacks.Clear();
            Rename(id, "two (too late)");
            Assert(tasks.Find(id)!.Title == "two (too late)", "predicted");
            b.Reducers.DeleteTask(id);
            Pump(Delay + 60);
            Assert(tasks.Find(id) == null, "gone: the rename has nothing to apply to");
            Assert(p.Stats.Skipped > skipped, "skipped on re-run");
            PumpUntil(() => p.PendingCount == 0);
            Pump(30);
            Assert(rollbacks.Any(r => r.Item2.Contains("no such task")), "the server refused it");
        });

        Test("click to visible: predicted vs waiting for the server", () =>
        {
            var pred = new List<double>();
            var server = new List<double>();
            var id = ByTitle("three")!.Id;
            for (var i = 0; i < 5; i++)
            {
                var sw = Stopwatch.StartNew();
                var want = !tasks.Find(id)!.Done;
                Toggle(id);
                Assert(tasks.Find(id)!.Done == want, "visible at once");
                pred.Add(sw.Elapsed.TotalMilliseconds);
                PumpUntil(() => p.PendingCount == 0);
                Pump(20);
                want = !a.Db.Task.Id.Find(id)!.Done;
                sw.Restart();
                a.Reducers.ToggleTask(id);
                PumpUntil(() => a.Db.Task.Id.Find(id)!.Done == want);
                server.Add(sw.Elapsed.TotalMilliseconds);
                Pump(20);
            }
            double Med(List<double> xs) => xs.OrderBy(x => x).ElementAt(xs.Count / 2);
            Console.WriteLine($"      median: predicted {Med(pred):F2} ms, server round trip {Med(server):F0} ms (+{Delay} ms each way)");
            Assert(Med(pred) < 5 && Med(server) >= 2 * Delay, "much faster");
        });

        Test("no answer in time: the prediction rolls back", () =>
        {
            rollbacks.Clear();
            var before = tasks.Rows().Count;
            // MoveTask is never settled in these tests, so the ghost it leaves can't steal a result.
            p.Call<Reducer.MoveTask>(() => { }, d => d.Of(tasks).Insert(new TaskRow(0, aId, "lost", false, 999)), TimeSpan.FromMilliseconds(200));
            Assert(tasks.Rows().Count == before + 1, "predicted");
            Pump(350);
            Assert(tasks.Rows().Count == before, "rolled back");
            Assert(rollbacks.Any(r => r.Item1 == typeof(Reducer.MoveTask) && r.Item2.Contains("in time")), "OnRolledBack says why");
            Assert(p.Stats.TimedOut == 1, "counted");
        });

        Console.WriteLine($"      stats: {p.Stats}");
        a.Disconnect();
        b.Disconnect();
    }

    // --- helpers -------------------------------------------------------------------------------

    static void Add(string title) =>
        p.Call<Reducer.AddTask>(() => a.Reducers.AddTask(title), d =>
        {
            var t = d.Of(tasks);
            var s = title.Trim();
            if (s.Length == 0) throw new Exception("title is empty");
            var last = t.Rows().Select(r => r.Pos).DefaultIfEmpty(0).Max();
            t.Insert(new TaskRow(0, aId, s, false, last + 1));
        });

    static void Toggle(ulong id) =>
        p.Call<Reducer.ToggleTask>(() => a.Reducers.ToggleTask(id), d =>
        {
            var t = d.Of(tasks);
            var row = t.Find(id) ?? throw new Exception("no such task");
            t.Update(new TaskRow(row.Id, row.Owner, row.Title, !row.Done, row.Pos));
        });

    static void Rename(ulong id, string title) =>
        p.Call<Reducer.RenameTask>(() => a.Reducers.RenameTask(id, title), d =>
        {
            var t = d.Of(tasks);
            var row = t.Find(id) ?? throw new Exception("no such task");
            t.Update(new TaskRow(row.Id, row.Owner, title.Trim(), row.Done, row.Pos));
        });

    static void ResetRate()
    {
        var done = false;
        void Handler(ReducerEventContext ctx) => done = true;
        b.Reducers.OnResetRate += Handler;
        b.Reducers.ResetRate();
        PumpUntil(() => done);
        b.Reducers.OnResetRate -= Handler;
    }

    static TaskRow? ByTitle(string t) => tasks.Rows().FirstOrDefault(r => r.Title == t);
    static string[] Titles() => tasks.Rows().OrderBy(r => r.Pos).Select(r => r.Title).ToArray();

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

    static void Test(string name, Action body)
    {
        body();
        passed++;
        Console.WriteLine($"  ok  {name}");
    }

    static void Assert(bool cond, string what)
    {
        if (!cond) throw new Exception($"assertion failed: {what}");
    }
}
