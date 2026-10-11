// The demo module in C#: the same task list as demo/module (TypeScript), with the reducer logic in
// ../logic/TaskLogic.cs, which C# clients also run as their prediction.

using System.Collections.Generic;
using System.Linq;
using SpacetimeDB;
using TaskLogic;

public static partial class Module
{
    [SpacetimeDB.Table(Accessor = "Task", Public = true)]
    public partial struct Task
    {
        [SpacetimeDB.PrimaryKey, SpacetimeDB.AutoInc]
        public ulong Id;
        public Identity Owner;
        public string Title;
        public bool Done;
        public double Pos;
    }

    /// <summary>
    /// Server-only state the client can't see: how many tasks each person added recently. At most
    /// 5 adds per 10 seconds, a rule a prediction can't know about.
    /// </summary>
    [SpacetimeDB.Table(Accessor = "Rate")]
    public partial struct Rate
    {
        [SpacetimeDB.PrimaryKey]
        public Identity Owner;
        public Timestamp WindowStart;
        public uint Count;
    }

    const long RateWindowMicros = 10_000_000;
    const uint RateMax = 5;

    /// <summary>The module's side of ITasks: the real tables.</summary>
    sealed class Db : ITasks
    {
        readonly ReducerContext ctx;
        public Db(ReducerContext ctx) => this.ctx = ctx;

        static TaskData ToLogic(Task t) => new() { Id = t.Id, Owner = t.Owner, Title = t.Title, Done = t.Done, Pos = t.Pos };
        static Task FromLogic(TaskData t) => new() { Id = t.Id, Owner = t.Owner, Title = t.Title, Done = t.Done, Pos = t.Pos };

        public Identity Sender => ctx.Sender;
        public IEnumerable<TaskData> All() => ctx.Db.Task.Iter().Select(ToLogic).ToList();
        public TaskData? Find(ulong id) => ctx.Db.Task.Id.Find(id) is Task t ? ToLogic(t) : null;
        public void Insert(TaskData task) => ctx.Db.Task.Insert(FromLogic(task));
        public void Update(TaskData task) => ctx.Db.Task.Id.Update(FromLogic(task));
        public void Delete(ulong id) => ctx.Db.Task.Id.Delete(id);
    }

    [SpacetimeDB.Reducer]
    public static void AddTask(ReducerContext ctx, string title)
    {
        var now = ctx.Timestamp.MicrosecondsSinceUnixEpoch;
        if (ctx.Db.Rate.Owner.Find(ctx.Sender) is Rate r)
        {
            if (now - r.WindowStart.MicrosecondsSinceUnixEpoch > RateWindowMicros)
            {
                ctx.Db.Rate.Owner.Update(r with { WindowStart = ctx.Timestamp, Count = 1 });
            }
            else if (r.Count >= RateMax)
            {
                throw new System.Exception("slow down: at most 5 new tasks per 10 seconds");
            }
            else
            {
                ctx.Db.Rate.Owner.Update(r with { Count = r.Count + 1 });
            }
        }
        else
        {
            ctx.Db.Rate.Insert(new Rate { Owner = ctx.Sender, WindowStart = ctx.Timestamp, Count = 1 });
        }
        Logic.AddTask(new Db(ctx), title);
    }

    [SpacetimeDB.Reducer]
    public static void ToggleTask(ReducerContext ctx, ulong id) => Logic.ToggleTask(new Db(ctx), id);

    [SpacetimeDB.Reducer]
    public static void RenameTask(ReducerContext ctx, ulong id, string title) => Logic.RenameTask(new Db(ctx), id, title);

    [SpacetimeDB.Reducer]
    public static void DeleteTask(ReducerContext ctx, ulong id) => Logic.DeleteTask(new Db(ctx), id);

    [SpacetimeDB.Reducer]
    public static void MoveTask(ReducerContext ctx, ulong id, ulong before) => Logic.MoveTask(new Db(ctx), id, before);

    /// <summary>Test helper: forget everyone's rate limits.</summary>
    [SpacetimeDB.Reducer]
    public static void ResetRate(ReducerContext ctx)
    {
        foreach (var r in ctx.Db.Rate.Iter().ToList()) ctx.Db.Rate.Owner.Delete(r.Owner);
    }
}
