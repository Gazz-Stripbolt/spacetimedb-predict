// The demo's reducer logic, written once. The C# module runs it against its tables; a C# client
// (a Unity or Godot game, say) runs the same code against a prediction draft. Both sides implement
// ITasks over their own row type, converting to and from TaskData.
//
// Mirrors demo/module/src/logic.ts and demo/rust/logic: same rules, same refusal messages.
// Plain C# (no records, no newer APIs), so it compiles for netstandard2.1 / Unity as well.

#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;

namespace TaskLogic
{
    /// <summary>A task as the logic sees it.</summary>
    public struct TaskData
    {
        public ulong Id;
        public SpacetimeDB.Identity Owner;
        public string Title;
        public bool Done;
        /// <summary>Sort position: new tasks go last; moves pick a value between neighbours.</summary>
        public double Pos;
    }

    /// <summary>The part of the database the logic touches.</summary>
    public interface ITasks
    {
        SpacetimeDB.Identity Sender { get; }
        IEnumerable<TaskData> All();
        TaskData? Find(ulong id);
        /// <summary>Insert; an Id of 0 means "assign one" (auto-inc on the server, a temp id in a prediction).</summary>
        void Insert(TaskData task);
        void Update(TaskData task);
        void Delete(ulong id);
    }

    /// <summary>A refusal the client can show. The module turns it into a reducer error.</summary>
    public class RefusedException : Exception
    {
        public RefusedException(string message) : base(message) { }
    }

    public static class Logic
    {
        public const int MaxTitle = 80;

        static string Title(string t)
        {
            var s = t.Trim();
            if (s.Length == 0) throw new RefusedException("title is empty");
            if (s.Length > MaxTitle) throw new RefusedException("title too long");
            return s;
        }

        static TaskData Task(ITasks db, ulong id) =>
            db.Find(id) ?? throw new RefusedException("no such task");

        public static void AddTask(ITasks db, string title)
        {
            var last = db.All().Select(t => t.Pos).DefaultIfEmpty(0).Max();
            db.Insert(new TaskData { Id = 0, Owner = db.Sender, Title = Title(title), Done = false, Pos = Math.Max(last, 0) + 1 });
        }

        public static void ToggleTask(ITasks db, ulong id)
        {
            var t = Task(db, id);
            t.Done = !t.Done;
            db.Update(t);
        }

        public static void RenameTask(ITasks db, ulong id, string title)
        {
            var t = Task(db, id);
            t.Title = Title(title);
            db.Update(t);
        }

        public static void DeleteTask(ITasks db, ulong id)
        {
            Task(db, id);
            db.Delete(id);
        }

        /// <summary>Move a task to just before <paramref name="before"/> (or to the end, with 0).</summary>
        public static void MoveTask(ITasks db, ulong id, ulong before)
        {
            var t = Task(db, id);
            var sorted = db.All().Where(x => x.Id != t.Id).OrderBy(x => x.Pos).ToList();
            double pos;
            if (before == 0)
            {
                pos = (sorted.Count > 0 ? sorted[sorted.Count - 1].Pos : 0) + 1;
            }
            else
            {
                var i = sorted.FindIndex(x => x.Id == before);
                if (i < 0) throw new RefusedException("no such task");
                pos = i == 0 ? sorted[0].Pos - 1 : (sorted[i - 1].Pos + sorted[i].Pos) / 2;
            }
            t.Pos = pos;
            db.Update(t);
        }
    }
}
