// spacetimedb-predict for C#: optimistic updates / client-side prediction for SpacetimeDB clients
// (Unity, Godot, .NET). The C# twin of client/src/index.ts.
//
// The rows a game or UI shows are the SDK's client cache (what the server has confirmed) plus the
// predicted effects of this client's reducer calls that haven't come back yet. Predictions are
// closures, not recorded diffs: whenever the server's rows change, every pending prediction is
// re-run, in call order, on top of the new server state ("rebase"). A prediction is dropped when
// its reducer result arrives: on success the server's own rows are in the cache by then (the SDK
// applies the caller's transaction before it raises the reducer event), on failure the
// prediction simply disappears, which is the rollback.
//
// Unity has no microtasks, so nothing is pushed while the SDK is mid-update: call Flush() once per
// frame, right after conn.FrameTick(). Flush rebuilds the predicted view if anything changed and
// raises row events (OnInsert / OnUpdate / OnDelete / OnReplaced) on it, so you can spawn and
// despawn GameObjects from the predicted rows.
//
// Not thread-safe: use it on the thread that calls FrameTick (Unity's main thread).
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using SpacetimeDB.BSATN;

namespace SpacetimeDB.Predict
{
    /// <summary>Counters, for debugging and demos.</summary>
    public sealed class PredictorStats
    {
        public int Calls;
        public int Confirmed;
        public int RolledBack;
        public int TimedOut;
        /// <summary>Server changes that arrived while predictions were pending (each re-runs them).</summary>
        public int Rebases;
        /// <summary>Predictions that threw while being re-run on newer server rows (and were skipped).</summary>
        public int Skipped;

        public override string ToString() =>
            $"calls={Calls} confirmed={Confirmed} rolledBack={RolledBack} timedOut={TimedOut} rebases={Rebases} skipped={Skipped}";
    }

    /// <summary>
    /// The auto-increment primary key of a table: how to read it and how to set it on a row. A
    /// predicted insert with 0 there gets a temporary id, counting down from <c>ulong.MaxValue</c>.
    /// </summary>
    public sealed class AutoInc<TRow>
    {
        public readonly Func<TRow, ulong> Get;
        public readonly Action<TRow, ulong> Set;

        public AutoInc(Func<TRow, ulong> get, Action<TRow, ulong> set)
        {
            Get = get;
            Set = set;
        }
    }

    /// <summary>Helpers that don't depend on a predictor.</summary>
    public static class Prediction
    {
        /// <summary>Whether an auto-inc id is a temporary one handed out by a prediction.</summary>
        public static bool IsTempId(ulong id) => id > ulong.MaxValue - (1UL << 32);
    }

    /// <summary>
    /// Layers predictions over a connection's client cache. <typeparamref name="TReducer"/> is your
    /// bindings' <c>Reducer</c> type (the generated <c>SpacetimeDB.Types.Reducer</c>).
    /// </summary>
    public sealed class Predictor<TReducer> where TReducer : class
    {
        public PredictorStats Stats { get; } = new PredictorStats();

        /// <summary>How long a call may wait for its result before it's rolled back. Default 10 s.</summary>
        public TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

        /// <summary>After Flush has raised the row events, if anything changed.</summary>
        public event Action? OnChanged;

        /// <summary>A call was refused (the server's reason), ran out of energy, or timed out. Raised by Flush.</summary>
        public event Action<Type, string>? OnRolledBack;

        internal readonly List<ITracked> tables = new List<ITracked>();
        internal readonly List<Pending> queue = new List<Pending>();
        internal ulong nextTemp = ulong.MaxValue;
        private readonly Stopwatch clock = Stopwatch.StartNew();
        private readonly List<(Type, string)> rolledBack = new List<(Type, string)>();
        private bool dirty = true;
        private bool changedSinceFlush = true;

        /// <summary>Calls still waiting for their result (timed-out ones excluded).</summary>
        public int PendingCount
        {
            get
            {
                var n = 0;
                foreach (var p in queue) if (!p.Ghost) n++;
                return n;
            }
        }

        /// <summary>
        /// Track one table. <paramref name="key"/> is its primary key. <paramref name="autoInc"/>, for an
        /// auto-increment key, lets predicted inserts get temporary ids. <paramref name="eventOf"/>
        /// (<c>ctx => ctx.Event</c>) lets Flush raise OnReplaced (temporary row → real row) instead of
        /// a delete and an insert when an insert is confirmed.
        /// </summary>
        public PredictedTable<TRow, TKey> Track<TCtx, TRow, TKey>(
            RemoteTableHandle<TCtx, TRow> handle,
            Func<TRow, TKey> key,
            AutoInc<TRow>? autoInc = null,
            Func<TCtx, Event<TReducer>>? eventOf = null)
            where TCtx : class, IEventContext
            where TRow : class, IStructuralReadWrite, new()
            where TKey : notnull
        {
            var t = new PredictedTable<TRow, TKey>(Rebuild, () => nextTemp--, handle.Iter, key, autoInc);
            handle.OnInsert += (ctx, row) => { t.ServerInsert(row, OwnReducerEvent(eventOf, ctx)); ServerChanged(); };
            handle.OnDelete += (ctx, row) => { t.ServerDelete(row); ServerChanged(); };
            handle.OnUpdate += (ctx, oldRow, newRow) => { t.ServerUpdate(oldRow, newRow); ServerChanged(); };
            tables.Add(t);
            dirty = true;
            return t;
        }

        private static object? OwnReducerEvent<TCtx>(Func<TCtx, Event<TReducer>>? eventOf, TCtx ctx) =>
            eventOf?.Invoke(ctx) is Event<TReducer>.Reducer r ? r.ReducerEvent : null;

        private void ServerChanged()
        {
            if (PendingCount > 0) Stats.Rebases++;
            dirty = true;
            changedSinceFlush = true;
        }

        /// <summary>
        /// Call a reducer with a prediction of its effect. <typeparamref name="TArgs"/> is the reducer's
        /// generated args type (e.g. <c>Reducer.ToggleTask</c>): its result settles the oldest pending
        /// call of that type. The prediction shows at once (reads and the next Flush see it) and is
        /// re-run on top of every server change until the result arrives. Remember to wire the
        /// reducer's event to <see cref="Settle"/>.
        /// </summary>
        public void Call<TArgs>(Action send, Action<Draft> prediction, TimeSpan? timeout = null) where TArgs : TReducer =>
            Call(typeof(TArgs), send, prediction, timeout);

        /// <summary>The same as <see cref="Call{TArgs}"/>, with the reducer's args type as a value.</summary>
        public void Call(Type reducer, Action send, Action<Draft> prediction, TimeSpan? timeout = null)
        {
            var t = timeout ?? DefaultTimeout;
            var p = new Pending(reducer, prediction, clock.Elapsed + t, t);
            queue.Add(p);
            Stats.Calls++;
            dirty = true;
            changedSinceFlush = true;
            try
            {
                send();
            }
            catch
            {
                queue.Remove(p);
                Stats.Calls--;
                throw;
            }
        }

        /// <summary>
        /// Settle the oldest pending call of this reducer with its result. Call it from the reducer's
        /// event, once per predicted reducer:
        /// <c>conn.Reducers.OnToggleTask += (ctx, _) => predictor.Settle(ctx.Event);</c>
        /// Reducer events only fire for this client's own calls, and in call order.
        /// </summary>
        public void Settle(ReducerEvent<TReducer> ev)
        {
            var type = ev.Reducer.GetType();
            var i = queue.FindIndex(p => p.Reducer == type);
            if (i < 0) return; // not a predicted call
            var p = queue[i];
            queue.RemoveAt(i);
            dirty = true;
            changedSinceFlush = true;
            if (p.Ghost) return; // already rolled back by its timeout
            switch (ev.Status)
            {
                case Status.Committed:
                    Stats.Confirmed++;
                    foreach (var t in tables) t.Confirmed(p, ev);
                    break;
                case Status.Failed(var reason):
                    Stats.RolledBack++;
                    rolledBack.Add((type, reason));
                    break;
                case Status.OutOfEnergy:
                    Stats.RolledBack++;
                    rolledBack.Add((type, "out of energy"));
                    break;
                default:
                    Stats.RolledBack++;
                    rolledBack.Add((type, "unknown result"));
                    break;
            }
        }

        /// <summary>
        /// Call once per frame, right after <c>conn.FrameTick()</c>: rolls back timed-out calls,
        /// rebuilds the predicted view if anything changed, and raises the tables' row events, then
        /// OnRolledBack and OnChanged.
        /// </summary>
        public void Flush()
        {
            var now = clock.Elapsed;
            // A timed-out call stays queued as a ghost, so a late result settles it and not the next
            // call of that reducer. If no result comes within another timeout, it's forgotten.
            queue.RemoveAll(p => p.Ghost && now > p.Deadline + p.Timeout);
            foreach (var p in queue)
            {
                if (!p.Ghost && now > p.Deadline)
                {
                    p.Ghost = true;
                    Stats.TimedOut++;
                    rolledBack.Add((p.Reducer, "no reply from the server in time"));
                    dirty = true;
                    changedSinceFlush = true;
                }
            }
            if (!changedSinceFlush && rolledBack.Count == 0) return;
            changedSinceFlush = false;
            Rebuild();
            foreach (var t in tables) t.RaiseEvents();
            if (rolledBack.Count > 0)
            {
                var list = rolledBack.ToArray();
                rolledBack.Clear();
                foreach (var (type, reason) in list) OnRolledBack?.Invoke(type, reason);
            }
            OnChanged?.Invoke();
        }

        /// <summary>Re-read the server rows from the cache (after a reconnect or a resubscribe, say).</summary>
        public void Resync()
        {
            foreach (var t in tables) t.ResetBase();
            dirty = true;
            changedSinceFlush = true;
        }

        /// <summary>Forget every pending call without rolling back events (e.g. after a disconnect).</summary>
        public void Clear()
        {
            queue.Clear();
            dirty = true;
            changedSinceFlush = true;
        }

        internal void Rebuild()
        {
            if (!dirty) return;
            dirty = false;
            foreach (var t in tables) t.BeginRebuild();
            var draft = new Draft();
            foreach (var p in queue)
            {
                if (p.Ghost) continue;
                draft.Current = p;
                p.TempCursor.Clear();
                foreach (var t in tables) t.BeginPrediction();
                bool ok;
                try
                {
                    p.Run(draft);
                    ok = true;
                }
                catch
                {
                    Stats.Skipped++;
                    ok = false;
                }
                foreach (var t in tables) t.EndPrediction(p, ok);
            }
            draft.Current = null;
        }

    }

    /// <summary>A pending call.</summary>
    internal sealed class Pending
    {
        public readonly Type Reducer;
        public readonly Action<Draft> Run;
        public readonly TimeSpan Deadline;
        public readonly TimeSpan Timeout;
        public bool Ghost;
        /// <summary>Temporary ids per table, reused on every re-run so keys stay stable.</summary>
        public readonly Dictionary<object, List<ulong>> Temps = new Dictionary<object, List<ulong>>();
        public readonly Dictionary<object, int> TempCursor = new Dictionary<object, int>();
        /// <summary>The keys this prediction inserted in its last run, per table, in order.</summary>
        public readonly Dictionary<object, System.Collections.IList> InsertedKeys = new Dictionary<object, System.Collections.IList>();

        public Pending(Type reducer, Action<Draft> run, TimeSpan deadline, TimeSpan timeout)
        {
            Reducer = reducer;
            Run = run;
            Deadline = deadline;
            Timeout = timeout;
        }
    }

    internal interface ITracked
    {
        void BeginRebuild();
        void BeginPrediction();
        void EndPrediction(Pending p, bool ok);
        void Confirmed(Pending p, object reducerEvent);
        void RaiseEvents();
        void ResetBase();
    }

    /// <summary>What a prediction writes to: <c>draft.Of(table)</c> gives one table.</summary>
    public sealed class Draft
    {
        internal Pending? Current;

        public DraftTable<TRow, TKey> Of<TRow, TKey>(PredictedTable<TRow, TKey> table)
            where TRow : class, IStructuralReadWrite, new()
            where TKey : notnull => table.DraftFor(this);

        internal Pending Pending => Current ?? throw new InvalidOperationException("draft used outside a prediction");
    }

    /// <summary>A predicted table, as a prediction sees it: like the cache, but writable.</summary>
    public sealed class DraftTable<TRow, TKey>
        where TRow : class, IStructuralReadWrite, new()
        where TKey : notnull
    {
        private readonly PredictedTable<TRow, TKey> t;
        private readonly Draft draft;

        internal DraftTable(PredictedTable<TRow, TKey> t, Draft draft)
        {
            this.t = t;
            this.draft = draft;
        }

        public TKey KeyOf(TRow row) => t.Key(row);
        public IEnumerable<TRow> Rows() => t.Merge(true);
        public TRow? Find(TKey key) => t.Lookup(key, true);

        /// <summary>Insert a row. With an auto-inc key at 0, a temporary id is assigned (on the row itself).</summary>
        public TRow Insert(TRow row) => t.DraftInsert(draft.Pending, row);

        /// <summary>Replace the row with the same key. False (and nothing changes) if it isn't there.</summary>
        public bool Update(TRow row) => t.DraftUpdate(row);

        /// <summary>Insert or replace by key.</summary>
        public TRow Upsert(TRow row) => Update(row) ? row : Insert(row);

        /// <summary>Delete by key. Returns whether a row was there.</summary>
        public bool Delete(TKey key) => t.DraftDelete(key);
    }

    /// <summary>The predicted rows of one table, and their events.</summary>
    public sealed class PredictedTable<TRow, TKey> : ITracked
        where TRow : class, IStructuralReadWrite, new()
        where TKey : notnull
    {
        private readonly Action rebuild;
        private readonly Func<ulong> nextTemp;
        private readonly Func<IEnumerable<TRow>> iter;
        internal readonly Func<TRow, TKey> Key;
        private readonly AutoInc<TRow>? autoInc;

        // Server rows (kept in step with the cache after the first read), the combined prediction
        // layer (null value = deleted), and the layer of the prediction being run.
        private Dictionary<TKey, TRow>? baseRows;
        private Dictionary<TKey, TRow?> over = new Dictionary<TKey, TRow?>();
        private Dictionary<TKey, TRow?>? mine;
        private List<(TKey key, bool insert)> marks = new List<(TKey, bool)>();
        private HashSet<TKey> touched = new HashSet<TKey>();
        private HashSet<TKey> inserted = new HashSet<TKey>();
        private HashSet<TKey> lastTouched = new HashSet<TKey>();
        private readonly HashSet<TKey> serverDirty = new HashSet<TKey>();
        private readonly List<(object ev, TRow row)> serverInserts = new List<(object, TRow)>();
        private readonly Dictionary<TKey, TRow> replacements = new Dictionary<TKey, TRow>();
        private List<TRow>? materialized;
        private Draft? draftOwner;
        private DraftTable<TRow, TKey>? draftTable;

        // What the last Flush told listeners about, for diffs.
        private readonly Dictionary<TKey, TRow> published = new Dictionary<TKey, TRow>();
        private bool publishedAll;

        /// <summary>A predicted row appeared (a predicted insert, or a server insert).</summary>
        public event Action<TRow>? OnInsert;
        /// <summary>A row changed: (old, new).</summary>
        public event Action<TRow, TRow>? OnUpdate;
        public event Action<TRow>? OnDelete;
        /// <summary>
        /// A predicted insert was confirmed: (temporary row, real row). Raised instead of OnDelete +
        /// OnInsert, so whatever you made for the temporary row can be re-keyed. Needs <c>eventOf</c>
        /// in Track.
        /// </summary>
        public event Action<TRow, TRow>? OnReplaced;

        internal PredictedTable(Action rebuild, Func<ulong> nextTemp, Func<IEnumerable<TRow>> iter, Func<TRow, TKey> key, AutoInc<TRow>? autoInc)
        {
            this.rebuild = rebuild;
            this.nextTemp = nextTemp;
            this.iter = iter;
            Key = key;
            this.autoInc = autoInc;
        }

        // --- reading -------------------------------------------------------------------------

        /// <summary>The predicted rows (server rows plus pending predictions). Unordered.</summary>
        public IReadOnlyList<TRow> Rows()
        {
            rebuild();
            return materialized ??= new List<TRow>(Merge(false));
        }

        public TRow? Find(TKey key)
        {
            rebuild();
            return Lookup(key, false);
        }

        /// <summary>Whether a pending prediction inserted, changed or deleted this key.</summary>
        public bool IsPending(TKey key)
        {
            rebuild();
            return touched.Contains(key);
        }

        /// <summary>Whether this row exists only as a prediction (a predicted insert).</summary>
        public bool IsPredicted(TKey key)
        {
            rebuild();
            return inserted.Contains(key);
        }

        // --- layers --------------------------------------------------------------------------

        private Dictionary<TKey, TRow> Base()
        {
            if (baseRows == null)
            {
                baseRows = new Dictionary<TKey, TRow>();
                foreach (var row in iter()) baseRows[Key(row)] = row;
            }
            return baseRows;
        }

        internal TRow? Lookup(TKey key, bool withMine)
        {
            if (withMine && mine != null && mine.TryGetValue(key, out var m)) return m;
            if (over.TryGetValue(key, out var o)) return o;
            return Base().TryGetValue(key, out var b) ? b : null;
        }

        private bool Has(TKey key) => Lookup(key, true) != null;

        internal IEnumerable<TRow> Merge(bool withMine)
        {
            var b = Base();
            var layers = new List<Dictionary<TKey, TRow?>>();
            if (withMine && mine != null && mine.Count > 0) layers.Add(mine);
            if (over.Count > 0) layers.Add(over);
            if (layers.Count == 0)
            {
                foreach (var row in b.Values) yield return row;
                yield break;
            }
            foreach (var kv in b)
            {
                var hit = false;
                foreach (var l in layers)
                {
                    if (l.TryGetValue(kv.Key, out var v))
                    {
                        hit = true;
                        if (v != null) yield return v;
                        break;
                    }
                }
                if (!hit) yield return kv.Value;
            }
            // Rows only the layers have (predicted inserts), oldest layer first.
            var seen = new HashSet<TKey>(b.Keys);
            for (var i = layers.Count - 1; i >= 0; i--)
            {
                foreach (var kv in layers[i])
                {
                    if (!seen.Add(kv.Key)) continue;
                    var top = Lookup(kv.Key, withMine);
                    if (top != null) yield return top;
                }
            }
        }

        internal DraftTable<TRow, TKey> DraftFor(Draft d)
        {
            if (draftOwner != d)
            {
                draftOwner = d;
                draftTable = new DraftTable<TRow, TKey>(this, d);
            }
            return draftTable!;
        }

        internal TRow DraftInsert(Pending p, TRow row)
        {
            if (autoInc != null && autoInc.Get(row) == 0)
            {
                if (!p.Temps.TryGetValue(this, out var list)) p.Temps[this] = list = new List<ulong>();
                p.TempCursor.TryGetValue(this, out var i);
                p.TempCursor[this] = i + 1;
                if (list.Count <= i) list.Add(nextTemp());
                autoInc.Set(row, list[i]);
            }
            var k = Key(row);
            mine![k] = row;
            marks.Add((k, true));
            return row;
        }

        internal bool DraftUpdate(TRow row)
        {
            var k = Key(row);
            if (!Has(k)) return false;
            mine![k] = row;
            marks.Add((k, false));
            return true;
        }

        internal bool DraftDelete(TKey key)
        {
            if (!Has(key)) return false;
            mine![key] = null;
            marks.Add((key, false));
            return true;
        }

        void ITracked.BeginRebuild()
        {
            Base();
            over = new Dictionary<TKey, TRow?>();
            // Keys touched since the last Flush, across however many rebuilds happen in between.
            lastTouched.UnionWith(touched);
            touched = new HashSet<TKey>();
            inserted = new HashSet<TKey>();
            materialized = null;
        }

        void ITracked.BeginPrediction()
        {
            mine = new Dictionary<TKey, TRow?>();
            marks = new List<(TKey, bool)>();
        }

        void ITracked.EndPrediction(Pending p, bool ok)
        {
            if (ok)
            {
                foreach (var kv in mine!) over[kv.Key] = kv.Value;
                var keys = new List<TKey>();
                foreach (var (k, ins) in marks)
                {
                    touched.Add(k);
                    if (ins)
                    {
                        inserted.Add(k);
                        keys.Add(k);
                    }
                }
                p.InsertedKeys[this] = keys;
            }
            mine = null;
            marks = new List<(TKey, bool)>();
        }

        // --- server side ---------------------------------------------------------------------

        internal void ServerInsert(TRow row, object? reducerEvent)
        {
            var k = Key(row);
            if (baseRows != null) baseRows[k] = row;
            serverDirty.Add(k);
            if (reducerEvent != null) serverInserts.Add((reducerEvent, row));
        }

        internal void ServerDelete(TRow row)
        {
            var k = Key(row);
            baseRows?.Remove(k);
            serverDirty.Add(k);
        }

        internal void ServerUpdate(TRow oldRow, TRow newRow)
        {
            var ko = Key(oldRow);
            var kn = Key(newRow);
            baseRows?.Remove(ko);
            if (baseRows != null) baseRows[kn] = newRow;
            serverDirty.Add(ko);
            serverDirty.Add(kn);
        }

        void ITracked.Confirmed(Pending p, object reducerEvent)
        {
            if (!p.InsertedKeys.TryGetValue(this, out var keys) || keys.Count == 0) return;
            var real = new List<TRow>();
            foreach (var (ev, row) in serverInserts)
            {
                if (ReferenceEquals(ev, reducerEvent)) real.Add(row);
            }
            for (var i = 0; i < keys.Count && i < real.Count; i++) replacements[(TKey)keys[i]!] = real[i];
        }

        void ITracked.ResetBase()
        {
            baseRows = null;
            publishedAll = false;
        }

        void ITracked.RaiseEvents()
        {
            var candidates = new HashSet<TKey>(serverDirty);
            candidates.UnionWith(lastTouched);
            candidates.UnionWith(touched);
            if (!publishedAll)
            {
                // First flush (or after Resync): everything is new to listeners.
                candidates.UnionWith(published.Keys);
                foreach (var row in Merge(false)) candidates.Add(Key(row));
                publishedAll = true;
            }
            serverDirty.Clear();
            serverInserts.Clear();
            lastTouched = new HashSet<TKey>();

            var replaced = new List<(TRow, TRow)>();
            foreach (var kv in replacements)
            {
                var tempKey = kv.Key;
                var realKey = Key(kv.Value);
                if (published.TryGetValue(tempKey, out var tempRow) && Lookup(tempKey, false) == null
                    && !published.ContainsKey(realKey) && Lookup(realKey, false) is TRow realRow)
                {
                    published.Remove(tempKey);
                    published[realKey] = realRow;
                    candidates.Remove(tempKey);
                    candidates.Remove(realKey);
                    replaced.Add((tempRow, realRow));
                }
            }
            replacements.Clear();

            var deletes = new List<TRow>();
            var updates = new List<(TRow, TRow)>();
            var inserts = new List<TRow>();
            foreach (var k in candidates)
            {
                published.TryGetValue(k, out var oldRow);
                var newRow = Lookup(k, false);
                if (oldRow == null && newRow == null) continue;
                if (oldRow == null)
                {
                    inserts.Add(newRow!);
                    published[k] = newRow!;
                }
                else if (newRow == null)
                {
                    deletes.Add(oldRow);
                    published.Remove(k);
                }
                else if (!ReferenceEquals(oldRow, newRow))
                {
                    published[k] = newRow;
                    if (!SameBytes(oldRow, newRow)) updates.Add((oldRow, newRow));
                }
            }
            foreach (var (t, r) in replaced) OnReplaced?.Invoke(t, r);
            foreach (var r in deletes) OnDelete?.Invoke(r);
            foreach (var (o, n) in updates) OnUpdate?.Invoke(o, n);
            foreach (var r in inserts) OnInsert?.Invoke(r);
        }

        // Rows are compared by their BSATN bytes: predictions make new row objects on every re-run,
        // and generated Equals(object) can't be trusted on SpacetimeDB 2.11 types (it recurses).
        private static bool SameBytes(TRow a, TRow b)
        {
            using var ma = new MemoryStream();
            using var mb = new MemoryStream();
            using (var wa = new BinaryWriter(ma, System.Text.Encoding.UTF8, true)) a.WriteFields(wa);
            using (var wb = new BinaryWriter(mb, System.Text.Encoding.UTF8, true)) b.WriteFields(wb);
            if (ma.Length != mb.Length) return false;
            var ba = ma.GetBuffer();
            var bb = mb.GetBuffer();
            for (var i = 0; i < ma.Length; i++) if (ba[i] != bb[i]) return false;
            return true;
        }
    }
}
