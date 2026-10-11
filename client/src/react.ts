/**
 * React bindings: read predicted rows with `usePredicted`, call reducers with a prediction with
 * `usePredictedCall`. Works next to the SDK's own hooks; this only adds the prediction layer.
 *
 * ```tsx
 * const p = useMemo(() => predict({ task: { table: conn.db.task, key: (t) => t.id, autoInc: 'id' } }), [conn]);
 * const tasks = usePredicted(p, 'task');
 * const toggle = usePredictedCall(p, (id: bigint) => conn.reducers.toggleTask({ id }), (d, id) => {
 *   const t = d.task.find(id);
 *   if (t) d.task.update({ ...t, done: !t.done });
 * });
 * ```
 */
import { useCallback, useRef, useSyncExternalStore } from 'react';
import type { Draft, Predictor, CallOptions } from './index.js';

/** The predicted rows of one table; re-renders when they change. */
export function usePredicted<T extends Record<string, unknown>, K extends keyof T & string>(p: Predictor<T>, table: K): readonly T[K][] {
  const t = p.table(table);
  return useSyncExternalStore(
    useCallback((cb: () => void) => p.subscribe(cb), [p]),
    () => t.rows()
  );
}

/**
 * A function that calls a reducer with a prediction. Returns the call's promise, which rejects
 * (after rolling the prediction back) if the server refuses.
 */
export function usePredictedCall<T extends Record<string, unknown>, A extends unknown[]>(
  p: Predictor<T>,
  send: (...args: A) => Promise<unknown>,
  prediction: (draft: Draft<T>, ...args: A) => void,
  opts?: CallOptions
): (...args: A) => Promise<void> {
  // Always call the latest `send` / `prediction`, without changing the returned function's identity.
  const latest = useRef({ send, prediction, opts });
  latest.current = { send, prediction, opts };
  return useCallback(
    (...args: A) => {
      const { send, prediction, opts } = latest.current;
      return p.call(() => send(...args), (d) => prediction(d, ...args), opts);
    },
    [p]
  );
}
