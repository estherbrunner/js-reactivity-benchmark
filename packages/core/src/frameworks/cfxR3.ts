import type { ReactiveFramework } from "../util/reactiveFramework";

export type Disposable = () => void;

type UnknownSignal = Signal<unknown & {}>;
type UnknownComputed = Computed<unknown>;
type UnknownFirewallSignal = FirewallSignal<unknown>;

// Type constraint: signal values must be non-nullable
export type Guard<T> = (value: unknown) => value is T;

export type SignalOptions<T extends {}> = {
  guard?: Guard<T>;
  equals?: (a: unknown, b: unknown) => boolean;
  firewall?: UnknownComputed;
};

type CacheFlag =
  | typeof CACHE_CLEAN
  | typeof CACHE_CHECK
  | typeof CACHE_DIRTY
  | typeof CACHE_RECOMPUTING;

type TaskState =
  | typeof TASK_IDLE
  | typeof TASK_PENDING
  | typeof TASK_ABORTED
  | typeof TASK_ERROR;

type TaskCallback<T extends {}> = (
  oldValue: T,
  abort: AbortSignal,
) => Promise<T>;

export interface Link {
  dep: UnknownSignal | UnknownComputed | UnknownTask;
  sub: UnknownComputed | UnknownTask;
  nextDep: Link | null;
  prevSub: Link | null;
  nextSub: Link | null;
}

export interface RawSignal<T> {
  subs: Link | null;
  subsTail: Link | null;
  value: T;
  equals: ((a: unknown, b: unknown) => boolean) | null;
  guard?: Guard<T>;
}

interface FirewallSignal<T> extends RawSignal<T> {
  owner: UnknownComputed;
  nextChild: UnknownFirewallSignal | null;
}

export type Signal<T extends {}> = RawSignal<T> | FirewallSignal<T>;

export interface Computed<T> extends RawSignal<T> {
  deps: Link | null;
  depsTail: Link | null;
  flags: CacheFlag;
  disposal: Disposable | Disposable[] | null;
  fn: () => T;
  child: UnknownFirewallSignal | null;
}

export interface Task<T extends {}> extends RawSignal<T> {
  deps: Link | null;
  depsTail: Link | null;
  flags: CacheFlag;
  disposal: Disposable | Disposable[] | null;
  fn: TaskCallback<T>;
  child: UnknownFirewallSignal | null;
  state: TaskState;
  controller: AbortController | undefined;
  error: Error | undefined;
}

type UnknownTask = Task<unknown & {}>;

/* === Constants === */

const CACHE_CLEAN = 0; // Signal value is valid, no need to recompute
const CACHE_CHECK = 1 << 0; // Signal value might be stale, check parent nodes to decide whether to recompute
const CACHE_DIRTY = 1 << 1; // Signal value is invalid, parents have changed, value needs to be recomputed
const CACHE_RECOMPUTING = 1 << 2; // Signal value is being recomputed

const TASK_IDLE = 0;
const TASK_PENDING = 1;
const TASK_ABORTED = 2;
const TASK_ERROR = 3;

/* === Internals === */

let context: UnknownComputed | UnknownTask | null = null;
const queuedEffects: UnknownComputed[] = [];
let batchDepth = 0;

/* === Errors === */

// Validation errors
class NullishSignalValueError extends Error {
  constructor(where: string) {
    super(`[${where}] Signal value cannot be null or undefined`);
    this.name = "NullishSignalValueError";
  }
}

class InvalidSignalValueError extends Error {
  constructor(where: string, value: unknown) {
    super(`[${where}] Invalid signal value: ${valueString(value)}`);
    this.name = "InvalidSignalValueError";
  }
}

/* === Functions === */

const valueString = (value: unknown): string =>
  typeof value === "string"
    ? `"${value}"`
    : !!value && typeof value === "object"
      ? JSON.stringify(value)
      : String(value);

const validateSignalValue = <T>(
  where: string,
  value: unknown,
  guard?: Guard<T>,
): void => {
  if (value == null) throw new NullishSignalValueError(where);
  if (guard && !guard(value)) throw new InvalidSignalValueError(where, value);
};

export function createMemo<T>(
  fn: () => T,
  options?: SignalOptions<NonNullable<T>>,
): Computed<T> {
  return {
    disposal: null,
    fn: fn,
    value: undefined as unknown as T,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: CACHE_DIRTY,
    equals: options?.equals || ((a: unknown, b: unknown) => a === b),
  } as Computed<T>;
}

/**
 * Create an async computed signal (Task) that awaits promises.
 *
 * Features:
 * - Automatically tracks dependencies like computed signals
 * - Provides AbortSignal to cancel in-flight work when dependencies change
 * - Catches errors and rethrows them on read() (colorless error propagation)
 * - Returns last committed value while pending
 *
 * @example
 * const userId = createState(1);
 * const userData = createTask(async (prev, signal) => {
 *   const id = read(userId);
 *   const response = await fetch(`/api/users/${id}`, { signal });
 *   return response.json();
 * }, initialUserData);
 */
export function createTask<T extends {}>(
  fn: TaskCallback<T>,
  initialValue: T,
  options?: SignalOptions<NonNullable<T>>,
): Task<T> {
  validateSignalValue("Task", initialValue, options?.guard);

  return {
    disposal: null,
    fn: fn,
    value: initialValue,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: CACHE_DIRTY,
    equals: options?.equals || ((a: unknown, b: unknown) => a === b),
    guard: options?.guard,
    state: TASK_IDLE,
    controller: undefined,
    error: undefined,
  };
}

/**
 * Check if a task is currently pending (executing async work).
 */
export function isPending<T extends {}>(task: Task<T>): boolean {
  return task.state === TASK_PENDING;
}

/**
 * Abort a task's in-flight execution.
 */
export function abortTask<T extends {}>(task: Task<T>): void {
  task.controller?.abort();
  task.controller = undefined;
  if (task.state === TASK_PENDING) task.state = TASK_ABORTED;
}

/**
 * Dispose a task, aborting in-flight work and unlinking from the reactive graph.
 */
export function disposeTask<T extends {}>(task: Task<T>): void {
  abortTask(task);

  // Unlink from dependencies
  let dep = task.deps;
  while (dep !== null) dep = unlinkSubs(dep);
  task.deps = null;

  // Run disposal callbacks
  runDisposal(task as unknown as UnknownTask);

  // Clear subscribers
  task.subs = null;
  task.subsTail = null;

  // Keep last committed value; clear error so it doesn't throw after disposal
  task.error = undefined;
  task.state = TASK_IDLE;
}

export function createState<T extends {}>(
  v: T,
  options?: SignalOptions<T>,
): Signal<T> {
  validateSignalValue("State", v, options?.guard);

  const firewall = options?.firewall;

  if (firewall) {
    firewall.child = {
      value: v,
      subs: null,
      subsTail: null,
      owner: firewall,
      nextChild: firewall.child,
      equals: options?.equals ?? ((a, b) => a === b),
      guard: options?.guard,
    };
    return firewall.child as unknown as Signal<T>;
  } else {
    return {
      value: v,
      subs: null,
      subsTail: null,
      equals: options?.equals ?? ((a, b) => a === b),
      guard: options?.guard,
    };
  }
}

function recompute(el: UnknownComputed) {
  runDisposal(el);
  const oldcontext = context;
  context = el;
  el.depsTail = null;
  el.flags = CACHE_RECOMPUTING;
  const value = el.fn();
  context = oldcontext;

  const depsTail = el.depsTail as Link | null;
  let toRemove = depsTail !== null ? depsTail.nextDep : el.deps;
  if (toRemove) {
    do {
      toRemove = unlinkSubs(toRemove);
    } while (toRemove !== null);
    // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
    depsTail ? (depsTail.nextDep = null) : (el.deps = null);
  }

  // CRITICAL: Only mark subscribers dirty if value actually changed
  // This is the key to handling diamond dependencies correctly.
  //
  // In a diamond (A->B->D, A->C->D), when A changes:
  // - B and C are marked Dirty
  // - D is marked Check
  // - When reading D, we check B (recomputes, value changes) and C (recomputes)
  // - If C's value doesn't change, D stays Check (not escalated to Dirty)
  // - D then doesn't need to recompute since its dependencies' values are stable
  if (!el.equals?.(value, el.value)) {
    el.value = value;

    // Mark subscribers as dirty/check
    for (let s = el.subs; s !== null; s = s.nextSub) {
      const o = s.sub;
      const flags = o.flags;
      // If already Check, escalate to Dirty; otherwise mark as Dirty
      flags & CACHE_CHECK
        ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
          (o.flags = flags | CACHE_DIRTY)
        : markNode(o, CACHE_DIRTY);
    }
  }

  // Clear flags after recompute
  el.flags = CACHE_CLEAN;
}

function recomputeTask(el: UnknownTask) {
  if (el.state === TASK_PENDING) return;

  // Abort any previous run
  el.controller?.abort();

  const controller = new AbortController();
  el.controller = controller;

  const oldValue = el.value;

  el.state = TASK_PENDING;
  el.error = undefined;

  runDisposal(el);
  const oldcontext = context;
  context = el;
  el.depsTail = null;
  el.flags = CACHE_RECOMPUTING;

  let promise: Promise<unknown>;
  try {
    promise = el.fn(oldValue, controller.signal);

    const depsTail = el.depsTail as Link | null;
    let toRemove = depsTail !== null ? depsTail.nextDep : el.deps;
    if (toRemove) {
      do {
        toRemove = unlinkSubs(toRemove);
      } while (toRemove !== null);
      // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
      depsTail ? (depsTail.nextDep = null) : (el.deps = null);
    }
  } catch (e) {
    // Synchronous throw from callback: treat as immediate error, keep old committed value
    context = oldcontext;
    el.state = TASK_ERROR;
    el.controller = undefined;
    el.error = e instanceof Error ? e : new Error(String(e));
    el.flags = CACHE_CLEAN;
    return;
  }

  context = oldcontext;

  promise.then(
    (next) => {
      if (controller.signal.aborted) return;

      el.controller = undefined;
      el.state = TASK_IDLE;
      el.error = undefined;

      // Only update if value changed
      if (!el.equals?.(next, el.value)) {
        el.value = next as typeof el.value;

        // Mark subscribers as dirty/check
        for (let s = el.subs; s !== null; s = s.nextSub) {
          const o = s.sub;
          const flags = o.flags;
          flags & CACHE_CHECK
            ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
              (o.flags = flags | CACHE_DIRTY)
            : markNode(o, CACHE_DIRTY);
        }
      }

      el.flags = CACHE_CLEAN;
    },
    (err: unknown) => {
      if (controller.signal.aborted) return;

      // On error: do not commit value; keep last committed
      el.controller = undefined;
      el.state = TASK_ERROR;
      el.error = err instanceof Error ? err : new Error(String(err));

      // Notify dependents so they can react/throw if they read
      for (let s = el.subs; s !== null; s = s.nextSub) {
        const o = s.sub;
        const flags = o.flags;
        flags & CACHE_CHECK
          ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
            (o.flags = flags | CACHE_DIRTY)
          : markNode(o, CACHE_CHECK);
      }

      el.flags = CACHE_CLEAN;
    },
  );
}

function updateIfNecessary(el: UnknownComputed | UnknownTask): void {
  // If marked Check, recursively update dependencies to see if we're actually dirty
  if (el.flags & CACHE_CHECK) {
    for (let d = el.deps; d !== null; d = d.nextDep) {
      "fn" in d.dep &&
        updateIfNecessary(d.dep as UnknownComputed | UnknownTask);
      // Early exit if dependency recomputation escalated us to Dirty
      if (el.flags & CACHE_DIRTY) break;
    }
  }

  // Only recompute if we're actually Dirty (not just Check)
  if (el.flags & CACHE_DIRTY) {
    // Check if this is a Task
    if ("state" in el) {
      recomputeTask(el as UnknownTask);
    } else {
      recompute(el as UnknownComputed);
    }
  }

  // Clear flags after checking/recomputing
  el.flags = CACHE_CLEAN;
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L100
function unlinkSubs(link: Link): Link | null {
  const dep = link.dep;
  const nextDep = link.nextDep;
  const nextSub = link.nextSub;
  const prevSub = link.prevSub;

  // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
  nextSub !== null ? (nextSub.prevSub = prevSub) : (dep.subsTail = prevSub);

  prevSub !== null
    ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
      (prevSub.nextSub = nextSub)
    : // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
      // biome-ignore lint/complexity/noCommaOperator: micro-optimization
      ((dep.subs = nextSub), nextSub === null && "fn" in dep && unwatched(dep));

  return nextDep;
}

function unwatched(el: UnknownComputed | UnknownTask) {
  let dep = el.deps;
  while (dep !== null) dep = unlinkSubs(dep);
  el.deps = null;
  runDisposal(el);
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L52
function link(
  dep: UnknownSignal | UnknownComputed | UnknownTask,
  sub: UnknownComputed | UnknownTask,
) {
  const prevDep = sub.depsTail;
  if (prevDep !== null && prevDep.dep === dep) return;
  let nextDep: Link | null = null;
  const isRecomputing = sub.flags & CACHE_RECOMPUTING;
  if (isRecomputing) {
    nextDep = prevDep !== null ? prevDep.nextDep : sub.deps;
    if (nextDep !== null && nextDep.dep === dep) {
      sub.depsTail = nextDep;
      return;
    }
  }

  const prevSub = dep.subsTail;
  if (
    prevSub !== null &&
    prevSub.sub === sub &&
    (!isRecomputing || isValidLink(prevSub, sub))
  )
    return;
  const newLink =
    // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
    (sub.depsTail =
    // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
    dep.subsTail =
      {
        dep,
        sub,
        nextDep,
        prevSub,
        nextSub: null,
      });

  // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
  prevDep !== null ? (prevDep.nextDep = newLink) : (sub.deps = newLink);
  // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
  prevSub !== null ? (prevSub.nextSub = newLink) : (dep.subs = newLink);
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L284
function isValidLink(
  checkLink: Link,
  sub: UnknownComputed | UnknownTask,
): boolean {
  const depsTail = sub.depsTail;
  if (depsTail !== null) {
    // biome-ignore lint/style/noNonNullAssertion: we know what we're doing
    let link = sub.deps!;
    do {
      if (link === checkLink) return true;
      if (link === depsTail) break;
      // biome-ignore lint/style/noNonNullAssertion: we know what we're doing
      link = link.nextDep!;
    } while (link !== null);
  }
  return false;
}

export function read<T>(
  el: Signal<NonNullable<T>> | Computed<T> | Task<T & {}>,
): T {
  // Update computed if dirty (pull-based)
  const owner = ("owner" in el ? el.owner : el) as
    | UnknownComputed
    | UnknownTask;
  if ("fn" in owner && owner.flags & (CACHE_DIRTY | CACHE_CHECK))
    updateIfNecessary(owner);

  // Link to current reactive context for dependency tracking
  if (context)
    link(el as UnknownSignal | UnknownComputed | UnknownTask, context);

  // Rethrow error if task failed (colorless error propagation)
  if ("error" in el && el.error) throw el.error;

  return el.value;
}

export function setSignal<T extends unknown & {}>(el: Signal<T>, v: T) {
  validateSignalValue("setSignal", v, el.guard);

  if (el.equals?.(v, el.value)) return;
  el.value = v;

  for (let link = el.subs; link !== null; link = link.nextSub)
    markNode(link.sub, CACHE_DIRTY);

  if (batchDepth === 0) flush();
}

function markNode(el: UnknownComputed | UnknownTask, newState = CACHE_DIRTY) {
  const flags = el.flags;
  if ((flags & (CACHE_DIRTY | CACHE_CHECK)) >= newState) return;

  el.flags = flags | newState;

  // Special handling for tasks: abort in-flight work when dependencies change
  if ("state" in el && el.state === TASK_PENDING) {
    el.controller?.abort();
    el.controller = undefined;
    el.state = TASK_ABORTED;
  }

  // Effects have equals === null - collect them for later execution
  if (el.equals === null && !(flags & (CACHE_DIRTY | CACHE_CHECK))) {
    queuedEffects.push(el as UnknownComputed);
    return;
  }

  // Propagate Check to subscribers
  for (let link = el.subs; link !== null; link = link.nextSub)
    markNode(link.sub, CACHE_CHECK);

  // Propagate to firewall children
  for (let child = el.child; child !== null; child = child.nextChild) {
    for (let link = child.subs; link !== null; link = link.nextSub)
      markNode(link.sub, CACHE_CHECK);
  }
}

export function flush(): void {
  for (let i = 0; i < queuedEffects.length; i++) {
    const effect = queuedEffects[i];
    if (effect.flags & (CACHE_DIRTY | CACHE_CHECK)) {
      updateIfNecessary(effect as UnknownComputed);
    }
  }
  queuedEffects.length = 0;
}

export function batch(fn: () => void): void {
  batchDepth++;
  try {
    fn();
  } finally {
    batchDepth--;
    if (batchDepth === 0) flush();
  }
}

export function onCleanup(fn: Disposable): Disposable {
  if (!context) return fn;

  const node = context;

  if (!node.disposal) node.disposal = fn;
  else if (Array.isArray(node.disposal)) node.disposal.push(fn);
  else node.disposal = [node.disposal, fn];
  return fn;
}

function runDisposal(node: UnknownComputed | UnknownTask): void {
  if (!node.disposal) return;

  if (Array.isArray(node.disposal)) {
    for (let i = 0; i < node.disposal.length; i++) {
      const callable = node.disposal[i];
      callable.call(callable);
    }
  } else {
    node.disposal.call(node.disposal);
  }

  node.disposal = null;
}

export function getContext(): UnknownComputed | UnknownTask | null {
  return context;
}

/**
 * Create an effect scope - runs a function once and tracks all effects created within.
 * Returns a disposer that cleans up all tracked effects.
 *
 * @example
 * const dispose = effectScope(() => {
 *   createEffect(() => console.log("effect 1"));
 *   createEffect(() => console.log("effect 2"));
 * });
 * // Later: dispose() cleans up both effects
 */
export function effectScope(fn: () => void): Disposable {
  // Create a dummy computed to act as owner
  const owner: Computed<void> = {
    fn: () => {},
    value: undefined,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: CACHE_DIRTY,
    disposal: null,
    equals: null,
    guard: undefined,
  };

  // Run function with owner as active context
  const prevContext = context;
  context = owner;
  try {
    fn();
  } finally {
    context = prevContext;
  }

  // Return disposer
  return () => {
    // Clean up all children (firewall signals)
    let child = owner.child;
    while (child !== null) {
      const next = child.nextChild;
      // Unlink all subscribers of this child signal
      let link = child.subs;
      while (link !== null) {
        link = unlinkSubs(link);
      }
      child = next;
    }
    owner.child = null;

    // Run disposal callbacks (effects registered via onCleanup)
    runDisposal(owner);
  };
}

/**
 * Create an effect that runs immediately and re-runs when dependencies change.
 * Returns a disposer function.
 */
export function createEffect(fn: () => void): Disposable {
  const effect: Computed<void> = {
    disposal: null,
    fn: fn,
    value: undefined,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: CACHE_DIRTY,
    equals: null,
  };

  if (context) link(effect as UnknownComputed, context);

  updateIfNecessary(effect as UnknownComputed); // Initial run

  const dispose = () => {
    unwatched(effect);
  };

  // Register with active owner if present
  if (context) onCleanup(dispose);

  return dispose;
}

/* === Test Framework === */

let scope: (() => void) | null = null;
const cleanups = new Set<() => void>();
export const cfxR3Framework: ReactiveFramework = {
  name: "cfxR3",
  // @ts-expect-error ReactiveFramework doesn't have non-nullable signals
  signal: <T extends {}>(initialValue: T) => {
    const s = createState(initialValue);
    return {
      // biome-ignore lint/suspicious/noExplicitAny: we suport updater functions in .set()
      write: (v) => setSignal(s, v as any),
      read: () => read(s),
    };
  },
  // @ts-expect-error ReactiveFramework doesn't have non-nullable signals
  computed: <T extends {}>(fn: () => T) => {
    const c = createMemo(fn);
    return {
      read: () => read(c),
    };
  },
  effect: (fn) => {
    const dispose = createEffect(fn);
    cleanups.add(dispose);
  },
  withBatch: (fn) => batch(fn),
  withBuild: <T>(fn: () => T) => {
    let out!: T;
    scope = effectScope(() => {
      out = fn();
    });
    cleanups.add(scope);
    return out;
  },
  cleanup: () => {
    for (const dispose of cleanups) dispose();
    cleanups.clear();
    scope = null;
  },
};
