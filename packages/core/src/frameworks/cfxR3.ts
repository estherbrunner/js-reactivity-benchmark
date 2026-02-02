import type { ReactiveFramework } from "../util/reactiveFramework";

export type Cleanup = () => void;
export type Guard<T extends {}> = (value: unknown) => value is T;

export type State<T extends {}> = {
  get(): T;
  set(value: T): void;
};

export type Memo<T extends {}> = {
  get(): T;
};

export type Task<T extends {}> = {
  get(): T;
  isPending(): boolean;
  abort(): void;
  dispose(): void;
};

type UnknownState = ProducerNode<unknown & {}>;
type UnknownChild = ChildNode<unknown & {}>;
type UnknownMemo = MemoNode<unknown & {}>;
type UnknownTask = TaskNode<unknown & {}>;

type ProducerNode<T extends {}> = StateNode<T> | ChildNode<T>;
type ConsumerNode = UnknownMemo | UnknownTask | EffectNode;

export type MemoCallback<T extends {}> = (prev: T) => T;
export type TaskCallback<T extends {}> = (
  prev: T,
  abort: AbortSignal,
) => Promise<T>;
export type EffectCallback = () => void;

type CacheFlag =
  | typeof FLAG_CLEAN
  | typeof FLAG_CHECK
  | typeof FLAG_DIRTY
  | typeof FLAG_RUNNING;

type TaskState =
  | typeof TASK_IDLE
  | typeof TASK_PENDING
  | typeof TASK_ABORTED
  | typeof TASK_ERROR;

interface Link {
  dep: UnknownState | ConsumerNode;
  sub: ConsumerNode;
  nextDep: Link | null;
  prevSub: Link | null;
  nextSub: Link | null;
}

interface StateNode<T extends {}> {
  subs: Link | null;
  subsTail: Link | null;
  value: T;
  equals: ((a: unknown, b: unknown) => boolean) | null;
  guard?: Guard<T>;
}

interface ChildNode<T extends {}> extends StateNode<T> {
  owner: ConsumerNode;
  nextChild: UnknownChild | null;
}

interface MemoNode<T extends {}> extends StateNode<T> {
  deps: Link | null;
  depsTail: Link | null;
  flags: CacheFlag;
  cleanup: Cleanup | Cleanup[] | null;
  fn: MemoCallback<T>;
  child: UnknownChild | null;
}

interface TaskNode<T extends {}> extends StateNode<T> {
  deps: Link | null;
  depsTail: Link | null;
  flags: CacheFlag;
  cleanup: Cleanup | Cleanup[] | null;
  fn: TaskCallback<T>;
  child: UnknownChild | null;
  state: TaskState;
  controller: AbortController | undefined;
  error: Error | undefined;
}

interface EffectNode {
  deps: Link | null;
  depsTail: Link | null;
  subs: Link | null;
  subsTail: Link | null;
  flags: CacheFlag;
  cleanup: Cleanup | Cleanup[] | null;
  fn: EffectCallback;
  child: UnknownChild | null;
}

export type SignalOptions<T extends {}> = {
  guard?: Guard<T>;
  equals?: (a: unknown, b: unknown) => boolean;
  owner?: ConsumerNode;
};

/* === Constants === */

const FLAG_CLEAN = 0; // Signal value is valid, no need to recompute
const FLAG_CHECK = 1 << 0; // Signal value might be stale, check parent nodes to decide whether to recompute
const FLAG_DIRTY = 1 << 1; // Signal value is invalid, parents have changed, value needs to be recomputed
const FLAG_RUNNING = 1 << 2; // Signal value is being recomputed

const TASK_IDLE = 0;
const TASK_PENDING = 1;
const TASK_ABORTED = 2;
const TASK_ERROR = 3;

/* === Internals === */

let context: ConsumerNode | null = null;
const queuedEffects: EffectNode[] = [];
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

const validateSignalValue = <T extends {}>(
  where: string,
  value: unknown,
  guard?: Guard<T>,
): void => {
  if (value == null) throw new NullishSignalValueError(where);
  if (guard && !guard(value)) throw new InvalidSignalValueError(where, value);
};

export function createMemo<T extends {}>(
  fn: MemoCallback<T>,
  options?: SignalOptions<T>,
): Memo<T> {
  const memo = {
    cleanup: null,
    fn,
    value: undefined as unknown as T,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: FLAG_DIRTY,
    equals: options?.equals ?? ((a, b) => a === b),
  };

  return {
    get(): T {
      return read(memo);
    },
  };
}

/**
 * Create an async computed signal (Task) that awaits promises.
 */
export function createTask<T extends {}>(
  fn: TaskCallback<T>,
  initialValue: T,
  options?: SignalOptions<T>,
): Task<T> {
  validateSignalValue("Task", initialValue, options?.guard);

  const task: TaskNode<T> = {
    cleanup: null,
    fn,
    value: initialValue,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: FLAG_DIRTY,
    equals: options?.equals || ((a: unknown, b: unknown) => a === b),
    guard: options?.guard,
    state: TASK_IDLE,
    controller: undefined,
    error: undefined,
  };

  return {
    get(): T {
      return read(task);
    },
    isPending(): boolean {
      return task.state === TASK_PENDING;
    },
    abort(): void {
      abortTask(task);
    },
    dispose(): void {
      disposeTask(task);
    },
  };
}

/**
 * Abort a task's in-flight execution.
 */
function abortTask<T extends {}>(task: TaskNode<T>): void {
  task.controller?.abort();
  task.controller = undefined;
  if (task.state === TASK_PENDING) task.state = TASK_ABORTED;
}

/**
 * Dispose a task, aborting in-flight work and unlinking from the reactive graph.
 */
function disposeTask<T extends {}>(task: TaskNode<T>): void {
  abortTask(task);

  // Unlink from dependencies
  let dep = task.deps;
  while (dep) dep = unlinkSubs(dep);
  task.deps = null;

  // Run disposal callbacks
  runCleanup(task as unknown as UnknownTask);

  // Clear subscribers
  task.subs = null;
  task.subsTail = null;

  // Keep last committed value; clear error so it doesn't throw after disposal
  task.error = undefined;
  task.state = TASK_IDLE;
}

export function createState<T extends {}>(
  value: T,
  options?: SignalOptions<T>,
): State<T> {
  validateSignalValue("State", value, options?.guard);

  const owner = options?.owner;

  const state: ProducerNode<T> = {
    value,
    subs: null,
    subsTail: null,
    equals: options?.equals ?? ((a, b) => a === b),
    guard: options?.guard,
  } as StateNode<T>;

  if (owner) {
    (state as ChildNode<T>).owner = owner;
    (state as ChildNode<T>).nextChild = state as unknown as UnknownChild;
    owner.child = state as ChildNode<T>;
  }

  return {
    get(): T {
      if (context) linkSub(state, context);
      return state.value;
    },
    set(next: T): void {
      if (next == null) throw new NullishSignalValueError("State");
      if (state.guard && !state.guard(next))
        throw new InvalidSignalValueError("State", next);

      if (state.equals?.(next, state.value)) return;
      state.value = next;

      for (let link = state.subs; link; link = link.nextSub) markNode(link.sub);
      if (!batchDepth) flush();
    },
  };
}

function recomputeMemo(el: UnknownMemo) {
  runCleanup(el);
  const prevContext = context;
  context = el;
  el.depsTail = null;
  el.flags = FLAG_RUNNING;
  const value = el.fn(el.value);
  context = prevContext;

  const depsTail = el.depsTail as Link | null;
  let toRemove = depsTail ? depsTail.nextDep : el.deps;
  if (toRemove) {
    do {
      toRemove = unlinkSubs(toRemove);
    } while (toRemove);
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
    for (let sub = el.subs; sub; sub = sub.nextSub) {
      const o = sub.sub;
      const flags = o.flags;
      // If already Check, escalate to Dirty; otherwise mark as Dirty
      flags & FLAG_CHECK
        ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
          (o.flags = flags | FLAG_DIRTY)
        : markNode(o);
    }
  }

  // Clear flags after recompute
  el.flags = FLAG_CLEAN;
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

  runCleanup(el);
  const prevContext = context;
  context = el;
  el.depsTail = null;
  el.flags = FLAG_RUNNING;

  let promise: Promise<unknown>;
  try {
    promise = el.fn(oldValue, controller.signal);

    const depsTail = el.depsTail as Link | null;
    let toRemove = depsTail ? depsTail.nextDep : el.deps;
    if (toRemove) {
      do {
        toRemove = unlinkSubs(toRemove);
      } while (toRemove);
      // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
      depsTail ? (depsTail.nextDep = null) : (el.deps = null);
    }
  } catch (err) {
    // Synchronous throw from callback: treat as immediate error, keep old committed value
    context = prevContext;
    el.state = TASK_ERROR;
    el.controller = undefined;
    el.error = err instanceof Error ? err : new Error(String(err));
    el.flags = FLAG_CLEAN;
    return;
  }

  context = prevContext;

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
        for (let sub = el.subs; sub; sub = sub.nextSub) {
          const o = sub.sub;
          const flags = o.flags;
          flags & FLAG_CHECK
            ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
              (o.flags = flags | FLAG_DIRTY)
            : markNode(o, FLAG_DIRTY);
        }
      }

      el.flags = FLAG_CLEAN;
    },
    (err: unknown) => {
      if (controller.signal.aborted) return;

      // On error: do not commit value; keep last committed
      el.controller = undefined;
      el.state = TASK_ERROR;
      el.error = err instanceof Error ? err : new Error(String(err));

      // Notify dependents so they can react/throw if they read
      for (let sub = el.subs; sub; sub = sub.nextSub) {
        const o = sub.sub;
        const flags = o.flags;
        flags & FLAG_CHECK
          ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
            (o.flags = flags | FLAG_DIRTY)
          : markNode(o, FLAG_CHECK);
      }

      el.flags = FLAG_CLEAN;
    },
  );
}

function runEffect(el: EffectNode) {
  runCleanup(el);
  const prevContext = context;
  context = el;
  el.depsTail = null;
  el.flags = FLAG_RUNNING;
  el.fn();
  context = prevContext;

  const depsTail = el.depsTail as Link | null;
  let toRemove = depsTail ? depsTail.nextDep : el.deps;
  if (toRemove) {
    do {
      toRemove = unlinkSubs(toRemove);
    } while (toRemove);
    // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
    depsTail ? (depsTail.nextDep = null) : (el.deps = null);
  }

  // Clear flags after recompute
  el.flags = FLAG_CLEAN;
}

function updateIfNecessary(el: ConsumerNode): void {
  // If marked Check, recursively update dependencies to see if we're actually dirty
  if (el.flags & FLAG_CHECK) {
    for (let dep = el.deps; dep; dep = dep.nextDep) {
      "fn" in dep.dep && updateIfNecessary(dep.dep);
      // Early exit if dependency recomputation escalated us to Dirty
      if (el.flags & FLAG_DIRTY) break;
    }
  }

  // Only recompute if we're actually Dirty (not just Check)
  if (el.flags & FLAG_DIRTY) {
    // Check if this is a Task
    if ("state" in el) recomputeTask(el);
    else if ("value" in el) recomputeMemo(el);
    else runEffect(el);
  }

  // Clear flags after checking/recomputing
  el.flags = FLAG_CLEAN;
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L100
function unlinkSubs(link: Link): Link | null {
  const dep = link.dep;
  const nextDep = link.nextDep;
  const nextSub = link.nextSub;
  const prevSub = link.prevSub;

  // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
  nextSub ? (nextSub.prevSub = prevSub) : (dep.subsTail = prevSub);

  prevSub
    ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
      (prevSub.nextSub = nextSub)
    : // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
      // biome-ignore lint/complexity/noCommaOperator: micro-optimization
      ((dep.subs = nextSub), !nextSub && "fn" in dep && unwatched(dep));

  return nextDep;
}

function unwatched(el: ConsumerNode) {
  let dep = el.deps;
  while (dep) dep = unlinkSubs(dep);
  el.deps = null;
  runCleanup(el);
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L52
function linkSub(dep: UnknownState | ConsumerNode, sub: ConsumerNode) {
  const prevDep = sub.depsTail;
  if (prevDep?.dep === dep) return;
  let nextDep: Link | null = null;
  const isRecomputing = sub.flags & FLAG_RUNNING;
  if (isRecomputing) {
    nextDep = prevDep ? prevDep.nextDep : sub.deps;
    if (nextDep && nextDep.dep === dep) {
      sub.depsTail = nextDep;
      return;
    }
  }

  const prevSub = dep.subsTail;
  if (prevSub?.sub === sub && (!isRecomputing || isValidLink(prevSub, sub)))
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
  prevDep ? (prevDep.nextDep = newLink) : (sub.deps = newLink);
  // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
  prevSub ? (prevSub.nextSub = newLink) : (dep.subs = newLink);
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L284
function isValidLink(checkLink: Link, sub: ConsumerNode): boolean {
  const depsTail = sub.depsTail;
  if (depsTail) {
    // biome-ignore lint/style/noNonNullAssertion: we know what we're doing
    let link = sub.deps!;
    do {
      if (link === checkLink) return true;
      if (link === depsTail) break;
      // biome-ignore lint/style/noNonNullAssertion: we know what we're doing
      link = link.nextDep!;
    } while (link);
  }
  return false;
}

function read<T extends {}>(el: MemoNode<T> | TaskNode<T>): T {
  if (el.flags & (FLAG_DIRTY | FLAG_CHECK))
    updateIfNecessary(el as unknown as ConsumerNode);
  if (context) linkSub(el, context);
  if ("error" in el && el.error) throw el.error;
  return el.value;
}

function markNode(el: ConsumerNode, newState = FLAG_DIRTY) {
  const flags = el.flags;
  if ((flags & (FLAG_DIRTY | FLAG_CHECK)) >= newState) return;

  el.flags = flags | newState;

  // Effects have no value field - collect them for later execution
  if (!("value" in el) && !(flags & (FLAG_DIRTY | FLAG_CHECK))) {
    queuedEffects.push(el);
    return;
  }

  // Special handling for tasks: abort in-flight work when dependencies change
  if ("state" in el && el.state === TASK_PENDING) {
    el.controller?.abort();
    el.controller = undefined;
    el.state = TASK_ABORTED;
  }

  // Propagate Check to subscribers
  for (let link = el.subs; link; link = link.nextSub)
    markNode(link.sub, FLAG_CHECK);

  // Propagate to firewall children
  for (let child = el.child; child; child = child.nextChild) {
    for (let link = child.subs; link; link = link.nextSub)
      markNode(link.sub, FLAG_CHECK);
  }
}

function flush(): void {
  for (let i = 0; i < queuedEffects.length; i++) {
    const effect = queuedEffects[i];
    if (effect.flags & (FLAG_DIRTY | FLAG_CHECK)) updateIfNecessary(effect);
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

export function onCleanup(fn: Cleanup): Cleanup {
  if (!context) return fn;

  const node = context;

  if (!node.cleanup) node.cleanup = fn;
  else if (Array.isArray(node.cleanup)) node.cleanup.push(fn);
  else node.cleanup = [node.cleanup, fn];
  return fn;
}

function runCleanup(node: UnknownMemo | UnknownTask | EffectNode): void {
  if (!node.cleanup) return;

  if (Array.isArray(node.cleanup))
    for (let i = 0; i < node.cleanup.length; i++) node.cleanup[i]();
  else node.cleanup();

  node.cleanup = null;
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
export function effectScope(fn: () => void): Cleanup {
  // Create an effect node to act as owner
  const owner: EffectNode = {
    fn: () => {},
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: FLAG_DIRTY,
    cleanup: null,
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
    while (child) {
      const next = child.nextChild;
      // Unlink all subscribers of this child signal
      let link = child.subs;
      while (link) {
        link = unlinkSubs(link);
      }
      child = next;
    }
    owner.child = null;

    // Run disposal callbacks (effects registered via onCleanup)
    runCleanup(owner);
  };
}

/**
 * Create an effect that runs immediately and re-runs when dependencies change.
 * Returns a disposer function.
 */
export function createEffect(fn: EffectCallback): Cleanup {
  const effect: EffectNode = {
    cleanup: null,
    fn,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: FLAG_DIRTY,
  };

  updateIfNecessary(effect); // Initial run

  const cleanup = () => {
    unwatched(effect);
  };

  // Register with active owner if present
  if (context) onCleanup(cleanup);

  return cleanup;
}

/* === Test Framework === */

let scope: (() => void) | null = null;
const cleanups = new Set<() => void>();
export const cfxR3Framework: ReactiveFramework = {
  name: "cfxR3",
  // @ts-expect-error ReactiveFramework doesn't have non-nullable signals
  signal: <T extends {}>(initialValue: T) => {
    const state = createState(initialValue);
    return {
      write: state.set,
      read: state.get,
    };
  },
  // @ts-expect-error ReactiveFramework doesn't have non-nullable signals
  computed: <T extends {}>(fn: () => T) => {
    const memo = createMemo(fn);
    return {
      read: memo.get,
    };
  },
  effect: createEffect,
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
