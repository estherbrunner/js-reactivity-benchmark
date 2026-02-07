import type { ReactiveFramework } from "../util/reactiveFramework";

/* === Internal Types === */

type SourceFields<T extends {}> = {
  value: T;
  sinks: Edge | null;
  sinksTail: Edge | null;
  stop?: Cleanup;
};

type OptionsFields<T extends {}> = {
  equals: (a: unknown, b: unknown) => boolean;
  guard?: Guard<T>;
};

type SinkFields = {
  fn: unknown;
  flags: number;
  sources: Edge | null;
  sourcesTail: Edge | null;
};

type OwnerFields = {
  cleanup: Cleanup | Cleanup[] | null;
};

type AsyncFields = {
  controller: AbortController | undefined;
  error: Error | undefined;
};

type RefNode<T extends {}> = SourceFields<T>;

type StateNode<T extends {}> = SourceFields<T> & OptionsFields<T>;

type MemoNode<T extends {}> = SourceFields<T> &
  OptionsFields<T> &
  SinkFields & {
    fn: MemoCallback<T>;
    error: Error | undefined;
  };

type TaskNode<T extends {}> = SourceFields<T> &
  OptionsFields<T> &
  SinkFields &
  AsyncFields & {
    fn: (prev: T, abort: AbortSignal) => Promise<T>;
  };

type EffectNode = SinkFields &
  OwnerFields & {
    fn: EffectCallback;
  };

type Scope = OwnerFields;

type SourceNode = RefNode<unknown & {}>;
type SinkNode = MemoNode<unknown & {}> | TaskNode<unknown & {}> | EffectNode;
type OwnerNode = EffectNode | Scope;

type Edge = {
  version: number;
  source: SourceNode;
  sink: SinkNode;
  nextSource: Edge | null;
  prevSink: Edge | null;
  nextSink: Edge | null;
};

/* === Public API Types === */

/**
 * A cleanup function that can be called to dispose of resources.
 */
type Cleanup = () => void;

// biome-ignore lint/suspicious/noConfusingVoidType: optional Cleanup return type
type MaybeCleanup = Cleanup | undefined | void;

/**
 * A type guard function that validates whether an unknown value is of type T.
 * Used to ensure type safety when updating signals.
 *
 * @template T - The type to guard against
 * @param value - The value to check
 * @returns True if the value is of type T
 */
type Guard<T extends {}> = (value: unknown) => value is T;

/**
 * Options for configuring signal behavior.
 *
 * @template T - The type of value in the signal
 */
type SignalOptions<T extends {}> = {
  /**
   * Optional type guard to validate values.
   * If provided, will throw an error if an invalid value is set.
   */
  guard?: Guard<T>;

  /**
   * Optional custom equality function.
   * Used to determine if a new value is different from the old value.
   * Defaults to reference equality (===).
   */
  equals?: (a: unknown, b: unknown) => boolean;
};

type ComputedOptions<T extends {}> = SignalOptions<T> & {
  /**
   * Optional initial value.
   * Useful for reducer patterns so that calculations start with a value of correct type.
   */
  value?: T;
};

/**
 * A callback function for memos that computes a value based on the previous value.
 *
 * @template T - The type of value computed
 * @param prev - The previous computed value
 * @returns The new computed value
 */
type MemoCallback<T extends {}> = (prev: T) => T;

/**
 * A callback function for tasks that asynchronously computes a value.
 *
 * @template T - The type of value computed
 * @param prev - The previous computed value
 * @param signal - An AbortSignal that will be triggered if the task is aborted
 * @returns A promise that resolves to the new computed value
 * /
type TaskCallback<T extends {}> = (prev: T, signal: AbortSignal) => Promise<T>; */

/**
 * A callback function for effects that can perform side effects.
 *
 * @param match - A function to register cleanup callbacks that will be called before the effect re-runs or is disposed
 */
type EffectCallback = () => MaybeCleanup;

/**
 * A callback function for states that updates a value based on the previous value.
 *
 * @template T - The type of value
 * @param prev - The previous state value
 * @returns The new state value
 */
type UpdateCallback<T extends {}> = (prev: T) => T;

/**
 * A mutable reactive state container.
 * Changes to the state will automatically propagate to dependent computations and effects.
 *
 * @template T - The type of value stored in the state
 */
type State<T extends {}> = {
  readonly [Symbol.toStringTag]: "State";

  /**
   * Gets the current value of the state.
   * When called inside a memo, task, or effect, creates a dependency.
   * @returns The current value
   */
  get(): T;

  /**
   * Sets a new value for the state.
   * If the new value is different (according to the equality function), all dependents will be notified.
   * @param next - The new value to set
   */
  set(next: T): void;

  /**
   * Updates the state with a new value computed by a callback function.
   * The callback receives the current value as an argument.
   * @param fn - The callback function to compute the new value
   */
  update(fn: UpdateCallback<T>): void;
};

/**
 * A derived reactive computation that caches its result.
 * Automatically tracks dependencies and recomputes when they change.
 *
 * @template T - The type of value computed by the memo
 */
type Memo<T extends {}> = {
  readonly [Symbol.toStringTag]: "Memo";

  /**
   * Gets the current value of the memo.
   * Recomputes if dependencies have changed since last access.
   * When called inside another reactive context, creates a dependency.
   * @returns The computed value
   */
  get(): T;
};

/**
 * An asynchronous reactive computation (colorless async).
 * Automatically tracks dependencies and re-executes when they change.
 * Provides abort semantics and pending state tracking.
 *
 * @template T - The type of value resolved by the task
 * /
type Task<T extends {}> = {
  readonly [Symbol.toStringTag]: "Task";

  /**
   * Gets the current value of the task.
   * Returns the last resolved value, even while a new computation is pending.
   * When called inside another reactive context, creates a dependency.
   * @returns The current value
   * /
  get(): T;

  /**
   * Checks if the task is currently executing.
   * @returns True if a computation is in progress
   * /
  isPending(): boolean;

  /**
   * Aborts the current computation if one is running.
   * The task's AbortSignal will be triggered.
   * /
  abort(): void;
}; */

/* === Constants === */

const TYPE_STATE = "State";
const TYPE_MEMO = "Memo";
const TYPE_TASK = "Task";

const FLAG_CLEAN = 0;
const FLAG_CHECK = 1 << 0;
const FLAG_DIRTY = 1 << 1;
const FLAG_RUNNING = 1 << 2;

/* === Module State === */

let activeSink: SinkNode | null = null;
let activeOwner: OwnerNode | null = null;
const queuedEffects: EffectNode[] = [];
let batchDepth = 0;
let flushing = false;
let currentVersion = 0;

/* === Utility Functions === */

const defaultEquals = /*#__PURE__*/ (a: unknown, b: unknown) => a === b;

const isFunction = /*#__PURE__*/ <T>(
  fn: unknown,
): fn is (...args: unknown[]) => T => typeof fn === "function";

const isSyncFunction = /*#__PURE__*/ <T extends unknown & { then?: undefined }>(
  fn: unknown,
): fn is (...args: unknown[]) => T =>
  isFunction(fn) && fn.constructor.name !== "AsyncFunction";

/* const isAsyncFunction = /*#__PURE__* / <T>(
  fn: unknown,
): fn is (...args: unknown[]) => Promise<T> =>
  isFunction(fn) && fn.constructor.name === "AsyncFunction"; */

/* === Link Management === */

const link = (source: SourceNode, sink: SinkNode) => {
  const prevSource = sink.sourcesTail;
  if (prevSource?.source === source) return;

  let nextSource: Edge | null = null;
  const isRecomputing = sink.flags & FLAG_RUNNING;
  if (isRecomputing) {
    nextSource = prevSource ? prevSource.nextSource : sink.sources;
    if (nextSource?.source === source) {
      nextSource.version = currentVersion;
      sink.sourcesTail = nextSource;
      return;
    }
  }

  const prevSink = source.sinksTail;
  if (prevSink?.sink === sink && prevSink.version === currentVersion) return;

  const newEdge = {
    version: currentVersion,
    source,
    sink,
    nextSource,
    prevSink,
    nextSink: null,
  };
  sink.sourcesTail = source.sinksTail = newEdge;
  if (prevSource) prevSource.nextSource = newEdge;
  else sink.sources = newEdge;
  if (prevSink) prevSink.nextSink = newEdge;
  else source.sinks = newEdge;
};

const unlink = (edge: Edge) => {
  const { source, nextSource, nextSink, prevSink } = edge;

  if (nextSink) nextSink.prevSink = prevSink;
  else source.sinksTail = prevSink;
  if (prevSink) prevSink.nextSink = nextSink;
  else source.sinks = nextSink;

  if (!source.sinks && source.stop) {
    source.stop();
    source.stop = undefined;
  }

  return nextSource;
};

const trimSources = (node: SinkNode) => {
  const tail = node.sourcesTail;
  let source = tail ? tail.nextSource : node.sources;
  while (source) source = unlink(source);
  if (tail) tail.nextSource = null;
  else node.sources = null;
};

/* === Propagation === */

const propagate = (node: SinkNode, newFlag = FLAG_DIRTY) => {
  const flags = node.flags;

  if ("sinks" in node) {
    if ((flags & (FLAG_DIRTY | FLAG_CHECK)) >= newFlag) return;

    node.flags = flags | newFlag;

    // Abort in-flight work when sources change
    if ("controller" in node && node.controller) {
      node.controller.abort();
      node.controller = undefined;
    }

    // Propagate Check to sinks
    for (let e = node.sinks; e; e = e.nextSink) propagate(e.sink, FLAG_CHECK);
  } else {
    if (flags & FLAG_DIRTY) return;

    // Enqueue effect for later execution
    node.flags = FLAG_DIRTY;
    queuedEffects.push(node as EffectNode);
  }
};

/* === State Management === */

const setState = <T extends {}>(node: StateNode<T>, next: T): void => {
  if (node.equals(node.value, next)) return;

  node.value = next;
  for (let e = node.sinks; e; e = e.nextSink) propagate(e.sink);
  if (batchDepth === 0) flush();
};

/* === Cleanup Management === */

const registerCleanup = (owner: OwnerNode, fn: Cleanup): void => {
  if (!owner.cleanup) owner.cleanup = fn;
  else if (Array.isArray(owner.cleanup)) owner.cleanup.push(fn);
  else owner.cleanup = [owner.cleanup, fn];
};

const runCleanup = (owner: OwnerNode): void => {
  if (!owner.cleanup) return;

  if (Array.isArray(owner.cleanup))
    for (let i = 0; i < owner.cleanup.length; i++) owner.cleanup[i]();
  else owner.cleanup();
  owner.cleanup = null;
};

/* === Recomputation === */

const recomputeMemo = (node: MemoNode<unknown & {}>) => {
  const prevWatcher = activeSink;
  activeSink = node;
  ++currentVersion;
  node.sourcesTail = null;
  node.flags = FLAG_RUNNING;

  let changed = false;
  try {
    const next = node.fn(node.value);
    if (node.error || !node.equals(next, node.value)) {
      node.value = next;
      node.error = undefined;
      changed = true;
    }
  } catch (err: unknown) {
    changed = true;
    node.error = err instanceof Error ? err : new Error(String(err));
  } finally {
    activeSink = prevWatcher;
    trimSources(node);
  }

  if (changed) {
    for (let e = node.sinks; e; e = e.nextSink)
      if (e.sink.flags & FLAG_CHECK) e.sink.flags |= FLAG_DIRTY;
  }

  node.flags = FLAG_CLEAN;
};

const recomputeTask = (node: TaskNode<unknown & {}>) => {
  node.controller?.abort();

  const controller = new AbortController();
  node.controller = controller;
  node.error = undefined;

  const prevWatcher = activeSink;
  activeSink = node;
  ++currentVersion;
  node.sourcesTail = null;
  node.flags = FLAG_RUNNING;

  let promise: Promise<unknown & {}>;
  try {
    promise = node.fn(node.value, controller.signal);
  } catch (err) {
    node.controller = undefined;
    node.error = err instanceof Error ? err : new Error(String(err));
    return;
  } finally {
    activeSink = prevWatcher;
    trimSources(node);
  }

  promise.then(
    (next) => {
      if (controller.signal.aborted) return;

      node.controller = undefined;
      if (node.error || !node.equals(next, node.value)) {
        node.value = next;
        node.error = undefined;
        for (let e = node.sinks; e; e = e.nextSink) propagate(e.sink);
        if (batchDepth === 0) flush();
      }
    },
    (err: unknown) => {
      if (controller.signal.aborted) return;

      node.controller = undefined;
      const error = err instanceof Error ? err : new Error(String(err));
      if (
        !node.error ||
        error.name !== node.error.name ||
        error.message !== node.error.message
      ) {
        // We don't clear old value on errors
        node.error = error;
        for (let e = node.sinks; e; e = e.nextSink) propagate(e.sink);
        if (batchDepth === 0) flush();
      }
    },
  );

  node.flags = FLAG_CLEAN;
};

const runEffect = (node: EffectNode) => {
  runCleanup(node);
  const prevContext = activeSink;
  const prevOwner = activeOwner;
  activeSink = activeOwner = node;
  ++currentVersion;
  node.sourcesTail = null;
  node.flags = FLAG_RUNNING;

  try {
    const out = node.fn();
    if (typeof out === "function") registerCleanup(node, out);
  } finally {
    activeSink = prevContext;
    activeOwner = prevOwner;
    trimSources(node);
  }

  node.flags = FLAG_CLEAN;
};

const refresh = (node: SinkNode): void => {
  if (node.flags & FLAG_CHECK) {
    for (let e = node.sources; e; e = e.nextSource) {
      if ("fn" in e.source) refresh(e.source as SinkNode);
      if (node.flags & FLAG_DIRTY) break;
    }
  }

  if (node.flags & FLAG_RUNNING) {
    throw new CircularDependencyError(
      "controller" in node ? TYPE_TASK : "value" in node ? TYPE_MEMO : "Effect",
    );
  }

  if (node.flags & FLAG_DIRTY) {
    if ("controller" in node) recomputeTask(node);
    else if ("value" in node) recomputeMemo(node);
    else runEffect(node);
  } else {
    node.flags = FLAG_CLEAN;
  }
};

/* === Batching === */

const flush = (): void => {
  if (flushing) return;
  flushing = true;
  try {
    for (let i = 0; i < queuedEffects.length; i++) {
      const effect = queuedEffects[i];
      if (effect.flags & FLAG_DIRTY) refresh(effect);
    }
    queuedEffects.length = 0;
  } finally {
    flushing = false;
  }
};

/**
 * Batches multiple signal updates together.
 * Effects will not run until the batch completes.
 * Batches can be nested; effects run when the outermost batch completes.
 *
 * @param fn - The function to execute within the batch
 *
 * @example
 * ```ts
 * const count = createState(0);
 * const double = createMemo(() => count.get() * 2);
 *
 * batch(() => {
 *   count.set(1);
 *   count.set(2);
 *   count.set(3);
 *   // Effects run only once at the end with count = 3
 * });
 * ```
 */
const batch = (fn: () => void): void => {
  batchDepth++;
  try {
    fn();
  } finally {
    batchDepth--;
    if (batchDepth === 0) flush();
  }
};

/* === Scope Management === */

/**
 * Creates a new ownership scope for managing cleanup of nested effects and resources.
 * All effects created within the scope will be automatically disposed when the scope is disposed.
 * Scopes can be nested - disposing a parent scope disposes all child scopes.
 *
 * @template T - The type of value returned by the scope function
 * @param fn - The function to execute within the scope, receives an onCleanup callback
 * @returns A tuple of [result, dispose] where result is the return value of fn and dispose cleans up the scope
 *
 * @example
 * ```ts
 * const [value, dispose] = createScope((onCleanup) => {
 *   const count = createState(0);
 *
 *   createEffect(() => {
 *     console.log(count.get());
 *   });
 *
 *   onCleanup(() => console.log('Scope disposed'));
 *
 *   return count;
 * });
 *
 * dispose(); // Cleans up the effect and runs cleanup callbacks
 * ```
 *
 * @example
 * ```ts
 * // Nested scopes
 * const [outer, disposeOuter] = createScope(() => {
 *   const [inner, disposeInner] = createScope(() => {
 *     // ...
 *   });
 *   // disposeOuter() will also dispose inner scope
 * });
 * ```
 */
const createScope = <T>(
  fn: (onCleanup: (fn: Cleanup) => void) => T,
): [T, Cleanup] => {
  const prevOwner = activeOwner;
  const scope: Scope = { cleanup: null };
  activeOwner = scope;

  try {
    const result = fn((cleanupFn) => registerCleanup(scope, cleanupFn));
    const dispose = () => runCleanup(scope);
    if (prevOwner) registerCleanup(prevOwner, dispose);
    return [result, dispose];
  } finally {
    activeOwner = prevOwner;
  }
};

/* === Errors === */

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

const validateCallback = (
  where: string,
  value: unknown,
  guard: (value: unknown) => boolean = isFunction,
): void => {
  if (!guard(value)) throw new InvalidCallbackError(where, value);
};

/**
 * Error thrown on re-entrance on an already running function.
 */
class CircularDependencyError extends Error {
  /**
   * Constructs a new CircularDependencyError.
   *
   * @param where - The location where the error occurred.
   */
  constructor(where: string) {
    super(`[${where}] Circular dependency detected`);
    this.name = "CircularDependencyError";
  }
}

/**
 * Error thrown when a signal value is null or undefined.
 */
class NullishSignalValueError extends TypeError {
  /**
   * Constructs a new NullishSignalValueError.
   *
   * @param where - The location where the error occurred.
   */
  constructor(where: string) {
    super(`[${where}] Signal value cannot be null or undefined`);
    this.name = "NullishSignalValueError";
  }
}

/**
 * Error thrown when a signal value is invalid.
 */
class InvalidSignalValueError extends TypeError {
  /**
   * Constructs a new InvalidSignalValueError.
   *
   * @param where - The location where the error occurred.
   * @param value - The invalid value.
   */
  constructor(where: string, value: unknown) {
    super(`[${where}] Signal value ${valueString(value)} is invalid`);
    this.name = "InvalidSignalValueError";
  }
}

/**
 * Error thrown when a callback is invalid.
 */
class InvalidCallbackError extends TypeError {
  /**
   * Constructs a new InvalidCallbackError.
   *
   * @param where - The location where the error occurred.
   * @param value - The invalid value.
   */
  constructor(where: string, value: unknown) {
    super(`[${where}] Callback ${valueString(value)} is invalid`);
    this.name = "InvalidCallbackError";
  }
}

/* === Public API === */

/**
 * Creates a mutable reactive state container.
 *
 * @template T - The type of value stored in the state
 * @param value - The initial value
 * @param options - Optional configuration for the state
 * @returns A State object with get() and set() methods
 *
 * @example
 * ```ts
 * const count = createState(0);
 * count.set(1);
 * console.log(count.get()); // 1
 * ```
 *
 * @example
 * ```ts
 * // With type guard
 * const count = createState(0, {
 *   guard: (v): v is number => typeof v === 'number'
 * });
 * ```
 */
const createState = <T extends {}>(
  value: T,
  options?: SignalOptions<T>,
): State<T> => {
  validateSignalValue(TYPE_STATE, value, options?.guard);

  const node: StateNode<T> = {
    value,
    sinks: null,
    sinksTail: null,
    equals: options?.equals ?? defaultEquals,
    guard: options?.guard,
  };

  const state: State<T> = {
    [Symbol.toStringTag]: TYPE_STATE,
    get(): T {
      if (activeSink) link(node, activeSink);
      return node.value;
    },
    set(next: T): void {
      validateSignalValue(TYPE_STATE, next, node.guard);
      setState(node, next);
    },
    update(fn: UpdateCallback<T>): void {
      validateCallback(TYPE_STATE, fn);
      const next = fn(node.value);
      validateSignalValue(TYPE_STATE, next, node.guard);
      setState(node, next);
    },
  };

  return state;
};

/**
 * Creates a derived reactive computation that caches its result.
 * The computation automatically tracks dependencies and recomputes when they change.
 * Uses lazy evaluation - only computes when the value is accessed.
 *
 * @template T - The type of value computed by the memo
 * @param fn - The computation function that receives the previous value
 * @param options - Optional configuration for the memo
 * @returns A Memo object with a get() method
 *
 * @example
 * ```ts
 * const count = createState(0);
 * const doubled = createMemo(() => count.get() * 2);
 * console.log(doubled.get()); // 0
 * count.set(5);
 * console.log(doubled.get()); // 10
 * ```
 *
 * @example
 * ```ts
 * // Using previous value
 * const sum = createMemo((prev) => prev + count.get(), { value: 0, equals: Object.is });
 * ```
 */
const createMemo = <T extends {}>(
  fn: MemoCallback<T>,
  options?: ComputedOptions<T>,
): Memo<T> => {
  validateCallback(TYPE_MEMO, fn, isSyncFunction);
  if (options?.value !== undefined)
    validateSignalValue(TYPE_MEMO, options.value, options?.guard);

  const node: MemoNode<T> = {
    fn,
    value: options?.value as T,
    flags: FLAG_DIRTY,
    sources: null,
    sourcesTail: null,
    sinks: null,
    sinksTail: null,
    equals: options?.equals ?? defaultEquals,
    error: undefined,
  };

  return {
    [Symbol.toStringTag]: TYPE_MEMO,
    get() {
      if (activeSink) link(node, activeSink);
      refresh(node as unknown as SinkNode);
      if (node.error) throw node.error;
      return node.value;
    },
  };
};

/**
 * Creates an asynchronous reactive computation (colorless async).
 * The computation automatically tracks dependencies and re-executes when they change.
 * Provides abort semantics - in-flight computations are aborted when dependencies change.
 *
 * @template T - The type of value resolved by the task
 * @param fn - The async computation function that receives the previous value and an AbortSignal
 * @param options - Optional configuration for the task
 * @returns A Task object with get(), isPending(), abort(), and stop() methods
 *
 * @example
 * ```ts
 * const userId = createState(1);
 * const user = createTask(async (prev, signal) => {
 *   const response = await fetch(`/api/users/${userId.get()}`, { signal });
 *   return response.json();
 * });
 *
 * // When userId changes, the previous fetch is aborted
 * userId.set(2);
 * ```
 *
 * @example
 * ```ts
 * // Check pending state
 * if (user.isPending()) {
 *   console.log('Loading...');
 * }
 * ```
 * /
const createTask = <T extends {}>(
  fn: TaskCallback<T>,
  options?: ComputedOptions<T>,
): Task<T> => {
  validateCallback(TYPE_TASK, fn, isAsyncFunction);
  if (options?.value !== undefined)
    validateSignalValue(TYPE_TASK, options.value, options?.guard);

  const node: TaskNode<T> = {
    fn,
    value: options?.value as T,
    sources: null,
    sourcesTail: null,
    sinks: null,
    sinksTail: null,
    flags: FLAG_DIRTY,
    equals: options?.equals ?? defaultEquals,
    controller: undefined,
    error: undefined,
  };

  return {
    [Symbol.toStringTag]: TYPE_TASK,
    get(): T {
      if (activeSink) link(node, activeSink);
      refresh(node as unknown as SinkNode);
      if (node.error) throw node.error;
      return node.value;
    },
    isPending(): boolean {
      return !node.controller;
    },
    abort(): void {
      node.controller?.abort();
      node.controller = undefined;
    },
  };
}; */

/**
 * Creates a reactive effect that automatically runs when its dependencies change.
 * Effects run immediately upon creation and re-run when any tracked signal changes.
 * Effects are executed during the flush phase, after all updates have been batched.
 *
 * @param fn - The effect function that can track dependencies and register cleanup callbacks
 * @returns A cleanup function that can be called to dispose of the effect
 *
 * @example
 * ```ts
 * const count = createState(0);
 * const dispose = createEffect(() => {
 *   console.log('Count is:', count.get());
 * });
 *
 * count.set(1); // Logs: "Count is: 1"
 * dispose(); // Stop the effect
 * ```
 *
 * @example
 * ```ts
 * // With cleanup
 * createEffect((onCleanup) => {
 *   const timer = setInterval(() => console.log(count.get()), 1000);
 *   onCleanup(() => clearInterval(timer));
 * });
 * ```
 */
const createEffect = (fn: EffectCallback): Cleanup => {
  validateCallback("Effect", fn);

  const node: EffectNode = {
    fn,
    flags: FLAG_DIRTY,
    sources: null,
    sourcesTail: null,
    cleanup: null,
  };

  const dispose = () => {
    runCleanup(node);
    node.fn = undefined as unknown as EffectCallback;
    node.flags = FLAG_CLEAN;
    node.sourcesTail = null;
    trimSources(node);
  };

  if (activeOwner) registerCleanup(activeOwner, dispose);

  runEffect(node);

  return dispose;
};

/* === Framework Export === */

let rootScope: Scope | null = null;
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
  effect: (fn) => {
    createEffect(() => fn());
  },
  withBatch: (fn) => batch(fn),
  withBuild: <T>(fn: () => T) => {
    const [result, dispose] = createScope(() => fn());
    rootScope = { cleanup: dispose };
    return result;
  },
  cleanup: () => {
    if (rootScope?.cleanup) {
      (rootScope.cleanup as Cleanup)();
      rootScope = null;
    }
  },
};
