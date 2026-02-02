import type { ReactiveFramework } from "../util/reactiveFramework";

export type Cleanup = () => void;
export type Guard<T extends {}> = (value: unknown) => value is T;

type Link = {
  dep: StateNode<unknown & {}> | ConsumerNode;
  sub: ConsumerNode;
  nextDep: Link | null;
  prevSub: Link | null;
  nextSub: Link | null;
};

type StateNode<T extends {}> = {
  value: T;
  subs: Link | null;
  subsTail: Link | null;
  equals: (a: unknown, b: unknown) => boolean;
  guard: Guard<T> | undefined;
};

type ConsumerNode =
  | MemoNode<unknown & {}>
  | TaskNode<unknown & {}>
  | EffectNode;

type MemoNode<T extends {}> = {
  fn: (prev: T) => T;
  value: T;
  flags: number;
  deps: Link | null;
  depsTail: Link | null;
  subs: Link | null;
  subsTail: Link | null;
  cleanup: Cleanup | Cleanup[] | null;
  equals: (a: unknown, b: unknown) => boolean;
  guard: Guard<T> | undefined;
};

type TaskNode<T extends {}> = {
  fn: (prev: T, abort: AbortSignal) => Promise<T>;
  value: T;
  flags: number;
  deps: Link | null;
  depsTail: Link | null;
  subs: Link | null;
  subsTail: Link | null;
  cleanup: Cleanup | Cleanup[] | null;
  equals: (a: unknown, b: unknown) => boolean;
  guard: Guard<T> | undefined;
  state: number;
  controller: AbortController | undefined;
  error: Error | undefined;
};

type EffectNode = {
  fn: () => void;
  flags: number;
  deps: Link | null;
  depsTail: Link | null;
  subs: Link | null;
  subsTail: Link | null;
  cleanup: Cleanup | Cleanup[] | null;
};

type Scope = {
  nodes: ConsumerNode[];
  cleanups: Cleanup[];
};

export type State<T extends {}> = {
  get(): T;
  set(next: T): void;
};

export type Memo<T extends {}> = {
  get(): T;
};

export type Task<T extends {}> = {
  get(): T;
  isPending(): boolean;
  abort(): void;
  disconnect(): void;
};

export type MemoCallback<T extends {}> = (prev: T) => T;
export type TaskCallback<T extends {}> = (
  prev: T,
  signal: AbortSignal,
) => Promise<T>;
export type EffectCallback = () => void;

export type SignalOptions<T extends {}> = {
  guard?: Guard<T>;
  equals?: (a: unknown, b: unknown) => boolean;
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

const DEFAULT_EQUALS = (a: unknown, b: unknown) => a === b;

/* === Module State === */

let context: ConsumerNode | null = null;
let currentScope: Scope | null = null;
let rootScope: Scope | null = null;
const queuedEffects: EffectNode[] = [];
let batchDepth = 0;

/* === Link Management === */

const unlinkSub = (link: Link) => {
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
      (dep.subs = nextSub);

  return nextDep;
};

const isValidLink = (checkLink: Link, sub: ConsumerNode): boolean => {
  const depsTail = sub.depsTail;
  if (depsTail) {
    let link = sub.deps;
    while (link) {
      if (link === checkLink) return true;
      if (link === depsTail) break;
      link = link.nextDep;
    }
  }
  return false;
};

const linkSub = (
  dep: StateNode<unknown & {}> | ConsumerNode,
  sub: ConsumerNode,
) => {
  const prevDep = sub.depsTail;
  if (prevDep && prevDep.dep === dep) return;

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
  if (
    prevSub &&
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
};

const trimDeps = (node: ConsumerNode) => {
  const tail = node.depsTail;
  let toRemove = tail ? tail.nextDep : node.deps;
  while (toRemove) toRemove = unlinkSub(toRemove);
  if (tail) tail.nextDep = null;
  else node.deps = null;
};

const unwatchNode = (node: ConsumerNode) => {
  let dep = node.deps;
  while (dep) dep = unlinkSub(dep);
  node.deps = null;
  if (node.cleanup) runCleanup(node);
};

/* === Cleanup Management === */

const runCleanup = (node: ConsumerNode) => {
  if (!node.cleanup) return;

  if (Array.isArray(node.cleanup))
    for (let i = 0; i < node.cleanup.length; i++) node.cleanup[i]();
  else node.cleanup();
  node.cleanup = null;
};

export const onCleanup = (fn: Cleanup): void => {
  if (!context) return;

  const node = context;

  if (!node.cleanup) node.cleanup = fn;
  else if (Array.isArray(node.cleanup)) node.cleanup.push(fn);
  else node.cleanup = [node.cleanup, fn];
};

/* === Propagation === */

const markNode = (node: ConsumerNode, newFlag = FLAG_DIRTY) => {
  const flags = node.flags;
  if ((flags & (FLAG_DIRTY | FLAG_CHECK)) >= newFlag) return;

  node.flags = flags | newFlag;

  // Special handling for tasks: abort in-flight work when dependencies change
  if ("state" in node && node.state === TASK_PENDING) {
    node.controller?.abort();
    node.controller = undefined;
    node.state = TASK_ABORTED;
  }

  // Effects have no value field - collect them for later execution
  if (!("value" in node) && !(flags & (FLAG_DIRTY | FLAG_CHECK))) {
    queuedEffects.push(node as EffectNode);
    return;
  }

  // Propagate Check to subscribers
  for (let link = node.subs; link !== null; link = link.nextSub)
    markNode(link.sub, FLAG_CHECK);
};

/* === Recomputation === */

const recomputeMemo = (el: MemoNode<unknown & {}>) => {
  if (el.cleanup) runCleanup(el);
  const prevContext = context;
  context = el;
  el.depsTail = null;
  el.flags = FLAG_RUNNING;
  const value = el.fn(el.value);
  context = prevContext;

  if (el.deps) trimDeps(el);

  if (!el.equals(value, el.value)) {
    el.value = value;

    for (let s = el.subs; s !== null; s = s.nextSub) {
      const o = s.sub;
      const flags = o.flags;
      flags & FLAG_CHECK
        ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
          (o.flags = flags | FLAG_DIRTY)
        : markNode(o);
    }
  }

  el.flags = FLAG_CLEAN;
};

const recomputeTask = (el: TaskNode<unknown & {}>) => {
  if (el.state === TASK_PENDING) return;

  el.controller?.abort();

  const controller = new AbortController();
  el.controller = controller;

  const oldValue = el.value;

  el.state = TASK_PENDING;
  el.error = undefined;

  if (el.cleanup) runCleanup(el);
  const prevContext = context;
  context = el;
  el.depsTail = null;
  el.flags = FLAG_RUNNING;

  let promise: Promise<unknown & {}>;
  try {
    promise = el.fn(oldValue, controller.signal);

    if (el.deps) trimDeps(el);
  } catch (e) {
    context = prevContext;
    el.state = TASK_ERROR;
    el.controller = undefined;
    el.error = e instanceof Error ? e : new Error(String(e));
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

      if (!el.equals(next, el.value)) {
        el.value = next;

        for (let s = el.subs; s !== null; s = s.nextSub) {
          const o = s.sub;
          const flags = o.flags;
          flags & FLAG_CHECK
            ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
              (o.flags = flags | FLAG_DIRTY)
            : markNode(o);
        }
      }

      el.flags = FLAG_CLEAN;
    },
    (err: unknown) => {
      if (controller.signal.aborted) return;

      el.controller = undefined;
      el.state = TASK_ERROR;
      el.error = err instanceof Error ? err : new Error(String(err));

      for (let s = el.subs; s !== null; s = s.nextSub) {
        const o = s.sub;
        const flags = o.flags;
        flags & FLAG_CHECK
          ? // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
            (o.flags = flags | FLAG_DIRTY)
          : markNode(o, FLAG_CHECK);
      }

      el.flags = FLAG_CLEAN;
    },
  );
};

const runEffect = (el: EffectNode) => {
  if (el.cleanup) runCleanup(el);
  const prevContext = context;
  context = el;
  el.depsTail = null;
  el.flags = FLAG_RUNNING;
  el.fn();
  context = prevContext;

  if (el.deps) trimDeps(el);

  el.flags = FLAG_CLEAN;
};

const updateIfNecessary = (el: ConsumerNode): void => {
  if (el.flags & FLAG_CHECK) {
    for (let d = el.deps; d !== null; d = d.nextDep) {
      if ("fn" in d.dep) updateIfNecessary(d.dep);
      if (el.flags & FLAG_DIRTY) break;
    }
  }

  if (el.flags & FLAG_DIRTY) {
    if ("state" in el) recomputeTask(el as TaskNode<unknown & {}>);
    else if ("value" in el) recomputeMemo(el as MemoNode<unknown & {}>);
    else runEffect(el as EffectNode);
  }

  el.flags = FLAG_CLEAN;
};

/* === Batching === */

const flush = (): void => {
  for (let i = 0; i < queuedEffects.length; i++) {
    const effect = queuedEffects[i];
    if (effect.flags & (FLAG_DIRTY | FLAG_CHECK)) updateIfNecessary(effect);
  }
  queuedEffects.length = 0;
};

export const batch = (fn: () => void): void => {
  batchDepth++;
  try {
    fn();
  } finally {
    batchDepth--;
    if (batchDepth === 0) flush();
  }
};

/* === Signal Creation === */

export const createState = <T extends {}>(
  value: T,
  options?: SignalOptions<T>,
): State<T> => {
  validateSignalValue("State", value, options?.guard);

  const node: StateNode<T> = {
    value,
    subs: null,
    subsTail: null,
    equals: options?.equals ?? DEFAULT_EQUALS,
    guard: options?.guard,
  };

  return {
    get(): T {
      if (context) linkSub(node, context);
      return node.value;
    },
    set(next: T): void {
      validateSignalValue("State", next, node.guard);

      if (node.equals?.(node.value, next)) return;
      node.value = next;
      for (let link = node.subs; link; link = link.nextSub) markNode(link.sub);
      if (batchDepth === 0) flush();
    },
  };
};

export const createMemo = <T extends {}>(
  fn: MemoCallback<T>,
  options?: SignalOptions<T>,
): Memo<T> => {
  const node: MemoNode<T> = {
    fn,
    value: undefined as unknown as T,
    flags: FLAG_DIRTY,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    cleanup: null,
    equals: options?.equals ?? DEFAULT_EQUALS,
    guard: options?.guard,
  };

  if (currentScope) currentScope.nodes.push(node as unknown as ConsumerNode);

  return {
    get() {
      updateIfNecessary(node as unknown as ConsumerNode);
      if (context) linkSub(node, context);
      return node.value;
    },
  };
};

export const createTask = <T extends {}>(
  fn: TaskCallback<T>,
  initialValue: T,
  options?: SignalOptions<T>,
): Task<T> => {
  validateSignalValue("Task", initialValue, options?.guard);

  const node: TaskNode<T> = {
    cleanup: null,
    fn,
    value: initialValue,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: FLAG_DIRTY,
    equals: options?.equals ?? DEFAULT_EQUALS,
    guard: options?.guard,
    state: TASK_IDLE,
    controller: undefined,
    error: undefined,
  };

  const abortTask = () => {
    node.controller?.abort();
    node.controller = undefined;
    if (node.state === TASK_PENDING) node.state = TASK_ABORTED;
  };

  return {
    get(): T {
      updateIfNecessary(node as unknown as ConsumerNode);
      if (context) linkSub(node, context);
      if (node.error) throw node.error;
      return node.value;
    },
    isPending(): boolean {
      return node.state === TASK_PENDING;
    },
    abort(): void {
      abortTask();
    },
    disconnect(): void {
      abortTask();

      let dep = node.deps;
      while (dep) dep = unlinkSub(dep);
      node.deps = null;
      node.subs = null;
      node.subsTail = null;
      node.error = undefined;
      node.state = TASK_IDLE;
    },
  };
};

export const createEffect = (fn: () => void) => {
  const node: EffectNode = {
    fn,
    flags: FLAG_DIRTY,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    cleanup: null,
  };

  if (currentScope) currentScope.nodes.push(node);
  runEffect(node);

  return () => {
    unwatchNode(node);
  };
};

/* === Scope Management === */

export const createScope = <T>(fn: () => T): T => {
  const prevScope = currentScope;
  const scope: Scope = { nodes: [], cleanups: [] };
  currentScope = scope;
  try {
    const result = fn();
    rootScope = scope;
    return result;
  } finally {
    currentScope = prevScope;
  }
};

const cleanupScope = () => {
  if (!rootScope) return;

  for (const node of rootScope.nodes) unwatchNode(node);
  rootScope.nodes.length = 0;

  for (const cleanup of rootScope.cleanups) cleanup();
  rootScope.cleanups.length = 0;
  rootScope = null;
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

export class NullishSignalValueError extends Error {
  constructor(where: string) {
    super(`[${where}] Signal value cannot be null or undefined`);
    this.name = "NullishSignalValueError";
  }
}

export class InvalidSignalValueError extends Error {
  constructor(where: string, value: unknown) {
    super(`[${where}] Signal value ${valueString(value)} is invalid`);
    this.name = "InvalidSignalValueError";
  }
}

/* === Framework Export === */

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
    const dispose = createEffect(fn);
    if (currentScope) currentScope.cleanups.push(dispose);
  },
  withBatch: (fn) => batch(fn),
  withBuild: <T>(fn: () => T) => {
    return createScope(fn);
  },
  cleanup: () => {
    cleanupScope();
  },
};
