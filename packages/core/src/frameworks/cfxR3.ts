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

export interface Link {
  dep: UnknownSignal | UnknownComputed;
  sub: UnknownComputed;
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
  height: number;
  disposal: Disposable | Disposable[] | null;
  fn: () => T;
  child: UnknownFirewallSignal | null;
}

/* === Constants === */

const CACHE_CLEAN = 0; // Signal value is valid, no need to recompute
const CACHE_CHECK = 1 << 0; // Signal value might be stale, check parent nodes to decide whether to recompute
const CACHE_DIRTY = 1 << 1; // Signal value is invalid, parents have changed, value needs to be recomputed
const CACHE_RECOMPUTING = 1 << 2; // Signal value is being recomputed

/* === Internals === */

let context: UnknownComputed | null = null;
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
  const self: Computed<T> = {
    disposal: null,
    fn: fn,
    value: undefined as unknown as T,
    height: 0,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: CACHE_DIRTY,
    equals: options?.equals || ((a: unknown, b: unknown) => a === b),
  };

  if (context) {
    self.height = context.height + 1;
    link(self, context);
  }

  return self;
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

function updateIfNecessary(el: UnknownComputed): void {
  // If marked Check, recursively update dependencies to see if we're actually dirty
  if (el.flags & CACHE_CHECK) {
    for (let d = el.deps; d !== null; d = d.nextDep) {
      "fn" in d.dep && updateIfNecessary(d.dep);
      // Early exit if dependency recomputation escalated us to Dirty
      if (el.flags & CACHE_DIRTY) break;
    }
  }

  // Only recompute if we're actually Dirty (not just Check)
  el.flags & CACHE_DIRTY && recompute(el);

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

function unwatched(el: UnknownComputed) {
  let dep = el.deps;
  while (dep !== null) dep = unlinkSubs(dep);
  el.deps = null;
  runDisposal(el);
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L52
function link(dep: UnknownSignal | UnknownComputed, sub: UnknownComputed) {
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
function isValidLink(checkLink: Link, sub: UnknownComputed): boolean {
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

export function read<T>(el: Signal<NonNullable<T>> | Computed<T>): T {
  // Link to current reactive context
  if (context) {
    link(el, context);

    const owner = "owner" in el ? el.owner : el;
    if ("fn" in owner) {
      owner.height >= context.height &&
        // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
        (context.height = owner.height + 1);
      if (owner.flags & (CACHE_DIRTY | CACHE_CHECK)) updateIfNecessary(owner);
    }
  } else {
    // Even outside reactive context, update computed if dirty
    const owner = "owner" in el ? el.owner : el;
    if ("fn" in owner && owner.flags & (CACHE_DIRTY | CACHE_CHECK))
      updateIfNecessary(owner);
  }
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

function markNode(el: UnknownComputed, newState = CACHE_DIRTY) {
  const flags = el.flags;
  if ((flags & (CACHE_DIRTY | CACHE_CHECK)) >= newState) return;

  el.flags = flags | newState;

  // Effects have equals === null - collect them for later execution
  if (el.equals === null && !(flags & (CACHE_DIRTY | CACHE_CHECK))) {
    queuedEffects.push(el);
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
    if (effect.flags & (CACHE_DIRTY | CACHE_CHECK)) recompute(effect);
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

function runDisposal(node: UnknownComputed): void {
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

export function getContext(): UnknownComputed | null {
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
    height: 0,
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
    height: 0,
    child: null,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: CACHE_DIRTY,
    equals: null,
  };

  if (context) {
    effect.height = context.height + 1;
    link(effect, context);
  }

  read(effect); // Initial run

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
