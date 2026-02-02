import type { ReactiveFramework } from "../util/reactiveFramework";

type Link = {
  source: SourceNode;
  target: Computation<unknown>;
  prevSub: Link | null;
  nextSub: Link | null;
  nextDep: Link | null;
};

type SignalNode<T> = {
  value: T;
  subsHead: Link | null;
  subsTail: Link | null;
};

type Computation<T> = {
  fn: () => T;
  value: T;
  flags: number;
  depsHead: Link | null;
  depsTail: Link | null;
  subsHead: Link | null;
  subsTail: Link | null;
};

type SourceNode = SignalNode<unknown> | Computation<unknown>;

type Scope = {
  comps: Computation<unknown>[];
};

const FLAG_CHECK = 1 << 0;
const FLAG_DIRTY = 1 << 1;
const FLAG_RUNNING = 1 << 2;
const FLAG_EFFECT = 1 << 3;

let current: Computation<unknown> | null = null;
let currentScope: Scope | null = null;
let rootScope: Scope | null = null;
const queuedEffects: Computation<unknown>[] = [];
let queuedHead = 0;
let batchDepth = 0;

const unlinkSub = (source: SourceNode, link: Link) => {
  const prev = link.prevSub;
  const next = link.nextSub;
  if (prev) prev.nextSub = next;
  else source.subsHead = next;
  if (next) next.prevSub = prev;
  else source.subsTail = prev;
  link.prevSub = null;
  link.nextSub = null;
};

const isValidLink = (checkLink: Link, sub: Computation<unknown>): boolean => {
  const depsTail = sub.depsTail;
  if (depsTail) {
    let link = sub.depsHead;
    while (link) {
      if (link === checkLink) return true;
      if (link === depsTail) break;
      link = link.nextDep;
    }
  }
  return false;
};

const unlinkDep = (target: Computation<unknown>, link: Link) => {
  let prev: Link | null = null;
  let cur = target.depsHead;
  while (cur) {
    if (cur === link) {
      if (prev) prev.nextDep = cur.nextDep;
      else target.depsHead = cur.nextDep;
      if (target.depsTail === cur) target.depsTail = prev;
      break;
    }
    prev = cur;
    cur = cur.nextDep;
  }
  link.nextDep = null;
};

const linkSub = (source: SourceNode, sub: Computation<unknown>) => {
  const prevDep = sub.depsTail;
  if (prevDep && prevDep.source === source) return;

  let nextDep: Link | null = null;
  const isRecomputing = sub.flags & FLAG_RUNNING;
  if (isRecomputing) {
    nextDep = prevDep ? prevDep.nextDep : sub.depsHead;
    if (nextDep && nextDep.source === source) {
      sub.depsTail = nextDep;
      return;
    }
  }

  const prevSub = source.subsTail;
  if (
    prevSub &&
    prevSub.target === sub &&
    (!isRecomputing || isValidLink(prevSub, sub))
  )
    return;

  const newLink =
    // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
    (sub.depsTail =
    // biome-ignore lint/suspicious/noAssignInExpressions: micro-optimization
    source.subsTail =
      {
        source,
        target: sub,
        prevSub,
        nextSub: null,
        nextDep,
      });

  if (prevDep) prevDep.nextDep = newLink;
  else sub.depsHead = newLink;

  if (prevSub) prevSub.nextSub = newLink;
  else source.subsHead = newLink;
};

const trimDeps = (comp: Computation<unknown>) => {
  const tail = comp.depsTail;
  let toRemove = tail ? tail.nextDep : comp.depsHead;
  while (toRemove) {
    unlinkSub(toRemove.source, toRemove);
    toRemove = toRemove.nextDep;
  }
  if (tail) tail.nextDep = null;
  else comp.depsHead = null;
};

const flushEffects = () => {
  while (queuedHead < queuedEffects.length)
    ensureFresh(queuedEffects[queuedHead++]);
  if (queuedHead) queuedEffects.length = queuedHead = 0;
};

const markNode = (comp: Computation<unknown>, newFlag = FLAG_DIRTY) => {
  const flags = comp.flags;
  if ((flags & (FLAG_DIRTY | FLAG_CHECK)) >= newFlag) return;
  comp.flags = flags | newFlag;
  if (comp.flags & FLAG_EFFECT && !(flags & (FLAG_DIRTY | FLAG_CHECK)))
    queuedEffects.push(comp);
  for (let link = comp.subsHead; link; link = link.nextSub)
    markNode(link.target, FLAG_CHECK);
};

const invalidateSource = (source: SourceNode) => {
  let link = source.subsHead;
  while (link) {
    markNode(link.target, FLAG_DIRTY);
    link = link.nextSub;
  }
};

const runComputed = <T>(comp: Computation<T>) => {
  if (comp.flags & FLAG_RUNNING) return;
  const prev = current;
  current = comp;
  comp.flags = (comp.flags & FLAG_EFFECT) | FLAG_RUNNING;
  comp.depsTail = null;
  let nextValue: T;
  try {
    nextValue = comp.fn();
  } finally {
    current = prev;
  }
  if (comp.depsHead) trimDeps(comp);
  let changed = false;
  if (comp.value !== nextValue) {
    comp.value = nextValue;
    changed = true;
  } else {
    comp.value = nextValue;
  }
  comp.flags &= ~(FLAG_DIRTY | FLAG_CHECK | FLAG_RUNNING);
  if (changed) invalidateSource(comp);
};

const runEffect = (comp: Computation<unknown>) => {
  if (comp.flags & FLAG_RUNNING) return;
  const prev = current;
  current = comp;
  comp.flags = (comp.flags & FLAG_EFFECT) | FLAG_RUNNING;
  comp.depsTail = null;
  try {
    comp.fn();
  } finally {
    current = prev;
  }
  if (comp.depsHead) trimDeps(comp);
  comp.flags &= ~(FLAG_DIRTY | FLAG_CHECK | FLAG_RUNNING);
};

const ensureFresh = (comp: Computation<unknown>) => {
  if (!(comp.flags & (FLAG_DIRTY | FLAG_CHECK))) return;
  if (comp.flags & FLAG_RUNNING) return;

  if (comp.flags & FLAG_CHECK) {
    if (!comp.depsHead) {
      comp.flags &= ~(FLAG_DIRTY | FLAG_CHECK);
      return;
    }
    for (let link: Link | null = comp.depsHead; link; link = link.nextDep) {
      const source = link.source as Computation<unknown>;
      if (source.flags & (FLAG_DIRTY | FLAG_CHECK)) ensureFresh(source);
      if (comp.flags & FLAG_DIRTY) break;
    }
  }
  if (comp.flags & FLAG_DIRTY) {
    if (comp.flags & FLAG_EFFECT) runEffect(comp);
    else runComputed(comp);
  }
  comp.flags &= ~(FLAG_DIRTY | FLAG_CHECK);
};

const disposeComputation = (comp: Computation<unknown>) => {
  let link = comp.depsHead;
  while (link) {
    const next = link.nextDep;
    unlinkSub(link.source, link);
    link.nextDep = null;
    link = next;
  }
  comp.depsHead = null;
  comp.depsTail = null;

  link = comp.subsHead;
  while (link) {
    const next = link.nextSub;
    unlinkDep(link.target, link);
    unlinkSub(comp, link);
    link = next;
  }
  comp.subsHead = null;
  comp.subsTail = null;
};

const createSignal = <T>(value: T): [() => T, (next: T) => void] => {
  const node: SignalNode<T> = {
    value,
    subsHead: null,
    subsTail: null,
  };
  const get = (): T => {
    if (current) linkSub(node, current);
    return node.value;
  };
  const set = (next: T) => {
    if (node.value !== next) {
      node.value = next;
      invalidateSource(node);
      if (!batchDepth) flushEffects();
    }
  };
  return [get, set];
};

const createComputed = <T>(fn: () => T) => {
  const comp: Computation<T> = {
    fn,
    value: undefined as T,
    flags: FLAG_DIRTY,
    depsHead: null,
    depsTail: null,
    subsHead: null,
    subsTail: null,
  };
  if (currentScope) currentScope.comps.push(comp);
  return () => {
    ensureFresh(comp);
    if (current) linkSub(comp, current);
    return comp.value as T;
  };
};

const createEffect = (fn: () => void) => {
  const comp: Computation<void> = {
    fn,
    value: undefined,
    flags: FLAG_DIRTY | FLAG_EFFECT,
    depsHead: null,
    depsTail: null,
    subsHead: null,
    subsTail: null,
  };
  if (currentScope) currentScope.comps.push(comp);
  runEffect(comp);
};

const batch = (fn: () => void): void => {
  batchDepth++;
  try {
    fn();
  } finally {
    batchDepth--;
    if (!batchDepth) flushEffects();
  }
};

export const baselineFramework: ReactiveFramework = {
  name: "baseline",
  signal: <T>(initialValue: T) => {
    const [get, set] = createSignal(initialValue);
    return {
      write: set,
      read: get,
    };
  },
  computed: <T>(fn: () => T) => {
    return {
      read: createComputed(fn),
    };
  },
  effect: (fn) => {
    createEffect(fn);
  },
  withBatch: (fn) => batch(fn),
  withBuild: (fn) => {
    const prevScope = currentScope;
    const scope: Scope = { comps: [] };
    currentScope = scope;
    try {
      const result = fn();
      rootScope = scope;
      return result;
    } finally {
      currentScope = prevScope;
    }
  },
  cleanup: () => {
    if (!rootScope) return;
    for (const comp of rootScope.comps) disposeComputation(comp);
    rootScope.comps.length = 0;
    rootScope = null;
  },
};
