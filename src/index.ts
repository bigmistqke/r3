export interface Disposable {
  (): void;
}

export const enum ReactiveFlags {
  None = 0,
  Check = 1 << 0,
  Dirty = 1 << 1,
  RecomputingDeps = 1 << 2,
  InHeap = 1 << 3,
  InHeapHeight = 1 << 4,
  Zombie = 1 << 5,
}

export const enum AsyncFlags {
  None = 0,
  Pending = 1 << 0,
  Error = 1 << 1,
  Uninitialized = 1 << 2,
}

export interface Link {
  dep: Signal<unknown> | Computed<unknown>;
  sub: Computed<unknown>;
  nextDep: Link | null;
  prevSub: Link | null;
  nextSub: Link | null;
}

export interface RawSignal<T> {
  subs: Link | null;
  subsTail: Link | null;
  value: T;
  error?: unknown;
  asyncFlags: AsyncFlags;
  time: number;
  pendingValue: T | typeof NOT_PENDING;
}

interface FirewallSignal<T> extends RawSignal<T> {
  owner: Computed<unknown>;
  nextChild: FirewallSignal<unknown> | null;
}

export type Signal<T> = RawSignal<T> | FirewallSignal<T>;
export interface Owner {
  disposal: Disposable | Disposable[] | null;
  parent: Owner | null;
  firstChild: Owner | null;
  nextSibling: Owner | null;
  pendingDisposal: Disposable | Disposable[] | null;
  pendingFirstChild: Owner | null;
}

export interface Computed<T> extends RawSignal<T>, Owner {
  deps: Link | null;
  depsTail: Link | null;
  flags: ReactiveFlags;
  height: number;
  nextHeap: Computed<any> | undefined;
  prevHeap: Computed<any>;
  fn: (prev?: T) => T;
  child: FirewallSignal<any> | null;
}

export class NotReadyError extends Error {
  constructor(public cause: Computed<unknown>) {
    super();
  }
}

let markedHeap = false;
let stale = false;
let context: Computed<unknown> | null = null;
let clock = 0;
let transition: Transition | null = null;
let asyncNodes: Computed<unknown>[] = [];
let pendingNodes: RawSignal<unknown>[] = [];
interface Transition {
  time: number;
  asyncNodes: Computed<unknown>[];
  pendingNodes: RawSignal<unknown>[];
}
interface Heap {
  heap: (Computed<unknown> | undefined)[];
  min: number;
  max: number;
}
const dirty: Heap = {
  heap: new Array(2000).fill(undefined),
  min: 0,
  max: 0,
};
const pending: Heap = {
  heap: new Array(2000).fill(undefined),
  min: 0,
  max: 0,
};
const NOT_PENDING = {};
export function increaseHeapSize(n: number) {
  if (n > dirty.heap.length) {
    dirty.heap.length = n;
  }
}

function actualInsertIntoHeap(n: Computed<unknown>, heap: Heap) {
  const height = n.height;
  const heapAtHeight = heap.heap[height];
  if (heapAtHeight === undefined) {
    heap.heap[height] = n;
  } else {
    const tail = heapAtHeight.prevHeap;
    tail.nextHeap = n;
    n.prevHeap = tail;
    heapAtHeight.prevHeap = n;
  }
  if (height > heap.max) {
    heap.max = height;
  }
}
function insertIntoHeap(n: Computed<any>, heap: Heap) {
  let flags = n.flags;
  if (flags & (ReactiveFlags.InHeap | ReactiveFlags.RecomputingDeps)) return;
  if (flags & ReactiveFlags.Check) {
    n.flags =
      (flags & ~(ReactiveFlags.Check | ReactiveFlags.Dirty)) |
      ReactiveFlags.Dirty |
      ReactiveFlags.InHeap;
  } else n.flags = flags | ReactiveFlags.InHeap;
  if (!(flags & ReactiveFlags.InHeapHeight)) {
    actualInsertIntoHeap(n, heap);
  }
}

function insertIntoHeapHeight(n: Computed<unknown>, heap: Heap) {
  let flags = n.flags;
  if (
    flags &
    (ReactiveFlags.InHeap |
      ReactiveFlags.RecomputingDeps |
      ReactiveFlags.InHeapHeight)
  )
    return;
  n.flags = flags | ReactiveFlags.InHeapHeight;
  actualInsertIntoHeap(n, heap);
}

function deleteFromHeap(n: Computed<unknown>, heap: Heap) {
  const flags = n.flags;
  if (!(flags & (ReactiveFlags.InHeap | ReactiveFlags.InHeapHeight))) return;
  n.flags = flags & ~(ReactiveFlags.InHeap | ReactiveFlags.InHeapHeight);
  const height = n.height;
  if (n.prevHeap === n) {
    heap.heap[height] = undefined;
  } else {
    const next = n.nextHeap;
    const dhh = heap.heap[height]!;
    const end = next ?? dhh;
    if (n === dhh) {
      heap.heap[height] = next;
    } else {
      n.prevHeap.nextHeap = next;
    }
    end.prevHeap = n.prevHeap;
  }
  n.prevHeap = n;
  n.nextHeap = undefined;
}

export function computed<T>(fn: (prev?: T) => T): Computed<T>;
export function computed<T>(fn: (prev: T) => T, initialValue: T): Computed<T>;
export function computed<T>(
  fn: (prev?: T) => T,
  initialValue?: T
): Computed<T> {
  const self: Computed<T> = {
    disposal: null,
    fn: fn,
    value: initialValue as T,
    height: 0,
    child: null,
    nextHeap: undefined,
    prevHeap: null as any,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    parent: context,
    nextSibling: null,
    firstChild: null,
    flags: ReactiveFlags.None,
    asyncFlags: AsyncFlags.Uninitialized,
    time: clock,
    pendingValue: NOT_PENDING,
    pendingDisposal: null,
    pendingFirstChild: null,
  };
  self.prevHeap = self;
  if (context) {
    const lastChild = context.firstChild;
    if (lastChild === null) {
      context.firstChild = self;
    } else {
      self.nextSibling = lastChild;
      context.firstChild = self;
    }
    if (context.depsTail === null) {
      self.height = context.height;
      recompute(self, true);
    } else {
      self.height = context.height + 1;
      insertIntoHeap(self, dirty);
    }
  } else {
    recompute(self, true);
  }

  return self;
}

export function asyncComputed<T>(
  asyncFn: (prev?: T) => T | Promise<T> | AsyncIterable<T>
): Computed<T>;
export function asyncComputed<T>(
  asyncFn: (prev: T) => T | Promise<T> | AsyncIterable<T>,
  initialValue: T
): Computed<T>;
export function asyncComputed<T>(
  asyncFn: (prev?: T) => T | Promise<T> | AsyncIterable<T>,
  initialValue?: T
): Computed<T> {
  let lastResult = undefined as T | undefined;
  const fn = (prev?: T) => {
    const result = asyncFn(prev);
    lastResult = result as T;
    const isPromise = result instanceof Promise;
    // @ts-expect-error
    const iterator = result[Symbol.asyncIterator];
    if (!isPromise && !iterator) {
      return result as T;
    }
    if (isPromise) {
      result
        .then((v) => {
          if (lastResult !== result) return;
          setSignal(self, v);
          stabilize();
        })
        .catch((e) => {
          if (lastResult !== result) return;
          setError(self, e as Error);
          stabilize();
        });
    } else {
      (async () => {
        try {
          for await (let value of result as AsyncIterable<T>) {
            if (lastResult !== result) return;
            setSignal(self, value);
            stabilize();
          }
        } catch (error) {
          if (lastResult !== result) return;
          setError(self, error as Error);
          stabilize();
        }
      })();
    }
    throw new NotReadyError(context!);
  };
  const self = computed<T>(fn, initialValue as T);
  return self;
}

export function signal<T>(v: T, firewall: Computed<any>): FirewallSignal<T>;
export function signal<T>(v: T): Signal<T>;
export function signal<T>(
  v: T,
  firewall: Computed<unknown> | null = null
): Signal<T> {
  if (firewall !== null) {
    return (firewall.child = {
      value: v,
      subs: null,
      subsTail: null,
      owner: firewall,
      nextChild: firewall.child,
      asyncFlags: AsyncFlags.None,
      time: clock,
      pendingValue: NOT_PENDING,
    });
  } else {
    return {
      value: v,
      subs: null,
      subsTail: null,
      asyncFlags: AsyncFlags.None,
      time: clock,
      pendingValue: NOT_PENDING,
    };
  }
}

function recompute(el: Computed<any>, create: boolean = false): void {
  deleteFromHeap(el, el.flags & ReactiveFlags.Zombie ? pending : dirty);
  if (
    el.pendingValue !== NOT_PENDING ||
    el.pendingFirstChild ||
    el.pendingDisposal
  )
    disposeChildren(el);
  else {
    markDisposal(el);
    pendingNodes.push(el);
    el.pendingDisposal = el.disposal;
    el.pendingFirstChild = el.firstChild;
    el.disposal = null;
    el.firstChild = null;
  }

  const oldcontext = context;
  context = el;
  el.depsTail = null;
  el.flags = ReactiveFlags.RecomputingDeps;
  let value = el.pendingValue === NOT_PENDING ? el.value : el.pendingValue;
  let oldHeight = el.height;
  el.time = clock;
  let prevAsyncFlags = el.asyncFlags;
  try {
    value = el.fn(value);
    clearAsyncFlags(el);
  } catch (e) {
    if (e instanceof NotReadyError) {
      asyncNodes.push(e.cause);
      setAsyncFlags(
        el,
        (prevAsyncFlags & ~AsyncFlags.Error) | AsyncFlags.Pending
      );
    } else {
      setError(el, e as Error);
    }
  }
  el.flags = ReactiveFlags.None;
  context = oldcontext;

  const depsTail = el.depsTail as Link | null;
  let toRemove = depsTail !== null ? depsTail.nextDep : el.deps;
  if (toRemove !== null) {
    do {
      toRemove = unlinkSubs(toRemove);
    } while (toRemove !== null);
    if (depsTail !== null) {
      depsTail.nextDep = null;
    } else {
      el.deps = null;
    }
  }
  const valueChanged =
    el.pendingValue === NOT_PENDING
      ? value !== el.value
      : el.pendingValue !== value;
  const asyncFlagsChanged = el.asyncFlags !== prevAsyncFlags;

  if (valueChanged || asyncFlagsChanged) {
    if (valueChanged) {
      if (!create && el.pendingValue === NOT_PENDING) pendingNodes.push(el);
      create ? (el.value = value) : (el.pendingValue = value);
    }

    for (let s = el.subs; s !== null; s = s.nextSub) {
      insertIntoHeap(
        s.sub,
        s.sub.flags & ReactiveFlags.Zombie ? pending : dirty
      );
    }
  } else if (el.height != oldHeight) {
    for (let s = el.subs; s !== null; s = s.nextSub) {
      insertIntoHeapHeight(
        s.sub,
        s.sub.flags & ReactiveFlags.Zombie ? pending : dirty
      );
    }
  }
}

function updateIfNecessary(el: Computed<unknown>): void {
  if (el.flags & ReactiveFlags.Check) {
    for (let d = el.deps; d; d = d.nextDep) {
      const dep1 = d.dep;
      const dep = ("owner" in dep1 ? dep1.owner : dep1) as Computed<unknown>;
      if ("fn" in dep) {
        updateIfNecessary(dep);
      }
      if (el.flags & ReactiveFlags.Dirty) {
        break;
      }
    }
  }

  if (el.flags & ReactiveFlags.Dirty) {
    recompute(el);
  }

  el.flags = ReactiveFlags.None;
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L100
function unlinkSubs(link: Link): Link | null {
  const dep = link.dep;
  const nextDep = link.nextDep;
  const nextSub = link.nextSub;
  const prevSub = link.prevSub;
  if (nextSub !== null) {
    nextSub.prevSub = prevSub;
  } else {
    dep.subsTail = prevSub;
  }
  if (prevSub !== null) {
    prevSub.nextSub = nextSub;
  } else {
    dep.subs = nextSub;
  }
  return nextDep;
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L52
function link(
  dep: Signal<unknown> | Computed<unknown>,
  sub: Computed<unknown>
) {
  const prevDep = sub.depsTail;
  if (prevDep !== null && prevDep.dep === dep) {
    return;
  }
  let nextDep: Link | null = null;
  const isRecomputing = sub.flags & ReactiveFlags.RecomputingDeps;
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
  ) {
    return;
  }
  const newLink =
    (sub.depsTail =
    dep.subsTail =
      {
        dep,
        sub,
        nextDep,
        prevSub,
        nextSub: null,
      });
  if (prevDep !== null) {
    prevDep.nextDep = newLink;
  } else {
    sub.deps = newLink;
  }
  if (prevSub !== null) {
    prevSub.nextSub = newLink;
  } else {
    dep.subs = newLink;
  }
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L284
function isValidLink(checkLink: Link, sub: Computed<unknown>): boolean {
  const depsTail = sub.depsTail;
  if (depsTail !== null) {
    let link = sub.deps!;
    do {
      if (link === checkLink) {
        return true;
      }
      if (link === depsTail) {
        break;
      }
      link = link.nextDep!;
    } while (link !== null);
  }
  return false;
}

export function read<T>(
  el: Signal<T> | Computed<T>,
  c: Computed<unknown> | null = context
): T {
  if (c) {
    link(el, c);

    const owner = "owner" in el ? el.owner : el;
    if ("fn" in owner) {
      const isZombie = (el as Computed<unknown>).flags & ReactiveFlags.Zombie;
      if (owner.height >= (isZombie ? pending.min : dirty.min)) {
        markNode(c);
        markHeap(isZombie ? pending : dirty);
        updateIfNecessary(owner);
      }
      const height = owner.height;
      if (height >= c.height) {
        c.height = height + 1;
      }
    }
  }
  if (el.asyncFlags & AsyncFlags.Pending) {
    if ((c && !stale) || el.asyncFlags & AsyncFlags.Uninitialized)
      throw new NotReadyError(el as Computed<unknown>);
  }
  if (el.asyncFlags & AsyncFlags.Error) {
    if (el.time < clock) {
      // treat error reset like create
      recompute(el as Computed<unknown>, true);
      return read(el);
    } else {
      throw el.error;
    }
  }
  return !c ||
    (stale && transition?.pendingNodes.includes(el) && !transitionComplete(transition)) ||
    el.pendingValue === NOT_PENDING
    ? el.value
    : (el.pendingValue as T);
}

export function setSignal(el: Signal<unknown>, v: unknown) {
  const valueChanged =
    el.pendingValue === NOT_PENDING ? el.value !== v : el.pendingValue !== v;
  if (!valueChanged && !el.asyncFlags) return;
  if (valueChanged) {
    if (el.pendingValue === NOT_PENDING) pendingNodes.push(el);
    el.pendingValue = v;
  }
  clearAsyncFlags(el);
  el.time = clock;

  for (let link = el.subs; link !== null; link = link.nextSub) {
    insertIntoHeap(
      link.sub,
      link.sub.flags & ReactiveFlags.Zombie ? pending : dirty
    );
  }
}

function setAsyncFlags<T>(
  signal: Signal<T>,
  flags: AsyncFlags,
  error: Error | null = null
) {
  signal.asyncFlags = flags;
  signal.error = error;
}

function setError<T>(signal: Signal<T>, error: Error) {
  setAsyncFlags(signal, AsyncFlags.Error | AsyncFlags.Uninitialized, error);
}

function clearAsyncFlags<T>(signal: Signal<T>) {
  setAsyncFlags(signal, AsyncFlags.None);
}

function markNode(el: Computed<unknown>, newState = ReactiveFlags.Dirty) {
  const flags = el.flags;
  if ((flags & (ReactiveFlags.Check | ReactiveFlags.Dirty)) >= newState) return;
  el.flags = (flags & ~(ReactiveFlags.Check | ReactiveFlags.Dirty)) | newState;
  for (let link = el.subs; link !== null; link = link.nextSub) {
    markNode(link.sub, ReactiveFlags.Check);
  }
  if (el.child !== null) {
    for (
      let child: FirewallSignal<unknown> | null = el.child;
      child !== null;
      child = child.nextChild
    ) {
      for (let link = child.subs; link !== null; link = link.nextSub) {
        markNode(link.sub, ReactiveFlags.Check);
      }
    }
  }
}

function markHeap(heap: Heap) {
  if (markedHeap) return;
  markedHeap = true;
  for (let i = 0; i <= heap.max; i++) {
    for (let el = heap.heap[i]; el !== undefined; el = el.nextHeap) {
      if (el.flags & ReactiveFlags.InHeap) markNode(el);
    }
  }
}

function adjustHeight(el: Computed<unknown>, heap: Heap) {
  deleteFromHeap(el, heap);
  let newHeight = el.height;
  for (let d = el.deps; d; d = d.nextDep) {
    const dep1 = d.dep;
    const dep = ("owner" in dep1 ? dep1.owner : dep1) as Computed<unknown>;
    if ("fn" in dep) {
      if (dep.height >= newHeight) {
        newHeight = dep.height + 1;
      }
    }
  }
  if (el.height !== newHeight) {
    el.height = newHeight;
    for (let s = el.subs; s !== null; s = s.nextSub) {
      insertIntoHeapHeight(s.sub, heap);
    }
  }
}

export function stabilize() {
  markedHeap = false;
  runHeap(dirty);
  if (asyncNodes.length > 0) {
    transition = {
      time: clock,
      asyncNodes,
      pendingNodes,
    };
    runHeap(pending);
    asyncNodes = [];
    pendingNodes = [];
    clock++;
    return;
  }
  if (transition && transitionComplete(transition)) {
    pendingNodes.push(...transition.pendingNodes);
    transition = null;
  }
  for (let i = 0; i < pendingNodes.length; i++) {
    const n = pendingNodes[i];
    if (n.pendingValue !== NOT_PENDING) {
      n.value = n.pendingValue as any;
      n.pendingValue = NOT_PENDING;
    }
    if ((n as Computed<unknown>).fn)
      disposeChildren(n as Computed<unknown>, true);
  }
  pendingNodes.length = 0;
  clock++;
}

function runHeap(heap: Heap) {
  for (heap.min = 0; heap.min <= heap.max; heap.min++) {
    let el = heap.heap[heap.min];
    while (el !== undefined) {
      if (el.flags & ReactiveFlags.InHeap) recompute(el);
      else {
        adjustHeight(el, heap);
      }
      el = heap.heap[heap.min];
    }
  }
  heap.max = 0;
}

function transitionComplete(transition: Transition): boolean {
  let done = true;
  for (let i = 0; i < transition.asyncNodes.length; i++) {
    if (transition.asyncNodes[i].asyncFlags & AsyncFlags.Pending) {
      done = false;
      break;
    }
  }
  return done;
}

export function onCleanup(fn: Disposable): Disposable {
  if (!context) return fn;

  const node = context;

  if (!node.disposal) {
    node.disposal = fn;
  } else if (Array.isArray(node.disposal)) {
    node.disposal.push(fn);
  } else {
    node.disposal = [node.disposal, fn];
  }
  return fn;
}

function markDisposal(el: Owner): void {
  let child = el.firstChild;
  while (child) {
    (child as Computed<unknown>).flags |= ReactiveFlags.Zombie;
    const inHeap = (child as Computed<unknown>).flags & ReactiveFlags.InHeap;
    if (inHeap) {
      deleteFromHeap(child as Computed<unknown>, dirty);
      insertIntoHeap(child as Computed<unknown>, pending);
    }
    markDisposal(child);
    child = child.nextSibling;
  }
}

function disposeChildren(node: Owner, zombie?: boolean): void {
  let child = zombie ? (node.pendingFirstChild as Owner) : node.firstChild;
  while (child) {
    const nextChild = child.nextSibling;
    if ((child as Computed<unknown>).deps) {
      const n = child as Computed<unknown>;
      deleteFromHeap(n, n.flags & ReactiveFlags.Zombie ? pending : dirty);
      let toRemove = n.deps;
      do {
        toRemove = unlinkSubs(toRemove!);
      } while (toRemove !== null);
      n.deps = null;
      n.depsTail = null;
      n.flags = ReactiveFlags.None;
    }
    disposeChildren(child);
    child = nextChild;
  }
  if (zombie) {
    node.pendingFirstChild = null;
  } else {
    node.firstChild = null;
    node.nextSibling = null;
  }
  runDisposal(node, zombie);
}

function runDisposal(node: Owner, zombie?: boolean): void {
  let disposal = zombie ? node.pendingDisposal : node.disposal;
  if (!disposal || disposal === NOT_PENDING) return;

  if (Array.isArray(disposal)) {
    for (let i = 0; i < disposal.length; i++) {
      const callable = disposal[i];
      callable.call(callable);
    }
  } else {
    (disposal as Disposable).call(disposal);
  }
  zombie ? (node.pendingDisposal = null) : (node.disposal = null);
}

export function getContext(): Computed<unknown> | null {
  return context;
}

export function runWithOwner<T>(
  owner: Computed<unknown> | null,
  fn: () => T
): T {
  const oldContext = context;
  context = owner;
  try {
    return fn();
  } finally {
    context = oldContext;
  }
}

export function latest<T>(fn: () => T): T {
  const prevStale = stale;
  stale = true;
  try {
    return fn();
  } finally {
    stale = prevStale;
  }
}
