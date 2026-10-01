export interface Disposable {
  (): void;
}

export const enum ReactiveFlags {
  None = 0,
  Check = 1 << 0,
  Dirty = 1 << 1,
  RecomputingDeps = 1 << 2,
  InHeap = 1 << 3,
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
}

interface FirewallSignal<T> extends RawSignal<T> {
  owner: Computed<unknown>;
  nextChild: FirewallSignal<unknown> | null;
}

export type Signal<T> = RawSignal<T> | FirewallSignal<T>;

export interface Computed<T> extends RawSignal<T> {
  deps: Link | null;
  depsTail: Link | null;
  flags: ReactiveFlags;
  height: number;
  nextHeap: Computed<unknown> | undefined;
  prevHeap: Computed<unknown>;
  disposal: Disposable | Disposable[] | null;
  fn: () => T;
  child: FirewallSignal<unknown> | null;
}

let markedHeap = false;
let context: Computed<unknown> | null = null;

let minDirty = 0;
let maxDirty = 0;
const dirtyHeap: (Computed<unknown> | undefined)[] = new Array(2000);
export function increaseHeapSize(n: number) {
  if (n > dirtyHeap.length) {
    dirtyHeap.length = n;
  }
}

function insertIntoHeap(n: Computed<unknown>) {
  const flags = n.flags;
  if (flags & ReactiveFlags.InHeap) return;
  n.flags = flags | ReactiveFlags.InHeap;
  const height = n.height;
  const heapAtHeight = dirtyHeap[height];
  if (heapAtHeight === undefined) {
    dirtyHeap[height] = n;
  } else {
    const tail = heapAtHeight.prevHeap;
    tail.nextHeap = n;
    n.prevHeap = tail;
    heapAtHeight.prevHeap = n;
  }
  if (height > maxDirty) {
    maxDirty = height;
  }
}

function deleteFromHeap(n: Computed<unknown>) {
  const flags = n.flags;
  if (!(flags & ReactiveFlags.InHeap)) return;
  n.flags = flags & ~ReactiveFlags.InHeap;
  const height = n.height;
  if (n.prevHeap === n) {
    dirtyHeap[height] = undefined;
  } else {
    const next = n.nextHeap;
    const dhh = dirtyHeap[height]!;
    const end = next ?? dhh;
    if (n === dhh) {
      dirtyHeap[height] = next;
    } else {
      n.prevHeap.nextHeap = next;
    }
    end.prevHeap = n.prevHeap;
  }
  n.prevHeap = n;
  n.nextHeap = undefined;
}

export function computed<T>(fn: () => T): Computed<T> {
  const self: Computed<T> = {
    disposal: null,
    fn: fn,
    value: undefined as T,
    height: 0,
    child: null,
    nextHeap: undefined,
    prevHeap: null as any,
    deps: null,
    depsTail: null,
    subs: null,
    subsTail: null,
    flags: ReactiveFlags.None,
  };
  self.prevHeap = self;
  if (context) {
    if (context.depsTail === null) {
      self.height = context.height;
      recompute(self, false);
    } else {
      self.height = context.height + 1;
      insertIntoHeap(self);
    }
    link(self, context);
  } else {
    recompute(self, false);
  }

  return self;
}

export function signal<T>(v: T, firewall: Computed<unknown>): FirewallSignal<T>;
export function signal<T>(v: T): Signal<T>;
export function signal<T>(
  v: T,
  firewall: Computed<unknown> | null = null,
): Signal<T> {
  if (firewall !== null) {
    return (firewall.child = {
      value: v,
      subs: null,
      subsTail: null,
      owner: firewall,
      nextChild: firewall.child,
    });
  } else {
    return {
      value: v,
      subs: null,
      subsTail: null,
    };
  }
}

function recompute(el: Computed<unknown>, del: boolean) {
  if (del) {
    deleteFromHeap(el);
  } else {
    el.nextHeap = undefined;
    el.prevHeap = el;
  }

  runDisposal(el);
  const oldcontext = context;
  context = el;
  el.depsTail = null;
  el.flags = ReactiveFlags.RecomputingDeps;
  let value: unknown;
  try {
    value = el.fn();
  } finally {
    el.flags = ReactiveFlags.None;
    context = oldcontext;
  }

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

  if (value !== el.value) {
    el.value = value;

    for (let s = el.subs; s !== null; s = s.nextSub) {
      const o = s.sub;
      const flags = o.flags;
      if (flags & ReactiveFlags.Check) {
        o.flags = flags | ReactiveFlags.Dirty;
      }
      insertIntoHeap(o);
    }
  }
}

function updateIfNecessary(el: Computed<unknown>): void {
  if (el.flags & ReactiveFlags.Check) {
    for (let d = el.deps; d; d = d.nextDep) {
      // A firewall signal is written by its owner's run, so bringing the
      // signal up to date means bringing its owner up to date.
      const dep = "owner" in d.dep ? d.dep.owner : d.dep;
      if ("fn" in dep) {
        updateIfNecessary(dep);
      }
      if (el.flags & ReactiveFlags.Dirty) {
        break;
      }
    }
  }

  if (el.flags & ReactiveFlags.Dirty) {
    recompute(el, true);
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
    if (nextSub === null && "fn" in dep) {
      unwatched(dep);
    }
  }
  return nextDep;
}

/**
 * Detach a computed from the graph: remove from the dirty heap, unlink from
 * all of its dependencies (cascading their cleanup if they lose their last
 * sub), and run its registered `onCleanup` callbacks. Fires automatically
 * when a computed loses its last sub; framework authors may call it directly
 * to dispose leaf nodes (e.g. effects) that have no subs.
 *
 * **Framework-author API.** Application code should not need this — r3's
 * automatic disposal handles the common case. Callers must ensure the node
 * has no live downstream subs at the time of the call (otherwise those subs
 * are left with dangling `deps` pointing at a non-recomputing node).
 */
export function unwatched(el: Computed<unknown>) {
  deleteFromHeap(el);
  let dep = el.deps;
  while (dep !== null) {
    dep = unlinkSubs(dep);
  }
  el.deps = null;
  runDisposal(el);
}

// https://github.com/stackblitz/alien-signals/blob/v2.0.3/src/system.ts#L52
function link(
  dep: Signal<unknown> | Computed<unknown>,
  sub: Computed<unknown>,
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

export function read<T>(el: Signal<T> | Computed<T>): T {
  if (context) {
    link(el, context);

    const owner = "owner" in el ? el.owner : el;
    if ("fn" in owner) {
      const height = owner.height;
      if (height >= context.height) {
        context.height = height + 1;
      }
      if (
        height >= minDirty ||
        owner.flags & (ReactiveFlags.Dirty | ReactiveFlags.Check)
      ) {
        markHeap();
        updateIfNecessary(owner);
      }
    }
  }
  return el.value;
}

/**
 * Bring `el` up to date and return its value, without running anything else
 * the heap holds. A read made inside a computed already does this: it marks
 * the heap and recomputes only what `el` depends on. A read made outside every
 * computed has no such path, and the only way to make it current is
 * `stabilize`, which runs every queued node — consumers with side effects
 * included — at whatever moment the read happens.
 *
 * What `pull` recomputes leaves the heap, so the next `stabilize` does not run
 * it again. Nodes that depend on it, and every other queued node, still run at
 * that next `stabilize`, as they would have without the pull.
 */
export function pull<T>(el: Signal<T> | Computed<T>): T {
  const owner = "owner" in el ? el.owner : el;
  if ("fn" in owner) {
    markHeap();
    pullQueued(owner);
  }
  return el.value;
}

/**
 * Bring `el` up to date the way `stabilize` would: recompute it, and what it
 * depends on, only where the node is queued in the heap. A node that
 * `cancelRecompute` withdrew from the heap while leaving it dirty is left as it
 * is, for a tracked read to bring up to date, as `stabilize` leaves it. Reads
 * made inside a recompute go through `read`, so they still pull such a node.
 */
function pullQueued(el: Computed<unknown>): void {
  if (el.flags & ReactiveFlags.Check) {
    for (let d = el.deps; d; d = d.nextDep) {
      const dep = "owner" in d.dep ? d.dep.owner : d.dep;
      if ("fn" in dep) {
        pullQueued(dep);
      }
      if (el.flags & ReactiveFlags.Dirty) {
        break;
      }
    }
  }

  // Queued: stabilize would run it, so the pull does, and takes it out of the
  // heap so the next stabilize does not run it again.
  if (el.flags & ReactiveFlags.InHeap) {
    recompute(el, true);
    return;
  }
  // Withdrawn from the heap but left dirty: stays dirty for a tracked read.
  if (el.flags & ReactiveFlags.Dirty) return;

  el.flags = ReactiveFlags.None;
}

export function setSignal(el: Signal<unknown>, v: unknown) {
  if (el.value === v) return;
  el.value = v;
  for (let link = el.subs; link !== null; link = link.nextSub) {
    markedHeap = false;
    // A subscriber marked only to check its dependencies now has one that
    // changed, as when a computed it reads changes value.
    const sub = link.sub;
    if (sub.flags & ReactiveFlags.Check) sub.flags |= ReactiveFlags.Dirty;
    insertIntoHeap(sub);
  }
}

function markNode(el: Computed<unknown>, newState = ReactiveFlags.Dirty) {
  const flags = el.flags;
  if ((flags & (ReactiveFlags.Check | ReactiveFlags.Dirty)) >= newState) return;
  el.flags = flags | newState;
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

function markHeap() {
  if (markedHeap) return;
  markedHeap = true;
  for (let i = 0; i <= maxDirty; i++) {
    for (let el = dirtyHeap[i]; el !== undefined; el = el.nextHeap) {
      markNode(el);
    }
  }
}

export function stabilize() {
  for (minDirty = 0; minDirty <= maxDirty; minDirty++) {
    let el = dirtyHeap[minDirty];
    dirtyHeap[minDirty] = undefined;
    while (el !== undefined) {
      const next = el.nextHeap;
      recompute(el, false);
      el = next;
    }
  }
}

/**
 * Whether a recompute of `el` is outstanding — queued in the heap, or marked
 * dirty and waiting to be pulled by a reader. Reported through a function
 * rather than by exposing the flags, because `ReactiveFlags` is a const enum
 * and does not survive a package boundary.
 */
export function isRecomputeQueued(el: Computed<unknown>): boolean {
  return (
    (el.flags &
      (ReactiveFlags.Dirty | ReactiveFlags.Check | ReactiveFlags.InHeap)) !==
    0
  );
}

/**
 * Withdraw an outstanding recompute of `el` and report whether there was one.
 * Clearing the flags alone is not enough: `stabilize` recomputes everything in
 * the heap without consulting them, so the node has to leave the heap.
 *
 * `keepDirty` leaves the node needing recomputation instead of clean, so it is
 * skipped by the next flush but recomputed by the next consumer that reads it.
 * That is the right state for a node whose input has moved but whose own work
 * was abandoned.
 *
 * A node in the middle of its own run is refused: `flags` currently holds
 * `RecomputingDeps`, and overwriting it would corrupt the dependency rebuild
 * that `recompute` performs when the body returns.
 */
export function cancelRecompute(
  el: Computed<unknown>,
  keepDirty = false,
): boolean {
  if (el.flags & ReactiveFlags.RecomputingDeps) return false;
  const queued = isRecomputeQueued(el);
  deleteFromHeap(el);
  el.flags = keepDirty ? ReactiveFlags.Dirty : ReactiveFlags.None;
  return queued;
}

/**
 * The inverse of `cancelRecompute`: schedule `el` for recomputation, as if
 * one of its own dependencies had just been written to. Marking it dirty
 * alone is not enough for a node nothing else reads through a tracked call —
 * `stabilize` only walks the heap, and a node that is merely flagged dirty
 * but sits outside the heap is picked up only by a consumer that happens to
 * pull it from within another node's own recompute. A node with no such
 * consumer — the common case for a leaf a caller reads directly — would stay
 * dirty and unscheduled forever. Putting it back in the heap is what makes
 * the next `stabilize` actually recompute it, matching what a real write to
 * one of its dependencies would have done.
 *
 * Existed for one caller: something that withdrew a node's queued recompute
 * on the expectation of immediately producing a replacement value, and then
 * did not — the withdrawal has to be undone, not merely left dirty.
 *
 * A node in the middle of its own run is refused, for the same reason
 * `cancelRecompute` refuses one: `flags` currently holds `RecomputingDeps`,
 * and overwriting it would corrupt the dependency rebuild `recompute`
 * performs when the body returns.
 */
export function requeueRecompute(el: Computed<unknown>): void {
  if (el.flags & ReactiveFlags.RecomputingDeps) return;
  markNode(el);
  insertIntoHeap(el);
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

function runDisposal(node: Computed<unknown>): void {
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

export function getContext(): Computed<unknown> | null {
  return context;
}

/**
 * Run `fn` outside any reactive tracking context. Signals read inside `fn`
 * are not recorded as dependencies of the enclosing computation.
 */
export function untrack<T>(fn: () => T): T {
  const saved = context;
  context = null;
  try {
    return fn();
  } finally {
    context = saved;
  }
}
