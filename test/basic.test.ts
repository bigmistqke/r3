import { expect, test } from "vitest";
import {
  cancelRecompute,
  computed,
  Computed,
  isRecomputeQueued,
  pull,
  read,
  requeueRecompute,
  setSignal,
  Signal,
  signal,
  stabilize,
} from "../src";

test("basic", () => {
  let aCount = 0;
  let bCount = 0;
  const s = signal(1);
  const a = computed(() => {
    aCount++;
    return read(s) + 1;
  });
  const b = computed(() => {
    bCount++;
    return read(a) + 1;
  });
  stabilize();

  expect(a.value).toBe(2);
  expect(b.value).toBe(3);

  expect(aCount).toBe(1);
  expect(bCount).toBe(1);

  expect(a.height).toBe(0);
  expect(b.height).toBe(1);

  setSignal(s, 2);

  stabilize();
  expect(a.value).toBe(3);
  expect(b.value).toBe(4);
  expect(aCount).toBe(2);
  expect(bCount).toBe(2);
});

test("diamond", () => {
  let callCount = 0;
  const s = signal(1);
  const a = computed(() => read(s) + 1);
  const b = computed(() => read(s) + 2);
  const c = computed(() => read(s) + 3);
  const d = computed(() => {
    callCount++;
    return read(a) * read(b) * read(c);
  });

  stabilize();
  expect(callCount).toBe(1);
  expect(d.value).toBe(2 * 3 * 4);
  setSignal(s, 2);
  stabilize();
  expect(callCount).toBe(2);
  expect(d.value).toBe(3 * 4 * 5);
});

test("dynamic sources recalculate correctly", () => {
  const a = signal(false);
  const b = signal(2);
  let count = 0;

  const c = computed(() => {
    count++;
    read(a) || read(b);
  });

  stabilize();

  expect(count).toBe(1);

  setSignal(a, true);
  stabilize();

  expect(count).toBe(2);

  setSignal(b, 4);
  stabilize();

  expect(count).toBe(2);
});

/*
    s
    |
    l
  */
test("dynamic source disappears entirely", () => {
  const s = signal(1);
  let done = false;
  let count = 0;

  const c = computed(() => {
    count++;

    if (done) {
      return 0;
    } else {
      const value = read(s);
      if (value > 2) {
        done = true; // break the link between s and c
      }
      return value;
    }
  });

  stabilize();

  expect(c.value).toBe(1);
  expect(count).toBe(1);

  setSignal(s, 3);
  stabilize();

  expect(c.value).toBe(3);
  expect(count).toBe(2);

  setSignal(s, 1); // we've now locked into 'done' state
  stabilize();

  expect(c.value).toBe(0);
  expect(count).toBe(3);

  // we're still locked into 'done' state, and count no longer advances
  // in fact, c() will never execute again..
  setSignal(s, 0);
  stabilize();

  expect(c.value).toBe(0);
  expect(count).toBe(3);
});

test("small dynamic graph with signal grandparents", () => {
  const z = signal(3);
  const x = signal(0);

  const y = signal(0);
  const i = computed(() => {
    let a = read(y);
    read(z);
    if (!a) {
      return read(x);
    } else {
      return a;
    }
  });
  const j = computed(() => {
    let a = read(i);
    read(z);
    if (!a) {
      return read(x);
    } else {
      return a;
    }
  });

  stabilize();
  setSignal(x, 1);
  stabilize();
  setSignal(y, 1);
  stabilize();
});

test("should not run untracked inner effect", () => {
  const a = signal(3);
  const b = computed(function f0() {
    return read(a) > 0;
  });

  computed(function f1() {
    if (read(b)) {
      computed(function f2() {
        if (read(a) == 0) {
          throw new Error("bad");
        }
      });
    }
  });
  stabilize();

  setSignal(a, 2);
  stabilize();

  setSignal(a, 1);
  stabilize();

  setSignal(a, 0);
  stabilize();
});

test("should not run untracked inner effect2", () => {
  const a = signal(0);
  const b = signal(0);

  let f1c = 0;
  let f2c = 0;
  computed(function f1() {
    f1c++;
    const x = computed(function f2() {
      f2c++;
      read(b);
      return read(a) == 0;
    });
    read(x);
  });
  stabilize();

  expect(f1c).toBe(1);
  expect(f2c).toBe(1);
  setSignal(a, 2);
  stabilize();

  expect(f1c).toBe(2);
  expect(f2c).toBe(3);

  setSignal(a, 1);
  stabilize();

  expect(f1c).toBe(2);
  expect(f2c).toBe(4);

  setSignal(b, 1);
  stabilize();
  expect(f1c).toBe(2);
  expect(f2c).toBe(5);
});

test("should not run inner effect3", () => {
  const a = signal(0);
  const b = signal(0);

  const order: string[] = [];
  let iter = 0;
  computed(function f1() {
    order.push("outer");
    read(a);

    let myiter = iter++;
    computed(function f2() {
      order.push("inner");
      read(b);
    });
  });

  stabilize();
  expect(order).toEqual(["outer", "inner"]);

  setSignal(a, 2);
  setSignal(b, 2);
  stabilize();

  expect(order).toEqual(["outer", "inner", "outer", "inner"]);
});

test("firewall signals", () => {
  const map = new Map<string, Signal<boolean>>();
  const selected = signal("a");
  let prev: string | null = null;
  const selector = computed(() => {
    if (prev) {
      const s = map.get(prev);
      if (s) {
        setSignal(s, false);
      }
    }
    prev = read(selected);
    const s = map.get(prev);
    if (s) setSignal(s, true);
  });

  const a = signal(true, selector);
  map.set("a", a);
  const b = signal(false, selector);
  map.set("b", b);
  const c = signal(false, selector);
  map.set("c", c);

  expect(a.value).toBe(true);

  setSignal(selected, "b");
  stabilize();

  expect(a.value).toBe(false);
  expect(b.value).toBe(true);
});

test("cancelRecompute withdraws a queued recompute", () => {
  let runs = 0;
  const s = signal(1);
  const c = computed(() => {
    runs++;
    return read(s) + 1;
  });
  stabilize();
  expect(runs).toBe(1);
  expect(c.value).toBe(2);

  setSignal(s, 10);
  expect(isRecomputeQueued(c)).toBe(true);
  expect(cancelRecompute(c)).toBe(true);
  expect(isRecomputeQueued(c)).toBe(false);

  stabilize();
  expect(runs).toBe(1); // the flush did not run it
  expect(c.value).toBe(2); // stale, by design

  setSignal(s, 20);
  stabilize();
  expect(runs).toBe(2); // a later change still schedules it
  expect(c.value).toBe(21);
});

test("cancelRecompute reports false when nothing was queued", () => {
  const s = signal(1);
  const c = computed(() => read(s) + 1);
  stabilize();
  expect(isRecomputeQueued(c)).toBe(false);
  expect(cancelRecompute(c)).toBe(false);
});

test("cancelRecompute can leave the node needing recomputation", () => {
  let cRuns = 0;
  const s = signal(1);
  const t = signal(0);
  const c = computed(() => {
    cRuns++;
    return read(s) + 1;
  });
  const d = computed(() => read(t) + read(c));
  stabilize();
  expect(cRuns).toBe(1);
  expect(d.value).toBe(2);

  setSignal(s, 10);
  cancelRecompute(c, true);
  stabilize();
  expect(cRuns).toBe(1); // the flush did not run it

  setSignal(t, 1);
  stabilize();
  expect(cRuns).toBe(2); // a consumer reading it pulled it up to date
  expect(d.value).toBe(1 + 11);
});

test("cancelRecompute refuses a node that is running and leaves its dependency rebuild intact", () => {
  let runs = 0;
  const s = signal(1);
  const t = signal(100);
  let c: Computed<number> | undefined;
  c = computed(() => {
    runs++;
    const a = read(s);
    if (c) cancelRecompute(c); // refused — must not disturb the rebuild
    const b = read(t); // read AFTER the call: this link must still form
    return a + b;
  });

  stabilize(); // run 1 — c is not yet assigned, so no mid-run call happens
  expect(c!.value).toBe(101);

  setSignal(s, 2);
  stabilize(); // run 2 — the mid-run call happens here
  expect(runs).toBe(2);
  expect(c!.value).toBe(102);

  setSignal(t, 200);
  stabilize(); // run 3 — only happens if the link to `t` survived run 2
  expect(runs).toBe(3);
  expect(c!.value).toBe(202);
});

test("requeueRecompute schedules a node with no consumer of its own", () => {
  let runs = 0;
  const s = signal(1);
  const c = computed(() => {
    runs++;
    return read(s) + 1;
  });
  stabilize();
  expect(runs).toBe(1);
  expect(c.value).toBe(2);

  setSignal(s, 10);
  expect(cancelRecompute(c, true)).toBe(true); // withdrawn, left dirty-not-heaped
  requeueRecompute(c);

  // No consumer pulls `c` — a plain `stabilize()` is the only thing that can
  // recompute it now, and it does, because requeueRecompute put it back in
  // the heap rather than leaving it merely flagged.
  stabilize();
  expect(runs).toBe(2);
  expect(c.value).toBe(11);
});

test("requeueRecompute refuses a node that is running", () => {
  let refused = true;
  const s = signal(1);
  let c: Computed<number> | undefined;
  c = computed(() => {
    if (c) {
      const before = c.flags;
      requeueRecompute(c);
      refused = c.flags === before; // no observable change
    }
    return read(s) + 1;
  });
  stabilize();
  setSignal(s, 2);
  stabilize();
  expect(refused).toBe(true);
  expect(c!.value).toBe(3);
});

test("pull brings one computed up to date without running the rest of the heap", () => {
  const s = signal(1);
  let doubledRuns = 0;
  let otherRuns = 0;
  const doubled = computed(() => {
    doubledRuns++;
    return read(s) * 2;
  });
  computed(() => {
    otherRuns++;
    return read(s);
  });
  stabilize();
  expect([doubledRuns, otherRuns]).toEqual([1, 1]);

  setSignal(s, 5);
  expect(pull(doubled)).toBe(10);
  expect([doubledRuns, otherRuns]).toEqual([2, 1]); // the other node waits

  stabilize();
  // The other node runs now, and the pulled one is not run again.
  expect([doubledRuns, otherRuns]).toEqual([2, 2]);
});

test("pull reaches through a chain and leaves what depends on the pulled node to the next stabilize", () => {
  const s = signal(1);
  let middleRuns = 0;
  let endRuns = 0;
  let consumerRuns = 0;
  const middle = computed(() => {
    middleRuns++;
    return read(s) + 1;
  });
  const end = computed(() => {
    endRuns++;
    return read(middle) * 10;
  });
  computed(() => {
    consumerRuns++;
    return read(end);
  });
  stabilize();

  setSignal(s, 2);
  expect(pull(end)).toBe(30);
  expect([middleRuns, endRuns, consumerRuns]).toEqual([2, 2, 1]);

  stabilize();
  expect([middleRuns, endRuns, consumerRuns]).toEqual([2, 2, 2]);
});

test("pull of a node whose sources have not changed runs nothing", () => {
  const s = signal(1);
  let runs = 0;
  const c = computed(() => {
    runs++;
    return read(s) + 1;
  });
  stabilize();
  expect(pull(c)).toBe(2);
  expect(runs).toBe(1);
});

test("pull of a signal returns its value", () => {
  const s = signal(1);
  setSignal(s, 7);
  expect(pull(s)).toBe(7);
});

test("pull brings the owner of a firewall signal up to date before reading through it", () => {
  const source = signal(1);
  let published!: Signal<number>;
  let ownerRuns = 0;
  const owner = computed(() => {
    ownerRuns++;
    const v = read(source);
    if (published) setSignal(published, v * 10);
  });
  published = signal(10, owner);
  let readerRuns = 0;
  let otherRuns = 0;
  const reader = computed(() => {
    readerRuns++;
    return read(published) + 1;
  });
  computed(() => {
    otherRuns++;
    return read(source);
  });
  stabilize();
  expect(reader.value).toBe(11);

  setSignal(source, 2);
  // The reader depends on the owner only through the signal the owner writes.
  expect(pull(reader)).toBe(21);
  expect([ownerRuns, readerRuns, otherRuns]).toEqual([2, 2, 1]);

  stabilize();
  expect([ownerRuns, readerRuns, otherRuns]).toEqual([2, 2, 2]);
});

test("pull of a firewall signal runs its owner first", () => {
  const source = signal(1);
  let published!: Signal<number>;
  const owner = computed(() => {
    const v = read(source);
    if (published) setSignal(published, v * 10);
  });
  published = signal(10, owner);
  stabilize();
  setSignal(source, 3);
  expect(pull(published)).toBe(30);
});
