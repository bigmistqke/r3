import {
  read,
  asyncComputed,
  AsyncSignal,
  setSignal,
  type FirewallSignal,
} from ".";

export class NotReadyError extends Error {
  constructor(public source: AsyncSignal<unknown>) {
    super("Async signal is not ready");
  }
}

export function readOrThrow<T>(node: AsyncSignal<T>): T {
  if (read(node.loading)) {
    throw new NotReadyError(node);
  }
  if (read(node.error)) {
    throw read(node.error);
  }
  return read(node.loaded) as T;
}

export function throwingMemo<T>(fn: () => T): AsyncSignal<T> {
  let loading: FirewallSignal<boolean> | null = null;
  let error;
  let loaded: T;

  const node = asyncComputed(() => {
    try {
      const value = fn();
      loaded = value;
      return value;
    } catch (e) {
      if (e instanceof NotReadyError) {
        loading = e.source.loading;
      } else {
        error = e;
      }
      return loaded;
    }
  });
  console.log("throwingMemo create", node.value);

  if (loading) {
    node.loading = loading;
  }
  if (error) {
    setSignal(node.error, error);
  }

  return node;
}

export function throwingAsync<T>(fn: () => Promise<T>): AsyncSignal<T> {
  let loading: FirewallSignal<boolean> | null = null;
  let error;
  let loaded: T;

  const node = asyncComputed(() => {
    try {
      const value = fn();
      value.then((v) => (loaded = v));
      return value;
    } catch (e) {
      if (e instanceof NotReadyError) {
        loading = e.source.loading;
      } else {
        error = e;
      }
      return loaded;
    }
  });

  if (loading) {
    node.loading = loading;
  }
  if (error) {
    setSignal(node.error, error);
  }

  return node;
}
