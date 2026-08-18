/**
 * Shared internals for the client / admin SDKs. Not part of the public API
 * (not re-exported from `index.ts`).
 */

/** Per-call options accepted by SDK methods that hit the server over HTTP. */
export interface CallOptions {
  /** Abort the request after this many milliseconds. */
  timeout?: number;
  /** Caller-provided abort signal, combined with the timeout. */
  signal?: AbortSignal;
  /**
   * `Idempotency-Key` header — the server replays the first response for a
   * duplicate key (matchmake calls and admin RPCs, 30s window).
   */
  idempotencyKey?: string;
}

/**
 * Combine a timeout and a caller signal into one AbortController. Call
 * `cancel()` once the request settles to clear the timer/listener.
 */
export function combineAbortSignals(
  timeoutMs: number | undefined,
  signal: AbortSignal | undefined,
): { signal: AbortSignal | undefined; cancel: () => void } {
  if (timeoutMs === undefined && signal === undefined) {
    return { signal: undefined, cancel: () => {} };
  }
  const controller = new AbortController();
  const timer =
    timeoutMs !== undefined
      ? setTimeout(() => controller.abort(new Error(`request timed out after ${timeoutMs}ms`)), timeoutMs)
      : undefined;
  const onAbort = () => controller.abort(signal!.reason);
  if (signal) {
    if (signal.aborted) controller.abort(signal.reason);
    else signal.addEventListener("abort", onAbort, { once: true });
  }
  return {
    signal: controller.signal,
    cancel: () => {
      if (timer !== undefined) clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    },
  };
}

/** RFC-4122 UUID via `crypto.randomUUID`, with a getRandomValues fallback. */
export function randomIdempotencyKey(): string {
  const c = globalThis.crypto;
  if (typeof c?.randomUUID === "function") return c.randomUUID();
  const bytes = new Uint8Array(16);
  if (typeof c?.getRandomValues === "function") {
    c.getRandomValues(bytes);
  } else {
    for (let i = 0; i < 16; i++) bytes[i] = Math.floor(Math.random() * 256);
  }
  bytes[6] = (bytes[6]! & 0x0f) | 0x40; // version 4
  bytes[8] = (bytes[8]! & 0x3f) | 0x80; // variant 1
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
