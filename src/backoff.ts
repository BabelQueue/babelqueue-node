import { BabelQueueError } from "./errors.js";

/**
 * Worker-side retry backoff parameters (ADR-0034 §9, ACTION_PLAN C5). Every field is optional;
 * the defaults (`0`, `1`, `0`, `"none"`) yield a delay of `0`, i.e. today's behaviour.
 *
 * Kafka does not apply these: on Kafka the retry tier sets the delay (broker-bindings.md §6.5).
 * A transport limit (e.g. SQS's 12 h visibility ceiling) still applies on top of the result.
 */
export interface RetryBackoffOptions {
  /** Base delay in seconds (`retry_backoff`). Default `0`. */
  retryBackoff?: number;
  /** Growth factor per attempt (`retry_backoff_multiplier`). Default `1`. */
  retryBackoffMultiplier?: number;
  /** Cap in seconds (`retry_backoff_max`); `0` means no cap. Default `0`. */
  retryBackoffMax?: number;
  /** `none` (default) or `full` (`uniform(0, d(n))`). */
  retryJitter?: RetryJitter;
}

/** Jitter mode: `none` returns `d(n)`, `full` returns `uniform(0, d(n))`. */
export type RetryJitter = "none" | "full";

/**
 * The deterministic part of the backoff: `d(n) = min(max, base * multiplier^(n-1))` seconds, where
 * `n` is the 1-based attempt number (`attempts + 1`) and `max = 0` means no cap. Jitter is not
 * applied here; see {@link retryBackoffDelay}.
 *
 * @throws BabelQueueError when a parameter is negative or not finite, the multiplier is below
 *   zero, the jitter mode is unknown, or `n` is not an integer >= 1.
 */
export function retryBackoffBound(options: RetryBackoffOptions, n: number): number {
  const base = options.retryBackoff ?? 0;
  const multiplier = options.retryBackoffMultiplier ?? 1;
  const max = options.retryBackoffMax ?? 0;
  if (!Number.isInteger(n) || n < 1) {
    throw new BabelQueueError(`Retry attempt number must be an integer >= 1, got ${String(n)}.`);
  }
  for (const [name, value] of [
    ["retryBackoff", base],
    ["retryBackoffMultiplier", multiplier],
    ["retryBackoffMax", max],
  ] as const) {
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
      throw new BabelQueueError(`${name} must be a finite number >= 0, got ${String(value)}.`);
    }
  }
  const grown = base * Math.pow(multiplier, n - 1);
  return max > 0 ? Math.min(max, grown) : grown;
}

/**
 * The retry delay in seconds for the 1-based attempt number `n` (`attempts + 1`).
 *
 * With `retryJitter: "none"` (default) the result is exactly `d(n)`; with `"full"` it is
 * `uniform(0, d(n))`. `random` is the uniform `[0, 1)` source (defaults to `Math.random`, which is
 * adequate here: jitter is load-spreading, not a secret); inject one for deterministic tests.
 *
 * @throws BabelQueueError on an invalid parameter (see {@link retryBackoffBound}).
 */
export function retryBackoffDelay(
  options: RetryBackoffOptions,
  n: number,
  random: () => number = Math.random,
): number {
  const jitter = options.retryJitter ?? "none";
  if (jitter !== "none" && jitter !== "full") {
    throw new BabelQueueError(`retryJitter must be "none" or "full", got ${String(jitter)}.`);
  }
  const bound = retryBackoffBound(options, n);
  return jitter === "full" ? random() * bound : bound;
}
