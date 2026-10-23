import assert from "node:assert/strict";
import { test } from "node:test";

import { BabelQueueError, retryBackoffBound, retryBackoffDelay } from "../src/index.js";

test("backoff: the defaults keep today's behaviour (no delay)", () => {
  assert.equal(retryBackoffDelay({}, 1), 0);
  assert.equal(retryBackoffDelay({}, 9), 0);
});

test("backoff: d(n) = min(max, base * multiplier^(n-1)), max 0 = uncapped", () => {
  const options = { retryBackoff: 1, retryBackoffMultiplier: 2 };
  assert.deepEqual([1, 2, 3, 4].map((n) => retryBackoffBound(options, n)), [1, 2, 4, 8]);
  assert.equal(retryBackoffBound({ ...options, retryBackoffMax: 5 }, 4), 5);
  assert.equal(retryBackoffBound({ ...options, retryBackoffMax: 5 }, 2), 2);
});

test("backoff: full jitter is uniform(0, d(n)) over the injected random source", () => {
  const options = { retryBackoff: 4, retryBackoffMultiplier: 1, retryJitter: "full" as const };
  assert.equal(retryBackoffDelay(options, 1, () => 0), 0);
  assert.equal(retryBackoffDelay(options, 1, () => 0.5), 2);
  assert.ok(retryBackoffDelay(options, 1, () => 0.999999) < 4);
});

test("backoff: jitter none never consults the random source", () => {
  const delay = retryBackoffDelay({ retryBackoff: 3 }, 2, () => {
    throw new Error("random must not be drawn");
  });
  assert.equal(delay, 3);
});

test("backoff: invalid parameters are rejected", () => {
  assert.throws(() => retryBackoffDelay({ retryBackoff: -1 }, 1), BabelQueueError);
  assert.throws(() => retryBackoffDelay({ retryBackoffMultiplier: Number.NaN }, 1), BabelQueueError);
  assert.throws(() => retryBackoffDelay({ retryBackoffMax: Number.POSITIVE_INFINITY }, 1), BabelQueueError);
  assert.throws(() => retryBackoffDelay({ retryJitter: "equal" as never }, 1), BabelQueueError);
  assert.throws(() => retryBackoffDelay({}, 0), BabelQueueError);
  assert.throws(() => retryBackoffDelay({}, 1.5), BabelQueueError);
});
