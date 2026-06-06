import assert from "node:assert/strict";
import { test } from "node:test";

import {
  annotate,
  deadLetter,
  EnvelopeCodec,
  SOURCE_LANG,
} from "../src/index.js";

test("annotate appends a dead_letter block without mutating the original", () => {
  const env = EnvelopeCodec.make(
    "urn:babel:orders:created",
    { order_id: 1 },
    { queue: "orders" },
  );

  const dl = annotate(env, "failed", "orders", {
    attempts: 3,
    error: "boom",
    exception: "Error",
  });

  assert.equal(env.dead_letter, undefined, "original must not be mutated");
  assert.ok(dl.dead_letter);
  assert.equal(dl.dead_letter.reason, "failed");
  assert.equal(dl.dead_letter.original_queue, "orders");
  assert.equal(dl.dead_letter.attempts, 3);
  assert.equal(dl.dead_letter.error, "boom");
  assert.equal(dl.dead_letter.exception, "Error");
  assert.equal(dl.dead_letter.lang, SOURCE_LANG);
  assert.ok(dl.dead_letter.failed_at > 0);
});

test("annotate defaults error/exception to null and attempts to the envelope's", () => {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 });
  const dl = annotate(env, "expired", "orders");

  assert.equal(dl.dead_letter?.error, null);
  assert.equal(dl.dead_letter?.exception, null);
  assert.equal(dl.dead_letter?.attempts, 0);
});

test("dead_letter is serialized as the last top-level field", () => {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 });
  const body = EnvelopeCodec.encode(annotate(env, "failed", "orders"));

  assert.ok(body.includes('"dead_letter":'));
  assert.ok(body.indexOf('"dead_letter"') > body.indexOf('"attempts"'));
});

test("deadLetter namespace re-exports annotate", () => {
  assert.equal(deadLetter.annotate, annotate);
});
