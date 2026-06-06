import assert from "node:assert/strict";
import { test } from "node:test";

import {
  BabelQueueError,
  EnvelopeCodec,
  SCHEMA_VERSION,
  SOURCE_LANG,
} from "../src/index.js";
import type { Meta } from "../src/index.js";

test("make produces the canonical shape", () => {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1042 });

  assert.equal(env.job, "urn:babel:orders:created");
  assert.equal(env.attempts, 0);
  assert.equal(env.meta.lang, SOURCE_LANG);
  assert.equal(env.meta.schema_version, SCHEMA_VERSION);
  assert.equal(env.meta.queue, "default");
  assert.ok(env.trace_id && env.meta.id, "trace_id and meta.id must be minted");
  assert.notEqual(env.trace_id, env.meta.id, "they must be distinct");
  assert.ok(env.meta.created_at > 0);
});

test("make rejects a blank URN", () => {
  assert.throws(() => EnvelopeCodec.make("   ", {}), BabelQueueError);
});

test("make honors queue and trace continuation", () => {
  const env = EnvelopeCodec.make(
    "urn:babel:orders:created",
    { order_id: 1 },
    { queue: "orders", traceId: "trace-123" },
  );

  assert.equal(env.meta.queue, "orders");
  assert.equal(env.trace_id, "trace-123");
});

test("encode/decode round-trips and validates", () => {
  const env = EnvelopeCodec.make(
    "urn:babel:orders:created",
    { order_id: 1042 },
    { queue: "orders" },
  );
  const body = EnvelopeCodec.encode(env);
  const got = EnvelopeCodec.decode(body);

  if (!EnvelopeCodec.accepts(got)) {
    assert.fail("round-tripped envelope must be accepted");
  }
  assert.equal(EnvelopeCodec.urn(got), env.job);
  assert.equal(got.trace_id, env.trace_id);
  assert.equal(got.meta.id, env.meta.id);
  assert.equal(got.data.order_id, 1042);
});

test("encode emits compact JSON, unescaped, with job first and no dead_letter", () => {
  const env = EnvelopeCodec.make(
    "urn:babel:catalog:item.indexed",
    { title: "Café — naïve ☕ A & B <x>/y" },
    { traceId: "t" },
  );
  const body = EnvelopeCodec.encode(env);

  assert.ok(body.startsWith('{"job":'), `job must be first: ${body}`);
  assert.ok(
    body.includes("Café — naïve ☕ A & B <x>/y"),
    `characters must be emitted literally (unescaped): ${body}`,
  );
  assert.ok(!body.includes("dead_letter"));
});

test("decode accepts the urn inbound alias", () => {
  const raw = JSON.stringify({
    urn: "urn:babel:orders:created",
    trace_id: "t",
    data: {},
    meta: {
      id: "i",
      queue: "q",
      lang: "node",
      schema_version: 1,
      created_at: 1,
    },
    attempts: 0,
  });
  const env = EnvelopeCodec.decode(raw);

  assert.equal(EnvelopeCodec.urn(env), "urn:babel:orders:created");
  assert.ok(EnvelopeCodec.accepts(env));
});

test("decode returns {} for malformed or non-object input", () => {
  assert.ok(!EnvelopeCodec.accepts(EnvelopeCodec.decode("not json")));
  assert.ok(!EnvelopeCodec.accepts(EnvelopeCodec.decode("[1,2,3]")));
  assert.ok(!EnvelopeCodec.accepts(EnvelopeCodec.decode("42")));
});

test("accepts rejects malformed envelopes", () => {
  const ok = EnvelopeCodec.decode(
    EnvelopeCodec.encode(
      EnvelopeCodec.make("urn:babel:orders:created", { x: 1 }),
    ),
  );
  assert.ok(EnvelopeCodec.accepts(ok));

  assert.ok(!EnvelopeCodec.accepts({ ...ok, job: undefined, urn: undefined }));
  assert.ok(
    !EnvelopeCodec.accepts({
      ...ok,
      meta: { ...(ok.meta as Meta), schema_version: 2 },
    }),
  );
  assert.ok(!EnvelopeCodec.accepts({ ...ok, trace_id: "  " }));
  assert.ok(!EnvelopeCodec.accepts({ ...ok, data: undefined }));
  assert.ok(!EnvelopeCodec.accepts({ ...ok, attempts: 1.5 }));
});

test("fromMessage builds from a PolyglotMessage and continues the trace", () => {
  const message = {
    getBabelUrn: () => "urn:babel:orders:created",
    toPayload: () => ({ order_id: 7 }),
    getBabelTraceId: () => "carry-over",
  };
  const env = EnvelopeCodec.fromMessage(message, "orders");

  assert.equal(env.job, "urn:babel:orders:created");
  assert.equal(env.data.order_id, 7);
  assert.equal(env.trace_id, "carry-over");
  assert.equal(env.meta.queue, "orders");
});
