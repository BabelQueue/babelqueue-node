import assert from "node:assert/strict";
import { test } from "node:test";

import {
  SpanKind,
  SpanStatusCode,
  type Attributes,
  type Counter,
  type Histogram,
  type Meter,
  type MetricOptions,
  type Tracer,
} from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";

import type { HeaderCarrier } from "../src/contracts.js";
import { EnvelopeCodec, type Envelope } from "../src/codec.js";
import type { DeadLetterEvent } from "../src/deadLetterObserver.js";
import {
  HEADER_TRACEPARENT,
  HEADER_TRACESTATE,
  NON_UUID_TRACE_ID_WARNING_CODE,
  SEMCONV_SCHEMA_URL,
  SEMCONV_VERSION,
  createDeadLetterObserver,
  injectTraceparent,
  publish,
  remoteParentFromHeaders,
  traceIdOf,
  uuidOf,
  wrapHandler,
} from "../src/otel.js";

const TRACE_ID = "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b";

function recorder(): { tracer: Tracer; exporter: InMemorySpanExporter } {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  return { tracer: provider.getTracer("test"), exporter };
}

/**
 * An in-memory transport hop: a producer span injects its `traceparent` into a headers carrier;
 * the carrier "travels" with the encoded body to the consumer side. No broker — this proves the
 * v0.2 mechanism the `babelqueue-node-adapters` transports will wire onto their native channels.
 */
function produceWithHeaders(
  tracer: Tracer,
  urn: string,
  data: Record<string, unknown>,
): Promise<{ body: string; headers: HeaderCarrier }> {
  const headers: HeaderCarrier = {};
  return publish(
    tracer,
    urn,
    data,
    (env) => ({ body: EnvelopeCodec.encode(env), headers }),
    { headers },
  );
}

test("traceId <-> UUID round-trips, and a non-uuid is hashed", () => {
  const hex = traceIdOf(TRACE_ID);
  assert.match(hex, /^[0-9a-f]{32}$/);
  assert.equal(uuidOf(hex), TRACE_ID);
  assert.equal(traceIdOf("not-a-uuid"), traceIdOf("not-a-uuid"));
  assert.notEqual(traceIdOf("not-a-uuid"), hex);
  assert.match(traceIdOf("z".repeat(32)), /^[0-9a-f]{32}$/); // 32 chars, not hex -> hashed
});

test("wrapHandler emits a CONSUMER span in the trace_id-derived trace", async () => {
  const { tracer, exporter } = recorder();
  let called = false;
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 }, { queue: "orders" });

  await wrapHandler(tracer, async () => {
    called = true;
  })(env);

  assert.ok(called);
  const spans = exporter.getFinishedSpans();
  assert.equal(spans.length, 1);
  const span = spans[0];
  assert.equal(span.name, "process orders"); // ADR-0038: the queue, never the URN
  assert.equal(span.kind, SpanKind.CONSUMER);
  assert.equal(span.attributes["babelqueue.urn"], "urn:babel:orders:created");
  assert.equal(span.spanContext().traceId, traceIdOf(env.trace_id));
  assert.equal(span.attributes["messaging.message.conversation_id"], env.trace_id);
  assert.equal(span.attributes["messaging.destination.name"], "orders");
});

test("wrapHandler tolerates an envelope missing optional meta/attempts", async () => {
  const { tracer, exporter } = recorder();
  const partial = { job: "urn:babel:orders:created", trace_id: TRACE_ID } as Envelope;

  await wrapHandler(tracer, async () => {})(partial);

  const span = exporter.getFinishedSpans()[0];
  // No meta: no destination (bare name), no message id; attempts default to 0.
  assert.equal(span.name, "process");
  assert.equal("messaging.destination.name" in span.attributes, false);
  assert.equal("messaging.message.id" in span.attributes, false);
  assert.equal(span.attributes["babelqueue.attempts"], 0);
  assert.equal(span.attributes["messaging.babelqueue.attempts"], 0);
  assert.equal(span.attributes["babelqueue.schema_version"], 1);
});

test("wrapHandler records the handler's error and re-throws", async () => {
  const { tracer, exporter } = recorder();
  const boom = new Error("boom");

  await assert.rejects(
    wrapHandler(tracer, () => {
      throw boom;
    })(EnvelopeCodec.make("urn:babel:orders:created", {})),
    /boom/,
  );

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.status.code, SpanStatusCode.ERROR);
  assert.ok(span.events.length >= 1); // recorded exception
});

test("publish emits a PRODUCER span and stamps trace_id from it", async () => {
  const { tracer, exporter } = recorder();
  let sent: Envelope | undefined;

  const id = await publish(
    tracer,
    "urn:babel:orders:created",
    { order_id: 7 },
    (env) => {
      sent = env;
      return env.meta.id;
    },
  );

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.kind, SpanKind.PRODUCER);
  assert.equal(span.name, "send default"); // make() writes the `default` queue
  assert.equal(span.attributes["messaging.message.id"], id);
  assert.ok(sent);
  // the published trace_id encodes the producer span's trace, so a consumer recovers it
  assert.equal(sent.trace_id, uuidOf(span.spanContext().traceId));
  assert.equal(traceIdOf(sent.trace_id), span.spanContext().traceId);
});

test("publish hands one envelope to a publishEnvelope-style target: span id == wire meta.id, headers carried out of band", async () => {
  const { tracer, exporter } = recorder();
  const headers: HeaderCarrier = {};
  // An adapter stand-in: sends a ready envelope as-is (never rebuilds it) beside the carrier.
  const wire: { body: string; headers: HeaderCarrier }[] = [];
  const target = {
    publishEnvelope(envelope: Envelope, options: { headers?: HeaderCarrier } = {}): string {
      wire.push({ body: EnvelopeCodec.encode(envelope), headers: { ...options.headers } });
      return envelope.meta.id;
    },
  };

  const id = await publish(
    tracer,
    "urn:babel:orders:created",
    { order_id: 7 },
    (env) => target.publishEnvelope(env, { headers }),
    { queue: "orders", headers },
  );

  assert.equal(wire.length, 1);
  const decoded = EnvelopeCodec.decode(wire[0]!.body) as Envelope;
  const span = exporter.getFinishedSpans()[0]!;
  assert.equal(span.attributes["messaging.message.id"], decoded.meta.id);
  assert.equal(id, decoded.meta.id);
  assert.equal(decoded.meta.schema_version, 1);
  assert.equal(decoded.trace_id, uuidOf(span.spanContext().traceId));
  assert.match(wire[0]!.headers[HEADER_TRACEPARENT] ?? "", /^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/);
  assert.ok(!wire[0]!.body.includes(HEADER_TRACEPARENT), "the traceparent never enters the envelope body");
});

test("publish records a failing send on the span and re-throws", async () => {
  const { tracer, exporter } = recorder();
  const boom = new Error("send failed");

  await assert.rejects(
    publish(tracer, "urn:babel:orders:created", { order_id: 7 }, () => {
      throw boom;
    }),
    /send failed/,
  );

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.kind, SpanKind.PRODUCER);
  assert.equal(span.status.code, SpanStatusCode.ERROR);
});

/* ----- ADR-0028 v0.2: W3C traceparent cross-hop span parent-child linkage ----- */

test("publish injects a traceparent header AND still stamps trace_id (v0.1 belt-and-braces)", async () => {
  const { tracer, exporter } = recorder();
  const { body, headers } = await produceWithHeaders(tracer, "urn:babel:orders:created", {
    order_id: 7,
  });

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.kind, SpanKind.PRODUCER);

  // The traceparent header encodes the producer span (trace id + span id), W3C wire form.
  const tp = headers[HEADER_TRACEPARENT];
  assert.ok(tp, "publish did not carry a traceparent transport header");
  assert.equal(tp, `00-${span.spanContext().traceId}-${span.spanContext().spanId}-01`);

  // v0.1 fallback intact: trace_id still encodes the same trace for a header-blind consumer.
  const env = EnvelopeCodec.decode(body) as Envelope;
  assert.equal(traceIdOf(env.trace_id), span.spanContext().traceId);
});

test("publish without a headers carrier carries no traceparent (opt-in, no regression)", async () => {
  const { tracer } = recorder();
  let sent: Envelope | undefined;

  await publish(tracer, "urn:babel:orders:created", { order_id: 1 }, (env) => {
    sent = env;
  });

  // No carrier was passed, so nothing rode out of band — pure v0.1 behaviour.
  assert.ok(sent);
  assert.match(sent.trace_id, /^[0-9a-f]{8}-/);
});

test("wrapHandler starts the consumer span as a true CHILD of the producer span across a hop", async () => {
  const { tracer, exporter } = recorder();

  // PRODUCER: inject traceparent into a headers carrier (the simulated transport metadata).
  const { body, headers } = await produceWithHeaders(tracer, "urn:babel:orders:created", {
    order_id: 1,
  });
  const producer = exporter.getFinishedSpans()[0];

  // HOP: the carrier rides to the consumer. The adapter surfaces it to wrapHandler.
  const env = EnvelopeCodec.decode(body) as Envelope;
  await wrapHandler(tracer, async () => {}, headers)(env);

  const consumer = exporter
    .getFinishedSpans()
    .find((s) => s.kind === SpanKind.CONSUMER);
  assert.ok(consumer);

  // Same trace across the hop, and the consumer's PARENT is exactly the producer span.
  assert.equal(consumer.spanContext().traceId, producer.spanContext().traceId);
  assert.equal(consumer.parentSpanContext?.spanId, producer.spanContext().spanId);
  assert.equal(consumer.parentSpanContext?.isRemote, true);
  // The consumer span is a fresh child, not the producer span itself.
  assert.notEqual(consumer.spanContext().spanId, producer.spanContext().spanId);
});

test("wrapHandler accepts a getter for delivered headers (the adapter seam shape)", async () => {
  const { tracer, exporter } = recorder();
  const { body, headers } = await produceWithHeaders(tracer, "urn:babel:orders:created", {
    order_id: 2,
  });
  const producer = exporter.getFinishedSpans()[0];

  const env = EnvelopeCodec.decode(body) as Envelope;
  // Adapter wires a (possibly async) getter that reads the delivery's headers.
  await wrapHandler(tracer, async () => {}, async () => headers)(env);

  const consumer = exporter.getFinishedSpans().find((s) => s.kind === SpanKind.CONSUMER);
  assert.ok(consumer);
  assert.equal(consumer.parentSpanContext?.spanId, producer.spanContext().spanId);
});

test("wrapHandler falls back to the trace_id-derived parent when no traceparent header is present", async () => {
  const { tracer, exporter } = recorder();
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 });

  // No headers at all — a message from a pre-0028 (or header-blind) producer.
  await wrapHandler(tracer, async () => {})(env);

  const consumer = exporter.getFinishedSpans()[0];
  // v0.1 behaviour: the span lands in the trace_id-derived trace (ADR-0025 Option 1), no regression.
  assert.equal(consumer.spanContext().traceId, traceIdOf(env.trace_id));
});

test("a malformed/empty traceparent never hijacks the trace — falls back to trace_id", async () => {
  const { tracer, exporter } = recorder();
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1 });

  // Present but malformed: extraction must yield no remote parent.
  assert.equal(remoteParentFromHeaders({ [HEADER_TRACEPARENT]: "garbage" }), undefined);
  assert.equal(remoteParentFromHeaders({}), undefined);
  assert.equal(remoteParentFromHeaders(null), undefined);
  // All-zero ids are rejected by the W3C acceptance rules.
  assert.equal(
    remoteParentFromHeaders({
      [HEADER_TRACEPARENT]: "00-00000000000000000000000000000000-0000000000000000-01",
    }),
    undefined,
  );

  await wrapHandler(tracer, async () => {}, { [HEADER_TRACEPARENT]: "garbage" })(env);
  const consumer = exporter.getFinishedSpans()[0];
  assert.equal(consumer.spanContext().traceId, traceIdOf(env.trace_id));
});

test("injectTraceparent carries tracestate and remoteParentFromHeaders round-trips it", () => {
  // A valid W3C traceparent + tracestate extracts back to the same remote span context.
  const headers: HeaderCarrier = {
    [HEADER_TRACEPARENT]: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    [HEADER_TRACESTATE]: "vendor=value",
  };
  const ctx = remoteParentFromHeaders(headers);
  assert.ok(ctx);

  // Re-injecting from that context reproduces the same traceparent + tracestate (round-trip).
  const out = injectTraceparent({}, ctx);
  assert.equal(out[HEADER_TRACEPARENT], headers[HEADER_TRACEPARENT]);
  assert.equal(out[HEADER_TRACESTATE], headers[HEADER_TRACESTATE]);
});

test("injectTraceparent on a context with no active span writes nothing (no-trace stays header-free)", () => {
  // Default active context has no span → carrier returned unchanged.
  const out = injectTraceparent({});
  assert.equal(out[HEADER_TRACEPARENT], undefined);
  assert.equal(out[HEADER_TRACESTATE], undefined);
});

/* ----- ADR-0038: semconv span shape ----- */

test("the semconv pin is v1.44.0 and the schema URL follows it", () => {
  assert.equal(SEMCONV_VERSION, "1.44.0");
  assert.equal(SEMCONV_SCHEMA_URL, "https://opentelemetry.io/schemas/1.44.0");
});

test("publish span: send <queue>, system, destination, ids and the dual-emitted operation key", async () => {
  const { tracer, exporter } = recorder();
  let sent: Envelope | undefined;

  await publish(
    tracer,
    "urn:babel:orders:created",
    {},
    (env) => {
      sent = env;
    },
    { queue: "orders.events", system: "kafka" },
  );

  const span = exporter.getFinishedSpans()[0];
  assert.ok(sent);
  assert.equal(span.name, "send orders.events");
  assert.deepEqual(span.attributes, {
    "messaging.system": "kafka",
    "messaging.operation.type": "send",
    "messaging.operation.name": "send",
    "messaging.destination.name": "orders.events",
    "messaging.message.id": sent.meta.id,
    "messaging.message.conversation_id": sent.trace_id,
    "babelqueue.urn": "urn:babel:orders:created",
    "babelqueue.schema_version": 1,
    "messaging.operation": "publish",
  });
  assert.equal(span.status.code, SpanStatusCode.UNSET);
});

test("publish span: a blank queue is no destination (bare send) and an empty system is the default", async () => {
  const { tracer, exporter } = recorder();
  await publish(tracer, "urn:babel:a", {}, () => undefined, { queue: " \t", system: "" });

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.name, "send");
  assert.equal("messaging.destination.name" in span.attributes, false);
  assert.equal(span.attributes["messaging.system"], "babelqueue");
});

test("publish span: a failing send keeps the ids, sets status Error and error.type, and rethrows the same error", async () => {
  const { tracer, exporter } = recorder();
  class TransportDown extends Error {}
  const boom = new TransportDown("down");
  let sent: Envelope | undefined;

  await assert.rejects(
    publish(
      tracer,
      "urn:babel:a",
      {},
      (env) => {
        sent = env;
        throw boom;
      },
      { queue: "orders" },
    ),
    (err) => err === boom,
  );

  const span = exporter.getFinishedSpans()[0];
  assert.ok(sent);
  assert.equal(span.status.code, SpanStatusCode.ERROR);
  assert.equal(span.status.message, "down");
  assert.equal(span.attributes["error.type"], "TransportDown");
  assert.equal(span.attributes["messaging.message.id"], sent.meta.id);
  assert.ok(span.events.some((e) => e.name === "exception"));
});

test("publish span: a build failure writes no message id and no conversation id", async () => {
  const { tracer, exporter } = recorder();

  await assert.rejects(
    publish(tracer, "urn:babel:a", {}, () => undefined, { queue: "orders", meta: { id: "collides" } }),
  );

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.status.code, SpanStatusCode.ERROR);
  assert.ok(String(span.attributes["error.type"]).length > 0);
  assert.equal("messaging.message.id" in span.attributes, false);
  assert.equal("messaging.message.conversation_id" in span.attributes, false);
});

test("consumer span: queue, system, consumer group, ids and the dual-emitted attempts keys", async () => {
  const { tracer, exporter } = recorder();
  const env = EnvelopeCodec.make("urn:babel:orders:created", {}, { queue: "orders.events" });
  env.attempts = 3;

  await wrapHandler(tracer, async () => {}, undefined, { system: "kafka", consumerGroup: "billing-workers" })(env);

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.name, "process orders.events");
  assert.deepEqual(span.attributes, {
    "messaging.system": "kafka",
    "messaging.operation.type": "process",
    "messaging.operation.name": "process",
    "messaging.destination.name": "orders.events",
    "messaging.message.id": env.meta.id,
    "messaging.message.conversation_id": env.trace_id,
    "messaging.consumer.group.name": "billing-workers",
    "babelqueue.urn": "urn:babel:orders:created",
    "babelqueue.schema_version": 1,
    "babelqueue.attempts": 3,
    "messaging.operation": "process",
    "messaging.babelqueue.attempts": 3,
  });
});

test("consumer span: the urn alias is the babelqueue.urn when job is absent", async () => {
  const { tracer, exporter } = recorder();
  const env = { ...EnvelopeCodec.make("urn:babel:a", {}, { queue: "q" }), job: undefined, urn: "urn:babel:alias" } as unknown as Envelope;

  await wrapHandler(tracer, async () => {})(env);

  assert.equal(exporter.getFinishedSpans()[0].attributes["babelqueue.urn"], "urn:babel:alias");
});

test("consumer span: a failing handler sets error.type from the constructor name (and _OTHER for a non-error)", async () => {
  const { tracer, exporter } = recorder();
  class HandlerBug extends Error {}
  const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders" });

  await assert.rejects(wrapHandler(tracer, () => { throw new HandlerBug("bug"); })(env), HandlerBug);
  await assert.rejects(wrapHandler(tracer, () => Promise.reject("just a string"))(env), (e) => e === "just a string");

  const [first, second] = exporter.getFinishedSpans();
  assert.equal(first.attributes["error.type"], "HandlerBug");
  assert.equal(first.status.code, SpanStatusCode.ERROR);
  assert.equal(second.attributes["error.type"], "_OTHER");
  assert.equal(second.status.code, SpanStatusCode.ERROR);
});

test("consumer span: a successful handler has no error.type and an unset status", async () => {
  const { tracer, exporter } = recorder();
  await wrapHandler(tracer, async () => {})(EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders" }));

  const span = exporter.getFinishedSpans()[0];
  assert.equal("error.type" in span.attributes, false);
  assert.equal(span.status.code, SpanStatusCode.UNSET);
});

test("a non-UUID trace_id is forwarded byte for byte, maps through the hash and warns once per value", async () => {
  const { tracer, exporter } = recorder();
  const warnings: Array<{ message: string; code: string | undefined }> = [];
  const original = process.emitWarning;
  process.emitWarning = ((message: string, options?: { code?: string }) => {
    warnings.push({ message, code: options?.code });
  }) as typeof process.emitWarning;
  try {
    const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders", traceId: "order 42 / trace" });
    const handle = wrapHandler(tracer, async () => {});
    await handle(env);
    await handle(env);
    const padded = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders" });
    padded.trace_id = " 7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b\t";
    await handle(padded);

    const [one, two, three] = exporter.getFinishedSpans();
    assert.equal(one.attributes["messaging.message.conversation_id"], "order 42 / trace");
    assert.equal(one.spanContext().traceId, traceIdOf("order 42 / trace"));
    assert.equal(two.spanContext().traceId, one.spanContext().traceId);
    assert.equal(three.attributes["messaging.message.conversation_id"], " 7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b\t");
    // two distinct hashed values -> two warnings, none for the repeat
    const mine = warnings.filter((w) => w.code === NON_UUID_TRACE_ID_WARNING_CODE);
    assert.equal(mine.length, 2);
    assert.match(mine[0].message, /not a UUID/);
  } finally {
    process.emitWarning = original;
  }
  // a UUID never warns
  const quiet: string[] = [];
  process.emitWarning = ((message: string) => {
    quiet.push(message);
  }) as typeof process.emitWarning;
  try {
    await wrapHandler(tracer, async () => {})(EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders" }));
    assert.deepEqual(quiet, []);
  } finally {
    process.emitWarning = original;
  }
});

/* ----- ADR-0039: metrics ----- */

interface Point {
  metric: string;
  kind: "add" | "record";
  value: number;
  attributes: Attributes;
}

/** A minimal in-memory `Meter`: every `add` / `record` lands in `points`. */
function fakeMeter(): { meter: Meter; points: Point[]; created: Array<{ name: string; options?: MetricOptions }> } {
  const points: Point[] = [];
  const created: Array<{ name: string; options?: MetricOptions }> = [];
  const meter = {
    createCounter(name: string, options?: MetricOptions): Counter {
      created.push({ name, options });
      return {
        add: (value: number, attributes: Attributes = {}) => {
          points.push({ metric: name, kind: "add", value, attributes });
        },
      };
    },
    createHistogram(name: string, options?: MetricOptions): Histogram {
      created.push({ name, options });
      return {
        record: (value: number, attributes: Attributes = {}) => {
          points.push({ metric: name, kind: "record", value, attributes });
        },
      };
    },
  } as unknown as Meter;
  return { meter, points, created };
}

test("consumer metrics: consumed, retries (attempts > 0) and the process duration with the exact attribute sets", async () => {
  const { tracer } = recorder();
  const { meter, points, created } = fakeMeter();
  const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders.events" });
  env.attempts = 2;
  const handle = wrapHandler(tracer, async () => {}, undefined, { system: "kafka", consumerGroup: "g", meter });

  await handle(env);
  await assert.rejects(
    wrapHandler(tracer, () => { throw new TypeError("x"); }, undefined, { system: "kafka", meter })(env),
    TypeError,
  );

  const base = {
    "messaging.system": "kafka",
    "messaging.operation.name": "process",
    "messaging.destination.name": "orders.events",
  };
  assert.deepEqual(
    points.filter((p) => p.metric !== "messaging.process.duration"),
    [
      { metric: "messaging.client.consumed.messages", kind: "add", value: 1, attributes: base },
      { metric: "babelqueue.retries", kind: "add", value: 1, attributes: base },
      { metric: "messaging.client.consumed.messages", kind: "add", value: 1, attributes: base },
      { metric: "babelqueue.retries", kind: "add", value: 1, attributes: base },
    ],
  );
  const durations = points.filter((p) => p.metric === "messaging.process.duration");
  assert.equal(durations.length, 2);
  assert.ok(durations.every((p) => p.kind === "record" && Number.isFinite(p.value) && p.value >= 0));
  assert.deepEqual(durations[0].attributes, base); // success: no error.type
  assert.deepEqual(durations[1].attributes, { ...base, "error.type": "TypeError" });

  const histogram = created.find((c) => c.name === "messaging.process.duration");
  assert.equal(histogram?.options?.unit, "s");
  assert.deepEqual(histogram?.options?.advice?.explicitBucketBoundaries, [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
  ]);
  assert.deepEqual(
    created.filter((c) => c.name !== "messaging.process.duration").map((c) => [c.name, c.options?.unit]),
    [
      ["messaging.client.consumed.messages", "{message}"],
      ["babelqueue.retries", "{message}"],
    ],
  );
});

test("consumer metrics: no retries point at attempts 0; no destination attribute for a blank queue; never a cardinality attribute", async () => {
  const { tracer } = recorder();
  const { meter, points } = fakeMeter();
  const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "   " });

  await wrapHandler(tracer, async () => {}, undefined, { meter })(env);

  assert.deepEqual(points.map((p) => p.metric).sort(), ["messaging.client.consumed.messages", "messaging.process.duration"]);
  for (const point of points) {
    assert.deepEqual(point.attributes, { "messaging.system": "babelqueue", "messaging.operation.name": "process" });
  }
});

test("consumer metrics: instruments are created once per meter, not per message", async () => {
  const { tracer } = recorder();
  const { meter, created } = fakeMeter();
  const first = wrapHandler(tracer, async () => {}, undefined, { meter });
  const second = wrapHandler(tracer, async () => {}, undefined, { meter });
  const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders" });

  await first(env);
  await first(env);
  await second(env);

  assert.equal(created.length, 3);
});

test("producer metrics: one sent point per call, error.type on failure, none without a meter", async () => {
  const { tracer } = recorder();
  const { meter, points } = fakeMeter();

  await publish(tracer, "urn:babel:a", {}, () => undefined, { queue: "orders.events", system: "kafka", meter });
  await assert.rejects(
    publish(tracer, "urn:babel:a", {}, () => { throw new RangeError("r"); }, { queue: "orders", system: "rabbitmq", meter }),
    RangeError,
  );
  await publish(tracer, "urn:babel:a", {}, () => undefined, { queue: "orders" });

  assert.deepEqual(points, [
    {
      metric: "messaging.client.sent.messages",
      kind: "add",
      value: 1,
      attributes: { "messaging.system": "kafka", "messaging.operation.name": "send", "messaging.destination.name": "orders.events" },
    },
    {
      metric: "messaging.client.sent.messages",
      kind: "add",
      value: 1,
      attributes: {
        "messaging.system": "rabbitmq",
        "messaging.operation.name": "send",
        "messaging.destination.name": "orders",
        "error.type": "RangeError",
      },
    },
  ]);
});

test("a throwing meter never changes the wrapped result or error", async () => {
  const { tracer } = recorder();
  const broken = {
    createCounter: () => ({ add: () => { throw new Error("meter down"); } }),
    createHistogram: () => ({ record: () => { throw new Error("meter down"); } }),
  } as unknown as Meter;

  const id = await publish(tracer, "urn:babel:a", {}, (env) => env.meta.id, { meter: broken });
  assert.match(id, /^[0-9a-f-]{36}$/);

  const boom = new Error("handler");
  const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders" });
  await assert.rejects(wrapHandler(tracer, () => { throw boom; }, undefined, { meter: broken })(env), (e) => e === boom);
  await wrapHandler(tracer, async () => {}, undefined, { meter: broken })(env);
});

test("dead-letter observer: +1 per event with system, source queue and operation process; nothing else", () => {
  const { meter, points } = fakeMeter();
  const observe = createDeadLetterObserver({ system: "redis", meter });
  const event: DeadLetterEvent = { queue: "orders", deadLetterQueue: "orders.dlq", reason: "failed" };

  observe(event);
  observe({ queue: "", deadLetterQueue: "", reason: "" }); // native dead-letter, unknown source queue

  assert.deepEqual(points, [
    {
      metric: "babelqueue.dlq.messages",
      kind: "add",
      value: 1,
      attributes: { "messaging.system": "redis", "messaging.operation.name": "process", "messaging.destination.name": "orders" },
    },
    {
      metric: "babelqueue.dlq.messages",
      kind: "add",
      value: 1,
      attributes: { "messaging.system": "redis", "messaging.operation.name": "process" },
    },
  ]);
});

test("dead-letter observer: defaults the system, records nothing without a meter and swallows a meter error", () => {
  const { meter, points } = fakeMeter();
  createDeadLetterObserver({ meter })({ queue: "q", deadLetterQueue: "q.dlq", reason: "" });
  assert.equal(points[0].attributes["messaging.system"], "babelqueue");

  assert.doesNotThrow(() => createDeadLetterObserver({ system: "redis" })({ queue: "q", deadLetterQueue: "", reason: "" }));
  assert.equal(points.length, 1);

  const broken = { createCounter: () => ({ add: () => { throw new Error("meter down"); } }) } as unknown as Meter;
  assert.doesNotThrow(() => createDeadLetterObserver({ meter: broken })({ queue: "q", deadLetterQueue: "", reason: "" }));
});

test("a meter whose instrument creation throws never stops the publish or the handler", async () => {
  const { tracer } = recorder();
  const noCounters = {
    createCounter: () => { throw new Error("no counters"); },
    createHistogram: () => { throw new Error("no histograms"); },
  } as unknown as Meter;
  let sends = 0;

  const id = await publish(tracer, "urn:babel:a", {}, (env) => { sends += 1; return env.meta.id; }, { meter: noCounters });
  assert.equal(sends, 1);
  assert.match(id, /^[0-9a-f-]{36}$/);

  let handled = 0;
  const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders" });
  const wrapped = wrapHandler(tracer, async () => { handled += 1; }, undefined, { meter: noCounters });
  await wrapped(env);
  assert.equal(handled, 1);

  const boom = new Error("handler");
  await assert.rejects(wrapHandler(tracer, () => { throw boom; }, undefined, { meter: noCounters })(env), (e) => e === boom);
});

test("the non-UUID warning set is keyed by the hash, so a very long trace_id is not retained", async () => {
  const { tracer } = recorder();
  const warnings: string[] = [];
  const original = process.emitWarning;
  process.emitWarning = ((message: string) => {
    warnings.push(message);
  }) as typeof process.emitWarning;
  try {
    const huge = `long-${"x".repeat(1_000_000)}`;
    const env = EnvelopeCodec.make("urn:babel:a", {}, { queue: "orders", traceId: huge });
    const handle = wrapHandler(tracer, async () => {});
    await handle(env);
    await handle(env);
    assert.equal(warnings.length, 1);
    assert.ok(warnings[0].length < 400, "the warning text is truncated");
  } finally {
    process.emitWarning = original;
  }
});
