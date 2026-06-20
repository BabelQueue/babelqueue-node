import assert from "node:assert/strict";
import { test } from "node:test";

import { SpanKind, SpanStatusCode, type Tracer } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";

import type { HeaderCarrier } from "../src/contracts.js";
import { EnvelopeCodec, type Envelope } from "../src/codec.js";
import {
  HEADER_TRACEPARENT,
  HEADER_TRACESTATE,
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
  assert.equal(span.name, "process urn:babel:orders:created");
  assert.equal(span.kind, SpanKind.CONSUMER);
  assert.equal(span.spanContext().traceId, traceIdOf(env.trace_id));
  assert.equal(span.attributes["messaging.message.conversation_id"], env.trace_id);
  assert.equal(span.attributes["messaging.destination.name"], "orders");
});

test("wrapHandler tolerates an envelope missing optional meta/attempts", async () => {
  const { tracer, exporter } = recorder();
  const partial = { job: "urn:babel:orders:created", trace_id: TRACE_ID } as Envelope;

  await wrapHandler(tracer, async () => {})(partial);

  const span = exporter.getFinishedSpans()[0];
  assert.equal(span.attributes["messaging.destination.name"], "");
  assert.equal(span.attributes["messaging.message.id"], "");
  assert.equal(span.attributes["messaging.babelqueue.attempts"], 0);
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
  assert.equal(span.attributes["messaging.message.id"], id);
  assert.ok(sent);
  // the published trace_id encodes the producer span's trace, so a consumer recovers it
  assert.equal(sent.trace_id, uuidOf(span.spanContext().traceId));
  assert.equal(traceIdOf(sent.trace_id), span.spanContext().traceId);
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
