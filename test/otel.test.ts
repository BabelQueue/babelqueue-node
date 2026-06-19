import assert from "node:assert/strict";
import { test } from "node:test";

import { SpanKind, SpanStatusCode, type Tracer } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";

import { EnvelopeCodec, type Envelope } from "../src/codec.js";
import { publish, traceIdOf, uuidOf, wrapHandler } from "../src/otel.js";

const TRACE_ID = "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b";

function recorder(): { tracer: Tracer; exporter: InMemorySpanExporter } {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  return { tracer: provider.getTracer("test"), exporter };
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
