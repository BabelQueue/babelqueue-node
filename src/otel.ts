/**
 * Optional OpenTelemetry tracing (ADR-0025) — the Node mirror of `babelqueue-go/otel`.
 *
 * Emits a CONSUMER span per handled message and a PRODUCER span per publish, correlating them
 * across every hop and SDK through the envelope's `trace_id` — a UUID, which maps 1:1 to a
 * 32-hex OTel trace id. The wire envelope is untouched (GR-1) and the zero-dependency core
 * never imports OpenTelemetry: this module pulls `@opentelemetry/api` as an **optional peer
 * dependency** and is reached only via the `@babelqueue/core/otel` subpath, so importing the
 * core itself stays dependency-free.
 *
 * ```ts
 * import { trace } from "@opentelemetry/api";
 * import { wrapHandler, publish } from "@babelqueue/core/otel";
 *
 * const tracer = trace.getTracer("orders");
 * const traced = wrapHandler(tracer, async (env) => { ... });        // consumer
 * await publish(tracer, "urn:babel:orders:created", { order_id: 1 }, // producer
 *   (env) => myTransport.send(env));
 * ```
 *
 * Every hop that shares a `trace_id` shares one OTel trace. Exact cross-hop *span* parent-child
 * linkage (W3C `traceparent` as a transport header) is a documented follow-up.
 */

import { createHash } from "node:crypto";

import {
  SpanKind,
  SpanStatusCode,
  TraceFlags,
  context as otelContext,
  trace,
  type Attributes,
  type Context,
  type Tracer,
} from "@opentelemetry/api";

import { EnvelopeCodec, type Envelope, type MakeOptions } from "./codec.js";
import type { Handler } from "./idempotency.js";

const SYSTEM = "babelqueue";
const INVALID_TRACE_ID = "00000000000000000000000000000000";
const INVALID_SPAN_ID = "0000000000000000";

/**
 * Map an envelope `trace_id` to a deterministic 32-hex OTel trace id: a UUID maps to its
 * hex bytes; any other string is hashed (SHA-256, first 16 bytes). The inverse of {@link uuidOf}
 * for the UUID case.
 */
export function traceIdOf(traceId: string): string {
  const hex = traceId.replace(/-/g, "").toLowerCase();
  if (/^[0-9a-f]{32}$/.test(hex) && hex !== INVALID_TRACE_ID) {
    return hex;
  }
  return createHash("sha256").update(traceId).digest("hex").slice(0, 32);
}

/**
 * Format a 32-hex OTel trace id as a canonical UUID string — the form a producer stamps into
 * the message's `trace_id` so a consumer can recover the same trace id via {@link traceIdOf}.
 */
export function uuidOf(traceIdHex: string): string {
  const h = traceIdHex.replace(/-/g, "").toLowerCase().padStart(32, "0").slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

/** Deterministic, non-zero 16-hex span id so the remote parent context is valid. */
function spanIdOf(traceId: string): string {
  const sid = createHash("sha256").update(`babelqueue-span:${traceId}`).digest("hex").slice(0, 16);
  return sid === INVALID_SPAN_ID ? "0000000000000001" : sid;
}

/** A context carrying a remote parent in the `trace_id`-derived trace. */
function parentContext(traceId: string): Context {
  return trace.setSpanContext(otelContext.active(), {
    traceId: traceIdOf(traceId),
    spanId: spanIdOf(traceId),
    traceFlags: TraceFlags.SAMPLED,
    isRemote: true,
  });
}

function consumeAttributes(env: Envelope): Attributes {
  return {
    "messaging.system": SYSTEM,
    "messaging.operation": "process",
    "messaging.destination.name": env.meta?.queue ?? "",
    "messaging.message.id": env.meta?.id ?? "",
    "messaging.message.conversation_id": env.trace_id,
    "messaging.babelqueue.attempts": env.attempts ?? 0,
  };
}

/**
 * Wrap a consume handler to emit a CONSUMER span per message, in the OTel trace derived from
 * the envelope's `trace_id`, recording the handler's error/status. The handler receives the
 * full {@link Envelope} as before.
 */
export function wrapHandler(
  tracer: Tracer,
  handler: Handler,
): (env: Envelope) => Promise<void> {
  return (env: Envelope): Promise<void> => {
    const ctx = parentContext(env.trace_id);
    return tracer.startActiveSpan(
      `process ${env.job ?? ""}`,
      { kind: SpanKind.CONSUMER, attributes: consumeAttributes(env) },
      ctx,
      async (span) => {
        try {
          await handler(env);
        } catch (err) {
          span.recordException(err as Error);
          span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
          throw err;
        } finally {
          span.end();
        }
      },
    );
  };
}

/**
 * Run a publish under a PRODUCER span `publish <urn>`, carrying the active trace's id into the
 * built envelope's `trace_id` so the downstream consumer recovers the same trace. `send`
 * performs the real transport write and its result is returned.
 */
export function publish<R>(
  tracer: Tracer,
  urn: string,
  data: Record<string, unknown>,
  send: (envelope: Envelope) => R | Promise<R>,
  options: MakeOptions = {},
): Promise<R> {
  return tracer.startActiveSpan(
    `publish ${urn}`,
    {
      kind: SpanKind.PRODUCER,
      attributes: {
        "messaging.system": SYSTEM,
        "messaging.operation": "publish",
        "messaging.destination.name": urn,
      },
    },
    async (span) => {
      try {
        const traceId = uuidOf(span.spanContext().traceId);
        const envelope = EnvelopeCodec.make(urn, data, { ...options, traceId });
        const result = await send(envelope);
        span.setAttribute("messaging.message.id", envelope.meta.id);
        return result;
      } catch (err) {
        span.recordException(err as Error);
        span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
        throw err;
      } finally {
        span.end();
      }
    },
  );
}
