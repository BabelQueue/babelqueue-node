/**
 * Optional OpenTelemetry tracing — the Node mirror of `babelqueue-go/otel`.
 *
 * Cross-hop trace propagation works at two layered levels:
 *
 *   - **`trace_id` ↔ OTel trace id (ADR-0025, v0.1):** the envelope's `trace_id` (a UUID)
 *     maps 1:1 to a 32-hex OTel trace id, so every hop that shares a `trace_id` shares one
 *     trace — correlation and per-hop timing with zero wire/transport change.
 *   - **W3C `traceparent` transport header (ADR-0028, v0.2):** the producer also injects the
 *     active span context as a `traceparent` (and `tracestate`) header onto an out-of-band
 *     {@link HeaderCarrier} that rides **beside** the frozen envelope, never inside it (GR-1).
 *     The consumer reads it and starts its span as a true **child** of the producer span —
 *     real cross-hop parent→child linkage and per-hop span timing, not just a shared trace.
 *     When no `traceparent` header is present it falls back to the v0.1 `trace_id` behaviour,
 *     so enabling propagation is a strict, backward-compatible upgrade — never a regression.
 *
 * The wire envelope is untouched (GR-1) and the zero-dependency core never imports
 * OpenTelemetry: this module pulls `@opentelemetry/api` as an **optional peer dependency**
 * (GR-7) and is reached only via the `@babelqueue/core/otel` subpath, so importing the core
 * itself stays dependency-free. The W3C `traceparent` parse/format is implemented here
 * directly against the frozen W3C Trace Context format rather than pulling
 * `@opentelemetry/core`, keeping the optional dependency at the API only.
 *
 * ```ts
 * import { trace } from "@opentelemetry/api";
 * import { publish, wrapHandler } from "@babelqueue/core/otel";
 * import type { HeaderCarrier } from "@babelqueue/core";
 *
 * const tracer = trace.getTracer("orders");
 *
 * // PRODUCER: the adapter passes a headers carrier; the active span's traceparent is
 * // written into it, and the adapter carries it on its transport's metadata channel.
 * const headers: HeaderCarrier = {};
 * await publish(tracer, "urn:babel:orders:created", { order_id: 1 },
 *   (env) => myTransport.send(env, headers), { headers });
 *
 * // CONSUMER: the adapter surfaces the delivered message's headers; wrapHandler reads the
 * // traceparent and starts the span as a child of the producer span.
 * const traced = wrapHandler(tracer, async (env) => { ... },
 *   () => deliveredMessage.headers);
 * ```
 *
 * **Adapter wiring is a documented follow-up.** The Node transports/brokers live in a
 * separate repo (`babelqueue-node-adapters`: bullmq/redis/rabbitmq/sqs). This core delivers
 * the v0.2 *mechanism* — the {@link HeaderCarrier} seam plus the `traceparent` inject/extract.
 * Carrying the headers on each transport's native per-message metadata channel (AMQP headers,
 * SQS `MessageAttributes`, a Redis transport-owned frame), beside the contract `bq-*`/`x-*`
 * headers, is the per-adapter rollout — the same seam ADR-0027 and the broker bindings roll
 * out per SDK. Until an adapter wires it, propagation degrades to the v0.1 `trace_id`
 * correlation with no error.
 */

import { createHash } from "node:crypto";

import {
  SpanKind,
  SpanStatusCode,
  TraceFlags,
  context as otelContext,
  createTraceState,
  isSpanContextValid,
  trace,
  type Attributes,
  type Context,
  type SpanContext,
  type Tracer,
} from "@opentelemetry/api";

import { EnvelopeCodec, type Envelope, type MakeOptions } from "./codec.js";
import type { HeaderCarrier } from "./contracts.js";
import type { Handler } from "./idempotency.js";

const SYSTEM = "babelqueue";
const INVALID_TRACE_ID = "00000000000000000000000000000000";
const INVALID_SPAN_ID = "0000000000000000";

/**
 * The out-of-band transport-header keys that carry W3C Trace Context across a hop
 * (ADR-0028). They ride on a {@link HeaderCarrier} beside the frozen envelope — the same
 * seam as the replay-bypass marker (ADR-0027) — so a consumer can start its span as a true
 * child of the producer span. The names are the W3C standard, so a babelqueue `traceparent`
 * interoperates with any OTel SDK or W3C-compliant peer.
 */
export const HEADER_TRACEPARENT = "traceparent";
export const HEADER_TRACESTATE = "tracestate";

// W3C Trace Context `traceparent` is a frozen, fixed-width format:
//   `<version>-<32-hex trace id>-<16-hex parent span id>-<2-hex flags>`
// This regex mirrors `@opentelemetry/core`'s parser exactly (version != ff, non-zero
// trace/span ids, optional trailing parts rejected for version 00), so acceptance matches
// the standard propagator without taking a dependency on it.
const VERSION_PART = "(?!ff)[\\da-f]{2}";
const TRACE_ID_PART = "(?![0]{32})[\\da-f]{32}";
const PARENT_ID_PART = "(?![0]{16})[\\da-f]{16}";
const FLAGS_PART = "[\\da-f]{2}";
const TRACEPARENT_REGEX = new RegExp(
  `^\\s?(${VERSION_PART})-(${TRACE_ID_PART})-(${PARENT_ID_PART})-(${FLAGS_PART})(-.*)?\\s?$`,
);

/* -------------------------------------------------------------------------- */
/*  trace_id ↔ OTel trace id (v0.1, ADR-0025 Option 1)                        */
/* -------------------------------------------------------------------------- */

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

/** A context carrying a remote parent in the `trace_id`-derived trace (v0.1 fallback). */
function parentContext(traceId: string): Context {
  return trace.setSpanContext(otelContext.active(), {
    traceId: traceIdOf(traceId),
    spanId: spanIdOf(traceId),
    traceFlags: TraceFlags.SAMPLED,
    isRemote: true,
  });
}

/* -------------------------------------------------------------------------- */
/*  W3C traceparent inject/extract (v0.2, ADR-0028 — the cross-hop span link) */
/* -------------------------------------------------------------------------- */

/**
 * Format a {@link SpanContext} as a W3C `traceparent` header string, or `undefined` when the
 * context is not valid. Identical wire form to `@opentelemetry/core`'s propagator.
 */
function formatTraceparent(sc: SpanContext): string | undefined {
  if (!isSpanContextValid(sc)) {
    return undefined;
  }
  const flags = `0${Number(sc.traceFlags || TraceFlags.NONE).toString(16)}`.slice(-2);
  return `00-${sc.traceId}-${sc.spanId}-${flags}`;
}

/**
 * Parse a W3C `traceparent` header into a remote {@link SpanContext}, or `undefined` when the
 * header is missing or malformed. Mirrors the standard propagator's acceptance rules, so a
 * garbage value never hijacks the trace — the caller falls back to the v0.1 parent.
 */
function parseTraceparent(traceparent: string | undefined, tracestate?: string): SpanContext | undefined {
  if (!traceparent) {
    return undefined;
  }
  const match = TRACEPARENT_REGEX.exec(traceparent);
  if (!match) {
    return undefined;
  }
  // Per the W3C spec, only reject trailing parts when the version is the known "00".
  if (match[1] === "00" && match[5]) {
    return undefined;
  }
  const sc: SpanContext = {
    traceId: match[2],
    spanId: match[3],
    traceFlags: parseInt(match[4], 16),
    isRemote: true,
  };
  if (tracestate) {
    sc.traceState = createTraceState(tracestate);
  }
  return sc;
}

/**
 * Write the active span context in `ctx` (default: the current active context) as a W3C
 * `traceparent` (and `tracestate`, when present) into `headers`, returning the same carrier.
 *
 * This is the **producer half** of cross-hop linkage (ADR-0028): the resulting carrier is
 * handed to a transport adapter, which carries it beside the frozen envelope so the consumer
 * can reconstruct the remote parent. When `ctx` carries no valid span context the carrier is
 * returned unchanged — a no-trace publish stays header-free (no regression).
 */
export function injectTraceparent(
  headers: HeaderCarrier = {},
  ctx: Context = otelContext.active(),
): HeaderCarrier {
  const sc = trace.getSpanContext(ctx);
  if (!sc) {
    return headers;
  }
  const traceparent = formatTraceparent(sc);
  if (!traceparent) {
    return headers;
  }
  headers[HEADER_TRACEPARENT] = traceparent;
  const serialized = sc.traceState?.serialize();
  if (serialized) {
    headers[HEADER_TRACESTATE] = serialized;
  }
  return headers;
}

/**
 * Build a {@link Context} carrying the remote parent extracted from a delivered message's
 * out-of-band `headers`, or `undefined` when there is no valid `traceparent`.
 *
 * This is the **consumer half** of cross-hop linkage (ADR-0028): a span started from the
 * returned context is a child of the producer's span (remote parent), preserving per-hop span
 * timing and real parent→child links. Returning `undefined` signals the caller to fall back to
 * the v0.1 `trace_id`-derived parent — so a malformed, empty, or absent header never regresses
 * a message produced by a pre-0028 (or header-blind) producer.
 */
export function remoteParentFromHeaders(
  headers: HeaderCarrier | null | undefined,
  base: Context = otelContext.active(),
): Context | undefined {
  if (!headers) {
    return undefined;
  }
  const sc = parseTraceparent(headers[HEADER_TRACEPARENT], headers[HEADER_TRACESTATE]);
  if (!sc) {
    return undefined;
  }
  return trace.setSpanContext(base, sc);
}

/* -------------------------------------------------------------------------- */
/*  Producer / consumer wrappers                                              */
/* -------------------------------------------------------------------------- */

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
 * Resolve a delivered message's out-of-band headers for the consumer span's parent. Accepts a
 * plain carrier or a (sync/async) getter the adapter wires to its transport, so reading headers
 * never forces a particular delivery shape.
 */
type DeliveredHeaders =
  | HeaderCarrier
  | null
  | undefined
  | (() => HeaderCarrier | null | undefined | Promise<HeaderCarrier | null | undefined>);

async function resolveHeaders(source: DeliveredHeaders): Promise<HeaderCarrier | null | undefined> {
  return typeof source === "function" ? source() : source;
}

/**
 * Wrap a consume handler to emit a CONSUMER span per message, recording the handler's
 * error/status. The handler receives the full {@link Envelope} as before.
 *
 * **Parent selection (ADR-0028):** when the adapter supplies the delivered message's
 * out-of-band headers via `headers` and they carry a valid W3C `traceparent`, the span is
 * started as a true **child** of the producer span — real cross-hop parent→child linkage with
 * per-hop span timing. When no `traceparent` is present (no `headers` argument, an empty
 * carrier, or a malformed value) it falls back to the v0.1 behaviour: a remote parent derived
 * from the envelope's `trace_id` (ADR-0025 Option 1), which shares the trace but not the exact
 * span edge. So enabling `traceparent` propagation is a strict, backward-compatible upgrade.
 *
 * `headers` may be a plain carrier or a getter `() => carrier` the adapter wires to its
 * transport (it is read once per delivery, after the envelope is in hand).
 */
export function wrapHandler(
  tracer: Tracer,
  handler: Handler,
  headers?: DeliveredHeaders,
): (env: Envelope) => Promise<void> {
  return async (env: Envelope): Promise<void> => {
    const delivered = await resolveHeaders(headers);
    const ctx = remoteParentFromHeaders(delivered) ?? parentContext(env.trace_id);
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

/** Options for {@link publish}. */
export interface PublishOptions extends MakeOptions {
  /**
   * An out-of-band {@link HeaderCarrier} to receive the active producer span's W3C
   * `traceparent` (and `tracestate`). The adapter passes a carrier here and carries the same
   * object on its transport's metadata channel inside `send`, so the consumer can start its
   * span as a true child of this producer span (ADR-0028). When omitted, only the v0.1
   * `trace_id` propagation applies — a strict, backward-compatible default.
   */
  headers?: HeaderCarrier;
}

/**
 * Run a publish under a PRODUCER span `publish <urn>`, propagating the trace downstream two ways:
 *
 *   - **W3C `traceparent` (ADR-0028, v0.2):** when `options.headers` is supplied, the active
 *     span context is injected into that carrier as a `traceparent` (and `tracestate`) header.
 *     The adapter's `send` carries it beside the frozen envelope on the transport's metadata
 *     channel, so a consumer can start its span as a true **child** of this producer span. The
 *     header rides out of band, never in the envelope (GR-1); a transport that can't carry
 *     headers simply doesn't — no error, no regression.
 *   - **`trace_id` (ADR-0025, v0.1):** it also stamps the active trace's id into the built
 *     envelope's `trace_id`, so even a consumer that ignores the header — or a transport that
 *     drops it — recovers the same trace (correlation without the exact span edge).
 *
 * `send` performs the real transport write and its result is returned.
 */
export function publish<R>(
  tracer: Tracer,
  urn: string,
  data: Record<string, unknown>,
  send: (envelope: Envelope) => R | Promise<R>,
  options: PublishOptions = {},
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
        if (options.headers) {
          // Inject from a context built explicitly from *this* producer span — not the ambient
          // active context, which is unreliable unless a global ContextManager is registered.
          injectTraceparent(options.headers, trace.setSpan(otelContext.active(), span));
        }
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
